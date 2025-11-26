import sys
import requests
import boto3   
import botocore
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# -------------------------------------------------------------------------
# 1. SETUP & CONFIGURAÇÕES (Onde entram as infos do Bucket)
# -------------------------------------------------------------------------

# Pega o argumento 's3_bucket' passado pelo Job do Glue/Terraform
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket'])
bucket_name = args['s3_bucket']

# ====================================================
# PASSO EXTRA: GARANTIR DADOS RAW (WEB -> S3)
# ====================================================

def ingest_raw_data(bucket_name):
    s3_client = boto3.client('s3')
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    print(f"--- [INGESTÃO] Verificando arquivos Raw no bucket {bucket_name} ---")

    for month in range(1, 13):
        month_str = f"{month:02d}"
        file_name = f"yellow_tripdata_2019-{month_str}.parquet"
        
        s3_key = f"raw/nyc_taxi_2019/{file_name}"
        download_url = f"{base_url}/{file_name}"

        try:
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            print(f"✅ [SKIP] {file_name} já existe no S3.")
        except botocore.exceptions.ClientError:
            print(f"⬇️ [DOWNLOADING] Baixando {file_name}...")
            
            with requests.get(download_url, stream=True) as r:
                r.raise_for_status()
                s3_client.upload_fileobj(r.raw, bucket_name, s3_key)
            
            print(f"⬆️ [UPLOADED] {file_name} salvo no S3 com sucesso!")

ingest_raw_data(bucket_name)

# ====================================================
# INÍCIO DO PIPELINE SPARK
# ====================================================

# Define os caminhos baseados no bucket recebido
raw_path_base = f"s3://{bucket_name}/raw/nyc_taxi_2019/*.parquet"
base_path = f"s3://{bucket_name}/lakehouse"
bronze_path = f"{base_path}/bronze/taxi_trips"
silver_path = f"{base_path}/silver/taxi_trips"
gold_fin_path = f"{base_path}/gold/financial_performance"

# Inicializa Spark no Glue com suporte a Delta Lake
builder = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8") 
spark.sql("CREATE DATABASE IF NOT EXISTS taxi_lakehouse_db")

print(f"🚀 Iniciando Pipeline no Bucket: {bucket_name}")

# -------------------------------------------------------------------------
# 2. CAMADA BRONZE (Ingestão Raw -> Delta)
# -------------------------------------------------------------------------
print("--- [BRONZE] Iniciando Ingestão ---")

# Schema Físico de 2019
schema_raw = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", IntegerType(), True)
])

df_raw = spark.read.schema(schema_raw).parquet(raw_path_base)

df_bronze = df_raw \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("source_file", input_file_name()) \
    .withColumn("year", year("tpep_pickup_datetime")) \
    .withColumn("month", month("tpep_pickup_datetime"))

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(bronze_path)

print("✅ [BRONZE] Concluída!")

# -------------------------------------------------------------------------
# 3. CAMADA SILVER (Limpeza e Enriquecimento)
# -------------------------------------------------------------------------
print("--- [SILVER] Iniciando Tratamento ---")

df = spark.read.format("delta").load(bronze_path)

# 3.1 Rename
df_step1 = df \
    .withColumnRenamed("VendorID", "vendor_id") \
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
    .withColumnRenamed("PULocationID", "pickup_location_id") \
    .withColumnRenamed("DOLocationID", "dropoff_location_id") \
    .withColumnRenamed("RatecodeID", "rate_code_id")

# 3.2 Drop Nulls (Regra dos 70% simplificada para demo)
# Dropamos apenas linhas críticas para o negócio
df_step2 = df_step1.dropna(subset=["pickup_datetime", "total_amount"])

# 3.3 Type Convert
df_step3 = df_step2 \
    .withColumn("passenger_count", col("passenger_count").cast("int")) \
    .withColumn("payment_type", col("payment_type").cast("int")) \
    .withColumn("rate_code_id", col("rate_code_id").cast("int")) \
    .withColumn("pickup_location_id", col("pickup_location_id").cast("int")) \
    .withColumn("dropoff_location_id", col("dropoff_location_id").cast("int"))

# 3.4 Categorical Values (UDFs Corrigidas)
payment_dict = {1:"Credit Card", 2:"Cash", 3:"No Charge", 4:"Dispute", 5:"Unknown", 6:"Voided Trip"}
rate_dict = {1:"Standard", 2:"JFK", 3:"Newark", 4:"Nassau", 5:"Negotiated", 6:"Group", 99:"Unknown"}
vendor_dict = {1:"Creative Mobile", 2:"Curb Mobility", 6:"Myle", 7:"Helix"}

# UDFs (Registrando)
udf_pay = udf(lambda x: payment_dict.get(x, "Unknown"), StringType())
udf_rate = udf(lambda x: rate_dict.get(x, "Unknown"), StringType())
udf_vendor = udf(lambda x: vendor_dict.get(x, "Unknown"), StringType())

df_step4 = df_step3 \
    .withColumn("payment_type_desc", udf_pay(col("payment_type"))) \
    .withColumn("rate_code_desc", udf_rate(col("rate_code_id"))) \
    .withColumn("vendor_desc", udf_vendor(col("vendor_id"))) \
    .withColumn("store_and_fwd_flag", when(col("store_and_fwd_flag") == "Y", "Y").otherwise("N"))

# 3.5 Feature Engineering & Filtros de Negócio
df_silver = df_step4 \
    .withColumn("trip_duration_minutes", round((unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60, 2)) \
    .withColumn("day_of_week", date_format("pickup_datetime", "EEEE")) \
    .withColumn("hour", hour("pickup_datetime")) \
    .filter(col("year") == 2019) \
    .filter(col("total_amount") > 0) \
    .filter(col("trip_duration_minutes") > 0) \
    .withColumn("total_amount", round(col("total_amount"), 2))

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

print("✅ [SILVER] Concluída!")

# -------------------------------------------------------------------------
# 4. CAMADA GOLD (Agregações de Negócio)
# -------------------------------------------------------------------------
print("--- [GOLD] Iniciando Agregações ---")

df_gold_input = df_silver.withColumn(
    "tip_percentage", 
    when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount")).otherwise(0.0)
).withColumn(
    "is_credit_card",
    when(col("payment_type") == 1, "Credit Card").when(col("payment_type") == 2, "Cash").otherwise("Other")
)

df_gold_agg = df_gold_input.groupBy("day_of_week", "is_credit_card") \
    .agg(
        count("*").alias("total_rides"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_ticket"),
        round(avg("tip_percentage") * 100, 2).alias("avg_tip_pct"),
        round(sum("tolls_amount"), 2).alias("total_tolls")
    )

window_daily = Window.partitionBy("day_of_week")
df_gold_refined = df_gold_agg.withColumn(
    "revenue_share_daily",
    round(col("total_revenue") / sum("total_revenue").over(window_daily) * 100, 2)
)

# Salva Gold e registra no Glue Catalog para o Athena
df_gold_refined \
    .orderBy(desc("total_revenue")) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", gold_fin_path) \
    .saveAsTable("taxi_lakehouse_db.gold_financial")

print("✅ [GOLD] Concluída e registrada no Athena!")