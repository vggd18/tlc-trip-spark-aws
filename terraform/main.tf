provider "aws" {
  region = "us-east-2" # Região onde está configurada sua AWS
}

variable "bucket_name" {
  default = "taxi-lakehouse-demo-2025" # Lembre de garantir que este nome é único
}

# 1. S3 BUCKET
resource "aws_s3_bucket" "datalake" {
  bucket        = var.bucket_name
  force_destroy = true # Permite destruir o bucket mesmo com dados (útil para demos)
}

# 2. UPLOAD DO SCRIPT PYTHON (Ajustado para nova estrutura de pastas)
resource "aws_s3_object" "script_upload" {
  bucket = aws_s3_bucket.datalake.id
  key    = "scripts/tlc_taxi_script.py"   # Caminho no S3
  
  # Caminho local: Sobe um nível (..) e entra em scripts/
  source = "../scripts/tlc_taxi_script.py" 
  
  etag   = filemd5("../scripts/tlc_taxi_script.py")
}

# 3. IAM ROLE & POLICIES
resource "aws_iam_role" "glue_role" {
  name = "glue-taxi-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Permissões S3 e Glue (Consolidadas)
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
          "glue:*" 
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        # Permissões de Log (CloudWatch)
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*:/aws-glue/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# 4. GLUE DATABASE
resource "aws_glue_catalog_database" "taxi_db" {
  name = "taxi_lakehouse_db"
}

# 5. GLUE JOB (ETL)
resource "aws_glue_job" "taxi_job" {
  name     = "taxi-pipeline-job"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.datalake.id}/scripts/tlc_taxi_script.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"               = "delta"
    "--conf"                           = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--s3_bucket"                      = aws_s3_bucket.datalake.id
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                        = "s3://${aws_s3_bucket.datalake.id}/temporary/"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
}

# 6. GLUE CRAWLER (Para catalogar a tabela Gold)
resource "aws_glue_crawler" "taxi_crawler" {
  database_name = aws_glue_catalog_database.taxi_db.name
  name          = "taxi-gold-crawler"
  role          = aws_iam_role.glue_role.arn

  delta_target {
    delta_tables   = ["s3://${aws_s3_bucket.datalake.id}/lakehouse/gold/financial_performance/"]
    write_manifest = true
  }
}

# 7. ATHENA WORKGROUP
resource "aws_athena_workgroup" "taxi_wg" {
  name = "taxi-analytics"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.datalake.id}/athena-results/"
    }
  }
}