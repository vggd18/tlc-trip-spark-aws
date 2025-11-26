provider "aws" {
  region = "us-east-2" 
}

variable "bucket_name" {
  default = "taxi-lakehouse-demo-2025"
}

resource "aws_s3_bucket" "datalake" {
  bucket = var.bucket_name
  force_destroy = true 
}

resource "aws_s3_object" "script_upload" {
  bucket = aws_s3_bucket.datalake.id
  key    = "scripts/tlc_taxi_script.py"
  source = "../tlc_taxi_script.py"
  etag   = filemd5("../tlc_taxi_script.py")
}

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
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*"
        ]
      },
      {
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*:/aws-glue/*"
      },
      {
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
          "glue:*"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_glue_catalog_database" "taxi_db" {
  name = "taxi_lakehouse_db"
}

resource "aws_glue_job" "taxi_job" {
  name     = "taxi-pipeline-job"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.datalake.id}/scripts/tlc_taxi_script.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"              = "delta" 
    "--conf"                          = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--s3_bucket"                     = aws_s3_bucket.datalake.id 
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                       = "s3://${aws_s3_bucket.datalake.id}/temporary/"
  }

  worker_type       = "G.1X"
  number_of_workers = 2
}
resource "aws_glue_crawler" "taxi_crawler" {
  database_name = aws_glue_catalog_database.taxi_db.name
  name          = "taxi-gold-crawler"
  role          = aws_iam_role.glue_role.arn

  delta_target {
    delta_tables = ["s3://${aws_s3_bucket.datalake.id}/lakehouse/gold/financial_performance/"]
    write_manifest = true 
  }
}
resource "aws_athena_workgroup" "taxi_wg" {
  name = "taxi-analytics"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.datalake.id}/athena-results/"
    }
  }
}