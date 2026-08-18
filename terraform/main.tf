# =====================================================================
# AWS SERVERLESS DATA ANALYTICS PIPELINE - TERRAFORM INFRASTRUCTURE
# Amazon S3 (Data Lake & Athena Results) + AWS Glue Catalog + Athena Workgroup
# =====================================================================

terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------
# 1. KMS Key for Server-Side Encryption (Security & Compliance)
# ---------------------------------------------------------------------
resource "aws_kms_key" "analytics_kms_key" {
  description             = "KMS key for AWS Serverless Data Analytics S3 Data Lake & Athena Results"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ---------------------------------------------------------------------
# 2. Amazon S3 - Raw Data Lake Landing Bucket
# ---------------------------------------------------------------------
resource "aws_s3_bucket" "data_lake_bucket" {
  bucket        = "${var.project_name}-lake-${var.environment}-${var.aws_account_id}"
  force_destroy = true

  tags = {
    Name        = "Serverless Data Lake Storage Bucket"
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.analytics_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake_block_public" {
  bucket                  = aws_s3_bucket.data_lake_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------
# 3. Amazon S3 - Athena Query Results Bucket
# ---------------------------------------------------------------------
resource "aws_s3_bucket" "athena_results_bucket" {
  bucket        = "${var.project_name}-athena-results-${var.environment}-${var.aws_account_id}"
  force_destroy = true

  tags = {
    Name        = "Athena Query Output Bucket"
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results_encryption" {
  bucket = aws_s3_bucket.athena_results_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.analytics_kms_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results_lifecycle" {
  bucket = aws_s3_bucket.athena_results_bucket.id

  rule {
    id     = "delete-old-query-results"
    status = "Enabled"

    expiration {
      days = var.query_results_retention_days
    }
  }
}

# ---------------------------------------------------------------------
# 4. AWS Glue Catalog Database
# ---------------------------------------------------------------------
resource "aws_glue_catalog_database" "analytics_db" {
  name        = "${replace(var.project_name, "-", "_")}_db_${var.environment}"
  description = "Glue Data Catalog Database for Serverless Log & Transaction Analytics"
}

# ---------------------------------------------------------------------
# 5. IAM Role for AWS Glue Crawler
# ---------------------------------------------------------------------
resource "aws_iam_role" "glue_crawler_role" {
  name = "${var.project_name}-glue-crawler-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_attachment" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_s3_kms_policy" {
  name        = "${var.project_name}-glue-s3-kms-policy-${var.environment}"
  description = "Grants Glue Crawler access to S3 Data Lake and KMS Key"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake_bucket.arn,
          "${aws_s3_bucket.data_lake_bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = aws_kms_key.analytics_kms_key.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_kms_attachment" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = aws_iam_policy.glue_s3_kms_policy.arn
}

# ---------------------------------------------------------------------
# 6. AWS Glue Crawler (Automatic Schema & Partition Discovery)
# ---------------------------------------------------------------------
resource "aws_glue_crawler" "analytics_crawler" {
  database_name = aws_glue_catalog_database.analytics_db.name
  name          = "${var.project_name}-crawler-${var.environment}"
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake_bucket.bucket}/raw_logs/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ---------------------------------------------------------------------
# 7. Amazon Athena Workgroup (Cost Management & Query Isolation)
# ---------------------------------------------------------------------
resource "aws_athena_workgroup" "analytics_workgroup" {
  name        = "${var.project_name}-workgroup-${var.environment}"
  description = "Dedicated Athena Workgroup with enforced S3 output and scan limits"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 10737418240 # 10 GB scan limit per query (Prevents runaways)

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results_bucket.bucket}/queries/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = aws_kms_key.analytics_kms_key.arn
      }
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ---------------------------------------------------------------------
# 8. Athena Named Queries (Standard Analytical Suite)
# ---------------------------------------------------------------------
resource "aws_athena_named_query" "descriptive_summary" {
  name      = "01_Descriptive_Daily_Traffic_And_Errors"
  workgroup = aws_athena_workgroup.analytics_workgroup.id
  database  = aws_glue_catalog_database.analytics_db.name
  query     = <<EOF
SELECT 
    year, month, day,
    COUNT(*) AS total_requests,
    COUNT(DISTINCT user_id_hash) AS unique_users,
    SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_requests,
    ROUND(AVG(response_time_ms), 2) AS avg_latency_ms
FROM raw_logs
GROUP BY year, month, day
ORDER BY year, month, day;
EOF
}
