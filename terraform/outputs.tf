output "data_lake_bucket_name" {
  description = "Name of the S3 Data Lake bucket"
  value       = aws_s3_bucket.data_lake_bucket.bucket
}

output "athena_results_bucket_name" {
  description = "Name of the S3 Athena Query Results bucket"
  value       = aws_s3_bucket.athena_results_bucket.bucket
}

output "glue_database_name" {
  description = "Name of the AWS Glue Data Catalog database"
  value       = aws_glue_catalog_database.analytics_db.name
}

output "glue_crawler_name" {
  description = "Name of the AWS Glue Crawler"
  value       = aws_glue_crawler.analytics_crawler.name
}

output "athena_workgroup_name" {
  description = "Name of the Athena Workgroup"
  value       = aws_athena_workgroup.analytics_workgroup.name
}

output "kms_key_arn" {
  description = "ARN of the KMS encryption key"
  value       = aws_kms_key.analytics_kms_key.arn
}
