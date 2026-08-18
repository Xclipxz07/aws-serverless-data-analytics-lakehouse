variable "aws_region" {
  description = "AWS Region for deploying resources"
  type        = string
  default     = "eu-west-2" # London
}

variable "aws_account_id" {
  description = "AWS Account ID (used to guarantee unique S3 bucket names)"
  type        = string
  default     = "123456789012"
}

variable "project_name" {
  description = "Prefix identifier for all provisioned cloud resources"
  type        = string
  default     = "aws-serverless-analytics"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "query_results_retention_days" {
  description = "Number of days before Athena query result files in S3 are automatically deleted"
  type        = number
  default     = 30
}
