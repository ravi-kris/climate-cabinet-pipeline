variable "aws_region" {
  description = "AWS region for pipeline infrastructure"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "S3 bucket name for bronze raw campaign finance data"
  type        = string
}
