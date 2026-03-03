terraform {
  required_version = ">= 1.5.0"

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

resource "aws_s3_bucket" "campaign_finance_raw" {
  bucket = var.raw_bucket_name

  tags = {
    Project     = "ClimateCabinetFinance"
    Environment = var.environment
    Layer       = "bronze"
  }
}

resource "aws_s3_bucket_versioning" "campaign_finance_raw_versioning" {
  bucket = aws_s3_bucket.campaign_finance_raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "campaign_finance_raw_sse" {
  bucket = aws_s3_bucket.campaign_finance_raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_iam_role" "pipeline_execution_role" {
  name = "${var.environment}-campaign-finance-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}
