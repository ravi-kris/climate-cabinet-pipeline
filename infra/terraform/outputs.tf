output "raw_bucket_id" {
  value       = aws_s3_bucket.campaign_finance_raw.id
  description = "S3 bucket ID for raw campaign finance ingestion"
}

output "pipeline_role_arn" {
  value       = aws_iam_role.pipeline_execution_role.arn
  description = "Execution role ARN for pipeline workloads"
}
