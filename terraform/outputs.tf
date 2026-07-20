output "bucket_name" {
  description = "Name of the provisioned S3 bucket — set this as S3_BUCKET_NAME in your .env."
  value       = aws_s3_bucket.crime_data.id
}

output "bucket_arn" {
  description = "ARN of the provisioned S3 bucket."
  value       = aws_s3_bucket.crime_data.arn
}

output "s3_uri_prefix" {
  description = "Base s3:// URI the pipeline writes Hive-partitioned CSVs under."
  value       = "s3://${aws_s3_bucket.crime_data.id}/${var.s3_prefix}"
}

output "iam_policy_arn" {
  description = "ARN of the least-privilege IAM policy scoped to this bucket's crime/ prefix."
  value       = aws_iam_policy.crime_pipeline_s3.arn
}

output "iam_user_name" {
  description = "Name of the dedicated pipeline IAM user, if created (see create_iam_user variable). Use this to generate an access key for AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY."
  value       = var.create_iam_user ? aws_iam_user.crime_pipeline[0].name : null
}
