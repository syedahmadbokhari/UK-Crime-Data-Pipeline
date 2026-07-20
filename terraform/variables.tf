variable "aws_region" {
  description = "AWS region the bucket and IAM resources are created in. Matches AWS_DEFAULT_REGION used by the pipeline (see .env.example)."
  type        = string
  default     = "eu-west-2"
}

variable "bucket_name" {
  description = <<-EOT
    Name of the S3 bucket the pipeline uploads/reads crime CSVs from.
    Must match the S3_BUCKET_NAME value in your .env file, and must be
    globally unique across all AWS accounts (S3 bucket names are global).
    No default on purpose — this is project/account specific, not something
    to hardcode or share across environments.
  EOT
  type        = string
}

variable "s3_prefix" {
  description = "Top-level key prefix under which the Hive-partitioned data lives (crime/year=YYYY/month=MM/force=<force>/...). Matches S3_PREFIX in .env.example."
  type        = string
  default     = "crime"
}

variable "environment" {
  description = "Environment tag applied to all resources (e.g. dev, prod). Purely for tagging/cost-tracking, not used to change resource behavior."
  type        = string
  default     = "dev"
}

variable "enable_versioning" {
  description = <<-EOT
    Whether to enable S3 object versioning on the bucket.
    Default is false: the pipeline's idempotency (see ingestion/upload_to_s3.py)
    is implemented via a HEAD-before-PUT check on the object key, not via S3
    object versions, and no code path in this project reads a specific object
    version. Set to true only if you want version history as a manual safety
    net (e.g. protection against accidental overwrite) — it is not required
    for correct pipeline operation.
  EOT
  type        = bool
  default     = false
}

variable "create_iam_user" {
  description = <<-EOT
    Whether to create a dedicated IAM user for the pipeline (crime-pipeline-<environment>)
    with the least-privilege policy attached. Set to false if you'd rather attach
    aws_iam_policy.crime_pipeline_s3 to an existing role (e.g. an EC2/ECS task role)
    instead of a standalone user.
  EOT
  type        = bool
  default     = true
}

variable "force_destroy" {
  description = "If true, allows `terraform destroy` to delete the bucket even if it still contains objects. Defaults to false so a stray `destroy` can't silently wipe ingested data."
  type        = bool
  default     = false
}
