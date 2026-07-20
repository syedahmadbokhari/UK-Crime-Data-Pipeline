# ---------------------------------------------------------------------------
# S3 bucket — Hive-partitioned crime data lake
#
# Mirrors the layout ingestion/upload_to_s3.py already writes to and
# warehouse/setup_duckdb.py reads from via DuckDB's httpfs extension:
#
#   s3://<bucket_name>/<s3_prefix>/year=YYYY/month=MM/force=<force>/<file>.csv
#
# Terraform does not create the partition "folders" themselves — S3 has no
# real directories, and the pipeline code creates keys on upload. This just
# provisions the bucket those keys land in.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "crime_data" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy
}

# Versioning is off by default — see variables.tf for why (the pipeline's
# idempotency is key-based, not version-based). Left toggleable rather than
# hardcoded off, in case a future user wants overwrite protection.
resource "aws_s3_bucket_versioning" "crime_data" {
  bucket = aws_s3_bucket.crime_data.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

# Baseline encryption at rest. This is a security default, not something the
# pipeline code depends on — boto3/DuckDB read and write transparently
# regardless of server-side encryption.
resource "aws_s3_bucket_server_side_encryption_configuration" "crime_data" {
  bucket = aws_s3_bucket.crime_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# This bucket only ever needs to be reached by the pipeline's own AWS
# credentials (boto3 / DuckDB httpfs) via the IAM policy below — nothing in
# the codebase serves objects publicly, so public access is blocked outright.
resource "aws_s3_bucket_public_access_block" "crime_data" {
  bucket = aws_s3_bucket.crime_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# No lifecycle/expiration rules: dbt marts (crime_by_month.sql) compute
# year-over-year comparisons, which means every historical partition needs
# to remain readable indefinitely. Adding an expiration rule here would
# silently break that analytics use case, so none is defined.

# ---------------------------------------------------------------------------
# IAM — least privilege for the pipeline
#
# Scoped to exactly what ingestion/upload_to_s3.py and
# warehouse/setup_duckdb.py (load_from_s3) do against this bucket:
#   - s3:PutObject / s3:GetObject on objects under the crime/ prefix
#     (upload_file's put, head_object's existence check, and DuckDB's
#     read_csv_auto('s3://...') reads all resolve to these two actions)
#   - s3:ListBucket, scoped to the crime/ prefix only, for
#     list_s3_partitions()'s paginated ListObjectsV2 call and for DuckDB's
#     glob expansion of s3://bucket/crime/year=*/month=*/force=*/*.csv
#
# No s3:DeleteObject, no s3:PutBucket*, no account-wide "s3:*" — nothing in
# the codebase deletes objects or manages the bucket itself.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "crime_pipeline_s3" {
  statement {
    sid    = "ListCrimePrefixOnly"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.crime_data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.s3_prefix}/*"]
    }
  }

  statement {
    sid    = "ReadWriteCrimeObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
    ]
    resources = ["${aws_s3_bucket.crime_data.arn}/${var.s3_prefix}/*"]
  }
}

resource "aws_iam_policy" "crime_pipeline_s3" {
  name        = "crime-pipeline-s3-${var.environment}"
  description = "Least-privilege access to the ${var.s3_prefix}/ prefix of ${var.bucket_name} for the UK Crime Data Pipeline (upload + DuckDB httpfs reads)."
  policy      = data.aws_iam_policy_document.crime_pipeline_s3.json
}

# Dedicated service user so the pipeline doesn't run under a personal/root
# AWS identity. No access key is generated here on purpose — an IAM access
# key/secret written into Terraform state is a credential leak waiting to
# happen. See terraform/README.md for how to issue one manually after apply.
resource "aws_iam_user" "crime_pipeline" {
  count = var.create_iam_user ? 1 : 0
  name  = "crime-pipeline-${var.environment}"
}

resource "aws_iam_user_policy_attachment" "crime_pipeline_s3" {
  count      = var.create_iam_user ? 1 : 0
  user       = aws_iam_user.crime_pipeline[0].name
  policy_arn = aws_iam_policy.crime_pipeline_s3.arn
}
