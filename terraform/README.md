# Terraform — AWS Infrastructure

Provisions the AWS resources this pipeline actually uses at runtime:

- **S3 bucket** — the Hive-partitioned data lake that [ingestion/upload_to_s3.py](../ingestion/upload_to_s3.py)
  writes CSVs to and [warehouse/setup_duckdb.py](../warehouse/setup_duckdb.py) reads from via DuckDB's
  `httpfs` extension. Layout: `s3://<bucket>/crime/year=YYYY/month=MM/force=<force>/<file>.csv`.
- **IAM policy + user** — least-privilege access to that bucket (scoped to the `crime/` prefix only),
  so the pipeline doesn't need to run under a personal or root AWS identity.

Nothing else in this project is provisioned here. Airflow, Postgres, Redis, and the FastAPI service
remain entirely Docker Compose-based (see the root [docker-compose.yml](../docker-compose.yml)) — this
Terraform config has no opinion on and does not touch compute, containers, or orchestration.

## Does this replace the existing manual setup?

**Yes, for the S3 bucket and IAM permissions specifically.** Previously the README's "Configure
environment" step assumed you'd already created an S3 bucket and IAM credentials by hand in the AWS
console, then just pasted the resulting values into `.env`. This Terraform config is the replacement
for that manual bucket/IAM creation step — run it once, then take the `bucket_name` output and put it
in `.env` as before.

It is **not** a replacement for anything else in the setup story: you still create/manage your own
AWS credentials to authenticate Terraform itself (see below), you still copy `.env.example` to `.env`
by hand, and Docker Compose / Airflow setup is unchanged.

## ⚠️ Before you run this

`terraform apply` creates real AWS resources (an S3 bucket and an IAM user/policy) in your AWS
account and **may incur AWS costs** (S3 storage/request charges — typically small for this project's
data volumes, but not zero). Nothing in this repository runs `apply` for you automatically; it is a
deliberate, manual step you take when you're ready to provision real infrastructure.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.5.0
- An AWS account and credentials with permission to create S3 buckets and IAM users/policies
  (e.g. via `aws configure`, or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars) — these are
  **your personal/admin AWS credentials**, used only to run Terraform. They are separate from the
  pipeline's own service credentials, which this config creates for you.

## Usage

```bash
cd terraform
terraform init

# Review the plan first — bucket_name is required and has no default
# because it must be globally unique and is account-specific.
terraform plan -var="bucket_name=your-globally-unique-bucket-name"

# Only when you're ready to actually create resources:
terraform apply -var="bucket_name=your-globally-unique-bucket-name"
```

Or persist your variables in a `terraform.tfvars` file (already gitignored — see root `.gitignore`)
instead of passing `-var` every time:

```hcl
# terraform/terraform.tfvars
bucket_name = "your-globally-unique-bucket-name"
aws_region  = "eu-west-2"
environment = "dev"
```

After `apply` succeeds, wire the output into the pipeline's own config:

```bash
terraform output bucket_name
# → copy this value into ../.env as S3_BUCKET_NAME
```

### Generating pipeline credentials

Terraform deliberately does **not** create an IAM access key for the `crime-pipeline-<environment>`
user — an access key/secret is a long-lived credential, and writing it into Terraform state is a
credential leak waiting to happen. Generate one manually instead, after `apply`:

```bash
aws iam create-access-key --user-name "$(terraform output -raw iam_user_name)"
```

Take the resulting `AccessKeyId` / `SecretAccessKey` and put them in `.env` as
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.

## What's configured on the bucket, and why

| Setting | Value | Reasoning |
|---|---|---|
| Versioning | Off by default (`enable_versioning = false`) | The pipeline's idempotency (`upload_to_s3.py`'s HEAD-before-PUT check) is key-based, not version-based, and nothing reads a specific object version. Toggle on if you want manual overwrite protection — it isn't required for correct operation. |
| Server-side encryption | AES256 (SSE-S3) | Baseline at-rest encryption; transparent to boto3/DuckDB, no code changes needed. |
| Public access | Fully blocked | Nothing in the codebase serves objects publicly — only the pipeline's own IAM credentials ever need to reach this bucket. |
| Lifecycle / expiration rules | None | `dbt_crime/models/marts/crime_by_month.sql` computes year-over-year comparisons, so every historical partition must stay readable indefinitely. An expiration rule would silently break that. |

## IAM scope

The attached policy grants exactly three actions, scoped to this bucket's `crime/` prefix:

- `s3:ListBucket` (prefix-restricted to `crime/*`) — used by `list_s3_partitions()`'s paginated
  `ListObjectsV2` call, and by DuckDB's glob expansion when reading `s3://bucket/crime/.../*.csv`.
- `s3:GetObject` — used by `upload_file()`'s `head_object` idempotency check and by DuckDB's
  `read_csv_auto('s3://...')` reads.
- `s3:PutObject` — used by `upload_file()`'s actual upload.

No `s3:DeleteObject`, no bucket-management actions, no `s3:*`. Nothing in the codebase deletes
objects or manages bucket configuration at runtime, so those permissions aren't granted.

## Files

| File | Purpose |
|---|---|
| `versions.tf` | Pins Terraform and the AWS provider version |
| `variables.tf` | All environment-specific inputs (bucket name, region, versioning, etc.) |
| `main.tf` | S3 bucket + IAM policy/user resources |
| `outputs.tf` | Bucket name/ARN and IAM policy/user references for use elsewhere (`.env`, CI secrets) |
