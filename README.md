# UK Crime Data Pipeline

[![CI](https://github.com/syedahmadbokhari/UK-Crime-Data-Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/syedahmadbokhari/UK-Crime-Data-Pipeline/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Docker](https://img.shields.io/badge/Docker-multi--stage-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

End-to-end data engineering project built on publicly available UK Police crime data.
Covers cloud ingestion, a DuckDB warehouse, dbt transformations, Airflow orchestration,
and a geospatial Streamlit dashboard — with an AI-powered crime report generator.

| | |
|---|---|
| **Live Dashboard** | [![Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://uk-crime-data-pipeline-mqev8qeujqyu2o5wtagkc4.streamlit.app/) |
| **Live API (Swagger)** | https://uk-crime-data-pipeline-production.up.railway.app/docs |
| **GitHub** | https://github.com/syedahmadbokhari/UK-Crime-Data-Pipeline |

---

## Business Context

West Yorkshire had **21,007 recorded crimes in February 2026 alone**, with violence and
sexual offences accounting for 41% of all incidents. Over 46% of cases remain under
active investigation. This pipeline ingests monthly snapshots from every UK police force,
cleans and models the data, and surfaces actionable insights — crime trends by category,
geographic hotspots at LSOA level, and force-level performance metrics.

---

## Architecture

```
data.police.uk
      │
      ▼
┌─────────────────┐
│  Download (boto3│   ingestion/download_data.py
│  + requests)    │   Watermark tracks last loaded month per force
└────────┬────────┘
         │ CSV
         ▼
┌─────────────────┐
│   AWS S3        │   s3://bucket/crime/year=YYYY/month=MM/force=<force>/
│  (partitioned)  │   Hive-style partitioning for cheap Athena/Glue compatibility
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   DuckDB        │   warehouse/setup_duckdb.py
│   raw.crimes    │   Reads S3 via httpfs extension — no Redshift needed
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   dbt           │   dbt_crime/
│   staging →     │   stg_crimes: clean, type-cast, derive district
│   marts         │   crime_by_category / crime_by_month / crime_hotspots
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Streamlit     │   dashboard/app.py
│   Dashboard     │   Trends · Category breakdown · Folium map · Force KPIs
└─────────────────┘

Orchestration: Airflow DAG (dags/crime_pipeline_dag.py) — runs 5th of every month
CI/CD:         GitHub Actions — pytest + dbt compile on every push
```

---

## Pipeline Components

| Layer | File | What it does |
|---|---|---|
| Download | [ingestion/download_data.py](ingestion/download_data.py) | Downloads CSVs from data.police.uk, extracts from zip |
| Watermark | [ingestion/watermark.py](ingestion/watermark.py) | Tracks last processed month per force — incremental loading |
| S3 Upload | [ingestion/upload_to_s3.py](ingestion/upload_to_s3.py) | Uploads with Hive partitioning, idempotent via HEAD check |
| Warehouse | [warehouse/setup_duckdb.py](warehouse/setup_duckdb.py) | Creates DuckDB schemas, loads CSV, deduplicates on crime_id |
| Staging | [dbt_crime/models/staging/stg_crimes.sql](dbt_crime/models/staging/stg_crimes.sql) | Cleans nulls, derives year/month/district columns |
| Mart: category | [dbt_crime/models/marts/crime_by_category.sql](dbt_crime/models/marts/crime_by_category.sql) | Crime counts + % under investigation by type/force/month |
| Mart: trend | [dbt_crime/models/marts/crime_by_month.sql](dbt_crime/models/marts/crime_by_month.sql) | Monthly totals with YoY % change |
| Mart: force | [dbt_crime/models/marts/crime_by_force.sql](dbt_crime/models/marts/crime_by_force.sql) | Outcome rates (resolved / no suspect / open) per force |
| Mart: hotspots | [dbt_crime/models/marts/crime_hotspots.sql](dbt_crime/models/marts/crime_hotspots.sql) | LSOA centroid + crime breakdown + High/Medium/Low tier |
| DAG | [dags/crime_pipeline_dag.py](dags/crime_pipeline_dag.py) | 9-task Airflow DAG with validation gates and watermark update |
| Data Quality | [data_quality/validate_raw_crimes.py](data_quality/validate_raw_crimes.py) | Great Expectations suite for raw.crimes, run as a DAG task |
| Streaming Producer | [streaming/producer.py](streaming/producer.py) | Publishes a CSV's rows to Kafka one at a time, with pacing |
| Streaming Consumer | [streaming/consumer.py](streaming/consumer.py) | Micro-batches, validates via the same GE suite, writes to DuckDB/S3 |
| Dashboard | [dashboard/app.py](dashboard/app.py) | Streamlit: trends, breakdown, Folium map, force comparison |
| Tests | [tests/](tests/) | 40+ pytest tests — mocked S3/Kafka, in-memory DuckDB, GE suite |
| CI | [.github/workflows/ci.yml](.github/workflows/ci.yml) | pytest + dbt compile on push |

---

## Key Analytics

**Crime in West Yorkshire (Feb 2026)**

| Crime Type | Count | % of Total |
|---|---:|---:|
| Violence and sexual offences | 8,614 | 41% |
| Anti-social behaviour | 1,797 | 9% |
| Criminal damage and arson | 1,557 | 7% |
| Public order | 1,548 | 7% |
| Shoplifting | 1,541 | 7% |
| Burglary | 1,083 | 5% |

**Outcome rates**

| Outcome | Count |
|---|---:|
| Under investigation | 9,765 (46%) |
| No suspect identified | 4,694 (22%) |
| Unable to prosecute | 3,515 (17%) |

---

## Stack

| Layer | Tool | Reason |
|---|---|---|
| Storage | AWS S3 | Cloud credential, Hive partitioning |
| Processing | Python + Pandas | Data wrangling, type safety |
| Warehouse | DuckDB | Free, S3-native via httpfs, dbt-compatible |
| Transformation | dbt | Modular SQL, schema tests, lineage |
| Orchestration | Airflow | DAG with validation gates and watermarks |
| Testing | pytest + moto | Mocked S3, in-memory DuckDB |
| CI/CD | GitHub Actions | pytest + dbt compile on every push |
| Dashboard | Streamlit + Folium | Interactive charts and geospatial map |
| REST API | FastAPI + SQLAlchemy | JWT-authenticated endpoints, deployed on Railway |
| IaC | Terraform | Provisions the S3 bucket + least-privilege IAM, replacing manual console setup |
| Data Quality | Great Expectations | Formalizes the pipeline's hand-written validation into an Expectation Suite + Data Docs |
| Streaming | Kafka (KRaft) + kafka-python | Producer/consumer architecture demo — see honesty note below |

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env — add your AWS credentials and S3 bucket name
```

### 3. Load the sample data (no AWS needed to start)

```bash
python -m warehouse.setup_duckdb --load-local ./2026-02-west-yorkshire-street.csv --force west-yorkshire
```

### 4. Run dbt transformations

```bash
cd dbt_crime
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 5. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

### 6. Download more data (requires internet)

```bash
python -m ingestion.download_data --force west-yorkshire --start 2024-01 --end 2025-12
```

### 7. Upload to S3 and run full pipeline

```bash
python -m ingestion.upload_to_s3 --force west-yorkshire --month 2025-01
```

### 8. Run with Docker Compose (Airflow + Streamlit)

```bash
docker-compose up airflow-init
docker-compose up
# Airflow UI: http://localhost:8080 (admin/admin)
# Dashboard:  http://localhost:8501
```

---

## Infrastructure as Code (Terraform)

The AWS side of this project — the S3 bucket and the IAM policy/user that read and write it — can now
be provisioned with Terraform instead of clicking through the AWS console:

```bash
cd terraform
terraform init
terraform plan -var="bucket_name=your-globally-unique-bucket-name"
terraform apply -var="bucket_name=your-globally-unique-bucket-name"
```

This replaces the manual "create a bucket and an IAM user in the console" step referenced in Quick
Start step 2 — take the `bucket_name` output and put it in `.env` as `S3_BUCKET_NAME`, same as before.

**What it does NOT cover:** Airflow, Postgres, Redis, and the FastAPI service are unchanged — they
remain entirely Docker Compose-based (`docker-compose.yml`), same as always. Terraform here is scoped
strictly to the S3 bucket + IAM, not the rest of the stack.

`terraform apply` creates real AWS resources and may incur (typically small) AWS costs — it's a
manual, deliberate step, not something CI or any script runs for you. Full details, the exact IAM
permissions granted and why, and the reasoning behind each bucket setting are in
[terraform/README.md](terraform/README.md).

---

## Data Quality (Great Expectations)

The pipeline already had hand-written data quality checks scattered across a few places:
[dags/crime_pipeline_dag.py](dags/crime_pipeline_dag.py)'s `_validate_raw`/`_validate_loaded`
row-count gates, `accepted_values`/`not_null` tests in
[dbt_crime/models/staging/schema.yml](dbt_crime/models/staging/schema.yml), and a silent
`WHERE month IS NOT NULL AND crime_type IS NOT NULL` filter in
[stg_crimes.sql](dbt_crime/models/staging/stg_crimes.sql) that drops bad rows with no
visibility. [data_quality/validate_raw_crimes.py](data_quality/validate_raw_crimes.py)
formalizes those into a single Great Expectations suite that runs against `raw.crimes`
right after the DuckDB load — the one point in the flow where none of the existing checks
looked at column-level validity before dbt got the data.

**Honestly, most of this is formalization, not brand-new validation:**

| Expectation | Status |
|---|---|
| Row count ≥ 100 | Formalizes `_validate_raw`'s existing threshold |
| Required columns exist | Formalizes `test_schema_has_required_columns` |
| `month` / `crime_type` not null | Formalizes + **strengthens** — previously just a silent filter in `stg_crimes.sql`, now a loud, gating check |
| `force` not null | Formalizes the dbt schema.yml test |
| `force` in the 4 known forces | Formalizes dbt's `accepted_values` + `download_data.py`'s `SUPPORTED_FORCES`, shifted earlier in the pipeline |
| `crime_type` in the 14 ONS categories | Formalizes dbt's `accepted_values` + `test_known_crime_types_only` |
| **Longitude/latitude within the UK bounding box** | **Genuinely new** — nothing previously checked coordinates were plausible, only that they parsed as a float |
| **`crime_id` unique among non-null values** | **Genuinely new** — formalizes an invariant `load_local_csv`'s dedup logic already assumed but never asserted |

None of the existing checks were removed — dbt's schema tests and the DAG's row-count gates
still run exactly as before. This is an additional, earlier gate, not a replacement.

### Running it locally

Requires `great_expectations` from `requirements-dev.txt` (not in the lighter `requirements.txt`
used for the Streamlit Cloud deployment):

```bash
pip install -r requirements-dev.txt
python -m data_quality.validate_raw_crimes
# or a single force+month partition:
python -m data_quality.validate_raw_crimes --force west-yorkshire --month 2026-02
```

This validates whatever's currently in your local DuckDB warehouse and builds Data Docs —
GE's HTML validation report. Open it at:

```
great_expectations/uncommitted/data_docs/local_site/index.html
```

(`great_expectations/uncommitted/` is gitignored — Data Docs and validation run history are
regenerated locally/in CI, not committed, since they can contain raw data values.)

### In the pipeline

Airflow runs this as its own task (`validate_quality_ge_<force>`), positioned right after
`validate_loaded_<force>` and before `dbt_run_<force>` in each force's task chain — same
spot the existing manual quality gate already ran, just with a formal suite instead of a
bare row-count check.

---

## Real-Time Streaming (Kafka)

**What this is honest about, up front:** UK Police crime data is published by data.police.uk as
monthly batch snapshots (see [Data Source](#data-source) below) — nothing about this data is
naturally real-time. This section adds a genuine Kafka producer/consumer pipeline as an
**architecture demonstration**, using that same batch-published data as a stand-in event source. It
is not a claim that crime reports arrive at data.police.uk in real time — they don't. Streaming
skills built on realistically batch-published data and stated plainly as such is a legitimate,
common portfolio approach; implying otherwise wouldn't be.

### What it adds, and why it's a separate path from the batch pipeline

- **[streaming/producer.py](streaming/producer.py)** reads the same local CSVs
  [ingestion/upload_to_s3.py](ingestion/upload_to_s3.py) uploads in bulk, but publishes each row as
  its own Kafka message, one at a time, with a small delay between sends (`STREAM_DELAY_SECONDS`,
  default 0.1s) — genuinely incremental arrival, not a bulk publish disguised as streaming.
- **[streaming/consumer.py](streaming/consumer.py)** subscribes to the topic and buffers incoming
  records into small micro-batches (`STREAMING_BATCH_SIZE`, default 20 — flushed early after
  `STREAMING_FLUSH_INTERVAL_SECONDS`, default 15s, if the topic is quiet). Each micro-batch is
  validated with the **exact same Great Expectations suite** the batch pipeline uses
  ([data_quality/validate_raw_crimes.py](data_quality/validate_raw_crimes.py)'s `build_suite()`,
  reused unmodified — just under a streaming-scoped suite name and a batch-size-appropriate row-count
  threshold, since a single record can't meaningfully satisfy a uniqueness/row-count check written for
  a whole partition). Valid batches are staged as a CSV and hand off to the **same**
  `warehouse.setup_duckdb.load_local_csv` and `ingestion.upload_to_s3.upload_file` functions the batch
  pipeline calls — one write path, not two to keep in sync.
- **Invalid batches are never silently dropped.** This project already found a silent-drop bug once
  (a `WHERE ... IS NOT NULL` filter quietly dropping bad rows in
  [stg_crimes.sql](dbt_crime/models/staging/stg_crimes.sql) — see the Data Quality section above) and
  deliberately doesn't repeat it here: a micro-batch that fails validation is written whole to
  `data/rejected/` and logged with the specific failed expectations, not discarded.
- **Offset handling:** the consumer commits Kafka offsets manually
  (`enable_auto_commit=False`), only after a micro-batch's outcome — written or rejected — is durable.
  If the consumer crashes mid-batch, restarting resumes from the last committed offset and re-consumes
  the unflushed messages (at-least-once delivery). Replaying a micro-batch after a crash is safe here
  specifically because `load_local_csv`'s `INSERT ... WHERE NOT EXISTS` already dedups on
  `(crime_id, month, force)` — reprocessing doesn't create duplicate rows.

**This is additive, not a replacement.** The Airflow DAG, dbt models, and batch ingestion scripts are
completely unchanged — Kafka is a second, independent path into the same DuckDB warehouse (and S3
layout), not a rework of the first one.

### Running it

Kafka only starts when you ask for it — plain `docker-compose up` behaves exactly as before, with no
Kafka container at all:

```bash
# Kafka (KRaft mode — no Zookeeper) + producer (one-shot) + consumer (long-running)
docker-compose --profile streaming up

# Or drive it by hand once Kafka is up:
docker-compose --profile streaming run --rm streaming-producer \
    python -m streaming.producer --force west-yorkshire --month 2026-02
docker-compose --profile streaming run --rm streaming-consumer \
    python -m streaming.consumer --max-messages 200
```

Why KRaft instead of Zookeeper: it's the modern, officially-supported single-binary setup (Kafka ≥
3.7's `apache/kafka` image), and one fewer container than a Zookeeper-based broker for a single-node
demo — see the `kafka` service in [docker-compose.yml](docker-compose.yml) for the exact config.

Watch it work: `docker-compose --profile streaming logs -f streaming-consumer` shows each micro-batch
being validated and written (or rejected) as the producer publishes.

---

## Running Tests

```bash
pytest tests/ -v
```

Tests cover:
- Watermark read/write and month-range calculation
- S3 upload (mocked with moto — no real AWS needed)
- DuckDB load idempotency and deduplication
- Data quality checks (null months, unknown crime types, schema columns)
- Row count gate validation
- Incremental load across multiple forces
- The Great Expectations suite catching malformed categories, unknown forces, out-of-range
  coordinates, and duplicate crime IDs against synthetic data (no real DuckDB/AWS needed)
- Kafka producer/consumer logic — message publishing, micro-batch validation, DuckDB/S3 writes,
  rejected-batch routing, and offset commit timing — against in-process fake Kafka clients (no live
  broker needed, same spirit as the moto-mocked S3 tests)

---

## Data Source

[data.police.uk](https://data.police.uk) — UK government open data, updated monthly.
No authentication required. Coverage: all 43 territorial police forces in England and Wales.
Natural incremental loading story: new months appear on the 5th of the following month.
