# UK Crime Data Pipeline

End-to-end data engineering project built on publicly available UK Police crime data.
Covers cloud ingestion, a DuckDB warehouse, dbt transformations, Airflow orchestration,
and a geospatial Streamlit dashboard — with an AI-powered crime report generator.

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://uk-crime-data-pipeline-mqev8qeujqyu2o5wtagkc4.streamlit.app/)

**Live app:** https://uk-crime-data-pipeline-mqev8qeujqyu2o5wtagkc4.streamlit.app/

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
| DAG | [dags/crime_pipeline_dag.py](dags/crime_pipeline_dag.py) | 8-task Airflow DAG with validation gates and watermark update |
| Dashboard | [dashboard/app.py](dashboard/app.py) | Streamlit: trends, breakdown, Folium map, force comparison |
| Tests | [tests/](tests/) | 20+ pytest tests — mocked S3, in-memory DuckDB |
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

---

## Data Source

[data.police.uk](https://data.police.uk) — UK government open data, updated monthly.
No authentication required. Coverage: all 43 territorial police forces in England and Wales.
Natural incremental loading story: new months appear on the 5th of the following month.
