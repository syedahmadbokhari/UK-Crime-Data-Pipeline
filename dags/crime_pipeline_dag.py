"""
Airflow DAG: UK Crime Data Pipeline

Flow per force per month:
  download → validate_raw → upload_s3 → load_duckdb → validate_loaded
    → dbt_run → validate_marts → dashboard_refresh

Schedule: monthly, 5th of the month (gives data.police.uk time to publish)
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

FORCES = os.getenv("PIPELINE_FORCES", "west-yorkshire").split(",")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "./data/raw")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./warehouse/crime.duckdb")
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_crime")

default_args = {
    "owner": "ahmad",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _get_target_month(**ctx) -> str:
    """Return the previous calendar month as YYYY-MM."""
    execution_date = ctx["execution_date"]
    first_of_month = execution_date.replace(day=1)
    prev_month = first_of_month - timedelta(days=1)
    return prev_month.strftime("%Y-%m")


def _download(force: str, **ctx) -> str:
    from ingestion.download_data import download_month
    from pathlib import Path
    month = _get_target_month(**ctx)
    path = download_month(force, month, Path(RAW_DATA_DIR))
    if path is None:
        raise RuntimeError(f"Download failed for {force} {month}")
    return str(path)


def _validate_raw(force: str, **ctx) -> bool:
    """Check downloaded CSV has a sensible row count."""
    import pandas as pd
    from pathlib import Path
    month = _get_target_month(**ctx)
    csv_path = Path(RAW_DATA_DIR) / f"{month}-{force}-street.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Raw CSV not found: {csv_path}")
    df = pd.read_csv(csv_path)
    if len(df) < 100:
        raise ValueError(f"Row count too low ({len(df)}) — possible bad download")
    return True


def _upload_s3(force: str, **ctx) -> None:
    from ingestion.upload_to_s3 import upload_month
    month = _get_target_month(**ctx)
    uris = upload_month(force, month)
    if not uris:
        raise RuntimeError(f"S3 upload produced no URIs for {force} {month}")


def _load_duckdb(force: str, **ctx) -> None:
    from warehouse.setup_duckdb import get_connection, initialise, load_local_csv
    from pathlib import Path
    month = _get_target_month(**ctx)
    csv_path = Path(RAW_DATA_DIR) / f"{month}-{force}-street.csv"
    con = get_connection()
    initialise(con)
    inserted = load_local_csv(con, str(csv_path), force)
    con.close()
    if inserted == 0:
        raise RuntimeError(f"No rows inserted — possible duplicate load or empty file")


def _validate_loaded(force: str, **ctx) -> None:
    """Assert raw.crimes has data for this force + month."""
    import duckdb
    month = _get_target_month(**ctx)
    con = duckdb.connect(DUCKDB_PATH)
    count = con.execute(
        "SELECT COUNT(*) FROM raw.crimes WHERE force=? AND month=?",
        [force, month],
    ).fetchone()[0]
    con.close()
    if count == 0:
        raise ValueError(f"raw.crimes has 0 rows for {force} {month} after load")


def _update_watermark(force: str, **ctx) -> None:
    from ingestion.watermark import set_watermark
    month = _get_target_month(**ctx)
    set_watermark(force, month)


with DAG(
    dag_id="crime_data_pipeline",
    description="Monthly UK Police crime data ingestion and transformation",
    schedule_interval="0 6 5 * *",  # 06:00 on the 5th of every month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["crime", "police", "data-engineering"],
) as dag:

    for force in FORCES:
        safe_force = force.replace("-", "_")

        t_download = PythonOperator(
            task_id=f"download_{safe_force}",
            python_callable=_download,
            op_kwargs={"force": force},
        )

        t_validate_raw = PythonOperator(
            task_id=f"validate_raw_{safe_force}",
            python_callable=_validate_raw,
            op_kwargs={"force": force},
        )

        t_upload_s3 = PythonOperator(
            task_id=f"upload_s3_{safe_force}",
            python_callable=_upload_s3,
            op_kwargs={"force": force},
        )

        t_load_duckdb = PythonOperator(
            task_id=f"load_duckdb_{safe_force}",
            python_callable=_load_duckdb,
            op_kwargs={"force": force},
        )

        t_validate_loaded = PythonOperator(
            task_id=f"validate_loaded_{safe_force}",
            python_callable=_validate_loaded,
            op_kwargs={"force": force},
        )

        t_dbt_run = BashOperator(
            task_id=f"dbt_run_{safe_force}",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                "dbt run --profiles-dir . --target prod"
            ),
        )

        t_dbt_test = BashOperator(
            task_id=f"dbt_test_{safe_force}",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                "dbt test --profiles-dir . --target prod"
            ),
        )

        t_watermark = PythonOperator(
            task_id=f"update_watermark_{safe_force}",
            python_callable=_update_watermark,
            op_kwargs={"force": force},
        )

        (
            t_download
            >> t_validate_raw
            >> t_upload_s3
            >> t_load_duckdb
            >> t_validate_loaded
            >> t_dbt_run
            >> t_dbt_test
            >> t_watermark
        )
