"""
DuckDB warehouse setup.

Creates the crime database, installs the httpfs extension so DuckDB can
read CSV files directly from S3, and initialises the raw schema.

Usage:
    python -m warehouse.setup_duckdb
    python -m warehouse.setup_duckdb --load-local ./data/raw/2026-02-west-yorkshire-street.csv
"""
import argparse
import os
from pathlib import Path

import duckdb
from loguru import logger

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./warehouse/crime.duckdb")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")


def get_connection() -> duckdb.DuckDBPyConnection:
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    if S3_BUCKET:
        con.execute(f"""
            SET s3_region='{AWS_REGION}';
            SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID", "")}';
            SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY", "")}';
        """)
    return con


def initialise(con: duckdb.DuckDBPyConnection) -> None:
    """Create schemas and the raw crimes table."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts;")

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.crimes (
            crime_id        VARCHAR,
            month           VARCHAR,
            reported_by     VARCHAR,
            falls_within    VARCHAR,
            longitude       DOUBLE,
            latitude        DOUBLE,
            location        VARCHAR,
            lsoa_code       VARCHAR,
            lsoa_name       VARCHAR,
            crime_type      VARCHAR,
            last_outcome    VARCHAR,
            context         VARCHAR,
            -- partition columns
            force           VARCHAR,
            _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    logger.success("Database schemas and raw.crimes table ready")


def load_local_csv(con: duckdb.DuckDBPyConnection, csv_path: str, force: str) -> int:
    """
    Load a local CSV into raw.crimes.
    Skips rows already present (idempotent via crime_id dedup).
    Returns number of rows inserted.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    # Read into a temp view then insert, deduplicating on crime_id
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _incoming AS
        SELECT
            "Crime ID"      AS crime_id,
            "Month"         AS month,
            "Reported by"   AS reported_by,
            "Falls within"  AS falls_within,
            TRY_CAST("Longitude" AS DOUBLE) AS longitude,
            TRY_CAST("Latitude"  AS DOUBLE) AS latitude,
            "Location"      AS location,
            "LSOA code"     AS lsoa_code,
            "LSOA name"     AS lsoa_name,
            "Crime type"    AS crime_type,
            "Last outcome category" AS last_outcome,
            "Context"       AS context,
            '{force}'       AS force
        FROM read_csv_auto('{path.as_posix()}', header=true)
    """)

    # Only insert rows not already present (by crime_id, month, force)
    before = con.execute("SELECT COUNT(*) FROM raw.crimes").fetchone()[0]
    con.execute("""
        INSERT INTO raw.crimes
            (crime_id, month, reported_by, falls_within, longitude, latitude,
             location, lsoa_code, lsoa_name, crime_type, last_outcome, context,
             force, _loaded_at)
        SELECT
            crime_id, month, reported_by, falls_within, longitude, latitude,
            location, lsoa_code, lsoa_name, crime_type, last_outcome, context,
            force, CURRENT_TIMESTAMP
        FROM _incoming i
        WHERE NOT EXISTS (
            SELECT 1 FROM raw.crimes r
            WHERE r.crime_id = i.crime_id
              AND r.month    = i.month
              AND r.force    = i.force
              AND r.crime_id IS NOT NULL
        )
    """)
    after = con.execute("SELECT COUNT(*) FROM raw.crimes").fetchone()[0]
    inserted = after - before
    logger.success(f"Loaded {inserted} new rows from {path.name} (total: {after})")
    return inserted


def load_from_s3(con: duckdb.DuckDBPyConnection, force: str, year_month: str) -> int:
    """Load a month's CSV directly from S3 into raw.crimes."""
    if not S3_BUCKET:
        raise EnvironmentError("S3_BUCKET_NAME not set")
    year, month = year_month.split("-")
    s3_path = f"s3://{S3_BUCKET}/crime/year={year}/month={month}/force={force}/*.csv"
    logger.info(f"Loading from S3: {s3_path}")

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW _incoming AS
        SELECT
            "Crime ID"      AS crime_id,
            "Month"         AS month,
            "Reported by"   AS reported_by,
            "Falls within"  AS falls_within,
            TRY_CAST("Longitude" AS DOUBLE) AS longitude,
            TRY_CAST("Latitude"  AS DOUBLE) AS latitude,
            "Location"      AS location,
            "LSOA code"     AS lsoa_code,
            "LSOA name"     AS lsoa_name,
            "Crime type"    AS crime_type,
            "Last outcome category" AS last_outcome,
            "Context"       AS context,
            '{force}'       AS force
        FROM read_csv_auto('{s3_path}', header=true)
    """)

    before = con.execute("SELECT COUNT(*) FROM raw.crimes").fetchone()[0]
    con.execute("""
        INSERT INTO raw.crimes
            (crime_id, month, reported_by, falls_within, longitude, latitude,
             location, lsoa_code, lsoa_name, crime_type, last_outcome, context,
             force, _loaded_at)
        SELECT
            crime_id, month, reported_by, falls_within, longitude, latitude,
            location, lsoa_code, lsoa_name, crime_type, last_outcome, context,
            force, CURRENT_TIMESTAMP
        FROM _incoming i
        WHERE NOT EXISTS (
            SELECT 1 FROM raw.crimes r
            WHERE r.crime_id = i.crime_id
              AND r.month    = i.month
              AND r.force    = i.force
              AND r.crime_id IS NOT NULL
        )
    """)
    after = con.execute("SELECT COUNT(*) FROM raw.crimes").fetchone()[0]
    inserted = after - before
    logger.success(f"Loaded {inserted} new rows from S3 for {force} {year_month}")
    return inserted


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up DuckDB warehouse")
    parser.add_argument("--load-local", metavar="CSV_PATH", help="Load a local CSV file")
    parser.add_argument("--force", default="west-yorkshire", help="Force name for the CSV")
    args = parser.parse_args()

    con = get_connection()
    initialise(con)

    if args.load_local:
        load_local_csv(con, args.load_local, args.force)

    con.close()
