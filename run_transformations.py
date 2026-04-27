"""
Runs all dbt SQL models directly via DuckDB Python.
Equivalent to: dbt run --profiles-dir .
Used when dbt CLI isn't available (e.g. Python 3.14 compatibility).
"""
import os
import duckdb
from loguru import logger

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./warehouse/crime.duckdb")

con = duckdb.connect(DUCKDB_PATH)

logger.info("Running transformations against: " + DUCKDB_PATH)

# ── Staging: stg_crimes (view) ──────────────────────────────────────────────
logger.info("Building staging.stg_crimes ...")
con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
con.execute("DROP VIEW IF EXISTS staging.stg_crimes;")
con.execute("""
CREATE VIEW staging.stg_crimes AS
WITH source AS (
    SELECT * FROM raw.crimes
),
cleaned AS (
    SELECT
        nullif(trim(crime_id), '')                          AS crime_id,
        month,
        split_part(month, '-', 1)::INTEGER                  AS year,
        split_part(month, '-', 2)::INTEGER                  AS month_num,
        trim(force)                                         AS force,
        trim(lsoa_code)                                     AS lsoa_code,
        trim(lsoa_name)                                     AS lsoa_name,
        regexp_replace(trim(lsoa_name), ' \\d+.*$', '')      AS district,
        longitude,
        latitude,
        trim(location)                                      AS location,
        trim(crime_type)                                    AS crime_type,
        coalesce(nullif(trim(last_outcome), ''), 'No outcome recorded') AS last_outcome,
        _loaded_at
    FROM source
    WHERE month IS NOT NULL
      AND crime_type IS NOT NULL
)
SELECT * FROM cleaned
""")
count = con.execute("SELECT COUNT(*) FROM staging.stg_crimes").fetchone()[0]
logger.success(f"staging.stg_crimes ready — {count:,} rows")

# ── Mart: crime_by_category ──────────────────────────────────────────────────
logger.info("Building marts.crime_by_category ...")
con.execute("DROP TABLE IF EXISTS marts.crime_by_category;")
con.execute("""
CREATE TABLE marts.crime_by_category AS
SELECT
    year,
    month_num,
    month,
    force,
    crime_type,
    COUNT(*)                                                    AS total_crimes,
    COUNT(crime_id)                                             AS crimes_with_id,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE last_outcome = 'Under investigation') / COUNT(*),
        2
    )                                                           AS pct_under_investigation
FROM staging.stg_crimes
GROUP BY year, month_num, month, force, crime_type
ORDER BY year, month_num, force, total_crimes DESC
""")
count = con.execute("SELECT COUNT(*) FROM marts.crime_by_category").fetchone()[0]
logger.success(f"marts.crime_by_category ready — {count:,} rows")

# ── Mart: crime_by_month ─────────────────────────────────────────────────────
logger.info("Building marts.crime_by_month ...")
con.execute("DROP TABLE IF EXISTS marts.crime_by_month;")
con.execute("""
CREATE TABLE marts.crime_by_month AS
WITH base AS (
    SELECT
        year, month_num, month, force,
        COUNT(*) AS total_crimes
    FROM staging.stg_crimes
    GROUP BY year, month_num, month, force
)
SELECT
    year, month_num, month, force,
    total_crimes,
    LAG(total_crimes) OVER (PARTITION BY force, month_num ORDER BY year) AS prev_year_crimes,
    ROUND(
        100.0 * (total_crimes - LAG(total_crimes) OVER (PARTITION BY force, month_num ORDER BY year))
              / NULLIF(LAG(total_crimes) OVER (PARTITION BY force, month_num ORDER BY year), 0),
        2
    ) AS yoy_pct_change
FROM base
ORDER BY force, year, month_num
""")
count = con.execute("SELECT COUNT(*) FROM marts.crime_by_month").fetchone()[0]
logger.success(f"marts.crime_by_month ready — {count:,} rows")

# ── Mart: crime_by_force ─────────────────────────────────────────────────────
logger.info("Building marts.crime_by_force ...")
con.execute("DROP TABLE IF EXISTS marts.crime_by_force;")
con.execute("""
CREATE TABLE marts.crime_by_force AS
SELECT
    force,
    year,
    month,
    COUNT(*)                                                        AS total_crimes,
    COUNT(DISTINCT crime_type)                                      AS distinct_crime_types,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE last_outcome = 'Under investigation') / COUNT(*),
        2
    )                                                               AS pct_under_investigation,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE last_outcome = 'Investigation complete; no suspect identified') / COUNT(*),
        2
    )                                                               AS pct_no_suspect,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE last_outcome NOT IN (
            'Under investigation', 'No outcome recorded',
            'Investigation complete; no suspect identified'
        )) / COUNT(*),
        2
    )                                                               AS pct_resolved
FROM staging.stg_crimes
GROUP BY force, year, month
ORDER BY force, year, month
""")
count = con.execute("SELECT COUNT(*) FROM marts.crime_by_force").fetchone()[0]
logger.success(f"marts.crime_by_force ready — {count:,} rows")

# ── Mart: crime_hotspots ──────────────────────────────────────────────────────
logger.info("Building marts.crime_hotspots ...")
con.execute("DROP TABLE IF EXISTS marts.crime_hotspots;")
con.execute("""
CREATE TABLE marts.crime_hotspots AS
SELECT
    lsoa_code,
    lsoa_name,
    district,
    force,
    year,
    month,
    ROUND(AVG(latitude),  6)    AS centroid_lat,
    ROUND(AVG(longitude), 6)    AS centroid_lon,
    COUNT(*)                    AS total_crimes,
    COUNT(*) FILTER (WHERE crime_type = 'Violence and sexual offences') AS violence_count,
    COUNT(*) FILTER (WHERE crime_type = 'Anti-social behaviour')        AS asb_count,
    COUNT(*) FILTER (WHERE crime_type = 'Burglary')                     AS burglary_count,
    COUNT(*) FILTER (WHERE crime_type = 'Vehicle crime')                AS vehicle_crime_count,
    COUNT(*) FILTER (WHERE crime_type = 'Shoplifting')                  AS shoplifting_count,
    COUNT(*) FILTER (WHERE crime_type = 'Drugs')                        AS drugs_count,
    CASE
        WHEN COUNT(*) >= 20 THEN 'High'
        WHEN COUNT(*) >= 10 THEN 'Medium'
        ELSE 'Low'
    END                         AS hotspot_tier
FROM staging.stg_crimes
WHERE latitude  IS NOT NULL
  AND longitude IS NOT NULL
  AND lsoa_code IS NOT NULL
GROUP BY lsoa_code, lsoa_name, district, force, year, month
ORDER BY total_crimes DESC
""")
count = con.execute("SELECT COUNT(*) FROM marts.crime_hotspots").fetchone()[0]
logger.success(f"marts.crime_hotspots ready — {count:,} rows")

con.close()
logger.success("All transformations complete.")
