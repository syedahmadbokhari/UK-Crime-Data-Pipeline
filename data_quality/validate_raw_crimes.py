"""
Great Expectations suite for raw.crimes — the DuckDB table
ingestion/upload_to_s3.py's uploads eventually land in and
warehouse/setup_duckdb.py's load_local_csv()/load_from_s3() populate.

This formalizes the pipeline's existing hand-written quality gates
(dags/crime_pipeline_dag.py's _validate_raw / _validate_loaded, and the
dbt schema.yml tests that run against stg_crimes) at the one point in the
flow where none of them currently check column-level validity: right after
the raw load, before dbt sees the data. See build_suite()'s docstring for
exactly which expectations formalize an existing check vs. are new.

The Expectation Suite / Checkpoint definitions below are idempotent
(add_or_update) and are also persisted as JSON under ../great_expectations/
(committed to git) so `great_expectations init`-style tooling and Data Docs
work against them without needing this module to run first.

Usage:
    python -m data_quality.validate_raw_crimes              # validate the local warehouse + build Data Docs
    python -m data_quality.validate_raw_crimes --force west-yorkshire --month 2026-02
"""
import argparse
from pathlib import Path

import great_expectations as gx
import pandas as pd
from great_expectations.core.expectation_suite import ExpectationSuite
from loguru import logger

# ../great_expectations — the GX project directory (great_expectations.yml,
# expectations/, checkpoints/, uncommitted/data_docs/). Deliberately not a
# Python package: a directory literally named `great_expectations` on
# sys.path would shadow the real installed `great_expectations` library.
GX_ROOT = Path(__file__).parent.parent / "great_expectations"

SUITE_NAME = "raw_crimes_suite"
DATASOURCE_NAME = "pandas_datasource"
ASSET_NAME = "raw_crimes"
BATCH_DEFINITION_NAME = "raw_crimes_batch"
VALIDATION_DEFINITION_NAME = "raw_crimes_validation"
CHECKPOINT_NAME = "raw_crimes_checkpoint"

# Same 4 forces ingestion/download_data.py's SUPPORTED_FORCES allows and
# dbt_crime/models/staging/schema.yml's accepted_values test enforces.
KNOWN_FORCES = [
    "west-yorkshire",
    "greater-manchester",
    "metropolitan",
    "west-midlands",
]

# Same 14 ONS crime categories from dbt_crime/models/staging/schema.yml's
# accepted_values test and tests/test_transformations.py's `valid` set.
KNOWN_CRIME_TYPES = [
    "Anti-social behaviour",
    "Bicycle theft",
    "Burglary",
    "Criminal damage and arson",
    "Drugs",
    "Other crime",
    "Other theft",
    "Possession of weapons",
    "Public order",
    "Robbery",
    "Shoplifting",
    "Theft from the person",
    "Vehicle crime",
    "Violence and sexual offences",
]

# Loose UK mainland + islands bounding box (covers England, Wales, Scotland,
# Northern Ireland and the Channel Islands). Deliberately not narrowed to
# just the 4 currently-supported forces so it doesn't need updating if
# PIPELINE_FORCES/SUPPORTED_FORCES grows to more forces later — it exists to
# catch gross geocoding errors (e.g. null-island 0,0, swapped lat/long), not
# to act as a tight geofence. Real sample data (2026-02-west-yorkshire) sits
# well inside it: longitude -2.15..-1.22, latitude 53.54..53.98.
UK_LONGITUDE_RANGE = (-8.65, 1.77)
UK_LATITUDE_RANGE = (49.86, 60.86)

# Matches tests/test_transformations.py::test_schema_has_required_columns
REQUIRED_COLUMNS = ["crime_id", "month", "force", "crime_type", "longitude", "latitude"]


def build_suite(name: str = SUITE_NAME, min_row_count: int = 100) -> ExpectationSuite:
    """
    Build the Expectation Suite for raw.crimes.

    name/min_row_count are parameterized so a different calling context can
    reuse this exact set of expectations against a differently-sized batch
    without redefining any of them — see streaming/consumer.py, which
    validates small Kafka micro-batches (tens of rows, not a whole
    force+month partition) under a separate suite name so it doesn't
    overwrite the batch pipeline's persisted suite/checkpoint on disk.

    FORMALIZES EXISTING CHECKS (already enforced elsewhere in the codebase;
    the two not-null checks are also *strengthened* here, since today they
    are only a silent `WHERE ... IS NOT NULL` filter in stg_crimes.sql that
    drops bad rows with no visibility — this makes that loud and gates the
    pipeline on it instead):
      - table has >= 100 rows        <- dags/crime_pipeline_dag.py::_validate_raw
      - required columns exist       <- tests/test_transformations.py::test_schema_has_required_columns
      - month is not null            <- previously a silent filter in stg_crimes.sql
      - crime_type is not null       <- previously a silent filter in stg_crimes.sql, plus
                                         dbt_crime/models/staging/schema.yml's not_null test
      - force is not null            <- dbt_crime/models/staging/schema.yml's not_null test
      - force in KNOWN_FORCES        <- dbt schema.yml accepted_values + download_data.py's
                                         SUPPORTED_FORCES, both enforced later/upstream of here
      - crime_type in KNOWN_CRIME_TYPES <- dbt schema.yml accepted_values +
                                         tests/test_transformations.py::test_known_crime_types_only

    GENUINELY NEW (nothing in the codebase checks these anywhere today):
      - longitude/latitude fall within the UK bounding box — previously only
        TRY_CAST to DOUBLE in warehouse/setup_duckdb.py, which just turns an
        unparseable value into NULL rather than checking it's plausible
      - crime_id is unique among its non-null values — the
        INSERT ... WHERE NOT EXISTS dedup logic in
        warehouse/setup_duckdb.py::load_local_csv assumes this invariant,
        but nothing has ever asserted it explicitly. Nulls (legitimate for
        Anti-social behaviour rows, which have no Crime ID) are excluded.
    """
    suite = ExpectationSuite(name=name)

    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=min_row_count))

    for column in REQUIRED_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=column))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="month"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="crime_type"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="force"))

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="force", value_set=KNOWN_FORCES)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="crime_type", value_set=KNOWN_CRIME_TYPES)
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="longitude",
            min_value=UK_LONGITUDE_RANGE[0],
            max_value=UK_LONGITUDE_RANGE[1],
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="latitude",
            min_value=UK_LATITUDE_RANGE[0],
            max_value=UK_LATITUDE_RANGE[1],
        )
    )

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="crime_id"))

    return suite


def get_context(gx_root: Path = GX_ROOT):
    """Load (or scaffold, on first run) the GX file-based project at gx_root."""
    return gx.get_context(mode="file", context_root_dir=gx_root)


def ensure_checkpoint(
    context,
    suite_name: str = SUITE_NAME,
    min_row_count: int = 100,
    checkpoint_name: str = CHECKPOINT_NAME,
    validation_definition_name: str = VALIDATION_DEFINITION_NAME,
) -> gx.Checkpoint:
    """
    Idempotently (re)create the suite/datasource/checkpoint from build_suite()
    above. Safe to call on every run — add_or_update means an unchanged suite
    is a no-op, and a suite that has been edited in code gets synced to disk.

    The pandas datasource/asset/batch-definition (ASSET_NAME,
    BATCH_DEFINITION_NAME) are shared plumbing — just "how to hand GX a
    dataframe" — and stay the same regardless of which suite validates it, so
    only the suite/checkpoint/validation-definition names are parameterized.
    """
    suite = context.suites.add_or_update(build_suite(name=suite_name, min_row_count=min_row_count))

    data_source = context.data_sources.add_or_update_pandas(DATASOURCE_NAME)
    if ASSET_NAME in data_source.get_asset_names():
        data_asset = data_source.get_asset(ASSET_NAME)
    else:
        data_asset = data_source.add_dataframe_asset(name=ASSET_NAME)

    try:
        batch_definition = data_asset.get_batch_definition(BATCH_DEFINITION_NAME)
    except KeyError:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(BATCH_DEFINITION_NAME)

    validation_definition = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=validation_definition_name, data=batch_definition, suite=suite)
    )

    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=[validation_definition],
            actions=[gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
        )
    )
    return checkpoint


def validate_dataframe(
    df: pd.DataFrame,
    gx_root: Path = GX_ROOT,
    suite_name: str = SUITE_NAME,
    min_row_count: int = 100,
    checkpoint_name: str = CHECKPOINT_NAME,
    validation_definition_name: str = VALIDATION_DEFINITION_NAME,
):
    """
    Run an Expectation Suite against df and return the CheckpointResult.
    Raises no exception on failed expectations — callers (the Airflow task,
    the streaming consumer, tests) decide what to do with result.success.

    Defaults reproduce the exact batch-pipeline behavior this function has
    always had. Pass the streaming-scoped names/threshold (see
    streaming/consumer.py) to validate a Kafka micro-batch with the same
    expectations under a separate suite/checkpoint instead.
    """
    context = get_context(gx_root)
    checkpoint = ensure_checkpoint(
        context,
        suite_name=suite_name,
        min_row_count=min_row_count,
        checkpoint_name=checkpoint_name,
        validation_definition_name=validation_definition_name,
    )
    return checkpoint.run(batch_parameters={"dataframe": df})


def validate_partition(force: str, year_month: str, gx_root: Path = GX_ROOT):
    """
    Load raw.crimes for a single force + month from the local DuckDB
    warehouse and validate it. Mirrors the (force, month) scoping
    dags/crime_pipeline_dag.py::_validate_loaded already uses.
    """
    from warehouse.setup_duckdb import get_connection

    con = get_connection()
    df = con.execute(
        "SELECT * FROM raw.crimes WHERE force = ? AND month = ?", [force, year_month]
    ).df()
    con.close()

    result = validate_dataframe(df, gx_root)
    logger.info(
        f"GE validation for {force} {year_month}: "
        f"{'PASSED' if result.success else 'FAILED'} ({len(df)} rows)"
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate raw.crimes with Great Expectations")
    parser.add_argument("--force", help="Validate a single force+month partition")
    parser.add_argument("--month", help="YYYY-MM, required if --force is given")
    args = parser.parse_args()

    if args.force:
        if not args.month:
            parser.error("--month is required when --force is given")
        run_result = validate_partition(args.force, args.month)
    else:
        from warehouse.setup_duckdb import get_connection

        con = get_connection()
        full_df = con.execute("SELECT * FROM raw.crimes").df()
        con.close()
        run_result = validate_dataframe(full_df)
        logger.info(
            f"GE validation for entire raw.crimes: "
            f"{'PASSED' if run_result.success else 'FAILED'} ({len(full_df)} rows)"
        )

    context = get_context()
    context.build_data_docs()
    for site in context.get_docs_sites_urls():
        logger.info(f"Data Docs: {site['site_url']}")

    if not run_result.success:
        raise SystemExit(1)
