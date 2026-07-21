"""
Tests for the Great Expectations suite in data_quality/validate_raw_crimes.py.

Uses synthetic pandas DataFrames only (no real AWS or DuckDB connection),
consistent with tests/conftest.py's SAMPLE_ROWS pattern. Each GX project is
scoped to a pytest tmp_path so runs don't touch the repo's own
great_expectations/ directory or its Data Docs.
"""
from pathlib import Path

import pandas as pd
import pytest

from data_quality.validate_raw_crimes import validate_dataframe

# A clean 120-row baseline (>= the suite's min row count of 100) that should
# pass every expectation. Individual tests below mutate exactly one row to
# introduce a specific, genuinely bad value and confirm the suite catches it.
N_ROWS = 120


def _good_dataframe(n: int = N_ROWS) -> pd.DataFrame:
    crime_types = [
        "Burglary", "Anti-social behaviour", "Shoplifting",
        "Vehicle crime", "Violence and sexual offences",
    ]
    return pd.DataFrame({
        "crime_id": [f"crime-{i}" for i in range(n)],
        "month": ["2026-02"] * n,
        "force": ["west-yorkshire"] * n,
        "crime_type": [crime_types[i % len(crime_types)] for i in range(n)],
        "longitude": [-1.87 - (i % 10) * 0.01 for i in range(n)],
        "latitude": [53.94 - (i % 10) * 0.01 for i in range(n)],
    })


def _failed_expectation_types(result) -> set[str]:
    types = set()
    for run_result in result.run_results.values():
        for r in run_result.results:
            if not r.success:
                types.add(r.expectation_config.type)
    return types


class TestRawCrimesSuite:
    def test_clean_data_passes(self, tmp_path):
        df = _good_dataframe()
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert result.success

    def test_null_month_is_caught(self, tmp_path):
        df = _good_dataframe()
        df.loc[0, "month"] = None
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert not result.success
        assert "expect_column_values_to_not_be_null" in _failed_expectation_types(result)

    def test_null_crime_type_is_caught(self, tmp_path):
        df = _good_dataframe()
        df.loc[0, "crime_type"] = None
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert not result.success
        assert "expect_column_values_to_not_be_null" in _failed_expectation_types(result)

    def test_unknown_crime_type_is_caught(self, tmp_path):
        """A malformed/unrecognised category value, e.g. from a bad manual edit or a new
        ONS category data.police.uk hasn't documented yet."""
        df = _good_dataframe()
        df.loc[0, "crime_type"] = "Not A Real Crime Type"
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert not result.success
        assert "expect_column_values_to_be_in_set" in _failed_expectation_types(result)

    def test_unknown_force_is_caught(self, tmp_path):
        df = _good_dataframe()
        df["force"] = "atlantis-police"
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert not result.success
        assert "expect_column_values_to_be_in_set" in _failed_expectation_types(result)

    def test_out_of_range_coordinates_are_caught(self, tmp_path):
        """Genuinely new check: nothing in the codebase validates lat/long today,
        only TRY_CAST to DOUBLE (which lets an implausible-but-numeric value through)."""
        df = _good_dataframe()
        df.loc[0, "latitude"] = 999.0
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert not result.success
        assert "expect_column_values_to_be_between" in _failed_expectation_types(result)

    def test_null_coordinates_do_not_fail_the_range_check(self, tmp_path):
        """Real data.police.uk exports do have null coordinates for privacy-redacted
        locations (confirmed in the real sample CSV) — nulls must not be treated
        as range violations."""
        df = _good_dataframe()
        df.loc[0, "latitude"] = None
        df.loc[0, "longitude"] = None
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert result.success

    def test_duplicate_crime_id_is_caught(self, tmp_path):
        """Genuinely new check: formalizes the uniqueness invariant that
        warehouse/setup_duckdb.py::load_local_csv's dedup INSERT already assumes
        but never explicitly asserts."""
        df = _good_dataframe()
        df.loc[1, "crime_id"] = df.loc[0, "crime_id"]
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert not result.success
        assert "expect_column_values_to_be_unique" in _failed_expectation_types(result)

    def test_null_crime_id_does_not_fail_uniqueness(self, tmp_path):
        """Anti-social behaviour rows legitimately have no Crime ID (see
        dbt_crime/models/staging/schema.yml's crime_id description) — multiple
        nulls must not be flagged as duplicates of each other."""
        df = _good_dataframe()
        df.loc[0, "crime_id"] = None
        df.loc[1, "crime_id"] = None
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert result.success

    def test_row_count_below_minimum_is_caught(self, tmp_path):
        """Formalizes dags/crime_pipeline_dag.py::_validate_raw's existing
        `len(df) < 100` gate."""
        df = _good_dataframe(n=10)
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert not result.success
        assert "expect_table_row_count_to_be_between" in _failed_expectation_types(result)

    def test_missing_required_column_is_caught(self, tmp_path):
        """Formalizes tests/test_transformations.py::test_schema_has_required_columns."""
        df = _good_dataframe().drop(columns=["longitude"])
        result = validate_dataframe(df, gx_root=tmp_path / "great_expectations")
        assert not result.success
        assert "expect_column_to_exist" in _failed_expectation_types(result)
