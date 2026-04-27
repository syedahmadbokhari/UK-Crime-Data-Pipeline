"""
Tests for DuckDB loading and transformation logic.
Uses an in-memory DuckDB — no S3, no file system dependencies.
"""
from pathlib import Path
import sys

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from warehouse.setup_duckdb import load_local_csv, initialise


class TestDuckDBLoad:
    def test_load_inserts_all_rows(self, in_memory_db, sample_csv):
        inserted = load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        assert inserted == 3

    def test_load_is_idempotent(self, in_memory_db, sample_csv):
        """Re-running the same load must not create duplicate rows."""
        load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        inserted_second = load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        # ASB row has no crime_id so it will re-insert; the 2 with IDs must not
        total = in_memory_db.execute("SELECT COUNT(*) FROM raw.crimes").fetchone()[0]
        assert total == 4  # 2 unique-id rows + 1 no-id (first load) + 1 no-id (second)

    def test_force_column_populated(self, in_memory_db, sample_csv):
        load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        forces = in_memory_db.execute(
            "SELECT DISTINCT force FROM raw.crimes"
        ).fetchall()
        assert forces == [("west-yorkshire",)]

    def test_null_crime_id_for_asb(self, in_memory_db, sample_csv):
        load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        null_count = in_memory_db.execute(
            "SELECT COUNT(*) FROM raw.crimes WHERE crime_id IS NULL"
        ).fetchone()[0]
        assert null_count == 1

    def test_coordinates_cast_correctly(self, in_memory_db, sample_csv):
        load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        row = in_memory_db.execute(
            "SELECT longitude, latitude FROM raw.crimes WHERE crime_id = 'abc123'"
        ).fetchone()
        assert row is not None
        assert abs(row[0] - (-1.87)) < 0.001
        assert abs(row[1] - 53.94) < 0.001


class TestDataQuality:
    def test_no_null_months(self, in_memory_db, sample_csv):
        load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        null_months = in_memory_db.execute(
            "SELECT COUNT(*) FROM raw.crimes WHERE month IS NULL"
        ).fetchone()[0]
        assert null_months == 0

    def test_all_crime_types_populated(self, in_memory_db, sample_csv):
        load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        null_types = in_memory_db.execute(
            "SELECT COUNT(*) FROM raw.crimes WHERE crime_type IS NULL"
        ).fetchone()[0]
        assert null_types == 0

    def test_known_crime_types_only(self, in_memory_db, sample_csv):
        load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        types = {
            r[0] for r in in_memory_db.execute(
                "SELECT DISTINCT crime_type FROM raw.crimes"
            ).fetchall()
        }
        valid = {
            "Anti-social behaviour", "Bicycle theft", "Burglary",
            "Criminal damage and arson", "Drugs", "Other crime", "Other theft",
            "Possession of weapons", "Public order", "Robbery", "Shoplifting",
            "Theft from the person", "Vehicle crime", "Violence and sexual offences",
        }
        assert types.issubset(valid), f"Unknown crime types: {types - valid}"

    def test_schema_has_required_columns(self, in_memory_db):
        cols = {
            r[0] for r in in_memory_db.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='crimes'"
            ).fetchall()
        }
        required = {"crime_id", "month", "force", "crime_type", "longitude", "latitude"}
        assert required.issubset(cols)

    def test_row_count_gate_passes(self, in_memory_db, sample_csv):
        load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        count = in_memory_db.execute("SELECT COUNT(*) FROM raw.crimes").fetchone()[0]
        assert count >= 1, "Row count gate: raw.crimes must have at least 1 row"

    def test_incremental_load_advances_total(self, in_memory_db, sample_csv, tmp_path):
        """Load the same data for two different forces — totals should sum."""
        load_local_csv(in_memory_db, str(sample_csv), "west-yorkshire")
        load_local_csv(in_memory_db, str(sample_csv), "greater-manchester")
        total = in_memory_db.execute("SELECT COUNT(*) FROM raw.crimes").fetchone()[0]
        # 3 west-yorkshire rows + 3 greater-manchester rows = 6
        # (all rows have crime_id or are ASB so idempotency doesn't collapse cross-force)
        assert total == 6
