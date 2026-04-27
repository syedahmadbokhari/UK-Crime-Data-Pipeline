"""
Shared fixtures for the test suite.
"""
import os
import textwrap
from pathlib import Path

import duckdb
import pandas as pd
import pytest


# --------------------------------------------------------------------------- #
# Sample crime data fixture                                                      #
# --------------------------------------------------------------------------- #

SAMPLE_ROWS = [
    {
        "Crime ID": "abc123",
        "Month": "2026-02",
        "Reported by": "West Yorkshire Police",
        "Falls within": "West Yorkshire Police",
        "Longitude": -1.87,
        "Latitude": 53.94,
        "Location": "On or near Test Street",
        "LSOA code": "E01010646",
        "LSOA name": "Bradford 001A",
        "Crime type": "Burglary",
        "Last outcome category": "Under investigation",
        "Context": "",
    },
    {
        "Crime ID": "",  # ASB has no ID
        "Month": "2026-02",
        "Reported by": "West Yorkshire Police",
        "Falls within": "West Yorkshire Police",
        "Longitude": -1.82,
        "Latitude": 53.92,
        "Location": "On or near Park Road",
        "LSOA code": "E01010692",
        "LSOA name": "Bradford 001D",
        "Crime type": "Anti-social behaviour",
        "Last outcome category": "",
        "Context": "",
    },
    {
        "Crime ID": "def456",
        "Month": "2026-02",
        "Reported by": "West Yorkshire Police",
        "Falls within": "West Yorkshire Police",
        "Longitude": -1.79,
        "Latitude": 53.91,
        "Location": "On or near High Street",
        "LSOA code": "E01010696",
        "LSOA name": "Bradford 002E",
        "Crime type": "Shoplifting",
        "Last outcome category": "Investigation complete; no suspect identified",
        "Context": "",
    },
]


@pytest.fixture
def sample_csv(tmp_path) -> Path:
    """Write sample rows to a temp CSV and return its path."""
    df = pd.DataFrame(SAMPLE_ROWS)
    path = tmp_path / "2026-02-west-yorkshire-street.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def in_memory_db() -> duckdb.DuckDBPyConnection:
    """Return an initialised in-memory DuckDB connection."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from warehouse.setup_duckdb import initialise
    con = duckdb.connect(":memory:")
    # httpfs not needed for in-memory tests
    initialise(con)
    yield con
    con.close()
