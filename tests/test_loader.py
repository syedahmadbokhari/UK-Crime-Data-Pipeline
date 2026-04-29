"""Tests for src/loader.py."""

import io
import pytest
import pandas as pd

from report_generator.loader import load_crime_data, REQUIRED_COLUMNS


def _make_csv(rows: list[dict]) -> io.BytesIO:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    buf.write(df.to_csv(index=False).encode("utf-8"))
    buf.seek(0)
    return buf


VALID_ROW = {
    "Month": "2026-02",
    "Crime type": "Violence and sexual offences",
    "Falls within": "West Yorkshire Police",
}


class TestLoadCrimeData:
    def test_valid_file_returns_dataframe(self):
        buf = _make_csv([VALID_ROW] * 5)
        df = load_crime_data(buf)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5

    def test_missing_required_column_raises(self):
        row = {"Month": "2026-02", "Crime type": "Burglary"}  # no 'Falls within'
        buf = _make_csv([row])
        with pytest.raises(ValueError, match="missing required columns"):
            load_crime_data(buf)

    def test_null_crime_type_rows_dropped(self):
        rows = [VALID_ROW.copy() for _ in range(4)]
        rows.append({"Month": "2026-02", "Crime type": None, "Falls within": "WYP"})
        buf = _make_csv(rows)
        df = load_crime_data(buf)
        assert len(df) == 4

    def test_all_null_crime_types_returns_empty(self):
        rows = [{"Month": "2026-02", "Crime type": None, "Falls within": "WYP"}] * 3
        buf = _make_csv(rows)
        df = load_crime_data(buf)
        assert len(df) == 0

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            load_crime_data("/nonexistent/path/file.csv")

    def test_invalid_month_format_raises(self):
        row = {"Month": "February 2026", "Crime type": "Burglary", "Falls within": "WYP"}
        buf = _make_csv([row])
        with pytest.raises(ValueError, match="YYYY-MM"):
            load_crime_data(buf)

    def test_required_columns_constant(self):
        assert "Month" in REQUIRED_COLUMNS
        assert "Crime type" in REQUIRED_COLUMNS
        assert "Falls within" in REQUIRED_COLUMNS
