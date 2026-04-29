"""Tests for src/features.py."""

import pandas as pd
import pytest

from report_generator.features import extract_features, _build_distribution, _month_on_month


def _make_df(crimes: list[str], force: str = "West Yorkshire Police", period: str = "2026-02") -> pd.DataFrame:
    return pd.DataFrame({
        "Crime type": crimes,
        "Falls within": force,
        "Month": period,
    })


class TestExtractFeatures:
    def test_basic_fields_present(self):
        df = _make_df(["Violence"] * 6 + ["Burglary"] * 4)
        f = extract_features(df)
        assert f["force"] == "West Yorkshire Police"
        assert f["period"] == "2026-02"
        assert f["total_crimes"] == 10
        assert "distribution" in f
        assert "top_categories" in f

    def test_top_categories_ordered_by_count(self):
        df = _make_df(["A"] * 5 + ["B"] * 3 + ["C"] * 2)
        f = extract_features(df)
        assert f["top_categories"][0] == "A"
        assert f["top_categories"][1] == "B"

    def test_distribution_sums_to_100(self):
        df = _make_df(["X"] * 3 + ["Y"] * 7)
        f = extract_features(df)
        total = sum(f["distribution"].values())
        assert abs(total - 100.0) < 0.05

    def test_mom_change_included_when_prev_given(self):
        df = _make_df(["Violence"] * 10)
        prev = _make_df(["Violence"] * 8)
        f = extract_features(df, prev_df=prev)
        assert "mom_change" in f
        assert f["mom_change"]["direction"] == "increase"

    def test_mom_change_absent_without_prev(self):
        df = _make_df(["Violence"] * 10)
        f = extract_features(df)
        assert "mom_change" not in f


class TestBuildDistribution:
    def test_percentages_round_to_one_decimal(self):
        counts = pd.Series({"A": 1, "B": 2, "C": 3})
        result = _build_distribution(counts, total=6)
        for v in result.values():
            assert round(v, 1) == v

    def test_sum_is_100(self):
        counts = pd.Series({"A": 33, "B": 33, "C": 34})
        result = _build_distribution(counts, total=100)
        assert abs(sum(result.values()) - 100.0) < 0.05


class TestMonthOnMonth:
    def test_increase_detected(self):
        prev = pd.DataFrame({"Crime type": ["X"] * 8, "Falls within": "F", "Month": "2026-01"})
        result = _month_on_month(10, prev)
        assert result["direction"] == "increase"
        assert result["absolute"] == 2

    def test_decrease_detected(self):
        prev = pd.DataFrame({"Crime type": ["X"] * 12, "Falls within": "F", "Month": "2026-01"})
        result = _month_on_month(10, prev)
        assert result["direction"] == "decrease"
        assert result["absolute"] == -2

    def test_empty_prev_returns_none_values(self):
        prev = pd.DataFrame({"Crime type": [], "Falls within": [], "Month": []})
        result = _month_on_month(10, prev)
        assert result["absolute"] is None
        assert result["pct"] is None
