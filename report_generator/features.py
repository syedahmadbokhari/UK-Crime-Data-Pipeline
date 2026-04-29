"""Feature extraction — aggregates raw crime data into a prompt-ready summary dict."""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def extract_features(
    df: pd.DataFrame,
    prev_df: Optional[pd.DataFrame] = None,
) -> dict:
    """Aggregate a crime DataFrame into a compact feature dictionary.

    Args:
        df: Current period crime data (output of loader.load_crime_data).
        prev_df: Optional prior period data. When supplied, month-on-month
                 figures are added under the key 'mom_change'.

    Returns:
        Feature dictionary with keys: force, period, total_crimes,
        top_categories, distribution, and optionally mom_change.
    """
    total = len(df)
    force = df["Falls within"].mode()[0]
    period = df["Month"].mode()[0]

    type_counts = df["Crime type"].value_counts()
    distribution = _build_distribution(type_counts, total)
    top_3 = type_counts.head(3).index.tolist()

    features: dict = {
        "force": force,
        "period": period,
        "total_crimes": total,
        "top_categories": top_3,
        "distribution": distribution,
    }

    if prev_df is not None:
        features["mom_change"] = _month_on_month(total, prev_df)
        logger.debug("Month-on-month comparison included")

    logger.debug(
        "Features extracted: force='%s', period='%s', total=%d, categories=%d",
        force, period, total, len(distribution),
    )
    return features


# ── Private helpers ───────────────────────────────────────────────────────────

def _build_distribution(counts: pd.Series, total: int) -> dict[str, float]:
    """Return crime-type percentages rounded to one decimal place.

    Ensures all percentages sum to exactly 100.0 by adjusting the largest
    category for any floating-point rounding residual.
    """
    raw = (counts / total * 100).round(1)
    result = raw.to_dict()

    residual = round(100.0 - sum(result.values()), 1)
    if residual and result:
        top_key = max(result, key=result.__getitem__)
        result[top_key] = round(result[top_key] + residual, 1)

    return result


def _month_on_month(current_total: int, prev_df: pd.DataFrame) -> dict:
    """Compute month-on-month change statistics.

    Returns a dict with keys: previous_total, absolute, pct, direction.
    If the previous dataset is empty, all numeric fields are None.
    """
    prev_total = len(prev_df)
    if prev_total == 0:
        logger.warning("Previous period DataFrame is empty — skipping MoM calculation")
        return {
            "previous_total": 0,
            "absolute": None,
            "pct": None,
            "direction": "unknown",
        }

    absolute = current_total - prev_total
    pct = round(abs(absolute) / prev_total * 100, 1)

    if absolute > 0:
        direction = "increase"
    elif absolute < 0:
        direction = "decrease"
    else:
        direction = "no change"

    return {
        "previous_total": prev_total,
        "absolute": absolute,
        "pct": pct,
        "direction": direction,
    }
