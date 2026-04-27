"""
Watermark tracking — persists the last successfully loaded year/month/force
so incremental runs only process new data.
"""
import json
import os
from datetime import datetime
from pathlib import Path

from loguru import logger

def _watermark_path() -> Path:
    return Path(os.getenv("WATERMARK_FILE", "./warehouse/watermark.json"))


def _load() -> dict:
    path = _watermark_path()
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def _save(state: dict) -> None:
    path = _watermark_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def get_watermark(force: str) -> str | None:
    """Return the last processed month (YYYY-MM) for a given force, or None."""
    state = _load()
    return state.get(force)


def set_watermark(force: str, month: str) -> None:
    """Persist the last successfully processed month for a force."""
    state = _load()
    state[force] = month
    _save(state)
    logger.info(f"Watermark updated: {force} → {month}")


def months_since(start: str, end: str | None = None) -> list[str]:
    """
    Return list of YYYY-MM strings from start (exclusive) to end (inclusive).
    end defaults to current month.
    """
    from dateutil.relativedelta import relativedelta

    end_dt = datetime.strptime(end, "%Y-%m") if end else datetime.now().replace(day=1)
    current = datetime.strptime(start, "%Y-%m") + relativedelta(months=1)
    result = []
    while current <= end_dt:
        result.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)
    return result
