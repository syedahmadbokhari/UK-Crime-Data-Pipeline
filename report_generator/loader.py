"""Crime CSV loader with validation, encoding fallback, and structured logging."""

import logging
from io import IOBase
from pathlib import Path
from typing import Union

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: set[str] = {"Month", "Crime type", "Falls within"}
_ENCODINGS: list[str] = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]


def load_crime_data(
    source: Union[str, Path, IOBase],
    encoding: str | None = None,
) -> pd.DataFrame:
    """Load and validate a UK police crime CSV file.

    Accepts a file path or a file-like object (e.g. Streamlit UploadedFile).
    Tries multiple encodings automatically when encoding is not specified.

    Args:
        source: Path to CSV file, or a file-like object.
        encoding: Force a specific encoding. If None, tries common encodings.

    Returns:
        Cleaned DataFrame with null crime-type rows removed.

    Raises:
        FileNotFoundError: If a path is given and the file does not exist.
        ValueError: If required columns are missing or no encoding succeeds.
    """
    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

    df = _read_csv(source, encoding)
    _validate_columns(df)
    _validate_month_format(df)
    df = _drop_invalid_rows(df)

    logger.info("Loaded %d rows — force='%s', period='%s'",
                len(df), _peek_force(df), _peek_period(df))
    return df


# ── Private helpers ───────────────────────────────────────────────────────────

def _read_csv(source: Union[str, Path, IOBase], encoding: str | None) -> pd.DataFrame:
    encodings = [encoding] if encoding else _ENCODINGS
    last_error: Exception | None = None

    for enc in encodings:
        try:
            if hasattr(source, "seek"):
                source.seek(0)
            df = pd.read_csv(source, encoding=enc, low_memory=False)
            logger.debug("Read CSV with encoding '%s'", enc)
            return df
        except UnicodeDecodeError as e:
            last_error = e
            logger.debug("Encoding '%s' failed — trying next", enc)

    raise ValueError(
        f"Could not decode the file with any of {encodings}. "
        f"Last error: {last_error}"
    )


def _validate_columns(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"CSV is missing required columns: {missing}. "
            f"Found: {set(df.columns)}"
        )


def _validate_month_format(df: pd.DataFrame) -> None:
    sample = df["Month"].dropna().astype(str)
    valid = sample.str.match(r"^\d{4}-\d{2}$")
    if valid.sum() == 0:
        raise ValueError(
            "Column 'Month' contains no YYYY-MM formatted values. "
            f"Sample values: {sample.head(3).tolist()}"
        )
    if valid.mean() < 0.9:
        logger.warning(
            "%.0f%% of 'Month' values do not match YYYY-MM format",
            (1 - valid.mean()) * 100,
        )


def _drop_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.dropna(subset=["Crime type"]).reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        logger.warning(
            "Dropped %d rows with null 'Crime type' (%.1f%% of total)",
            dropped, dropped / before * 100,
        )
    return df


def _peek_force(df: pd.DataFrame) -> str:
    return df["Falls within"].mode()[0] if not df.empty else "unknown"


def _peek_period(df: pd.DataFrame) -> str:
    return df["Month"].mode()[0] if not df.empty else "unknown"
