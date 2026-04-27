"""
Downloads UK Police crime data from data.police.uk for specified forces and months.

Usage:
    python -m ingestion.download_data --force west-yorkshire --start 2024-01 --end 2024-12
"""
import argparse
import os
import time
import zipfile
from pathlib import Path

import requests
from loguru import logger

BASE_URL = "https://data.police.uk/data/fetch"
OUTPUT_DIR = Path(os.getenv("RAW_DATA_DIR", "./data/raw"))

SUPPORTED_FORCES = [
    "west-yorkshire",
    "greater-manchester",
    "metropolitan",
    "west-midlands",
]


def download_month(force: str, year_month: str, output_dir: Path) -> Path | None:
    """
    Download crime CSV for a single force + month.
    data.police.uk serves data via a zip — we extract the street CSV.
    Returns the path to the extracted CSV, or None on failure.
    """
    year, month = year_month.split("-")
    out_path = output_dir / f"{year_month}-{force}-street.csv"

    if out_path.exists():
        logger.info(f"Already downloaded: {out_path.name} — skipping")
        return out_path

    output_dir.mkdir(parents=True, exist_ok=True)

    # data.police.uk bulk download endpoint
    params = {
        "force": force,
        "date": year_month,
        "include_crime": "1",
    }
    zip_path = output_dir / f"{year_month}-{force}.zip"

    logger.info(f"Downloading {force} {year_month}...")
    try:
        resp = requests.get(BASE_URL, params=params, timeout=60, stream=True)
        resp.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
    except requests.RequestException as e:
        logger.error(f"Download failed for {force} {year_month}: {e}")
        return None

    # Extract the street-level CSV from the zip
    try:
        with zipfile.ZipFile(zip_path) as z:
            csv_files = [n for n in z.namelist() if "street" in n and n.endswith(".csv")]
            if not csv_files:
                logger.warning(f"No street CSV found in zip for {force} {year_month}")
                zip_path.unlink()
                return None
            # Use the first matching file
            with z.open(csv_files[0]) as src, open(out_path, "wb") as dst:
                dst.write(src.read())
        zip_path.unlink()
        logger.success(f"Downloaded → {out_path.name}")
        return out_path
    except zipfile.BadZipFile:
        logger.error(f"Bad zip for {force} {year_month}")
        zip_path.unlink(missing_ok=True)
        return None


def download_range(force: str, start: str, end: str) -> list[Path]:
    """Download all months in [start, end] for a force. Returns list of CSV paths."""
    from ingestion.watermark import months_since

    months = months_since(
        # subtract one month so start is inclusive
        _prev_month(start),
        end,
    )
    results = []
    for month in months:
        path = download_month(force, month, OUTPUT_DIR)
        if path:
            results.append(path)
        time.sleep(1)  # be polite to the API
    return results


def _prev_month(ym: str) -> str:
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    dt = datetime.strptime(ym, "%Y-%m") - relativedelta(months=1)
    return dt.strftime("%Y-%m")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download UK Police crime data")
    parser.add_argument("--force", required=True, choices=SUPPORTED_FORCES)
    parser.add_argument("--start", required=True, help="Start month YYYY-MM")
    parser.add_argument("--end", required=True, help="End month YYYY-MM")
    args = parser.parse_args()

    paths = download_range(args.force, args.start, args.end)
    logger.info(f"Downloaded {len(paths)} files for {args.force}")
