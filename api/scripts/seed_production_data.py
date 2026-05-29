"""Seed the production PostgreSQL database with West Yorkshire crime data.

Uses only stdlib — no pandas dependency.

Usage (from the api/ directory):
    python scripts/seed_production_data.py
    python scripts/seed_production_data.py --csv ../data/2026-02-west-yorkshire-street.csv
    python scripts/seed_production_data.py --csv ../data/file.csv --batch-size 500 --force

Safe to rerun — skips if data already exists unless --force is passed.
"""
import argparse
import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import engine, SessionLocal, Base
from app.models.crime import Crime

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

DEFAULT_CSV = Path(__file__).parent.parent.parent / "data" / "2026-02-west-yorkshire-street.csv"


def _parse_float(value: str) -> float | None:
    try:
        stripped = value.strip() if value else ""
        return float(stripped) if stripped else None
    except ValueError:
        return None


def seed(csv_path: Path, batch_size: int = 500, force_reseed: bool = False) -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        existing = db.query(Crime).count()
        if existing > 0 and not force_reseed:
            logger.info("Database already contains %d crimes — skipping. Pass --force to reseed.", existing)
            return

        if existing > 0 and force_reseed:
            logger.info("--force passed — deleting %d existing records before reseed.", existing)
            db.query(Crime).delete()
            db.commit()

        logger.info("Seeding from: %s", csv_path)

        batch: list[Crime] = []
        total_inserted = 0
        skipped = 0

        with open(csv_path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                crime_type = (row.get("Crime type") or "").strip()
                if not crime_type:
                    skipped += 1
                    continue

                batch.append(Crime(
                    month=(row.get("Month") or "").strip() or None,
                    force=(row.get("Falls within") or "").strip() or None,
                    crime_type=crime_type,
                    lsoa_code=(row.get("LSOA code") or "").strip() or None,
                    lsoa_name=(row.get("LSOA name") or "").strip() or None,
                    outcome=(row.get("Last outcome category") or "").strip() or None,
                    latitude=_parse_float(row.get("Latitude", "")),
                    longitude=_parse_float(row.get("Longitude", "")),
                ))

                if len(batch) >= batch_size:
                    db.bulk_save_objects(batch)
                    db.commit()
                    total_inserted += len(batch)
                    logger.info("  %d records inserted...", total_inserted)
                    batch = []

        if batch:
            db.bulk_save_objects(batch)
            db.commit()
            total_inserted += len(batch)

        logger.info("Seed complete — %d inserted, %d skipped (missing crime type).", total_inserted, skipped)

        # Verify
        count = db.query(Crime).count()
        logger.info("Verification: %d total records in database.", count)

    except Exception as exc:
        logger.error("Seed failed: %s", exc)
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed crime data into the API database.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help="Path to the crime CSV file",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Records per database commit (default: 500)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete existing records and reseed",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        logger.error("CSV not found: %s", args.csv)
        sys.exit(1)

    seed(args.csv, batch_size=args.batch_size, force_reseed=args.force)
