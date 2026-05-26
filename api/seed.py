"""Load West Yorkshire crime CSV into the API database.

Usage:
    python seed.py --csv ../data/2026-02-west-yorkshire-street.csv
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

sys.path.insert(0, str(Path(__file__).parent))

from app.database import engine, SessionLocal
from app.models.crime import Crime
from app.database import Base


def seed(csv_path: Path) -> None:
    Base.metadata.create_all(bind=engine)

    db: Session = SessionLocal()
    try:
        existing = db.query(Crime).count()
        if existing > 0:
            print(f"Database already contains {existing:,} crimes — skipping seed.")
            return

        df = pd.read_csv(csv_path, low_memory=False)
        df = df.dropna(subset=["Crime type"])

        column_map = {
            "Month": "month",
            "Falls within": "force",
            "Crime type": "crime_type",
            "LSOA code": "lsoa_code",
            "LSOA name": "lsoa_name",
            "Last outcome category": "outcome",
            "Latitude": "latitude",
            "Longitude": "longitude",
        }
        df = df.rename(columns=column_map)
        cols = list(column_map.values())
        df = df[[c for c in cols if c in df.columns]]

        crimes = [Crime(**row) for row in df.to_dict(orient="records")]
        db.bulk_save_objects(crimes)
        db.commit()
        print(f"Seeded {len(crimes):,} crime records.")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, default=Path("../data/2026-02-west-yorkshire-street.csv"))
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"CSV not found: {args.csv}")
        sys.exit(1)

    seed(args.csv)
