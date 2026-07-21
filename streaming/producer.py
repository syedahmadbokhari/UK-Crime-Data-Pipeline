"""
Streaming producer — publishes crime records to Kafka one message at a time,
simulating incremental arrival instead of the existing full-file batch upload
(ingestion/upload_to_s3.py's upload_file() uploads a whole CSV in a single
call; warehouse/setup_duckdb.py's load_local_csv() loads it in a single
INSERT).

Honesty note (see README.md's "Real-Time Streaming (Kafka)" section for the
full version): UK Police crime data is genuinely published as monthly batch
snapshots by data.police.uk — nothing about the underlying data is naturally
real-time. This producer demonstrates a real Kafka producer/consumer
architecture using that batch-published data as a stand-in event source, one
record per message, with realistic pacing between sends. It is not a claim
that this data is a live feed.

Usage:
    python -m streaming.producer --force west-yorkshire --month 2026-02
    python -m streaming.producer --force west-yorkshire --month 2026-02 --delay 0.2
"""
import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from loguru import logger

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crime-reports")
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "./data/raw"))
DEFAULT_DELAY_SECONDS = float(os.getenv("STREAM_DELAY_SECONDS", "0.1"))

# Same raw column names data.police.uk's CSVs use (and warehouse/setup_duckdb.py
# reads by name) — kept unchanged on the wire so the consumer doesn't need a
# separate message schema to stay in sync with.
RAW_CSV_COLUMNS = [
    "Crime ID", "Month", "Reported by", "Falls within", "Longitude", "Latitude",
    "Location", "LSOA code", "LSOA name", "Crime type", "Last outcome category", "Context",
]


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def find_csv(force: str, year_month: str) -> Path | None:
    """
    Locate a local crime CSV for force+year_month. Same filename pattern
    ingestion/upload_to_s3.py::upload_month looks for, plus the repo root and
    ./data/, where the committed sample CSV actually lives (not under
    data/raw/, which is gitignored/empty on a fresh clone).
    """
    pattern = f"{year_month}-{force}-street.csv"
    search_dirs = [RAW_DATA_DIR, Path("./data"), Path(".")]

    for directory in search_dirs:
        matches = list(directory.glob(pattern))
        if not matches:
            matches = list(directory.glob(f"*{year_month}*{force}*street*.csv"))
        if matches:
            return matches[0]
    return None


def stream_file(
    csv_path: Path,
    force: str,
    producer: KafkaProducer = None,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    topic: str = KAFKA_TOPIC,
) -> int:
    """
    Publish every row of csv_path to `topic`, one message per row, pausing
    delay_seconds between sends so a consumer genuinely sees records arrive
    incrementally rather than all at once.

    Each message value carries the original CSV columns unchanged plus
    `_force` (the raw CSVs don't reliably encode the force machine-slug the
    rest of the pipeline uses — see ingestion/upload_to_s3.py's `--force` CLI
    arg for the same reasoning).
    """
    owns_producer = producer is None
    producer = producer or get_producer()

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    sent = 0
    try:
        for _, row in df.iterrows():
            record = {col: row.get(col, "") for col in RAW_CSV_COLUMNS}
            record["_force"] = force
            key = record["Crime ID"] or None
            producer.send(topic, key=key, value=record)
            sent += 1
            if delay_seconds:
                time.sleep(delay_seconds)
        producer.flush()
    finally:
        if owns_producer:
            producer.close()

    logger.success(f"Published {sent} record(s) from {csv_path.name} to '{topic}'")
    return sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stream a crime CSV to Kafka, one record at a time (not a bulk publish)"
    )
    parser.add_argument("--force", required=True)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY_SECONDS,
        help="Seconds to sleep between messages (default: %(default)s)",
    )
    args = parser.parse_args()

    path = find_csv(args.force, args.month)
    if not path:
        raise SystemExit(f"No local CSV found for {args.force} {args.month}")

    stream_file(path, args.force, delay_seconds=args.delay)
