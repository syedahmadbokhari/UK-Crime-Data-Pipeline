"""
Streaming consumer — subscribes to KAFKA_TOPIC, buffers incoming records into
small micro-batches, validates each micro-batch with the SAME Great
Expectations suite the batch pipeline uses
(data_quality/validate_raw_crimes.py — reused unmodified, just under a
streaming-scoped suite/checkpoint name; see that module's build_suite()
docstring), and writes valid batches to DuckDB (+ S3, if configured) using
the exact same functions the batch pipeline already uses:
warehouse.setup_duckdb.load_local_csv and ingestion.upload_to_s3.upload_file.

Why micro-batch instead of validating/writing one record at a time:
  1. The reused Expectation Suite has table-level checks (row count,
     crime_id uniqueness) that are only meaningful across more than one
     row — a single-record DataFrame would make uniqueness trivially true
     and would need an entirely separate, invented per-record suite, which
     the task this was built for explicitly said not to do.
  2. It mirrors how real Kafka consumers feeding a warehouse actually work
     (Kafka Connect sink connectors, Debezium, Spark Structured Streaming —
     all batch their commits for throughput) rather than one DuckDB INSERT
     and one S3 PUT per message, which doesn't scale.

Offset handling: the consumer disables auto-commit (enable_auto_commit=False)
and commits offsets manually, only once a micro-batch's outcome is durable —
either written to DuckDB/S3, or routed to the rejected/ directory. If the
process dies mid-batch, the next run resumes from the last committed offset
and re-consumes the unflushed messages: at-least-once delivery, never
at-most-once. Reprocessing after a crash is safe specifically because
warehouse.setup_duckdb.load_local_csv's INSERT ... WHERE NOT EXISTS dedups
on (crime_id, month, force) — replaying the same micro-batch does not create
duplicate rows in DuckDB.

Invalid micro-batches are quarantined whole (not silently dropped, and not
split row-by-row) — see _write_rejected()'s docstring for why, and what a
finer-grained version would look like.

Usage:
    python -m streaming.consumer                  # run until stopped (Ctrl+C)
    python -m streaming.consumer --max-messages 200   # bounded run, e.g. for a demo
"""
import argparse
import json
import os
import time
import uuid
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer
from loguru import logger

from data_quality.validate_raw_crimes import GX_ROOT, validate_dataframe
from ingestion.upload_to_s3 import upload_file
from warehouse.setup_duckdb import get_connection, initialise, load_local_csv

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crime-reports")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "crime-warehouse-loader")

# Small enough to demonstrate visibly incremental processing, large enough
# for the reused suite's row-count/uniqueness checks to mean something.
STREAMING_BATCH_SIZE = int(os.getenv("STREAMING_BATCH_SIZE", "20"))
# Flush whatever's buffered after this long even if the batch isn't full,
# so records don't sit unvalidated indefinitely on a slow topic.
STREAMING_FLUSH_INTERVAL_SECONDS = float(os.getenv("STREAMING_FLUSH_INTERVAL_SECONDS", "15"))

STAGING_DIR = Path(os.getenv("STREAMING_STAGING_DIR", "./data/raw"))
REJECTED_DIR = Path(os.getenv("REJECTED_DATA_DIR", "./data/rejected"))

# Separate names from data_quality.validate_raw_crimes's batch-pipeline suite
# so this doesn't overwrite that suite's persisted definition on disk — see
# build_suite()'s docstring.
STREAMING_SUITE_NAME = "raw_crimes_streaming_suite"
STREAMING_CHECKPOINT_NAME = "raw_crimes_streaming_checkpoint"
STREAMING_VALIDATION_DEFINITION_NAME = "raw_crimes_streaming_validation"

RAW_CSV_COLUMNS = [
    "Crime ID", "Month", "Reported by", "Falls within", "Longitude", "Latitude",
    "Location", "LSOA code", "LSOA name", "Crime type", "Last outcome category", "Context",
]


def get_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        enable_auto_commit=False,  # see module docstring: manual commit after a batch is durable
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def _to_validation_dataframe(records: list[dict]) -> pd.DataFrame:
    """
    Shape a micro-batch of raw messages into the same crime_id/month/force/
    crime_type/longitude/latitude columns data_quality.validate_raw_crimes's
    suite checks (raw.crimes' column names) — same rename
    warehouse/setup_duckdb.py's SQL does for the batch path, kept in sync
    deliberately with RAW_CSV_COLUMNS above.
    """
    df = pd.DataFrame(records)
    return pd.DataFrame({
        "crime_id": df["Crime ID"].replace("", None),
        "month": df["Month"],
        "force": df["_force"],
        "crime_type": df["Crime type"],
        "longitude": pd.to_numeric(df["Longitude"], errors="coerce"),
        "latitude": pd.to_numeric(df["Latitude"], errors="coerce"),
    })


def _write_valid(records: list[dict], batch_id: str) -> None:
    """
    Stage the micro-batch as a CSV shaped exactly like a data.police.uk
    export, then hand it to the *exact same* load_local_csv/upload_file
    functions the batch pipeline uses — no separate write path to keep in
    sync. Filenames include batch_id so repeated micro-batches for the same
    force+month don't collide under upload_file's idempotent HEAD-check key,
    and grouped by (force, month) since both functions write one partition
    at a time — the same Hive layout the batch path uses, just with a
    filename suffix marking these as stream-sourced.
    """
    df = pd.DataFrame(records)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    con = get_connection()
    initialise(con)

    for (force, month), group in df.groupby(["_force", "Month"]):
        staging_path = STAGING_DIR / f"{month}-{force}-street-stream-{batch_id}.csv"
        group[RAW_CSV_COLUMNS].to_csv(staging_path, index=False)

        load_local_csv(con, str(staging_path), force)

        if os.getenv("S3_BUCKET_NAME"):
            upload_file(staging_path, force, month)
        else:
            logger.warning(
                "S3_BUCKET_NAME not set — skipping S3 upload for this micro-batch "
                "(DuckDB write still happened; same 'no AWS needed to start' as Quick Start step 3)"
            )

    con.close()
    logger.success(f"Wrote {len(records)} validated record(s) to DuckDB (batch {batch_id})")


def _write_rejected(records: list[dict], result, batch_id: str) -> None:
    """
    Route a failed micro-batch to REJECTED_DIR instead of dropping it — the
    earlier finding in this project was a silent `WHERE ... IS NOT NULL`
    filter in dbt_crime/models/staging/stg_crimes.sql that dropped bad rows
    with zero visibility; this is deliberately the opposite of that.

    This quarantines the WHOLE micro-batch, not just the specific bad
    row(s) — table-level expectations (row count, crime_id uniqueness) fail
    for the batch as a whole with no single row to blame, so there's no
    universally correct way to partially accept a failed batch. A finer-grained
    version is possible for the per-row expectations (not_null/in_set/between
    all expose which row indices violated them via GE's
    result['partial_unexpected_index_list']) but isn't implemented here to
    keep the quarantine behavior simple and unambiguous.
    """
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    forces = sorted({r.get("_force", "unknown") for r in records})
    label = forces[0] if len(forces) == 1 else "mixed-force"
    rejected_path = REJECTED_DIR / f"{label}-{batch_id}-rejected.csv"

    pd.DataFrame(records)[RAW_CSV_COLUMNS + ["_force"]].to_csv(rejected_path, index=False)

    failed_types = [
        r.expectation_config.type
        for run_result in result.run_results.values()
        for r in run_result.results
        if not r.success
    ]
    logger.error(
        f"GE validation FAILED for micro-batch {batch_id} ({len(records)} record(s)) — "
        f"routed to {rejected_path}. Failed expectations: {failed_types}"
    )


def _flush_batch(records: list[dict], gx_root: Path = GX_ROOT) -> bool:
    """Validate a micro-batch and route it to DuckDB/S3 or rejected/. Returns success.

    gx_root defaults to the real repo's great_expectations/ project (same as
    data_quality.validate_raw_crimes) but is overridable — tests pass a
    tmp_path so runs don't touch the committed GX project, same convention
    tests/test_data_quality_ge.py already uses for the batch suite.
    """
    if not records:
        return True

    batch_id = uuid.uuid4().hex[:8]
    validation_df = _to_validation_dataframe(records)
    result = validate_dataframe(
        validation_df,
        gx_root=gx_root,
        suite_name=STREAMING_SUITE_NAME,
        min_row_count=len(records),
        checkpoint_name=STREAMING_CHECKPOINT_NAME,
        validation_definition_name=STREAMING_VALIDATION_DEFINITION_NAME,
    )

    if result.success:
        _write_valid(records, batch_id)
        return True

    _write_rejected(records, result, batch_id)
    return False


def run_consumer(consumer: KafkaConsumer = None, max_messages: int = None, gx_root: Path = GX_ROOT) -> dict:
    """
    Main consumer loop. Buffers messages and flushes a micro-batch once
    STREAMING_BATCH_SIZE records have accumulated or
    STREAMING_FLUSH_INTERVAL_SECONDS have elapsed since the last flush,
    whichever comes first.

    consumer: injectable for testing (pass a fake with .poll()/.commit()/
    .close()) — defaults to a real KafkaConsumer otherwise.
    max_messages: stop after processing this many messages total (used for
    bounded demo runs and tests). None runs forever, as the docker-compose
    streaming-consumer service does.
    gx_root: overridable so tests can point validation at a tmp_path GX
    project instead of the real repo's great_expectations/ — see
    _flush_batch()'s docstring.
    """
    owns_consumer = consumer is None
    consumer = consumer or get_consumer()

    buffer: list[dict] = []
    last_flush = time.monotonic()
    stats = {"processed": 0, "written": 0, "rejected": 0, "batches": 0}

    try:
        while max_messages is None or stats["processed"] < max_messages:
            poll_result = consumer.poll(timeout_ms=1000, max_records=STREAMING_BATCH_SIZE)
            for messages in poll_result.values():
                for msg in messages:
                    buffer.append(msg.value)
                    stats["processed"] += 1

            timed_out = (time.monotonic() - last_flush) >= STREAMING_FLUSH_INTERVAL_SECONDS
            if buffer and (len(buffer) >= STREAMING_BATCH_SIZE or timed_out):
                success = _flush_batch(buffer, gx_root=gx_root)
                stats["written" if success else "rejected"] += len(buffer)
                stats["batches"] += 1
                consumer.commit()  # manual commit — only after the batch's outcome is durable
                buffer = []
                last_flush = time.monotonic()

        if buffer:
            success = _flush_batch(buffer, gx_root=gx_root)
            stats["written" if success else "rejected"] += len(buffer)
            stats["batches"] += 1
            consumer.commit()
    finally:
        if owns_consumer:
            consumer.close()

    logger.info(f"Consumer stopped. Stats: {stats}")
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume crime-reports from Kafka into DuckDB/S3")
    parser.add_argument(
        "--max-messages", type=int, default=None,
        help="Stop after this many messages (default: run forever)",
    )
    args = parser.parse_args()

    run_consumer(max_messages=args.max_messages)
