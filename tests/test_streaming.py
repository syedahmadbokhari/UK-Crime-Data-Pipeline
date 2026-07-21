"""
Tests for streaming/producer.py and streaming/consumer.py.

No live Kafka broker required: streaming.producer.stream_file and
streaming.consumer.run_consumer both accept an injectable client (producer=/
consumer=), so tests pass small in-process fakes implementing only the
.send()/.poll()/.commit()/.close() surface the code actually calls — the same
"fully in-process fake, no live service" spirit as this repo's existing
moto-mocked S3 tests (tests/test_ingestion.py), just without a third-party
Kafka mocking library, since dependency injection makes one unnecessary here.
"""
import time
from pathlib import Path

import boto3
import duckdb
import pandas as pd
import pytest
from moto import mock_aws

import streaming.consumer as consumer
from streaming.producer import stream_file
from warehouse.setup_duckdb import initialise


# --------------------------------------------------------------------------- #
# Fakes                                                                       #
# --------------------------------------------------------------------------- #

class FakeMessage:
    def __init__(self, value):
        self.value = value


class FakeKafkaProducer:
    """Records every send() call instead of talking to a broker."""

    def __init__(self):
        self.sent: list[tuple] = []
        self.flushed = False
        self.closed = False

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


class FakeKafkaConsumer:
    """
    In-memory stand-in for kafka.KafkaConsumer. poll() honors max_records the
    same way the real client does, so tests can exercise multi-poll
    micro-batching without a broker.
    """

    def __init__(self, records: list[dict]):
        self._queue = [FakeMessage(r) for r in records]
        self.commit_count = 0
        self.closed = False

    def poll(self, timeout_ms=1000, max_records=500):
        if not self._queue:
            return {}
        batch, self._queue = self._queue[:max_records], self._queue[max_records:]
        return {"fake-topic-partition-0": batch}

    def commit(self):
        self.commit_count += 1

    def close(self):
        self.closed = True


# --------------------------------------------------------------------------- #
# Fixtures                                                                    #
# --------------------------------------------------------------------------- #

GOOD_CRIME_TYPES = ["Burglary", "Anti-social behaviour", "Shoplifting", "Vehicle crime"]


def _make_records(n: int = 20, force: str = "west-yorkshire", month: str = "2026-02", bad_field: dict = None) -> list[dict]:
    records = [
        {
            "Crime ID": f"id-{i}",
            "Month": month,
            "Reported by": "West Yorkshire Police",
            "Falls within": "West Yorkshire Police",
            "Longitude": "-1.87",
            "Latitude": "53.94",
            "Location": "On or near Test Street",
            "LSOA code": "E01010646",
            "LSOA name": "Bradford 001A",
            "Crime type": GOOD_CRIME_TYPES[i % len(GOOD_CRIME_TYPES)],
            "Last outcome category": "Under investigation",
            "Context": "",
            "_force": force,
        }
        for i in range(n)
    ]
    if bad_field:
        records[0].update(bad_field)
    return records


@pytest.fixture
def isolated_consumer_env(tmp_path, monkeypatch):
    """Point every module-level path/env the consumer touches at tmp_path, and disable S3 by default."""
    monkeypatch.setattr("warehouse.setup_duckdb.DUCKDB_PATH", str(tmp_path / "test.duckdb"))
    monkeypatch.setattr("streaming.consumer.STAGING_DIR", tmp_path / "raw")
    monkeypatch.setattr("streaming.consumer.REJECTED_DIR", tmp_path / "rejected")
    monkeypatch.delenv("S3_BUCKET_NAME", raising=False)
    return tmp_path


def _row_count(db_path: Path) -> int:
    con = duckdb.connect(str(db_path))
    initialise(con)
    count = con.execute("SELECT COUNT(*) FROM raw.crimes").fetchone()[0]
    con.close()
    return count


# --------------------------------------------------------------------------- #
# Producer tests                                                              #
# --------------------------------------------------------------------------- #

class TestProducer:
    def test_stream_file_sends_one_message_per_row(self, sample_csv):
        producer = FakeKafkaProducer()
        sent = stream_file(sample_csv, "west-yorkshire", producer=producer, delay_seconds=0)
        assert sent == 3
        assert len(producer.sent) == 3
        assert producer.flushed

    def test_message_carries_raw_columns_plus_force(self, sample_csv):
        producer = FakeKafkaProducer()
        stream_file(sample_csv, "west-yorkshire", producer=producer, delay_seconds=0)
        topic, key, value = producer.sent[0]
        assert value["_force"] == "west-yorkshire"
        assert value["Crime type"] == "Burglary"
        assert key == "abc123"

    def test_asb_row_with_no_crime_id_has_no_message_key(self, sample_csv):
        producer = FakeKafkaProducer()
        stream_file(sample_csv, "west-yorkshire", producer=producer, delay_seconds=0)
        # SAMPLE_ROWS[1] in conftest.py is the ASB row with an empty Crime ID
        _, key, value = producer.sent[1]
        assert value["Crime type"] == "Anti-social behaviour"
        assert key is None

    def test_delay_is_applied_between_sends(self, sample_csv, monkeypatch):
        sleeps = []
        monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
        producer = FakeKafkaProducer()
        stream_file(sample_csv, "west-yorkshire", producer=producer, delay_seconds=0.05)
        assert sleeps == [0.05, 0.05, 0.05]

    def test_injected_producer_is_not_closed_by_caller(self, sample_csv):
        """stream_file only closes producers it created itself — same convention as
        streaming.consumer.run_consumer not closing an injected consumer."""
        producer = FakeKafkaProducer()
        stream_file(sample_csv, "west-yorkshire", producer=producer, delay_seconds=0)
        assert producer.closed is False


# --------------------------------------------------------------------------- #
# Consumer: micro-batch validate + write/reject                              #
# --------------------------------------------------------------------------- #

class TestFlushBatch:
    def test_valid_batch_is_written_to_duckdb(self, isolated_consumer_env):
        records = _make_records(20)
        success = consumer._flush_batch(records, gx_root=isolated_consumer_env / "great_expectations")
        assert success is True
        assert _row_count(isolated_consumer_env / "test.duckdb") == 20

    def test_invalid_batch_is_rejected_not_written(self, isolated_consumer_env):
        records = _make_records(20, bad_field={"Crime type": "Not A Real Type"})
        success = consumer._flush_batch(records, gx_root=isolated_consumer_env / "great_expectations")
        assert success is False
        assert _row_count(isolated_consumer_env / "test.duckdb") == 0

    def test_rejected_batch_is_routed_to_rejected_dir_not_dropped(self, isolated_consumer_env):
        records = _make_records(20, bad_field={"Month": None})
        success = consumer._flush_batch(records, gx_root=isolated_consumer_env / "great_expectations")
        assert success is False

        rejected_files = list((isolated_consumer_env / "rejected").glob("*.csv"))
        assert len(rejected_files) == 1
        rejected_df = pd.read_csv(rejected_files[0])
        assert len(rejected_df) == 20  # whole micro-batch quarantined, not dropped

    def test_valid_batch_skips_s3_when_bucket_not_configured(self, isolated_consumer_env):
        """Mirrors Quick Start's 'no AWS needed to start' — S3 upload is best-effort.
        S3_BUCKET_NAME is unset (isolated_consumer_env fixture), so this must not
        raise even though it never touches AWS — only the DuckDB write is required."""
        records = _make_records(20)
        success = consumer._flush_batch(records, gx_root=isolated_consumer_env / "great_expectations")
        assert success is True
        assert _row_count(isolated_consumer_env / "test.duckdb") == 20


@mock_aws
class TestFlushBatchWithS3:
    BUCKET = "test-crime-bucket"

    def _setup_bucket(self):
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(Bucket=self.BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        return s3

    def test_valid_batch_uploads_to_s3_under_hive_partition_with_stream_suffix(self, isolated_consumer_env, monkeypatch):
        monkeypatch.setenv("S3_BUCKET_NAME", self.BUCKET)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        s3 = self._setup_bucket()

        records = _make_records(20)
        success = consumer._flush_batch(records, gx_root=isolated_consumer_env / "great_expectations")
        assert success is True

        objects = s3.list_objects_v2(
            Bucket=self.BUCKET, Prefix="crime/year=2026/month=02/force=west-yorkshire/"
        )
        keys = [o["Key"] for o in objects.get("Contents", [])]
        assert len(keys) == 1
        assert "-street-stream-" in keys[0]  # distinguishes from the batch path's monthly file


# --------------------------------------------------------------------------- #
# Consumer: full run loop, batching + offset commit                          #
# --------------------------------------------------------------------------- #

class TestRunConsumer:
    def test_flushes_in_batch_size_chunks_and_commits_each_time(self, isolated_consumer_env, monkeypatch):
        monkeypatch.setattr("streaming.consumer.STREAMING_BATCH_SIZE", 10)
        monkeypatch.setattr("streaming.consumer.STREAMING_FLUSH_INTERVAL_SECONDS", 9999)

        records = _make_records(25)
        fake_consumer = FakeKafkaConsumer(records)

        stats = consumer.run_consumer(
            consumer=fake_consumer, max_messages=25, gx_root=isolated_consumer_env / "great_expectations"
        )

        assert stats["processed"] == 25
        assert stats["written"] == 25
        assert stats["rejected"] == 0
        # 10 + 10 + 5 (final partial flush on loop exit) = 3 batches/commits
        assert stats["batches"] == 3
        assert fake_consumer.commit_count == 3
        assert _row_count(isolated_consumer_env / "test.duckdb") == 25

    def test_injected_consumer_is_not_closed_by_run_consumer(self, isolated_consumer_env, monkeypatch):
        monkeypatch.setattr("streaming.consumer.STREAMING_BATCH_SIZE", 10)
        fake_consumer = FakeKafkaConsumer(_make_records(5))
        consumer.run_consumer(
            consumer=fake_consumer, max_messages=5, gx_root=isolated_consumer_env / "great_expectations"
        )
        assert fake_consumer.closed is False

    def test_mixed_valid_and_invalid_micro_batches(self, isolated_consumer_env, monkeypatch):
        """First 10 records are clean, next 10 share one bad crime_type — two
        separate micro-batches, one written, one rejected."""
        monkeypatch.setattr("streaming.consumer.STREAMING_BATCH_SIZE", 10)
        monkeypatch.setattr("streaming.consumer.STREAMING_FLUSH_INTERVAL_SECONDS", 9999)

        good = _make_records(10, force="west-yorkshire")
        bad = _make_records(10, force="west-yorkshire", bad_field={"Crime type": "Not A Real Type"})
        fake_consumer = FakeKafkaConsumer(good + bad)

        stats = consumer.run_consumer(
            consumer=fake_consumer, max_messages=20, gx_root=isolated_consumer_env / "great_expectations"
        )

        assert stats["written"] == 10
        assert stats["rejected"] == 10
        assert fake_consumer.commit_count == 2
        assert _row_count(isolated_consumer_env / "test.duckdb") == 10
        assert len(list((isolated_consumer_env / "rejected").glob("*.csv"))) == 1
