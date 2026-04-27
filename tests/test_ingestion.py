"""
Tests for the ingestion layer.
Mocks S3 with moto so no real AWS credentials are needed.
"""
import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import boto3
import pytest
from moto import mock_aws

# Make sure project root is importable
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.watermark import get_watermark, set_watermark, months_since


# --------------------------------------------------------------------------- #
# Watermark tests                                                               #
# --------------------------------------------------------------------------- #

class TestWatermark:
    def test_get_watermark_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WATERMARK_FILE", str(tmp_path / "watermark.json"))
        assert get_watermark("west-yorkshire") is None

    def test_set_and_get_roundtrip(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WATERMARK_FILE", str(tmp_path / "watermark.json"))
        set_watermark("west-yorkshire", "2026-01")
        assert get_watermark("west-yorkshire") == "2026-01"

    def test_multiple_forces_independent(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WATERMARK_FILE", str(tmp_path / "watermark.json"))
        set_watermark("west-yorkshire", "2026-01")
        set_watermark("greater-manchester", "2025-12")
        assert get_watermark("west-yorkshire") == "2026-01"
        assert get_watermark("greater-manchester") == "2025-12"

    def test_overwrite_watermark(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WATERMARK_FILE", str(tmp_path / "watermark.json"))
        set_watermark("west-yorkshire", "2025-11")
        set_watermark("west-yorkshire", "2025-12")
        assert get_watermark("west-yorkshire") == "2025-12"

    def test_months_since_returns_correct_range(self):
        result = months_since("2025-10", "2026-02")
        assert result == ["2025-11", "2025-12", "2026-01", "2026-02"]

    def test_months_since_empty_when_already_current(self):
        result = months_since("2026-02", "2026-02")
        assert result == []


# --------------------------------------------------------------------------- #
# S3 upload tests (mocked)                                                     #
# --------------------------------------------------------------------------- #

@mock_aws
class TestS3Upload:
    BUCKET = "test-crime-bucket"

    def _setup_bucket(self):
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket=self.BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        return s3

    def test_upload_file_creates_correct_key(self, sample_csv, monkeypatch):
        monkeypatch.setenv("S3_BUCKET_NAME", self.BUCKET)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

        self._setup_bucket()

        from ingestion.upload_to_s3 import upload_file
        uri = upload_file(sample_csv, "west-yorkshire", "2026-02")

        assert uri is not None
        assert "year=2026/month=02/force=west-yorkshire" in uri

    def test_upload_is_idempotent(self, sample_csv, monkeypatch):
        monkeypatch.setenv("S3_BUCKET_NAME", self.BUCKET)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

        self._setup_bucket()

        from ingestion.upload_to_s3 import upload_file
        uri1 = upload_file(sample_csv, "west-yorkshire", "2026-02")
        uri2 = upload_file(sample_csv, "west-yorkshire", "2026-02")
        assert uri1 == uri2

    def test_upload_fails_gracefully_without_bucket(self, sample_csv, monkeypatch):
        monkeypatch.setenv("S3_BUCKET_NAME", "")
        from ingestion.upload_to_s3 import upload_file
        with pytest.raises(EnvironmentError, match="S3_BUCKET_NAME"):
            upload_file(sample_csv, "west-yorkshire", "2026-02")
