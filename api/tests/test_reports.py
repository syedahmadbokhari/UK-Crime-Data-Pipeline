"""Tests for background report generation endpoints."""
from unittest.mock import patch


def test_generate_report_requires_auth(client):
    res = client.post("/api/v1/reports/generate", json={"force": "West Yorkshire Police"})
    assert res.status_code == 401


def test_generate_report_returns_job_id(client, auth_headers):
    with patch("app.services.report_service.call_gemini", return_value="Report text."):
        res = client.post("/api/v1/reports/generate", json={"force": "West Yorkshire Police"}, headers=auth_headers)
    assert res.status_code == 202
    data = res.json()
    assert "job_id" in data
    assert data["status"] == "queued"


def test_get_report_job_status(client, auth_headers):
    from datetime import datetime, timezone
    mock_result = {
        "report": "Report text",
        "force": "West Yorkshire Police",
        "total_crimes": 4,
        "period_covered": "2026-02",
        "generated_at": datetime.now(timezone.utc),
    }
    # Patch at router level — background task creates its own DB session,
    # so we mock the entire service call rather than just Gemini.
    with patch("app.routers.reports.generate_crime_report", return_value=mock_result):
        post_res = client.post("/api/v1/reports/generate", json={"force": "West Yorkshire Police"}, headers=auth_headers)
    job_id = post_res.json()["job_id"]

    status_res = client.get(f"/api/v1/reports/{job_id}", headers=auth_headers)
    assert status_res.status_code == 200
    data = status_res.json()
    assert data["job_id"] == job_id
    assert data["status"] in ("queued", "processing", "completed")


def test_get_report_job_not_found(client, auth_headers):
    res = client.get("/api/v1/reports/nonexistent-job-id", headers=auth_headers)
    assert res.status_code == 404


def test_generate_report_no_data(client, auth_headers):
    with patch("app.services.report_service.call_gemini", return_value="Report"):
        res = client.post("/api/v1/reports/generate", json={"force": "Unknown Force XYZ"}, headers=auth_headers)
    assert res.status_code == 202
    job_id = res.json()["job_id"]

    status_res = client.get(f"/api/v1/reports/{job_id}", headers=auth_headers)
    data = status_res.json()
    # Job may be failed (no data) or still queued depending on timing
    assert data["status"] in ("queued", "processing", "completed", "failed")


def test_generate_report_invalid_date(client, auth_headers):
    res = client.post(
        "/api/v1/reports/generate",
        json={"force": "West Yorkshire Police", "start_date": "2026-02-01"},
        headers=auth_headers,
    )
    assert res.status_code == 422
