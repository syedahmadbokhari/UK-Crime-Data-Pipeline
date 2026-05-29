"""Tests for POST /reports/generate endpoint."""
from unittest.mock import patch


def test_generate_report_requires_auth(client):
    res = client.post("/reports/generate", json={"force": "West Yorkshire Police"})
    assert res.status_code == 401


def test_generate_report_unknown_force(client, auth_headers):
    res = client.post(
        "/reports/generate",
        json={"force": "Unknown Force XYZ"},
        headers=auth_headers,
    )
    assert res.status_code == 404
    assert "No crime data found" in res.json()["detail"]


def test_generate_report_invalid_date_format(client, auth_headers):
    res = client.post(
        "/reports/generate",
        json={"force": "West Yorkshire Police", "start_date": "2026-02-01"},
        headers=auth_headers,
    )
    assert res.status_code == 422


def test_generate_report_success(client, auth_headers):
    mock_report = (
        "Overview\n\nWest Yorkshire Police recorded crimes.\n\n"
        "Crime Breakdown\n\nBurglary was the top category.\n\n"
        "Implications\n\nContinued monitoring is recommended."
    )
    with patch("app.services.report_service.call_gemini", return_value=mock_report):
        res = client.post(
            "/reports/generate",
            json={"force": "West Yorkshire Police"},
            headers=auth_headers,
        )
    assert res.status_code == 200
    data = res.json()
    assert "report" in data
    assert data["total_crimes"] == 4
    assert data["force"] == "West Yorkshire Police"
    assert "generated_at" in data
    assert "period_covered" in data


def test_generate_report_with_date_range(client, auth_headers):
    mock_report = "Overview\n\nReport.\n\nCrime Breakdown\n\nBreakdown.\n\nImplications\n\nNote."
    with patch("app.services.report_service.call_gemini", return_value=mock_report):
        res = client.post(
            "/reports/generate",
            json={
                "force": "West Yorkshire Police",
                "start_date": "2026-02",
                "end_date": "2026-02",
            },
            headers=auth_headers,
        )
    assert res.status_code == 200
    assert res.json()["period_covered"] == "2026-02"


def test_generate_report_gemini_unavailable(client, auth_headers):
    with patch("app.services.report_service.call_gemini", side_effect=EnvironmentError("No API key")):
        res = client.post(
            "/reports/generate",
            json={"force": "West Yorkshire Police"},
            headers=auth_headers,
        )
    assert res.status_code == 503


def test_generate_report_gemini_error(client, auth_headers):
    with patch("app.services.report_service.call_gemini", side_effect=RuntimeError("API error")):
        res = client.post(
            "/reports/generate",
            json={"force": "West Yorkshire Police"},
            headers=auth_headers,
        )
    assert res.status_code == 502
