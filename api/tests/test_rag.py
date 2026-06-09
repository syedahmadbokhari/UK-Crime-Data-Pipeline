"""Tests for POST /api/v1/crimes/ask RAG endpoint."""
from unittest.mock import patch


def test_ask_requires_auth(client):
    res = client.post("/api/v1/crimes/ask", json={"question": "What is the most common crime?"})
    assert res.status_code == 401


def test_ask_empty_question(client, auth_headers):
    res = client.post("/api/v1/crimes/ask", json={"question": "   "}, headers=auth_headers)
    assert res.status_code == 422


def test_ask_question_too_long(client, auth_headers):
    res = client.post("/api/v1/crimes/ask", json={"question": "x" * 501}, headers=auth_headers)
    assert res.status_code == 422


def test_ask_no_matching_data(client, auth_headers):
    res = client.post("/api/v1/crimes/ask", json={"question": "What crimes happened in 2020-01?"}, headers=auth_headers)
    assert res.status_code == 200
    data = res.json()
    assert data["records_analysed"] == 0
    assert data["confidence"] == 0.0
    assert "No crime data" in data["answer"]


def test_ask_success(client, auth_headers):
    with patch("app.services.rag.answer_generator.call_gemini", return_value="Burglary was most common."):
        res = client.post("/api/v1/crimes/ask", json={"question": "What crimes in West Yorkshire?"}, headers=auth_headers)
    assert res.status_code == 200
    data = res.json()
    assert data["answer"] == "Burglary was most common."
    assert isinstance(data["sources"], list)
    assert 0 <= data["confidence"] <= 1
    assert data["records_analysed"] > 0


def test_ask_gemini_error(client, auth_headers):
    with patch("app.services.rag.answer_generator.call_gemini", side_effect=RuntimeError("API error")):
        res = client.post("/api/v1/crimes/ask", json={"question": "What crimes in West Yorkshire?"}, headers=auth_headers)
    assert res.status_code == 502
