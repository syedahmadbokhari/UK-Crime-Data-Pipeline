"""Tests for POST /crimes/ask RAG endpoint."""
from unittest.mock import patch


def test_ask_requires_auth(client):
    res = client.post("/crimes/ask", json={"question": "What is the most common crime?"})
    assert res.status_code == 401


def test_ask_empty_question(client, auth_headers):
    res = client.post("/crimes/ask", json={"question": "   "}, headers=auth_headers)
    assert res.status_code == 422


def test_ask_question_too_long(client, auth_headers):
    res = client.post("/crimes/ask", json={"question": "x" * 501}, headers=auth_headers)
    assert res.status_code == 422


def test_ask_no_matching_data(client, auth_headers):
    # 2020-01 does not exist in the test database — parse_question extracts the
    # month filter, the DB returns 0 records, and the early-return path is taken.
    res = client.post(
        "/crimes/ask",
        json={"question": "What crimes happened in 2020-01?"},
        headers=auth_headers,
    )
    assert res.status_code == 200
    data = res.json()
    assert data["records_analysed"] == 0
    assert data["confidence"] == 0.0
    assert "No crime data" in data["answer"]


def test_ask_success_with_mock_gemini(client, auth_headers):
    mock_answer = "The most common crime in West Yorkshire was Burglary with 2 recorded incidents."
    with patch("app.services.rag.answer_generator.call_gemini", return_value=mock_answer):
        res = client.post(
            "/crimes/ask",
            json={"question": "What is the most common crime in West Yorkshire?"},
            headers=auth_headers,
        )
    assert res.status_code == 200
    data = res.json()
    assert data["answer"] == mock_answer
    assert isinstance(data["sources"], list)
    assert isinstance(data["confidence"], float)
    assert 0 <= data["confidence"] <= 1
    assert data["records_analysed"] > 0


def test_ask_response_schema(client, auth_headers):
    with patch("app.services.rag.answer_generator.call_gemini", return_value="Burglary was most common."):
        res = client.post(
            "/crimes/ask",
            json={"question": "What crimes happened in yorkshire?"},
            headers=auth_headers,
        )
    assert res.status_code == 200
    data = res.json()
    assert "answer" in data
    assert "sources" in data
    assert "confidence" in data
    assert "records_analysed" in data


def test_ask_gemini_error(client, auth_headers):
    with patch("app.services.rag.answer_generator.call_gemini", side_effect=RuntimeError("API error")):
        res = client.post(
            "/crimes/ask",
            json={"question": "What crimes happened in West Yorkshire?"},
            headers=auth_headers,
        )
    assert res.status_code == 502
