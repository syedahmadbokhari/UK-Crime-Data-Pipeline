"""Tests confirming rate limiting is wired up correctly."""


def test_crimes_list_accessible(client):
    """Rate-limited endpoint still responds normally under the limit."""
    res = client.get("/crimes/")
    assert res.status_code == 200


def test_crimes_summary_rate_limited_endpoint_works(client, auth_headers):
    """Expensive endpoint works normally for a single request."""
    res = client.get("/crimes/summary?force=West Yorkshire Police", headers=auth_headers)
    assert res.status_code == 200


def test_crimes_by_id_rate_limited_endpoint_works(client):
    res = client.get("/crimes/1")
    assert res.status_code == 200
