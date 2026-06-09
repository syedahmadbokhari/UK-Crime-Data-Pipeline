"""Confirm rate-limited endpoints are accessible under the limit."""


def test_crimes_list_accessible(client):
    res = client.get("/api/v1/crimes/")
    assert res.status_code == 200


def test_crimes_cursor_accessible(client):
    res = client.get("/api/v1/crimes/cursor")
    assert res.status_code == 200


def test_crimes_summary_accessible(client, auth_headers):
    res = client.get("/api/v1/crimes/summary?force=West Yorkshire Police", headers=auth_headers)
    assert res.status_code == 200


def test_crimes_by_id_accessible(client):
    res = client.get("/api/v1/crimes/1")
    assert res.status_code == 200
