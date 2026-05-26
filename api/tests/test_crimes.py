def test_list_crimes_no_filters(client):
    res = client.get("/crimes/")
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 4
    assert len(data["results"]) == 4
    assert data["page"] == 1


def test_list_crimes_filter_by_crime_type(client):
    res = client.get("/crimes/?crime_type=Burglary")
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 2
    assert all(r["crime_type"] == "Burglary" for r in data["results"])


def test_list_crimes_filter_by_month(client):
    res = client.get("/crimes/?month=2026-01")
    assert res.status_code == 200
    assert res.json()["total"] == 1


def test_list_crimes_pagination(client):
    res = client.get("/crimes/?page=1&page_size=2")
    assert res.status_code == 200
    data = res.json()
    assert len(data["results"]) == 2
    assert data["total"] == 4
    assert data["page_size"] == 2


def test_list_crimes_page_size_limit(client):
    res = client.get("/crimes/?page_size=501")
    assert res.status_code == 422


def test_get_crime_by_id(client):
    res = client.get("/crimes/1")
    assert res.status_code == 200
    assert res.json()["id"] == 1
    assert res.json()["force"] == "West Yorkshire Police"


def test_get_crime_not_found(client):
    res = client.get("/crimes/99999")
    assert res.status_code == 404
    assert res.json()["detail"] == "Crime not found"


def test_summary_requires_auth(client):
    res = client.get("/crimes/summary?force=West Yorkshire Police")
    assert res.status_code == 401


def test_summary_with_auth(client, auth_headers):
    res = client.get(
        "/crimes/summary?force=West Yorkshire Police",
        headers=auth_headers,
    )
    assert res.status_code == 200
    data = res.json()
    assert data["force"] == "West Yorkshire Police"
    assert data["total_crimes"] == 4
    assert data["top_crime_type"] == "Burglary"
    assert isinstance(data["under_investigation_pct"], float)


def test_summary_unknown_force(client, auth_headers):
    res = client.get(
        "/crimes/summary?force=Unknown Force XYZ",
        headers=auth_headers,
    )
    assert res.status_code == 200
    assert res.json()["total_crimes"] == 0
