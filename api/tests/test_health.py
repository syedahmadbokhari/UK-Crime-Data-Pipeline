def test_health_check(client):
    res = client.get("/health")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "ok"
    assert data["service"] == "crime-data-api"


def test_readiness_check(client):
    res = client.get("/health/ready")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] in ("healthy", "degraded")
    assert "services" in data
    assert "database" in data["services"]
    assert "redis" in data["services"]
    assert "gemini" in data["services"]
    assert "uptime_seconds" in data
    assert "version" in data
