def test_register_success(client):
    res = client.post("/auth/register", json={"email": "new@test.com", "password": "securepass"})
    assert res.status_code == 201
    assert res.json()["email"] == "new@test.com"


def test_register_duplicate_email(client):
    client.post("/auth/register", json={"email": "dup@test.com", "password": "pass"})
    res = client.post("/auth/register", json={"email": "dup@test.com", "password": "pass"})
    assert res.status_code == 400
    assert "already registered" in res.json()["detail"]


def test_login_success(client):
    client.post("/auth/register", json={"email": "login@test.com", "password": "password123"})
    res = client.post("/auth/login", json={"email": "login@test.com", "password": "password123"})
    assert res.status_code == 200
    assert "access_token" in res.json()
    assert res.json()["token_type"] == "bearer"


def test_login_wrong_password(client):
    client.post("/auth/register", json={"email": "fail@test.com", "password": "correct"})
    res = client.post("/auth/login", json={"email": "fail@test.com", "password": "wrong"})
    assert res.status_code == 401


def test_login_unknown_email(client):
    res = client.post("/auth/login", json={"email": "ghost@test.com", "password": "any"})
    assert res.status_code == 401


def test_protected_route_no_token(client):
    res = client.get("/crimes/summary?force=West Yorkshire Police")
    assert res.status_code == 401


def test_protected_route_invalid_token(client):
    res = client.get(
        "/crimes/summary?force=West Yorkshire Police",
        headers={"Authorization": "Bearer not-a-valid-token"},
    )
    assert res.status_code == 401
