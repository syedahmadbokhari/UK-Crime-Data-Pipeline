import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.main import app
from app.database import Base, get_async_db
from app.models.crime import Crime

# ── Sync engine — table creation and test data seeding ───────────────────────
_SYNC_URL = "sqlite:///./test_crimes.db"
test_sync_engine = create_engine(_SYNC_URL, connect_args={"check_same_thread": False})
TestSyncSession = sessionmaker(autocommit=False, autoflush=False, bind=test_sync_engine)

# ── Async engine — used by route handlers via dependency override ─────────────
_ASYNC_URL = "sqlite+aiosqlite:///./test_crimes.db"
test_async_engine = create_async_engine(_ASYNC_URL)
TestAsyncSession = async_sessionmaker(test_async_engine, class_=AsyncSession, expire_on_commit=False)


async def override_get_async_db():
    async with TestAsyncSession() as session:
        yield session


@pytest.fixture
def client():
    Base.metadata.create_all(bind=test_sync_engine)
    app.dependency_overrides[get_async_db] = override_get_async_db

    db = TestSyncSession()
    db.add_all([
        Crime(month="2026-02", force="West Yorkshire Police", crime_type="Burglary",
              lsoa_code="E01010646", outcome="Under investigation", latitude=53.94, longitude=-1.87),
        Crime(month="2026-02", force="West Yorkshire Police", crime_type="Violence and sexual offences",
              lsoa_code="E01010692", outcome="Under investigation", latitude=53.92, longitude=-1.82),
        Crime(month="2026-02", force="West Yorkshire Police", crime_type="Shoplifting",
              lsoa_code="E01010696", outcome="Investigation complete; no suspect identified",
              latitude=53.91, longitude=-1.79),
        Crime(month="2026-01", force="West Yorkshire Police", crime_type="Burglary",
              outcome="Unable to prosecute suspect", latitude=53.90, longitude=-1.78),
    ])
    db.commit()
    db.close()

    with TestClient(app) as c:
        yield c

    Base.metadata.drop_all(bind=test_sync_engine)
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers(client):
    client.post("/api/v1/auth/register", json={"email": "test@test.com", "password": "password123"})
    res = client.post("/api/v1/auth/login", json={"email": "test@test.com", "password": "password123"})
    token = res.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}
