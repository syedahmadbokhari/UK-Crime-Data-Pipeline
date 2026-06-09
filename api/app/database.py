"""Database engines and session factories.

Sync engine  — used exclusively by Alembic migrations and test setup.
Async engine — used by all application route handlers at runtime.
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import settings


def _async_url(url: str) -> str:
    return (
        url
        .replace("postgresql://", "postgresql+asyncpg://")
        .replace("sqlite:///", "sqlite+aiosqlite:///")
    )


# ── Sync (Alembic / test setup) ───────────────────────────────────────────────
_sync_connect_args = {"check_same_thread": False} if "sqlite" in settings.database_url else {}
engine = create_engine(settings.database_url, connect_args=_sync_connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# ── Async (app runtime) ───────────────────────────────────────────────────────
_async_connect_args = {"check_same_thread": False} if "sqlite" in settings.database_url else {}
async_engine = create_async_engine(
    _async_url(settings.database_url),
    connect_args=_async_connect_args,
)
AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


# ── Dependencies ──────────────────────────────────────────────────────────────
def get_db():
    """Sync session — Alembic only. Do not use in route handlers."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def get_async_db():
    """Async session — use in all route handlers."""
    async with AsyncSessionLocal() as session:
        yield session
