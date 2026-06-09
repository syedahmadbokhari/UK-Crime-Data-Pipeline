"""Enhanced health check — liveness + readiness with service-level status."""
import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from app.config import settings

router = APIRouter(tags=["health"])
logger = logging.getLogger(__name__)

_start_time = time.time()


async def _check_database() -> str:
    try:
        from app.database import AsyncSessionLocal
        async with AsyncSessionLocal() as db:
            await db.execute(text("SELECT 1"))
        return "healthy"
    except Exception as exc:
        logger.warning("DB health check failed: %s", exc)
        return "unhealthy"


async def _check_redis() -> str:
    if not settings.redis_url:
        return "not_configured"
    try:
        import redis.asyncio as aioredis
        client = aioredis.from_url(settings.redis_url, socket_connect_timeout=1)
        await client.ping()
        await client.aclose()
        return "healthy"
    except Exception:
        return "unhealthy"


@router.get("/health", summary="Health Check (liveness)")
async def health_check():
    """Fast liveness probe — no external calls."""
    return {"status": "ok", "service": "crime-data-api"}


@router.get("/health/ready", summary="Readiness Check")
async def readiness_check():
    """Readiness probe — verifies all downstream services."""
    db_status = await _check_database()
    redis_status = await _check_redis()
    gemini_status = "configured" if settings.gemini_api_key else "not_configured"

    overall = "healthy" if db_status == "healthy" else "degraded"

    return {
        "status": overall,
        "version": settings.app_version,
        "uptime_seconds": round(time.time() - _start_time, 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": {
            "database": db_status,
            "redis": redis_status,
            "gemini": gemini_status,
        },
    }
