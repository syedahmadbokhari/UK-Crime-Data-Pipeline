"""Async Redis client with graceful degradation."""
import logging

from app.config import settings

logger = logging.getLogger(__name__)

_client = None


async def get_async_redis():
    """Return an async Redis client, or None if unavailable/unconfigured."""
    global _client
    if not settings.redis_url:
        return None
    if _client is not None:
        return _client
    try:
        import redis.asyncio as aioredis
        client = aioredis.from_url(settings.redis_url, decode_responses=True, socket_connect_timeout=2, socket_timeout=2)
        await client.ping()
        _client = client
        logger.info("Redis connected: %s", settings.redis_url)
    except Exception as exc:
        logger.warning("Redis unavailable — caching disabled: %s", exc)
        _client = None
    return _client
