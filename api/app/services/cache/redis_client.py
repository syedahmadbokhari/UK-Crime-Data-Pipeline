"""Redis connection manager with graceful degradation.

If Redis is unavailable or REDIS_URL is not set, all cache operations
are silently skipped — the application continues to work without caching.
"""
import logging

from app.config import settings

logger = logging.getLogger(__name__)

_client = None


def get_redis():
    """Return a connected Redis client, or None if unavailable."""
    global _client

    if not settings.redis_url:
        return None

    if _client is not None:
        return _client

    try:
        import redis
        client = redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        client.ping()
        _client = client
        logger.info("Redis connected: %s", settings.redis_url)
    except Exception as exc:
        logger.warning("Redis unavailable — caching disabled: %s", exc)
        _client = None

    return _client
