"""Cache-aside helpers — get/set with graceful Redis failure handling."""
import json
import logging
from typing import Any, Optional

from app.services.cache.redis_client import get_redis
from app.config import settings

logger = logging.getLogger(__name__)


def get_cached(key: str) -> Optional[dict]:
    client = get_redis()
    if client is None:
        return None
    try:
        value = client.get(key)
        if value:
            logger.debug("Cache hit: %s", key)
            return json.loads(value)
    except Exception as exc:
        logger.warning("Cache get error (key=%s): %s", key, exc)
    return None


def set_cached(key: str, value: Any, ttl: Optional[int] = None) -> None:
    client = get_redis()
    if client is None:
        return
    try:
        ttl = ttl if ttl is not None else settings.cache_ttl_seconds
        client.setex(key, ttl, json.dumps(value, default=str))
        logger.debug("Cache set: %s (TTL=%ds)", key, ttl)
    except Exception as exc:
        logger.warning("Cache set error (key=%s): %s", key, exc)


def invalidate(key: str) -> None:
    client = get_redis()
    if client is None:
        return
    try:
        client.delete(key)
        logger.debug("Cache invalidated: %s", key)
    except Exception as exc:
        logger.warning("Cache invalidate error (key=%s): %s", key, exc)
