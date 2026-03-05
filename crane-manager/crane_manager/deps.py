"""Shared dependencies for crane-manager API."""

from crane_shared import RedisClient

_redis: RedisClient | None = None


def get_redis() -> RedisClient:
    global _redis
    if _redis is None:
        _redis = RedisClient.from_env()
    return _redis
