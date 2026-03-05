"""Cursor persistence for Redis stream consumption.

Stores last-read entry IDs in a Redis hash so the render service can
resume after restart without re-reading or missing events.

Redis key: crane:render:cursors (hash: topic -> last_entry_id)
"""

from __future__ import annotations

from crane_shared.redis_client import RedisClient

DEFAULT_CURSOR = "0-0"


class CursorStore:
    def __init__(self, redis_client: RedisClient, key: str = "crane:render:cursors"):
        self._redis = redis_client.client
        self._key = key

    def get(self, topic: str) -> str:
        raw = self._redis.hget(self._key, topic)
        if raw is None:
            return DEFAULT_CURSOR
        return raw.decode() if isinstance(raw, bytes) else raw

    def get_all(self, topics: list[str]) -> dict[str, str]:
        return {topic: self.get(topic) for topic in topics}

    def save(self, topic: str, entry_id: str) -> None:
        self._redis.hset(self._key, topic, entry_id)

    def save_batch(self, cursors: dict[str, str]) -> None:
        if cursors:
            self._redis.hset(self._key, mapping=cursors)
