"""Redis client wrapper with typed accessors for Crane models.

Provides a single connection interface used by all three services.
Key namespace conventions:
    crane:feed:*        — market data written by crane-feed
    crane:engine:*      — signals and intents written by crane-engine
    crane:manager:*     — config and state written by crane-manager
    options:{symbol}    — backward-compatible OptionsRecord storage
"""

from __future__ import annotations

import json
import os
from typing import Optional, TypeVar

import redis
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

# Key prefixes
FEED_PREFIX = "crane:feed"
ENGINE_PREFIX = "crane:engine"
MANAGER_PREFIX = "crane:manager"


class RedisClient:
    """Thin wrapper around redis-py with Crane-specific helpers."""

    def __init__(self, host: str, port: int = 6379, password: Optional[str] = None, db: int = 0):
        self._pool = redis.ConnectionPool(host=host, port=port, password=password, db=db)
        self._client = redis.Redis(connection_pool=self._pool)

    @classmethod
    def from_env(cls, host_var: str = "REDIS_HOST", password_var: str = "REDIS_PASSWORD",
                 port_var: str = "REDIS_PORT") -> RedisClient:
        host = os.environ.get(host_var, "localhost")
        password = os.environ.get(password_var)
        port = int(os.environ.get(port_var, "6379"))
        return cls(host=host, port=port, password=password)

    @property
    def client(self) -> redis.Redis:
        return self._client

    def ping(self) -> bool:
        try:
            return self._client.ping()
        except redis.ConnectionError:
            return False

    # ── Generic model helpers ────────────────────────────────────────────

    def put_model(self, key: str, model: BaseModel, ttl: Optional[int] = None) -> None:
        data = model.model_dump_json()
        if ttl:
            self._client.setex(key, ttl, data)
        else:
            self._client.set(key, data)

    def get_model(self, key: str, model_cls: type[T]) -> Optional[T]:
        raw = self._client.get(key)
        if raw is None:
            return None
        return model_cls.model_validate_json(raw)

    def put_hash(self, key: str, model: BaseModel, ttl: Optional[int] = None) -> None:
        data = {}
        for field_name, value in model.model_dump().items():
            if isinstance(value, (dict, list)):
                data[field_name] = json.dumps(value)
            elif value is not None:
                data[field_name] = str(value)
        if data:
            self._client.hset(key, mapping=data)
            if ttl:
                self._client.expire(key, ttl)

    def get_hash(self, key: str, model_cls: type[T]) -> Optional[T]:
        raw = self._client.hgetall(key)
        if not raw:
            return None
        decoded = {k.decode(): v.decode() for k, v in raw.items()}
        return model_cls.model_validate(decoded)

    # ── List helpers (history/queues) ────────────────────────────────────

    def push(self, key: str, model: BaseModel, max_len: int = 10000) -> None:
        self._client.lpush(key, model.model_dump_json())
        self._client.ltrim(key, 0, max_len - 1)

    def pop_all(self, key: str, model_cls: type[T]) -> list[T]:
        pipe = self._client.pipeline()
        pipe.lrange(key, 0, -1)
        pipe.delete(key)
        results = pipe.execute()
        items = results[0] if results[0] else []
        return [model_cls.model_validate_json(item) for item in items]

    # ── Set helpers (indices) ────────────────────────────────────────────

    def add_to_index(self, index_key: str, *members: str) -> None:
        if members:
            self._client.sadd(index_key, *members)

    def get_index(self, index_key: str) -> set[str]:
        return {m.decode() for m in self._client.smembers(index_key)}

    # ── Atomic budget operations ─────────────────────────────────────────

    def atomic_spend(self, budget_key: str, amount: float, daily_limit: float) -> bool:
        """Atomically increment spend if within budget. Returns True if approved."""
        pipe = self._client.pipeline(transaction=True)
        pipe.hget(budget_key, "spent")
        pipe.hget(budget_key, "frozen")
        results = pipe.execute()

        current = float(results[0] or 0)
        frozen = results[1] and results[1].decode() == "True"

        if frozen or (current + amount) > daily_limit:
            return False

        self._client.hincrbyfloat(budget_key, "spent", amount)
        self._client.hincrby(budget_key, "trade_count", 1)
        return True

    # ── Idempotency / dedup ──────────────────────────────────────────────

    def dedup_check(self, key: str, ttl: int = 86400) -> bool:
        """Returns True if this is a NEW key (not seen before)."""
        return bool(self._client.set(key, "1", nx=True, ex=ttl))

    def acquire_lock(self, key: str, ttl: int = 30) -> bool:
        """Simple distributed lock via SETNX."""
        return bool(self._client.set(key, "1", nx=True, ex=ttl))

    def release_lock(self, key: str) -> None:
        self._client.delete(key)
