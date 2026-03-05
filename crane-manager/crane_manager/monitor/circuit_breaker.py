"""Circuit breaker for order execution.

Three states:
    CLOSED  — normal operation, orders flow through
    OPEN    — halted, no orders pass (triggered by N consecutive failures)
    HALF    — testing recovery, allows one order through

State is persisted in Redis so all service instances share it.
"""

from __future__ import annotations

import logging
from enum import Enum

from crane_shared import RedisClient

log = logging.getLogger("crane-manager.circuit-breaker")

CB_KEY = "crane:manager:circuit_breaker"
FAILURE_THRESHOLD = 3


class CBState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client

    def state(self) -> dict:
        raw = self._redis.client.hgetall(CB_KEY)
        if not raw:
            return {"state": CBState.CLOSED.value, "failures": 0}
        return {
            "state": raw.get(b"state", b"closed").decode(),
            "failures": int(raw.get(b"failures", b"0")),
        }

    def record_success(self) -> None:
        self._redis.client.hset(CB_KEY, mapping={"state": CBState.CLOSED.value, "failures": "0"})

    def record_failure(self) -> None:
        failures = self._redis.client.hincrby(CB_KEY, "failures", 1)
        if failures >= FAILURE_THRESHOLD:
            self._redis.client.hset(CB_KEY, "state", CBState.OPEN.value)
            log.warning(f"Circuit breaker OPEN after {failures} failures")

    def is_open(self) -> bool:
        s = self.state()
        return s["state"] == CBState.OPEN.value

    def reset(self) -> None:
        self._redis.client.hset(CB_KEY, mapping={"state": CBState.CLOSED.value, "failures": "0"})

    def half_open(self) -> None:
        self._redis.client.hset(CB_KEY, "state", CBState.HALF_OPEN.value)
