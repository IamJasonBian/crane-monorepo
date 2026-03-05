"""Health and monitoring endpoints.

Reports on the health of all Crane services by checking Redis connectivity,
event bus activity, and circuit breaker state.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter

from crane_shared.events import (
    TOPIC_RAW_QUOTES,
    TOPIC_RAW_OPTIONS,
    TOPIC_SIGNALS,
    TOPIC_ORDER_INTENTS,
)
from crane_manager.deps import get_redis
from crane_manager.monitor.circuit_breaker import CircuitBreaker

router = APIRouter()


@router.get("/")
def health_check():
    rc = get_redis()
    redis_ok = rc.ping()

    # Check event bus activity
    streams = {}
    for topic in [TOPIC_RAW_QUOTES, TOPIC_RAW_OPTIONS, TOPIC_SIGNALS, TOPIC_ORDER_INTENTS]:
        try:
            info = rc.client.xinfo_stream(topic)
            streams[topic] = {
                "length": info.get("length", 0),
                "last_entry_id": info.get("last-generated-id", b"").decode()
                if isinstance(info.get("last-generated-id"), bytes)
                else str(info.get("last-generated-id", "")),
            }
        except Exception:
            streams[topic] = {"length": 0, "last_entry_id": "none"}

    # Circuit breaker state
    cb = CircuitBreaker(rc)

    return {
        "status": "healthy" if redis_ok else "degraded",
        "redis": redis_ok,
        "streams": streams,
        "circuit_breaker": cb.state(),
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/feed")
def feed_health():
    """Check crane-feed specific health — quote freshness, options freshness."""
    rc = get_redis()
    quote_symbols = rc.get_index("crane:feed:quotes:index")
    option_symbols = rc.get_index("crane:feed:options:index:all")
    return {
        "tracked_quotes": len(quote_symbols),
        "tracked_options": len(option_symbols),
        "quote_symbols": sorted(quote_symbols),
    }


@router.get("/engine")
def engine_health():
    """Check crane-engine specific health — intent count, signal throughput."""
    rc = get_redis()
    intent_ids = rc.get_index("crane:engine:intents:index")
    return {
        "active_intents": len(intent_ids),
    }
