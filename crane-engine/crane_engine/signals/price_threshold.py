"""Price threshold signal generator.

Compares incoming quotes against watch targets to generate
allocation signals when price crosses a configured threshold.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from crane_shared import RedisClient
from crane_shared.events import Event
from crane_shared.models import (
    AllocationSignal,
    MarketQuote,
    SignalType,
    WatchTarget,
)

log = logging.getLogger("crane-engine.signals")


class PriceThresholdSignaler:
    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client
        self._targets_cache: dict[str, WatchTarget] = {}
        self._cache_ttl = 0.0

    def evaluate(self, event: Event) -> list[AllocationSignal]:
        """Evaluate an incoming event against watch targets. Returns signals."""
        if event.event_type not in ("quote", "option"):
            return []

        symbol = event.payload.get("symbol", "")
        if not symbol:
            return []

        targets = self._load_targets()
        matching = [t for t in targets.values() if t.symbol == symbol and t.enabled]

        signals = []
        for target in matching:
            current_price = self._extract_price(event)
            if current_price <= 0:
                continue

            if target.threshold_price > 0 and current_price <= target.threshold_price:
                score = (target.threshold_price - current_price) / target.threshold_price
                signal = AllocationSignal(
                    signal_id=str(uuid.uuid4()),
                    signal_type=SignalType.PRICE_THRESHOLD,
                    symbol=symbol,
                    underlying=target.underlying,
                    score=min(score, 1.0),
                    target_price=target.threshold_price,
                    current_price=current_price,
                    metadata={"target_id": target.target_id, "strategy_id": target.strategy_id},
                    timestamp=datetime.utcnow().isoformat(),
                )
                signals.append(signal)
                log.info(f"Signal: {symbol} at {current_price} <= threshold {target.threshold_price}")

        return signals

    def _extract_price(self, event: Event) -> float:
        p = event.payload
        if "mid" in p and p["mid"] > 0:
            return p["mid"]
        if "last" in p and p["last"] > 0:
            return p["last"]
        bid = p.get("bid", 0)
        ask = p.get("ask", 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2
        return 0.0

    def _load_targets(self) -> dict[str, WatchTarget]:
        """Load watch targets from Redis (cached briefly)."""
        import time
        now = time.time()
        if now - self._cache_ttl < 10:
            return self._targets_cache

        target_ids = self._redis.get_index("crane:manager:targets:index")
        targets = {}
        for tid in target_ids:
            t = self._redis.get_model(f"crane:manager:targets:{tid}", WatchTarget)
            if t:
                targets[tid] = t
        self._targets_cache = targets
        self._cache_ttl = now
        return targets
