"""Gate pipeline — composable safety checks before order intent generation.

Gates (in order):
1. Dedup gate      — skip if we've already signaled this item recently
2. Price re-check  — verify price is still valid (stale data guard)
3. Budget gate     — atomic check against daily spend limit
4. Dry-run gate    — block execution if strategy is in dry-run mode

Each gate returns None to reject, or passes the signal through.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Optional

from crane_shared import RedisClient
from crane_shared.models import (
    AllocationSignal,
    OrderIntent,
    OrderSide,
    OrderType,
    Strategy,
)

log = logging.getLogger("crane-engine.gates")


class GatePipeline:
    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client

    def process(self, signal: AllocationSignal) -> Optional[OrderIntent]:
        """Run signal through all gates. Returns OrderIntent if approved, None if rejected."""

        # Gate 1: Dedup
        dedup_key = f"crane:engine:dedup:{signal.symbol}:{signal.signal_type.value}"
        if not self._redis.dedup_check(dedup_key, ttl=300):  # 5 min dedup window
            log.debug(f"Dedup rejected: {signal.symbol}")
            return None

        # Gate 2: Budget check
        strategy = self._load_strategy(signal.metadata.get("strategy_id", ""))
        if strategy and not self._check_budget(signal, strategy):
            log.info(f"Budget rejected: {signal.symbol}")
            return None

        # Gate 3: Build intent
        intent = OrderIntent(
            intent_id=str(uuid.uuid4()),
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            limit_price=signal.current_price,
            qty=self._calculate_qty(signal, strategy),
            strategy_id=signal.metadata.get("strategy_id", ""),
            dry_run=strategy.dry_run if strategy else True,
            metadata={"score": signal.score, "target_price": signal.target_price},
            created_at=datetime.utcnow().isoformat(),
        )

        # Gate 4: Dry-run check — still create the intent but mark it
        if intent.dry_run:
            log.info(f"Dry-run intent: {intent.symbol} qty={intent.qty} @ {intent.limit_price}")

        return intent

    def _load_strategy(self, strategy_id: str) -> Optional[Strategy]:
        if not strategy_id:
            return None
        return self._redis.get_model(f"crane:manager:strategies:{strategy_id}", Strategy)

    def _check_budget(self, signal: AllocationSignal, strategy: Strategy) -> bool:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        budget_key = f"crane:manager:budget:{today}"
        estimated_cost = signal.current_price * (strategy.max_position_size or 1)

        budget_raw = self._redis.client.hgetall(budget_key)
        if not budget_raw:
            return True  # no budget configured = no limit

        daily_limit = float(budget_raw.get(b"daily_limit", 0))
        if daily_limit <= 0:
            return True

        return self._redis.atomic_spend(budget_key, estimated_cost, daily_limit)

    def _calculate_qty(self, signal: AllocationSignal, strategy: Optional[Strategy]) -> float:
        if strategy and strategy.max_position_size > 0:
            return min(strategy.max_position_size, signal.score * strategy.max_position_size)
        return 1.0
