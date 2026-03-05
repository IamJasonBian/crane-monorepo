"""Intent publisher — writes approved order intents to Redis and event bus.

The order_intents topic uses a single consumer guarantee (like the reference
architecture's single-partition purchase.intents topic) to ensure total
ordering and prevent race conditions.
"""

from __future__ import annotations

import logging

from crane_shared import RedisClient, EventBus, OrderIntent
from crane_shared.events import TOPIC_ORDER_INTENTS

log = logging.getLogger("crane-engine.publisher")


class IntentPublisher:
    def __init__(self, redis_client: RedisClient, event_bus: EventBus):
        self._redis = redis_client
        self._bus = event_bus

    def publish(self, intent: OrderIntent) -> None:
        # Acquire lock on symbol to ensure single concurrent intent per symbol
        lock_key = f"crane:engine:lock:{intent.symbol}"
        if not self._redis.acquire_lock(lock_key, ttl=30):
            log.warning(f"Lock contention on {intent.symbol}, skipping")
            return

        try:
            # Write to Redis for state tracking
            self._redis.put_model(
                f"crane:engine:intents:{intent.intent_id}",
                intent,
                ttl=86400,
            )
            self._redis.add_to_index("crane:engine:intents:index", intent.intent_id)

            # Publish to event bus (single-consumer topic)
            self._bus.publish_model(TOPIC_ORDER_INTENTS, "order_intent", intent)

        finally:
            self._redis.release_lock(lock_key)
