"""Entry point for crane-engine.

Subscribes to market data events from crane-feed, applies signal logic
and allocation gates, and publishes order intents.
"""

from __future__ import annotations

import logging
import os

from crane_shared import RedisClient, EventBus
from crane_shared.events import TOPIC_RAW_QUOTES, TOPIC_RAW_OPTIONS, TOPIC_SIGNALS, TOPIC_ORDER_INTENTS
from crane_engine.signals.price_threshold import PriceThresholdSignaler
from crane_engine.allocation.gate_pipeline import GatePipeline
from crane_engine.execution.intent_publisher import IntentPublisher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
log = logging.getLogger("crane-engine")


def main():
    log.info("Starting crane-engine")

    redis_client = RedisClient.from_env()
    event_bus = EventBus(redis_client)

    # Load strategies from Redis (written by crane-manager)
    signaler = PriceThresholdSignaler(redis_client)
    gates = GatePipeline(redis_client)
    publisher = IntentPublisher(redis_client, event_bus)

    consumer_group = os.environ.get("ENGINE_CONSUMER_GROUP", "crane-engine")

    log.info("Subscribing to feed events...")

    for event in event_bus.subscribe(
        TOPIC_RAW_QUOTES, TOPIC_RAW_OPTIONS,
        group=consumer_group,
        consumer="engine-0",
        last_id="0",
    ):
        try:
            # 1. Generate signals from raw data
            signals = signaler.evaluate(event)

            for signal in signals:
                # Publish signal to signal topic
                event_bus.publish_model(TOPIC_SIGNALS, "signal", signal)

                # 2. Run through gates (dedup, price recheck, budget)
                intent = gates.process(signal)
                if intent is None:
                    continue

                # 3. Publish order intent
                publisher.publish(intent)
                log.info(f"Intent published: {intent.intent_id} {intent.symbol} {intent.side.value}")

        except Exception as e:
            log.error(f"Processing error: {e}")


if __name__ == "__main__":
    main()
