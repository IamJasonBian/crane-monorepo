"""Redis writer for feed data.

Writes market quotes and options records, and publishes events
to the event bus for downstream consumers (crane-engine).
"""

from __future__ import annotations

import logging

from crane_shared import RedisClient, EventBus, MarketQuote, OptionsRecord
from crane_shared.events import TOPIC_RAW_QUOTES, TOPIC_RAW_OPTIONS

log = logging.getLogger("crane-feed.writer")

# TTLs
QUOTE_TTL = 7 * 86400       # 7 days
OPTION_TTL = 7 * 86400      # 7 days
HISTORY_MAX = 10000


class FeedRedisWriter:
    def __init__(self, redis_client: RedisClient, event_bus: EventBus):
        self._redis = redis_client
        self._bus = event_bus

    def write_quote(self, quote: MarketQuote) -> None:
        key = f"crane:feed:quotes:{quote.symbol}"
        self._redis.put_model(key, quote, ttl=QUOTE_TTL)
        self._redis.push(f"crane:feed:quotes:history:{quote.symbol}", quote, max_len=HISTORY_MAX)
        self._redis.add_to_index("crane:feed:quotes:index", quote.symbol)

        # Publish to event bus
        try:
            self._bus.publish_model(TOPIC_RAW_QUOTES, "quote", quote)
        except Exception as e:
            log.debug(f"Event publish failed (non-fatal): {e}")

    def write_option(self, record: OptionsRecord) -> None:
        # Backward-compatible key (matches existing Scala writer)
        key = f"options:{record.symbol}"
        self._redis.put_hash(key, record, ttl=OPTION_TTL)

        # Also write to crane namespaced key
        crane_key = f"crane:feed:options:{record.symbol}"
        self._redis.put_model(crane_key, record, ttl=OPTION_TTL)

        # Index by underlying
        self._redis.add_to_index(f"crane:feed:options:index:{record.underlying}", record.symbol)
        self._redis.add_to_index("crane:feed:options:index:all", record.symbol)

        # Publish to event bus
        try:
            self._bus.publish_model(TOPIC_RAW_OPTIONS, "option", record)
        except Exception as e:
            log.debug(f"Event publish failed (non-fatal): {e}")
