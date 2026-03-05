"""Event bus abstraction for inter-service communication via Redis streams.

Uses Redis Streams (XADD/XREAD) as a lightweight Redpanda stand-in.
When we move to Redpanda/Kafka later, only this module changes.

Stream topics:
    crane:events:raw_quotes      — raw market quotes from feed
    crane:events:raw_options      — raw options data from feed
    crane:events:signals          — allocation signals from engine
    crane:events:order_intents    — order intents from engine (single consumer)
    crane:events:order_results    — execution results
    crane:events:config_updates   — config changes from manager
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional, Callable

from pydantic import BaseModel

from crane_shared.redis_client import RedisClient


# Stream topic constants
TOPIC_RAW_QUOTES = "crane:events:raw_quotes"
TOPIC_RAW_OPTIONS = "crane:events:raw_options"
TOPIC_SIGNALS = "crane:events:signals"
TOPIC_ORDER_INTENTS = "crane:events:order_intents"
TOPIC_ORDER_RESULTS = "crane:events:order_results"
TOPIC_CONFIG_UPDATES = "crane:events:config_updates"


@dataclass
class Event:
    """A single event from a Redis stream."""

    stream: str
    event_id: str
    event_type: str
    payload: dict
    raw_id: str = ""  # Redis stream ID e.g. "1234567890-0"


class EventBus:
    """Publish/subscribe on Redis Streams.

    This is the inter-service communication layer. Each service publishes
    events to topics and subscribes to topics it cares about.

    Usage:
        bus = EventBus(redis_client)

        # Publish
        bus.publish(TOPIC_RAW_QUOTES, "quote", quote.model_dump())

        # Subscribe (blocking consumer)
        for event in bus.subscribe(TOPIC_SIGNALS):
            handle(event)
    """

    def __init__(self, redis_client: RedisClient, max_len: int = 10000):
        self._redis = redis_client.client
        self._max_len = max_len

    def publish(self, topic: str, event_type: str, payload: dict) -> str:
        """Publish an event. Returns the stream entry ID."""
        data = {
            "type": event_type,
            "payload": json.dumps(payload),
        }
        entry_id = self._redis.xadd(topic, data, maxlen=self._max_len)
        return entry_id.decode() if isinstance(entry_id, bytes) else entry_id

    def publish_model(self, topic: str, event_type: str, model: BaseModel) -> str:
        return self.publish(topic, event_type, model.model_dump())

    def subscribe(
        self,
        *topics: str,
        group: Optional[str] = None,
        consumer: str = "default",
        last_id: str = "$",
        block_ms: int = 5000,
        batch_size: int = 100,
    ):
        """Yield events from one or more topics.

        If group is set, uses consumer groups (at-most-once with ack).
        Otherwise uses plain XREAD (at-least-once, no ack needed).
        """
        streams = {t: last_id for t in topics}

        if group:
            for topic in topics:
                try:
                    self._redis.xgroup_create(topic, group, id="0", mkstream=True)
                except Exception:
                    pass  # group already exists
            streams = {t: ">" for t in topics}

        while True:
            if group:
                results = self._redis.xreadgroup(
                    group, consumer, streams, count=batch_size, block=block_ms,
                )
            else:
                results = self._redis.xread(streams, count=batch_size, block=block_ms)

            if not results:
                continue

            for stream_name, entries in results:
                stream_str = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                for entry_id, data in entries:
                    eid = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                    etype = data.get(b"type", b"unknown").decode()
                    raw_payload = data.get(b"payload", b"{}").decode()

                    yield Event(
                        stream=stream_str,
                        event_id=eid,
                        event_type=etype,
                        payload=json.loads(raw_payload),
                        raw_id=eid,
                    )

                    # Update cursor for plain XREAD
                    if not group:
                        streams[stream_str] = eid

                    # Ack for consumer groups
                    if group:
                        self._redis.xack(stream_str, group, eid)

    def read_latest(self, topic: str, count: int = 1) -> list[Event]:
        """Read the latest N events from a topic (non-blocking)."""
        entries = self._redis.xrevrange(topic, count=count)
        events = []
        for entry_id, data in entries:
            eid = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
            etype = data.get(b"type", b"unknown").decode()
            raw_payload = data.get(b"payload", b"{}").decode()
            events.append(Event(
                stream=topic,
                event_id=eid,
                event_type=etype,
                payload=json.loads(raw_payload),
                raw_id=eid,
            ))
        return events
