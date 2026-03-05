"""Bounded stream reader that wraps raw XREAD.

Unlike EventBus.subscribe() (infinite generator), this reader returns
control after each batch so the caller can flush to Parquet on a timer.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

log = logging.getLogger("crane-render.reader")


@dataclass
class Event:
    """A single event from a Redis stream."""
    stream: str
    event_id: str
    event_type: str
    payload: dict


class StreamReader:
    def __init__(self, redis_client, topics: list[str],
                 cursors: dict[str, str], batch_size: int = 500,
                 block_ms: int = 2000):
        self._redis = redis_client.client
        self._topics = topics
        self._cursors = dict(cursors)
        self._batch_size = batch_size
        self._block_ms = block_ms

    @property
    def cursors(self) -> dict[str, str]:
        return dict(self._cursors)

    def read_batch(self) -> list[Event]:
        """Perform one XREAD across all topics. Returns events (possibly empty)."""
        streams = {t: self._cursors[t] for t in self._topics}
        results = self._redis.xread(streams, count=self._batch_size, block=self._block_ms)

        if not results:
            return []

        events = []
        for stream_name, entries in results:
            stream_str = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
            for entry_id, data in entries:
                eid = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                etype = (data.get(b"type") or data.get("type", b"unknown"))
                if isinstance(etype, bytes):
                    etype = etype.decode()
                raw_payload = (data.get(b"payload") or data.get("payload", b"{}"))
                if isinstance(raw_payload, bytes):
                    raw_payload = raw_payload.decode()

                events.append(Event(
                    stream=stream_str,
                    event_id=eid,
                    event_type=etype,
                    payload=json.loads(raw_payload),
                ))
                self._cursors[stream_str] = eid

        return events
