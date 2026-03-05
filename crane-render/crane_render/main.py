"""Entry point for crane-render.

Reads events from Redis Streams and periodically writes Parquet snapshots.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from crane_shared import RedisClient

from crane_render.config import RenderConfig
from crane_render.reader.stream_reader import StreamReader
from crane_render.reader.cursor_store import CursorStore
from crane_render.writer.parquet_writer import ParquetWriter
from crane_render.compaction.compactor import Compactor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
log = logging.getLogger("crane-render")

TOPICS = [
    "crane:events:raw_quotes",
    "crane:events:raw_options",
    "crane:events:raw_listings",
]


def main():
    log.info("Starting crane-render")
    config = RenderConfig()

    redis_client = RedisClient.from_env()
    if not redis_client.ping():
        log.warning("Redis not reachable — cannot start render service")
        return

    cursor_store = CursorStore(redis_client, key=config.cursor_prefix)
    cursors = cursor_store.get_all(TOPICS)
    log.info(f"Loaded cursors: {cursors}")

    reader = StreamReader(
        redis_client, TOPICS, cursors,
        batch_size=config.batch_size,
        block_ms=config.block_ms,
    )
    writer = ParquetWriter(config.output_dir)
    compactor = Compactor(config.output_dir)

    buffer: list = []
    buffer_lock = threading.Lock()

    def reader_loop():
        while True:
            try:
                events = reader.read_batch()
                if events:
                    with buffer_lock:
                        buffer.extend(events)
            except Exception as e:
                log.error(f"Read error: {e}")
                time.sleep(5)

    t_reader = threading.Thread(target=reader_loop, name="stream-reader", daemon=True)
    t_reader.start()

    log.info(f"Render running — flush every {config.flush_interval_s}s, output={config.output_dir}")

    last_compact_date = None

    try:
        while True:
            time.sleep(config.flush_interval_s)

            with buffer_lock:
                to_flush = list(buffer)
                buffer.clear()

            if to_flush:
                try:
                    counts = writer.flush(to_flush)
                    cursor_store.save_batch(reader.cursors)
                    total = sum(counts.values())
                    log.info(f"Flushed {total} events across {len(counts)} topics")
                except Exception as e:
                    log.error(f"Flush error: {e}")
                    with buffer_lock:
                        buffer[:0] = to_flush

            if config.auto_compact:
                now = datetime.now(timezone.utc)
                if now.hour == config.compact_hour_utc:
                    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                    if last_compact_date != yesterday:
                        log.info(f"Running EOD compaction for {yesterday}")
                        try:
                            results = compactor.compact_all(yesterday)
                            last_compact_date = yesterday
                            log.info(f"Compaction complete: {results}")
                        except Exception as e:
                            log.error(f"Compaction error: {e}")

    except KeyboardInterrupt:
        log.info("Shutting down crane-render")


if __name__ == "__main__":
    main()
