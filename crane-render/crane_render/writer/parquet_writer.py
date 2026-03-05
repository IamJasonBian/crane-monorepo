"""Parquet writer with Hive-style partitioning.

Writes batches of flattened events to:
    {output_dir}/{topic_short}/date=YYYY-MM-DD/hour=HH/part-{uuid}.parquet
"""

from __future__ import annotations

import logging
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from crane_render.reader.stream_reader import Event
from crane_render.writer.schema import TOPIC_SCHEMAS, flatten_event

log = logging.getLogger("crane-render.writer")


class ParquetWriter:
    def __init__(self, output_dir: str):
        self._output_dir = output_dir

    def flush(self, events: list[Event]) -> dict[str, int]:
        """Write events to Parquet files grouped by topic. Returns {topic: num_rows}."""
        if not events:
            return {}

        by_topic: dict[str, list[dict]] = defaultdict(list)
        for event in events:
            flat = flatten_event(event)
            by_topic[event.stream].append(flat)

        written = {}
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")

        for topic, rows in by_topic.items():
            schema = TOPIC_SCHEMAS.get(topic)
            if schema is None:
                log.warning(f"No schema for topic {topic}, skipping {len(rows)} events")
                continue

            topic_short = topic.rsplit(":", 1)[-1]
            part_id = uuid.uuid4().hex[:12]
            dir_path = os.path.join(
                self._output_dir, topic_short,
                f"date={date_str}", f"hour={hour_str}",
            )
            os.makedirs(dir_path, exist_ok=True)
            file_path = os.path.join(dir_path, f"part-{part_id}.parquet")

            columns = {field.name: [] for field in schema}
            for row in rows:
                for field in schema:
                    columns[field.name].append(row.get(field.name))

            table = pa.table(columns, schema=schema)
            pq.write_table(table, file_path, compression="snappy")

            written[topic] = len(rows)
            log.info(f"Wrote {len(rows)} events to {file_path}")

        return written
