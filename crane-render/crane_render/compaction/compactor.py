"""EOD compaction: merge hourly parts into a single daily Parquet file.

Reads all part-*.parquet files under a date partition, deduplicates by
_event_id, sorts chronologically, and writes date=YYYY-MM-DD/eod.parquet.
"""

from __future__ import annotations

import glob
import logging
import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

log = logging.getLogger("crane-render.compactor")


class Compactor:
    def __init__(self, output_dir: str):
        self._output_dir = output_dir

    def compact_date(self, topic_short: str, date_str: str) -> int | None:
        """Merge all parts for a topic/date into eod.parquet. Returns row count or None."""
        base = os.path.join(self._output_dir, topic_short, f"date={date_str}")
        if not os.path.isdir(base):
            return None

        pattern = os.path.join(base, "hour=*", "part-*.parquet")
        part_files = sorted(glob.glob(pattern))

        if not part_files:
            return None

        tables = [pq.read_table(f) for f in part_files]
        merged = pa.concat_tables(tables, promote_options="default")

        # Deduplicate on _event_id
        event_ids = merged.column("_event_id").to_pylist()
        seen: set[str] = set()
        keep_mask = []
        for eid in event_ids:
            if eid not in seen:
                seen.add(eid)
                keep_mask.append(True)
            else:
                keep_mask.append(False)

        deduped = merged.filter(pa.array(keep_mask))

        # Sort by _event_id (Redis stream IDs are timestamp-based)
        sort_indices = pc.sort_indices(deduped, sort_keys=[("_event_id", "ascending")])
        sorted_table = deduped.take(sort_indices)

        eod_path = os.path.join(base, "eod.parquet")
        pq.write_table(sorted_table, eod_path, compression="snappy")

        row_count = sorted_table.num_rows
        log.info(f"Compacted {len(part_files)} parts into {eod_path}: "
                 f"{row_count} rows (deduped from {merged.num_rows})")

        # Clean up part files and empty hour dirs
        for f in part_files:
            os.remove(f)
        for hour_dir in glob.glob(os.path.join(base, "hour=*")):
            try:
                os.rmdir(hour_dir)
            except OSError:
                pass

        return row_count

    def compact_all(self, date_str: str) -> dict[str, int]:
        """Compact all topics for a given date."""
        results = {}
        if not os.path.isdir(self._output_dir):
            return results

        for topic_dir in os.listdir(self._output_dir):
            topic_path = os.path.join(self._output_dir, topic_dir)
            if not os.path.isdir(topic_path):
                continue
            count = self.compact_date(topic_dir, date_str)
            if count is not None:
                results[topic_dir] = count

        return results
