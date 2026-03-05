"""Tests for EOD compaction: merge, dedup, sort."""

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from crane_render.compaction.compactor import Compactor
from crane_render.writer.schema import QUOTE_SCHEMA


def _write_part(base_dir: str, topic: str, date: str, hour: str,
                part_name: str, event_ids: list[str], symbols: list[str]):
    """Write a test part file with minimal quote data."""
    dir_path = os.path.join(base_dir, topic, f"date={date}", f"hour={hour}")
    os.makedirs(dir_path, exist_ok=True)
    n = len(event_ids)
    columns = {
        "_event_id": event_ids,
        "_stream": ["crane:events:raw_quotes"] * n,
        "_ingested_at": ["2026-03-05T14:00:00Z"] * n,
        "symbol": symbols,
        "bid": [100.0] * n,
        "ask": [101.0] * n,
        "mid": [100.5] * n,
        "last": [100.5] * n,
        "volume": [1000] * n,
        "timestamp": ["2026-03-05T14:00:00Z"] * n,
    }
    table = pa.table(columns, schema=QUOTE_SCHEMA)
    pq.write_table(table, os.path.join(dir_path, part_name))


def test_compact_merges_parts():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_part(tmpdir, "raw_quotes", "2026-03-05", "14", "part-aaa.parquet",
                    ["100-0", "100-1"], ["AAPL", "MSFT"])
        _write_part(tmpdir, "raw_quotes", "2026-03-05", "15", "part-bbb.parquet",
                    ["200-0", "200-1"], ["GOOGL", "AMZN"])

        compactor = Compactor(tmpdir)
        count = compactor.compact_date("raw_quotes", "2026-03-05")

        assert count == 4

        eod_path = os.path.join(tmpdir, "raw_quotes", "date=2026-03-05", "eod.parquet")
        assert os.path.exists(eod_path)

        table = pq.read_table(eod_path)
        assert table.num_rows == 4


def test_compact_deduplicates():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Same event_id "100-0" appears in both parts (simulating at-least-once replay)
        _write_part(tmpdir, "raw_quotes", "2026-03-05", "14", "part-aaa.parquet",
                    ["100-0", "100-1"], ["AAPL", "MSFT"])
        _write_part(tmpdir, "raw_quotes", "2026-03-05", "14", "part-bbb.parquet",
                    ["100-0", "200-0"], ["AAPL", "GOOGL"])

        compactor = Compactor(tmpdir)
        count = compactor.compact_date("raw_quotes", "2026-03-05")

        assert count == 3  # 100-0, 100-1, 200-0 (100-0 deduped)


def test_compact_sorts_by_event_id():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_part(tmpdir, "raw_quotes", "2026-03-05", "15", "part-bbb.parquet",
                    ["300-0"], ["AMZN"])
        _write_part(tmpdir, "raw_quotes", "2026-03-05", "14", "part-aaa.parquet",
                    ["100-0"], ["AAPL"])

        compactor = Compactor(tmpdir)
        compactor.compact_date("raw_quotes", "2026-03-05")

        eod_path = os.path.join(tmpdir, "raw_quotes", "date=2026-03-05", "eod.parquet")
        table = pq.read_table(eod_path)
        ids = table.column("_event_id").to_pylist()
        assert ids == ["100-0", "300-0"]


def test_compact_removes_part_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_part(tmpdir, "raw_quotes", "2026-03-05", "14", "part-aaa.parquet",
                    ["100-0"], ["AAPL"])

        compactor = Compactor(tmpdir)
        compactor.compact_date("raw_quotes", "2026-03-05")

        # Part file should be gone
        part_path = os.path.join(tmpdir, "raw_quotes", "date=2026-03-05",
                                 "hour=14", "part-aaa.parquet")
        assert not os.path.exists(part_path)

        # Hour dir should be gone (empty after removal)
        hour_dir = os.path.join(tmpdir, "raw_quotes", "date=2026-03-05", "hour=14")
        assert not os.path.exists(hour_dir)


def test_compact_no_data_returns_none():
    with tempfile.TemporaryDirectory() as tmpdir:
        compactor = Compactor(tmpdir)
        assert compactor.compact_date("raw_quotes", "2026-03-05") is None


def test_compact_all():
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_part(tmpdir, "raw_quotes", "2026-03-05", "14", "part-a.parquet",
                    ["100-0"], ["AAPL"])
        _write_part(tmpdir, "raw_listings", "2026-03-05", "14", "part-b.parquet",
                    ["200-0"], ["item1"])

        compactor = Compactor(tmpdir)
        results = compactor.compact_all("2026-03-05")

        assert "raw_quotes" in results
        assert "raw_listings" in results
