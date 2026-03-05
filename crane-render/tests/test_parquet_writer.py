"""Tests for Parquet writer with Hive-style partitioning."""

import os
import tempfile

import pyarrow.parquet as pq

from crane_render.reader.stream_reader import Event
from crane_render.writer.parquet_writer import ParquetWriter
from crane_render.writer.schema import TOPIC_RAW_QUOTES, TOPIC_RAW_LISTINGS


def _make_quote_event(eid: str, symbol: str = "AAPL", price: float = 150.0) -> Event:
    return Event(
        stream=TOPIC_RAW_QUOTES,
        event_id=eid,
        event_type="quote",
        payload={
            "symbol": symbol,
            "bid": price - 0.5,
            "ask": price + 0.5,
            "mid": price,
            "last": price,
            "volume": 1000,
            "timestamp": "2026-03-05T14:00:00Z",
        },
    )


def _make_listing_event(eid: str, epid: str = "item1") -> Event:
    return Event(
        stream=TOPIC_RAW_LISTINGS,
        event_id=eid,
        event_type="listing",
        payload={
            "epid": epid,
            "title": "Test Item",
            "link": "",
            "image": "",
            "condition": "New",
            "price": 99.99,
            "price_raw": "$99.99",
            "is_auction": False,
            "buy_it_now": True,
            "free_returns": False,
            "best_offer": False,
            "sponsored": False,
            "item_location": "US",
            "seller": {"name": "seller1", "review_count": "10", "positive_feedback_percent": 98.0},
            "search_term": "test",
            "first_seen": "2026-03-05T14:00:00Z",
            "last_seen": "2026-03-05T14:00:00Z",
        },
    )


def test_flush_creates_parquet_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        writer = ParquetWriter(tmpdir)
        events = [_make_quote_event("100-0"), _make_quote_event("100-1", symbol="MSFT")]
        counts = writer.flush(events)

        assert counts[TOPIC_RAW_QUOTES] == 2

        # Find the written file
        topic_dir = os.path.join(tmpdir, "raw_quotes")
        assert os.path.isdir(topic_dir)

        # Walk to find part file
        part_files = []
        for root, dirs, files in os.walk(topic_dir):
            for f in files:
                if f.startswith("part-") and f.endswith(".parquet"):
                    part_files.append(os.path.join(root, f))

        assert len(part_files) == 1

        table = pq.read_table(part_files[0])
        assert table.num_rows == 2
        assert "symbol" in table.column_names
        assert "_event_id" in table.column_names


def test_flush_empty_returns_empty():
    with tempfile.TemporaryDirectory() as tmpdir:
        writer = ParquetWriter(tmpdir)
        assert writer.flush([]) == {}


def test_flush_multiple_topics_creates_separate_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        writer = ParquetWriter(tmpdir)
        events = [
            _make_quote_event("100-0"),
            _make_listing_event("100-1"),
        ]
        counts = writer.flush(events)

        assert TOPIC_RAW_QUOTES in counts
        assert TOPIC_RAW_LISTINGS in counts
        assert os.path.isdir(os.path.join(tmpdir, "raw_quotes"))
        assert os.path.isdir(os.path.join(tmpdir, "raw_listings"))


def test_hive_partitioning_structure():
    with tempfile.TemporaryDirectory() as tmpdir:
        writer = ParquetWriter(tmpdir)
        writer.flush([_make_quote_event("100-0")])

        # Check directory structure: raw_quotes/date=.../hour=.../part-*.parquet
        topic_dir = os.path.join(tmpdir, "raw_quotes")
        date_dirs = [d for d in os.listdir(topic_dir) if d.startswith("date=")]
        assert len(date_dirs) == 1

        hour_dirs = [d for d in os.listdir(os.path.join(topic_dir, date_dirs[0]))
                     if d.startswith("hour=")]
        assert len(hour_dirs) == 1


def test_parquet_file_readable_with_correct_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        writer = ParquetWriter(tmpdir)
        writer.flush([_make_quote_event("100-0", symbol="GOOGL", price=2800.0)])

        for root, dirs, files in os.walk(tmpdir):
            for f in files:
                if f.endswith(".parquet"):
                    table = pq.read_table(os.path.join(root, f))
                    df = table.to_pydict()
                    assert df["symbol"] == ["GOOGL"]
                    assert df["mid"] == [2800.0]
                    assert df["_event_id"] == ["100-0"]
