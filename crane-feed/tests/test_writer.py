"""Tests for feed Redis writer (unit-level, mocked Redis)."""

from unittest.mock import MagicMock

from crane_shared.models import MarketQuote, OptionsRecord
from crane_shared.redis_client import RedisClient
from crane_shared.events import EventBus
from crane_feed.store.redis_writer import FeedRedisWriter


def _mock_writer():
    rc = MagicMock(spec=RedisClient)
    bus = MagicMock(spec=EventBus)
    return FeedRedisWriter(rc, bus), rc, bus


def test_write_quote_publishes_event():
    writer, rc, bus = _mock_writer()
    q = MarketQuote(symbol="AAPL", bid=150.0, ask=150.5, mid=150.25)
    writer.write_quote(q)
    rc.put_model.assert_called_once()
    bus.publish_model.assert_called_once()


def test_write_option_creates_index():
    writer, rc, bus = _mock_writer()
    rec = OptionsRecord(symbol="AAPL250620C00150000", underlying="AAPL")
    writer.write_option(rec)
    assert rc.add_to_index.call_count == 2  # underlying index + all index
