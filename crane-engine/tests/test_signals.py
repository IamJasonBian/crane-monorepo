"""Tests for signal generation."""

from unittest.mock import MagicMock

from crane_shared.redis_client import RedisClient
from crane_shared.events import Event
from crane_shared.models import WatchTarget
from crane_engine.signals.price_threshold import PriceThresholdSignaler


def test_generates_signal_below_threshold():
    rc = MagicMock(spec=RedisClient)
    rc.get_index.return_value = {"t1"}
    rc.get_model.return_value = WatchTarget(
        target_id="t1", symbol="AAPL", threshold_price=155.0, enabled=True,
    )

    signaler = PriceThresholdSignaler(rc)
    event = Event(
        stream="crane:events:raw_quotes",
        event_id="1",
        event_type="quote",
        payload={"symbol": "AAPL", "mid": 150.0},
    )
    signals = signaler.evaluate(event)
    assert len(signals) == 1
    assert signals[0].current_price == 150.0
    assert signals[0].score > 0


def test_no_signal_above_threshold():
    rc = MagicMock(spec=RedisClient)
    rc.get_index.return_value = {"t1"}
    rc.get_model.return_value = WatchTarget(
        target_id="t1", symbol="AAPL", threshold_price=140.0, enabled=True,
    )

    signaler = PriceThresholdSignaler(rc)
    event = Event(
        stream="crane:events:raw_quotes",
        event_id="1",
        event_type="quote",
        payload={"symbol": "AAPL", "mid": 150.0},
    )
    signals = signaler.evaluate(event)
    assert len(signals) == 0
