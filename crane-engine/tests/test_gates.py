"""Tests for the gate pipeline."""

from unittest.mock import MagicMock

from crane_shared.models import AllocationSignal, SignalType, OrderSide
from crane_shared.redis_client import RedisClient
from crane_engine.allocation.gate_pipeline import GatePipeline


def test_dedup_rejects_duplicate():
    rc = MagicMock(spec=RedisClient)
    rc.dedup_check.return_value = False  # already seen
    pipeline = GatePipeline(rc)

    signal = AllocationSignal(
        signal_id="s1", symbol="AAPL", score=0.5, current_price=150.0,
        target_price=155.0, signal_type=SignalType.PRICE_THRESHOLD,
    )
    result = pipeline.process(signal)
    assert result is None


def test_passes_new_signal():
    rc = MagicMock(spec=RedisClient)
    rc.dedup_check.return_value = True  # new signal
    rc.get_model.return_value = None  # no strategy loaded
    pipeline = GatePipeline(rc)

    signal = AllocationSignal(
        signal_id="s1", symbol="AAPL", score=0.8, current_price=145.0,
        target_price=150.0, signal_type=SignalType.PRICE_THRESHOLD,
    )
    result = pipeline.process(signal)
    assert result is not None
    assert result.symbol == "AAPL"
    assert result.side == OrderSide.BUY
    assert result.dry_run is True  # default when no strategy
