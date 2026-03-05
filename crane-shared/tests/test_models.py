"""Tests for shared models — ensures serialization contract is stable."""

from crane_shared.models import (
    MarketQuote,
    OptionsRecord,
    AllocationSignal,
    OrderIntent,
    OrderSide,
    OrderType,
    SignalType,
    Strategy,
    WatchTarget,
    BudgetState,
)


def test_market_quote_spread():
    q = MarketQuote(symbol="AAPL", bid=150.0, ask=150.5)
    assert q.spread == 0.5


def test_options_record_roundtrip():
    rec = OptionsRecord(symbol="AAPL250620C00150000", underlying="AAPL", strike=150.0, option_type="C")
    data = rec.model_dump_json()
    restored = OptionsRecord.model_validate_json(data)
    assert restored.symbol == "AAPL250620C00150000"
    assert restored.strike == 150.0


def test_allocation_signal_defaults():
    sig = AllocationSignal(signal_id="s1", symbol="AAPL")
    assert sig.signal_type == SignalType.PRICE_THRESHOLD
    assert sig.score == 0.0
    assert sig.timestamp  # auto-populated


def test_order_intent_dry_run_default():
    intent = OrderIntent(
        intent_id="o1", signal_id="s1", symbol="AAPL", side=OrderSide.BUY,
    )
    assert intent.dry_run is True


def test_strategy_serialization():
    s = Strategy(strategy_id="strat1", name="momentum", symbols=["AAPL", "MSFT"],
                 params={"lookback": 20, "threshold": 0.02})
    data = s.model_dump()
    assert data["params"]["lookback"] == 20


def test_budget_state():
    b = BudgetState(date="2025-01-01", daily_limit=10000, spent=3500, remaining=6500)
    assert b.remaining == 6500
