"""Integration smoke tests — verifies all three services can share data via Redis.

Requires a running Redis instance (provided by docker-compose or CI service).
"""

import os
import uuid

import pytest

# Skip if no Redis available
redis_host = os.environ.get("REDIS_HOST", "localhost")

try:
    from crane_shared import RedisClient, EventBus
    from crane_shared.models import (
        MarketQuote,
        AllocationSignal,
        OrderIntent,
        OrderSide,
        Strategy,
        WatchTarget,
        BudgetState,
        SignalType,
    )
    from crane_shared.events import TOPIC_RAW_QUOTES, TOPIC_SIGNALS
except ImportError:
    pytest.skip("crane-shared not installed", allow_module_level=True)


@pytest.fixture
def redis_client():
    rc = RedisClient.from_env()
    if not rc.ping():
        pytest.skip("Redis not available")
    return rc


@pytest.fixture
def event_bus(redis_client):
    return EventBus(redis_client)


class TestFeedToEngine:
    """Test data flow: crane-feed writes → crane-engine reads."""

    def test_quote_roundtrip(self, redis_client):
        quote = MarketQuote(symbol="TEST_AAPL", bid=150.0, ask=150.5, mid=150.25)
        key = f"crane:feed:quotes:TEST_AAPL"
        redis_client.put_model(key, quote, ttl=60)

        loaded = redis_client.get_model(key, MarketQuote)
        assert loaded is not None
        assert loaded.symbol == "TEST_AAPL"
        assert loaded.mid == 150.25

        # Cleanup
        redis_client.client.delete(key)

    def test_event_publish_and_read(self, event_bus):
        topic = f"crane:test:{uuid.uuid4().hex[:8]}"
        quote = MarketQuote(symbol="TEST_MSFT", bid=400.0, ask=400.5)

        event_bus.publish_model(topic, "quote", quote)
        events = event_bus.read_latest(topic, count=1)

        assert len(events) == 1
        assert events[0].event_type == "quote"
        assert events[0].payload["symbol"] == "TEST_MSFT"

        # Cleanup
        event_bus._redis.delete(topic)


class TestManagerToEngine:
    """Test config flow: crane-manager writes → crane-engine reads."""

    def test_strategy_config_flow(self, redis_client):
        strategy = Strategy(
            strategy_id="test_strat",
            name="test_momentum",
            symbols=["AAPL"],
            dry_run=True,
        )
        redis_client.put_model("crane:manager:strategies:test_strat", strategy)
        redis_client.add_to_index("crane:manager:strategies:index", "test_strat")

        # Engine reads it back
        loaded = redis_client.get_model("crane:manager:strategies:test_strat", Strategy)
        assert loaded is not None
        assert loaded.name == "test_momentum"
        assert loaded.dry_run is True

        # Cleanup
        redis_client.client.delete("crane:manager:strategies:test_strat")
        redis_client.client.srem("crane:manager:strategies:index", "test_strat")

    def test_watch_target_flow(self, redis_client):
        target = WatchTarget(
            target_id="test_t1",
            symbol="AAPL",
            threshold_price=145.0,
            strategy_id="test_strat",
        )
        redis_client.put_model("crane:manager:targets:test_t1", target)
        redis_client.add_to_index("crane:manager:targets:index", "test_t1")

        index = redis_client.get_index("crane:manager:targets:index")
        assert "test_t1" in index

        loaded = redis_client.get_model("crane:manager:targets:test_t1", WatchTarget)
        assert loaded.threshold_price == 145.0

        # Cleanup
        redis_client.client.delete("crane:manager:targets:test_t1")
        redis_client.client.srem("crane:manager:targets:index", "test_t1")


class TestBudgetGate:
    """Test budget enforcement across services."""

    def test_atomic_spend(self, redis_client):
        budget_key = f"crane:manager:budget:test-{uuid.uuid4().hex[:8]}"
        redis_client.client.hset(budget_key, mapping={
            "daily_limit": "1000",
            "spent": "0",
            "frozen": "False",
        })

        # First spend should succeed
        assert redis_client.atomic_spend(budget_key, 500, 1000) is True

        # Second spend should succeed
        assert redis_client.atomic_spend(budget_key, 400, 1000) is True

        # Third spend should fail (over budget)
        assert redis_client.atomic_spend(budget_key, 200, 1000) is False

        # Cleanup
        redis_client.client.delete(budget_key)
