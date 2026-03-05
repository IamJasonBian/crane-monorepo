"""Tests for Parquet schema definitions and event flattening."""

from crane_render.reader.stream_reader import Event
from crane_render.writer.schema import (
    flatten_event, QUOTE_SCHEMA, OPTIONS_SCHEMA, LISTING_SCHEMA,
    TOPIC_RAW_QUOTES, TOPIC_RAW_OPTIONS, TOPIC_RAW_LISTINGS,
)


def test_flatten_quote_event():
    event = Event(
        stream=TOPIC_RAW_QUOTES,
        event_id="1709654400000-0",
        event_type="quote",
        payload={
            "symbol": "AAPL",
            "bid": 150.0,
            "ask": 151.0,
            "mid": 150.5,
            "last": 150.75,
            "volume": 1000,
            "timestamp": "2026-03-05T14:00:00Z",
        },
    )
    flat = flatten_event(event)

    assert flat["_event_id"] == "1709654400000-0"
    assert flat["_stream"] == TOPIC_RAW_QUOTES
    assert flat["symbol"] == "AAPL"
    assert flat["bid"] == 150.0
    assert flat["volume"] == 1000

    # Verify all schema columns are present
    for field in QUOTE_SCHEMA:
        assert field.name in flat, f"Missing field: {field.name}"


def test_flatten_listing_event():
    event = Event(
        stream=TOPIC_RAW_LISTINGS,
        event_id="1709654400000-1",
        event_type="listing",
        payload={
            "epid": "abc123",
            "title": "NVIDIA A30 GPU",
            "link": "https://ebay.com/itm/abc123",
            "image": "https://img.ebay.com/abc.jpg",
            "condition": "New",
            "price": 299.99,
            "price_raw": "$299.99",
            "is_auction": False,
            "buy_it_now": True,
            "free_returns": True,
            "best_offer": False,
            "sponsored": False,
            "item_location": "US",
            "seller": {
                "name": "gpu_seller",
                "review_count": "1234",
                "positive_feedback_percent": 99.5,
            },
            "search_term": "nvidia a30 gpu",
            "first_seen": "2026-03-05T14:00:00Z",
            "last_seen": "2026-03-05T14:00:00Z",
        },
    )
    flat = flatten_event(event)

    assert flat["epid"] == "abc123"
    assert flat["price"] == 299.99
    assert flat["seller_name"] == "gpu_seller"
    assert flat["seller_positive_feedback_percent"] == 99.5
    assert flat["is_auction"] is False
    assert flat["buy_it_now"] is True

    for field in LISTING_SCHEMA:
        assert field.name in flat, f"Missing field: {field.name}"


def test_flatten_options_event():
    event = Event(
        stream=TOPIC_RAW_OPTIONS,
        event_id="1709654400000-2",
        event_type="option",
        payload={
            "symbol": "AAPL250620C00150000",
            "underlying": "AAPL",
            "expiration": "2026-06-20",
            "strike": 150.0,
            "option_type": "C",
            "pricing": {"bid": 5.0, "ask": 5.5, "mid": 5.25, "last": 5.3,
                        "spread": 0.5, "limit_price": 0.0, "stop_price": 0.0, "avg_entry": 0.0},
            "greeks": {"delta": 0.65, "gamma": 0.03, "theta": -0.05,
                       "vega": 0.15, "rho": 0.01, "iv": 0.25},
            "sizing": {"qty": 0.0, "filled_qty": 0.0, "volume": 500, "open_interest": 2000},
            "pnl": {"unrealized_pl": 0.0, "unrealized_pl_pct": 0.0, "market_value": 0.0},
            "orders": [],
            "bars": [],
            "updated_at": "2026-03-05T14:00:00Z",
            "created_at": "2026-03-05T13:00:00Z",
        },
    )
    flat = flatten_event(event)

    assert flat["symbol"] == "AAPL250620C00150000"
    assert flat["pricing_bid"] == 5.0
    assert flat["greeks_delta"] == 0.65
    assert flat["sizing_volume"] == 500
    assert flat["pnl_unrealized_pl"] == 0.0
    assert flat["orders_json"] == "[]"

    for field in OPTIONS_SCHEMA:
        assert field.name in flat, f"Missing field: {field.name}"


def test_flatten_listing_missing_seller():
    """Seller info should default gracefully when missing."""
    event = Event(
        stream=TOPIC_RAW_LISTINGS,
        event_id="1709654400000-3",
        event_type="listing",
        payload={"epid": "xyz", "price": 100.0},
    )
    flat = flatten_event(event)

    assert flat["seller_name"] == ""
    assert flat["seller_positive_feedback_percent"] == 0.0
