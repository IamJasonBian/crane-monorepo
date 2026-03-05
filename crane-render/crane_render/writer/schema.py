"""PyArrow schema definitions and event flattening.

Each Pydantic model type is mapped to a flat Parquet schema.
Nested sub-models are prefixed: pricing.bid -> pricing_bid, seller.name -> seller_name.
List/dict fields are serialized as JSON strings.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pyarrow as pa

from crane_render.reader.stream_reader import Event

# Stream topic constants (string literals, no crane-shared dependency)
TOPIC_RAW_QUOTES = "crane:events:raw_quotes"
TOPIC_RAW_OPTIONS = "crane:events:raw_options"
TOPIC_RAW_LISTINGS = "crane:events:raw_listings"

# Common columns prepended to every schema
_META_FIELDS = [
    pa.field("_event_id", pa.string()),
    pa.field("_stream", pa.string()),
    pa.field("_ingested_at", pa.string()),
]

QUOTE_SCHEMA = pa.schema(_META_FIELDS + [
    pa.field("symbol", pa.string()),
    pa.field("bid", pa.float64()),
    pa.field("ask", pa.float64()),
    pa.field("mid", pa.float64()),
    pa.field("last", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("timestamp", pa.string()),
])

LISTING_SCHEMA = pa.schema(_META_FIELDS + [
    pa.field("epid", pa.string()),
    pa.field("title", pa.string()),
    pa.field("link", pa.string()),
    pa.field("image", pa.string()),
    pa.field("condition", pa.string()),
    pa.field("price", pa.float64()),
    pa.field("price_raw", pa.string()),
    pa.field("is_auction", pa.bool_()),
    pa.field("buy_it_now", pa.bool_()),
    pa.field("free_returns", pa.bool_()),
    pa.field("best_offer", pa.bool_()),
    pa.field("sponsored", pa.bool_()),
    pa.field("item_location", pa.string()),
    pa.field("seller_name", pa.string()),
    pa.field("seller_review_count", pa.string()),
    pa.field("seller_positive_feedback_percent", pa.float64()),
    pa.field("search_term", pa.string()),
    pa.field("first_seen", pa.string()),
    pa.field("last_seen", pa.string()),
])

OPTIONS_SCHEMA = pa.schema(_META_FIELDS + [
    pa.field("symbol", pa.string()),
    pa.field("underlying", pa.string()),
    pa.field("expiration", pa.string()),
    pa.field("strike", pa.float64()),
    pa.field("option_type", pa.string()),
    # Pricing (flattened)
    pa.field("pricing_bid", pa.float64()),
    pa.field("pricing_ask", pa.float64()),
    pa.field("pricing_mid", pa.float64()),
    pa.field("pricing_last", pa.float64()),
    pa.field("pricing_spread", pa.float64()),
    pa.field("pricing_limit_price", pa.float64()),
    pa.field("pricing_stop_price", pa.float64()),
    pa.field("pricing_avg_entry", pa.float64()),
    # Greeks (flattened)
    pa.field("greeks_delta", pa.float64()),
    pa.field("greeks_gamma", pa.float64()),
    pa.field("greeks_theta", pa.float64()),
    pa.field("greeks_vega", pa.float64()),
    pa.field("greeks_rho", pa.float64()),
    pa.field("greeks_iv", pa.float64()),
    # Sizing (flattened)
    pa.field("sizing_qty", pa.float64()),
    pa.field("sizing_filled_qty", pa.float64()),
    pa.field("sizing_volume", pa.int64()),
    pa.field("sizing_open_interest", pa.int64()),
    # PnL (flattened)
    pa.field("pnl_unrealized_pl", pa.float64()),
    pa.field("pnl_unrealized_pl_pct", pa.float64()),
    pa.field("pnl_market_value", pa.float64()),
    # Scalar fields
    pa.field("side", pa.string()),
    pa.field("order_type", pa.string()),
    pa.field("status", pa.string()),
    pa.field("order_id", pa.string()),
    # Complex fields as JSON
    pa.field("orders_json", pa.string()),
    pa.field("bars_json", pa.string()),
    pa.field("updated_at", pa.string()),
    pa.field("created_at", pa.string()),
])

# Topic -> schema mapping
TOPIC_SCHEMAS: dict[str, pa.Schema] = {
    TOPIC_RAW_QUOTES: QUOTE_SCHEMA,
    TOPIC_RAW_OPTIONS: OPTIONS_SCHEMA,
    TOPIC_RAW_LISTINGS: LISTING_SCHEMA,
}


def flatten_event(event: Event) -> dict:
    """Flatten an Event's payload into a dict matching the Parquet schema for its stream."""
    base = {
        "_event_id": event.event_id,
        "_stream": event.stream,
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    p = event.payload

    if event.stream == TOPIC_RAW_QUOTES:
        for k in ("symbol", "bid", "ask", "mid", "last", "volume", "timestamp"):
            base[k] = p.get(k)

    elif event.stream == TOPIC_RAW_OPTIONS:
        for k in ("symbol", "underlying", "expiration", "strike", "option_type",
                   "side", "order_type", "status", "order_id", "updated_at", "created_at"):
            base[k] = p.get(k)
        # Flatten nested dicts
        pricing = p.get("pricing") or {}
        for k in ("bid", "ask", "mid", "last", "spread", "limit_price", "stop_price", "avg_entry"):
            base[f"pricing_{k}"] = pricing.get(k, 0.0)
        greeks = p.get("greeks") or {}
        for k in ("delta", "gamma", "theta", "vega", "rho", "iv"):
            base[f"greeks_{k}"] = greeks.get(k, 0.0)
        sizing = p.get("sizing") or {}
        base["sizing_qty"] = sizing.get("qty", 0.0)
        base["sizing_filled_qty"] = sizing.get("filled_qty", 0.0)
        base["sizing_volume"] = sizing.get("volume", 0)
        base["sizing_open_interest"] = sizing.get("open_interest", 0)
        pnl = p.get("pnl") or {}
        for k in ("unrealized_pl", "unrealized_pl_pct", "market_value"):
            base[f"pnl_{k}"] = pnl.get(k, 0.0)
        base["orders_json"] = json.dumps(p.get("orders", []))
        base["bars_json"] = json.dumps(p.get("bars", []))

    elif event.stream == TOPIC_RAW_LISTINGS:
        for k in ("epid", "title", "link", "image", "condition", "price", "price_raw",
                   "is_auction", "buy_it_now", "free_returns", "best_offer", "sponsored",
                   "item_location", "search_term", "first_seen", "last_seen"):
            base[k] = p.get(k)
        seller = p.get("seller") or {}
        base["seller_name"] = seller.get("name", "")
        base["seller_review_count"] = seller.get("review_count", "")
        base["seller_positive_feedback_percent"] = seller.get("positive_feedback_percent", 0.0)

    return base
