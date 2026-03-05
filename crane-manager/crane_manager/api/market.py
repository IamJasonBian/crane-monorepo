"""Market data proxy endpoints.

Reads quote and options data from Redis and serves it to the UI.
Supports both:
  - Legacy format (market-quotes hash, options-chain:* hashes) from Scala service
  - Crane format (crane:feed:quotes:*, crane:feed:options:*) from crane-feed
"""

from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, HTTPException

from crane_shared.models import MarketQuote, OptionsRecord
from crane_manager.deps import get_redis

router = APIRouter()


def _parse_legacy_quote(symbol: str, raw: str) -> dict:
    """Parse a quote from the legacy market-quotes hash format."""
    data = json.loads(raw)
    return {
        "symbol": symbol,
        "bid": data.get("bid", 0),
        "ask": data.get("ask", 0),
        "mid": data.get("mid", 0),
        "last": data.get("mid", 0),
        "volume": data.get("bid_size", 0) + data.get("ask_size", 0),
        "timestamp": data.get("timestamp", ""),
    }


def _parse_legacy_option(symbol: str, raw: str) -> dict:
    """Parse an option from the legacy options-chain:* hash format."""
    data = json.loads(raw)
    quote = data.get("latest_quote", {})
    greeks = data.get("greeks", {})

    # Parse OCC symbol for strike/type/expiration
    option_type = ""
    strike = 0.0
    expiration = ""
    underlying = ""
    if len(symbol) >= 15:
        # OCC format: UNDERLYING + YYMMDD + C/P + STRIKE*1000
        # Find where the date starts (6 digits before C/P)
        for i in range(len(symbol) - 9, 0, -1):
            if symbol[i:i+6].isdigit() and symbol[i+6] in ("C", "P"):
                underlying = symbol[:i]
                date_str = symbol[i:i+6]
                option_type = symbol[i+6]
                strike = int(symbol[i+7:]) / 1000.0
                expiration = f"20{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]}"
                break

    bid = quote.get("bid", 0) or 0
    ask = quote.get("ask", 0) or 0

    return {
        "symbol": symbol,
        "underlying": underlying,
        "expiration": expiration,
        "strike": strike,
        "option_type": option_type,
        "pricing": {
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2 if bid and ask else 0,
            "last": 0,
            "spread": ask - bid if bid and ask else 0,
        },
        "greeks": {
            "delta": greeks.get("delta", 0) or 0,
            "gamma": greeks.get("gamma", 0) or 0,
            "theta": greeks.get("theta", 0) or 0,
            "vega": greeks.get("vega", 0) or 0,
            "iv": greeks.get("impliedVolatility", 0) or greeks.get("iv", 0) or 0,
        },
        "sizing": {
            "volume": data.get("dailyBar", {}).get("v", 0) if data.get("dailyBar") else 0,
            "open_interest": data.get("openInterest", 0) or 0,
        },
        "updated_at": quote.get("timestamp", ""),
    }


@router.get("/quotes")
def list_quotes():
    rc = get_redis()
    quotes = []

    # Try crane format first
    crane_symbols = rc.get_index("crane:feed:quotes:index")
    for sym in sorted(crane_symbols):
        q = rc.get_model(f"crane:feed:quotes:{sym}", MarketQuote)
        if q:
            quotes.append(q.model_dump())

    # Fall back to legacy market-quotes hash
    if not quotes:
        raw = rc.client.hgetall("market-quotes")
        for k, v in raw.items():
            key = k.decode()
            if key == "_meta":
                continue
            try:
                quotes.append(_parse_legacy_quote(key, v.decode()))
            except Exception:
                continue

    return quotes


@router.get("/quotes/{symbol}")
def get_quote(symbol: str):
    rc = get_redis()

    # Crane format
    q = rc.get_model(f"crane:feed:quotes:{symbol}", MarketQuote)
    if q:
        return q.model_dump()

    # Legacy format
    raw = rc.client.hget("market-quotes", symbol)
    if raw:
        return _parse_legacy_quote(symbol, raw.decode())

    raise HTTPException(status_code=404, detail=f"No quote for {symbol}")


@router.get("/quotes/{symbol}/history")
def get_quote_history(symbol: str, limit: int = 500):
    """Return price history as [{timestamp, price}, ...] for charting."""
    rc = get_redis()
    points = []

    # Try crane format
    key = f"crane:feed:quotes:history:{symbol}"
    raw = rc.client.lrange(key, 0, limit - 1)

    # Fall back to legacy history
    if not raw:
        key = f"market-quotes:history"
        raw = rc.client.lrange(key, 0, limit - 1)

    for item in reversed(raw):  # oldest first for charting
        try:
            data = json.loads(item)
            # Could be a crane MarketQuote or legacy format
            ts = data.get("timestamp", "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                epoch_ms = int(dt.timestamp() * 1000)
            except Exception:
                epoch_ms = 0

            price = data.get("mid", 0) or data.get("last", 0) or data.get("close", 0)
            if price > 0:
                points.append({"timestamp": epoch_ms, "price": price})
        except Exception:
            continue

    return points


@router.get("/options/{underlying}")
def list_options(underlying: str):
    rc = get_redis()
    records = []

    # Try crane format
    crane_symbols = rc.get_index(f"crane:feed:options:index:{underlying}")
    for sym in sorted(crane_symbols):
        rec = rc.get_model(f"crane:feed:options:{sym}", OptionsRecord)
        if rec:
            records.append(rec.model_dump())

    # Fall back to legacy options-chain:UNDERLYING hash
    if not records:
        raw = rc.client.hgetall(f"options-chain:{underlying}")
        for k, v in raw.items():
            key = k.decode()
            if key == "_meta":
                continue
            try:
                records.append(_parse_legacy_option(key, v.decode()))
            except Exception:
                continue

    return records
