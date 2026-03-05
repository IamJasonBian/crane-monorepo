"""Market data proxy endpoints.

Reads quote and options data written by crane-feed from Redis and
serves it to the UI. This keeps the frontend talking to a single API.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from crane_shared.models import MarketQuote, OptionsRecord
from crane_manager.deps import get_redis

router = APIRouter()


@router.get("/quotes")
def list_quotes():
    rc = get_redis()
    symbols = rc.get_index("crane:feed:quotes:index")
    quotes = []
    for sym in sorted(symbols):
        q = rc.get_model(f"crane:feed:quotes:{sym}", MarketQuote)
        if q:
            quotes.append(q.model_dump())
    return quotes


@router.get("/quotes/{symbol}")
def get_quote(symbol: str):
    rc = get_redis()
    q = rc.get_model(f"crane:feed:quotes:{symbol}", MarketQuote)
    if not q:
        raise HTTPException(status_code=404, detail=f"No quote for {symbol}")
    return q.model_dump()


@router.get("/quotes/{symbol}/history")
def get_quote_history(symbol: str, limit: int = 500):
    """Return price history as [{timestamp, price}, ...] for charting."""
    rc = get_redis()
    key = f"crane:feed:quotes:history:{symbol}"
    raw = rc.client.lrange(key, 0, limit - 1)
    points = []
    for item in reversed(raw):  # oldest first for charting
        q = MarketQuote.model_validate_json(item)
        ts = q.timestamp
        # Convert ISO timestamp to epoch ms if possible
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            epoch_ms = int(dt.timestamp() * 1000)
        except Exception:
            epoch_ms = 0
        price = q.mid if q.mid > 0 else q.last
        if price > 0:
            points.append({"timestamp": epoch_ms, "price": price})
    return points


@router.get("/options/{underlying}")
def list_options(underlying: str):
    rc = get_redis()
    symbols = rc.get_index(f"crane:feed:options:index:{underlying}")
    records = []
    for sym in sorted(symbols):
        rec = rc.get_model(f"crane:feed:options:{sym}", OptionsRecord)
        if rec:
            records.append(rec.model_dump())
    return records
