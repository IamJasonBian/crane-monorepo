"""Historical price endpoints.

Reads the per-day price series written by crane-feed's PriceSnapshotter.
"""

from __future__ import annotations

import json

from fastapi import APIRouter

from crane_manager.deps import get_redis

router = APIRouter()

SERIES_KEY = "crane:feed:prices:series:{term_id}:{source}"


def read_series(
    rc,
    term_id: str,
    source: str = "ebay",
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Return a term's price snapshots sorted ascending by date.

    Args:
        term_id: Which SearchTerm's series to read.
        source: Which source series ("ebay", "bestbuy", ...).
        date_from: Inclusive lower bound (YYYY-MM-DD), or None.
        date_to: Inclusive upper bound (YYYY-MM-DD), or None.

    Returns:
        A list of snapshot dicts ordered oldest → newest.
    """
    raw = rc.client.hgetall(SERIES_KEY.format(term_id=term_id, source=source))
    points: list[dict] = []
    for field, value in raw.items():
        date = field.decode() if isinstance(field, bytes) else field
        if date_from and date < date_from:
            continue
        if date_to and date > date_to:
            continue
        try:
            points.append(json.loads(value))
        except (json.JSONDecodeError, ValueError):
            continue
    points.sort(key=lambda p: p.get("date", ""))
    return points


@router.get("/history/{term_id}")
def get_price_history(
    term_id: str,
    source: str = "ebay",
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 365,
):
    """Historical daily price snapshots for one product."""
    rc = get_redis()
    points = read_series(rc, term_id, source, date_from, date_to)
    return points[-limit:]
