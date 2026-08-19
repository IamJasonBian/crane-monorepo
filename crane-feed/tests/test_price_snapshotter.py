"""Tests for the PriceSnapshotter daily rollup."""

from unittest.mock import MagicMock

from crane_shared.models import EbayListing, SearchTerm
from crane_shared.redis_client import RedisClient
from crane_feed.sources.price_snapshotter import PriceSnapshotter, series_key


def _listing(epid: str, price: float, title: str) -> EbayListing:
    return EbayListing(epid=epid, title=title, price=price)


def _listing_json(epid: str, price: float, title: str, last_seen: str) -> str:
    return EbayListing(epid=epid, title=title, price=price, last_seen=last_seen).model_dump_json()


def test_snapshot_computes_low_median_high_and_respects_bounds():
    rc = MagicMock(spec=RedisClient)
    rc.client = MagicMock()
    term = SearchTerm(term_id="datacenter-tower", query="dell poweredge tower server",
                      min_price=200, max_price=2000)

    rc.get_index.return_value = {"a", "b", "c", "cheap", "dear"}
    listings = {
        "a": _listing("a", 300, "Dell PowerEdge Tower Server"),
        "b": _listing("b", 500, "Dell PowerEdge Tower Server T340"),
        "c": _listing("c", 700, "Dell PowerEdge Tower Server T440"),
        "cheap": _listing("cheap", 50, "PowerEdge bezel only"),      # below min_price
        "dear": _listing("dear", 5000, "PowerEdge fully loaded"),    # above max_price
    }
    rc.get_model.side_effect = lambda key, cls: next(
        (v for k, v in listings.items() if k == key.rsplit(":", 1)[-1]), None,
    )

    snap = PriceSnapshotter(rc).snapshot_term(term)

    assert snap is not None
    assert snap.low == 300
    assert snap.median == 500
    assert snap.high == 700
    assert snap.sample_count == 3  # out-of-bounds listings excluded

    # Written to the per-day hash field, keyed by today's date (idempotent).
    args = rc.client.hset.call_args[0]
    assert args[0] == series_key("datacenter-tower", "ebay")
    assert args[1] == snap.date


def test_snapshot_returns_none_when_no_qualifying_listings():
    rc = MagicMock(spec=RedisClient)
    rc.client = MagicMock()
    term = SearchTerm(term_id="empty", query="nothing here")
    rc.get_index.return_value = set()

    assert PriceSnapshotter(rc).snapshot_term(term) is None
    rc.client.hset.assert_not_called()


def test_backfill_buckets_history_by_day_and_fills_gaps():
    rc = MagicMock(spec=RedisClient)
    rc.client = MagicMock()
    term = SearchTerm(term_id="datacenter-tower", query="dell poweredge tower server",
                      min_price=200, max_price=2000)
    rc.get_index.return_value = {"a", "b"}

    # Two epids, each with dated history entries (full EbayListing JSON).
    history = {
        "crane:feed:listings:history:a": [
            _listing_json("a", 300, "Dell PowerEdge Tower Server", "2026-08-01T10:00:00"),
            _listing_json("a", 320, "Dell PowerEdge Tower Server", "2026-08-02T10:00:00"),
        ],
        "crane:feed:listings:history:b": [
            _listing_json("b", 500, "Dell PowerEdge Tower Server T340", "2026-08-01T12:00:00"),
            _listing_json("b", 50, "PowerEdge bezel", "2026-08-01T12:00:00"),  # below min
        ],
    }
    rc.client.lrange.side_effect = lambda key, s, e: history.get(key, [])
    # HSETNX: pretend all days are new (return 1).
    rc.client.hsetnx.return_value = 1

    written = PriceSnapshotter(rc).backfill_term(term)

    # Two distinct days written (2026-08-01, 2026-08-02).
    assert written == 2
    days = {c[0][1] for c in rc.client.hsetnx.call_args_list}
    assert days == {"2026-08-01", "2026-08-02"}
    # Day 08-01 low is 300 (the $50 bezel was filtered out by min_price).
    day01 = next(c[0][2] for c in rc.client.hsetnx.call_args_list if c[0][1] == "2026-08-01")
    assert '"low":300' in day01.replace(" ", "")
