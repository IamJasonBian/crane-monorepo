"""Rolls the churning stream of live listings into durable daily price points.

Individual eBay listings carry a 7-day TTL and turn over constantly, so they
can't answer "what did this product cost last month?". The snapshotter reads the
*current* qualifying listings for each tracked term and writes one aggregated
point (low / median / high / sample_count) per source per UTC day into a Redis
hash. Re-running the same day overwrites that day's field — idempotent — so it
can safely piggyback on every poll cycle and keep the latest intraday low.

Key schema:
    crane:feed:prices:series:{term_id}:{source}   (hash)
        field = YYYY-MM-DD  ->  value = PriceSnapshot JSON
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from crane_shared.classifier import catalog_classifier
from crane_shared.models import EbayListing, PriceSnapshot, SearchTerm
from crane_shared.redis_client import RedisClient

log = logging.getLogger("crane-feed.snapshotter")

SERIES_KEY = "crane:feed:prices:series:{term_id}:{source}"


def series_key(term_id: str, source: str = "ebay") -> str:
    return SERIES_KEY.format(term_id=term_id, source=source)


class PriceSnapshotter:
    """Aggregates live listings into per-day price snapshots."""

    def __init__(self, redis_client: RedisClient):
        self._redis = redis_client

    def _qualifying_prices(self, term: SearchTerm) -> list[float]:
        """Prices of listings that pass the catalog classifier + price bounds.

        Mirrors crane-manager's list_by_term so the series matches what the UI
        shows for the same term.
        """
        epids = self._redis.get_index(
            f"crane:feed:listings:index:{term.query}",
        )
        prices: list[float] = []
        for epid in epids:
            listing = self._redis.get_model(
                f"crane:feed:listings:{epid}", EbayListing,
            )
            if not listing or listing.price <= 0:
                continue
            if term.min_price and listing.price < term.min_price:
                continue
            if term.max_price and listing.price > term.max_price:
                continue
            if not catalog_classifier(term.query, listing.title):
                continue
            prices.append(listing.price)
        return sorted(prices)

    def snapshot_term(self, term: SearchTerm, source: str = "ebay") -> PriceSnapshot | None:
        """Compute and persist today's snapshot for one term. Returns it, or None."""
        prices = self._qualifying_prices(term)
        if not prices:
            return None

        snapshot = PriceSnapshot(
            term_id=term.term_id,
            source=source,
            date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            low=prices[0],
            median=prices[len(prices) // 2],
            high=prices[-1],
            sample_count=len(prices),
        )
        # Hash field = date → idempotent per-day overwrite (keeps latest intraday value)
        self._redis.client.hset(
            series_key(term.term_id, source),
            snapshot.date,
            snapshot.model_dump_json(),
        )
        return snapshot

    def snapshot_all(self, terms: list[SearchTerm], source: str = "ebay") -> int:
        """Snapshot every enabled term. Returns the number of points written."""
        written = 0
        for term in terms:
            if not term.enabled:
                continue
            try:
                if self.snapshot_term(term, source=source):
                    written += 1
            except Exception as e:  # never let one term kill the rollup
                log.error(f"snapshot failed for '{term.query}': {e}")
        if written:
            log.info(f"Wrote {written} price snapshots (source={source})")
        return written

    def backfill_term(self, term: SearchTerm, source: str = "ebay") -> int:
        """Reconstruct past daily points from existing per-listing history.

        crane-feed already pushes a full EbayListing (with a `last_seen`
        timestamp) into crane:feed:listings:history:{epid} on every poll. This
        replays those entries, buckets qualifying prices by day, and fills any
        missing day in the series. Uses HSETNX so a real live snapshot for a day
        is never clobbered. Returns the number of days written.

        Args:
            term: The search term to backfill.
            source: Which source series to write into.

        Returns:
            Count of new day-points written (skips days already present).
        """
        prices_by_date: dict[str, list[float]] = {}
        for epid in self._redis.get_index(f"crane:feed:listings:index:{term.query}"):
            raw = self._redis.client.lrange(
                f"crane:feed:listings:history:{epid}", 0, -1,
            )
            for item in raw:
                try:
                    listing = EbayListing.model_validate_json(item)
                except Exception:
                    continue
                if listing.price <= 0 or not listing.last_seen:
                    continue
                if term.min_price and listing.price < term.min_price:
                    continue
                if term.max_price and listing.price > term.max_price:
                    continue
                if not catalog_classifier(term.query, listing.title):
                    continue
                day = listing.last_seen[:10]  # YYYY-MM-DD
                prices_by_date.setdefault(day, []).append(listing.price)

        written = 0
        key = series_key(term.term_id, source)
        for day, prices in prices_by_date.items():
            prices.sort()
            snapshot = PriceSnapshot(
                term_id=term.term_id,
                source=source,
                date=day,
                low=prices[0],
                median=prices[len(prices) // 2],
                high=prices[-1],
                sample_count=len(prices),
            )
            # HSETNX: only fill gaps — never overwrite an existing live point.
            if self._redis.client.hsetnx(key, day, snapshot.model_dump_json()):
                written += 1
        return written

    def backfill_all(self, terms: list[SearchTerm], source: str = "ebay") -> int:
        """Backfill every enabled term from per-listing history. Returns days written."""
        written = 0
        for term in terms:
            if not term.enabled:
                continue
            try:
                written += self.backfill_term(term, source=source)
            except Exception as e:
                log.error(f"backfill failed for '{term.query}': {e}")
        if written:
            log.info(f"Backfilled {written} historical price points (source={source})")
        return written
