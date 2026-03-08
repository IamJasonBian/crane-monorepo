"""Countdown API eBay search poller.

Polls the Countdown API for eBay BIN listings matching tracked search terms.
Writes EbayListing records to Redis and publishes events.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import httpx

from crane_shared.models import EbayListing, SellerInfo, SearchTerm
from crane_shared.redis_client import RedisClient
from crane_shared.events import EventBus
from crane_feed.classifier import classify_listing
from crane_feed.notifier import notify_listing

log = logging.getLogger("crane-feed.ebay")

COUNTDOWN_API_URL = "https://api.countdownapi.com/request"


class CountdownEbayPoller:
    def __init__(
        self,
        redis_client: RedisClient,
        event_bus: EventBus,
        poll_interval: float = 300.0,  # 5 min default (API credits are limited)
    ):
        self._redis = redis_client
        self._bus = event_bus
        self._api_key = os.environ.get("COUNTDOWN_API_KEY", "")
        self.poll_interval = poll_interval

    def run(self):
        log.info("eBay poller started")
        while True:
            terms = self._load_search_terms()
            if not terms:
                log.info("No search terms configured, sleeping...")
                time.sleep(self.poll_interval)
                continue

            for term in terms:
                if not term.enabled:
                    continue
                try:
                    self._poll_term(term)
                except Exception as e:
                    log.error(f"Poll error for '{term.query}': {e}")
                time.sleep(2)  # small delay between API calls

            time.sleep(self.poll_interval)

    def poll_once(self, query: str, **kwargs) -> list[EbayListing]:
        """One-shot poll for a search term. Returns listings."""
        params = {
            "api_key": self._api_key,
            "type": "search",
            "ebay_domain": "ebay.com",
            "search_term": query,
            "sort_by": kwargs.get("sort_by", "price_low_to_high"),
            "listing_type": kwargs.get("listing_type", "buy_it_now"),
        }

        with httpx.Client() as client:
            resp = client.get(COUNTDOWN_API_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

        if not data.get("request_info", {}).get("success"):
            log.warning(f"API returned failure for '{query}'")
            return []

        results = data.get("search_results", [])
        now = datetime.utcnow().isoformat()
        listings = []

        for item in results:
            price_data = item.get("price", {})
            seller = item.get("seller_info", {})

            listing = EbayListing(
                epid=str(item.get("epid", "")),
                title=item.get("title", ""),
                link=item.get("link", ""),
                image=item.get("image", ""),
                condition=item.get("condition", ""),
                price=price_data.get("value", 0),
                price_raw=price_data.get("raw", ""),
                is_auction=item.get("is_auction", False),
                buy_it_now=item.get("buy_it_now", False),
                free_returns=item.get("free_returns", False),
                best_offer=item.get("best_offer", False),
                sponsored=item.get("sponsored", False),
                item_location=item.get("item_location", ""),
                seller=SellerInfo(
                    name=seller.get("name", ""),
                    review_count=str(seller.get("review_count", "")),
                    positive_feedback_percent=seller.get("positive_feedback_percent", 0),
                ),
                search_term=query,
                first_seen=now,
                last_seen=now,
            )
            listings.append(listing)

        return listings

    def _poll_term(self, term: SearchTerm):
        listings = self.poll_once(
            term.query,
            sort_by=term.sort_by,
            listing_type=term.listing_type,
        )
        log.info(f"'{term.query}': {len(listings)} listings found")

        now = datetime.utcnow().isoformat()
        current_epids = {l.epid for l in listings if l.epid}

        # Mark disappeared listings as sold
        prev_epids = set(self._redis.get_index(
            f"crane:feed:listings:index:{term.query}",
        ))
        for epid in prev_epids - current_epids:
            existing = self._redis.get_model(
                f"crane:feed:listings:{epid}", EbayListing,
            )
            if existing and not existing.sold:
                existing.sold = True
                existing.sold_at = now
                self._redis.put_model(
                    f"crane:feed:listings:{epid}", existing, ttl=30 * 86400,
                )

        for listing in listings:
            if not listing.epid:
                continue

            # Check if we've seen this item before
            existing = self._redis.get_model(
                f"crane:feed:listings:{listing.epid}", EbayListing,
            )
            if existing:
                listing.first_seen = existing.first_seen

            # Write listing
            self._redis.put_model(
                f"crane:feed:listings:{listing.epid}",
                listing,
                ttl=7 * 86400,
            )

            # Index by search term
            self._redis.add_to_index(
                f"crane:feed:listings:index:{term.query}",
                listing.epid,
            )
            self._redis.add_to_index("crane:feed:listings:index:all", listing.epid)

            # Classify and notify
            if classify_listing(term.query, listing.title):
                is_new = existing is None
                price_dropped = (
                    existing is not None
                    and listing.price < existing.price
                )
                if is_new:
                    notify_listing(listing, reason="New T705 2TB listing")
                elif price_dropped:
                    notify_listing(
                        listing,
                        reason=f"Price drop: ${existing.price:.2f} → ${listing.price:.2f}",
                    )

            # Publish event
            try:
                self._bus.publish_model(
                    "crane:events:raw_listings", "listing", listing,
                )
            except Exception:
                pass

            # Track price history
            self._redis.push(
                f"crane:feed:listings:history:{listing.epid}",
                listing,
                max_len=1000,
            )

        # Update term metadata
        term.last_polled = now
        term.result_count = len(listings)
        self._redis.put_model(f"crane:manager:terms:{term.term_id}", term)

    def _load_search_terms(self) -> list[SearchTerm]:
        term_ids = self._redis.get_index("crane:manager:terms:index")
        terms = []
        for tid in term_ids:
            t = self._redis.get_model(f"crane:manager:terms:{tid}", SearchTerm)
            if t:
                terms.append(t)
        return terms
