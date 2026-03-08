"""Slickdeals RSS feed poller.

Free deal aggregator that surfaces price drops from Best Buy, Amazon,
Newegg, B&H, and other retailers. No API key required.

RSS URL:
  https://slickdeals.net/newsearch.php?searchin=deals&q={query}&rss=1
"""

from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from hashlib import md5

import httpx

from crane_shared.models import EbayListing, SellerInfo, SearchTerm
from crane_shared.redis_client import RedisClient
from crane_shared.events import EventBus
from crane_feed.classifier import classify_listing
from crane_feed.notifier import notify_listing

log = logging.getLogger("crane-feed.slickdeals")

SLICKDEALS_RSS = "https://slickdeals.net/newsearch.php"


def _parse_price(text: str) -> float:
    """Extract price from text like '$159.99'. Requires $ prefix to avoid model numbers."""
    # Match dollar sign followed by number, e.g. $159.99 or $1,299.00
    matches = re.findall(r"\$([\d,]+\.?\d*)", text)
    for m in matches:
        try:
            val = float(m.replace(",", ""))
            # Skip implausible prices (likely not a real price)
            if 10.0 <= val <= 50000.0:
                return val
        except ValueError:
            continue
    return 0.0


def _extract_retailer(title: str, link: str) -> str:
    """Guess retailer from deal title or link."""
    t = title.lower()
    retailers = [
        ("amazon", "Amazon"), ("best buy", "Best Buy"), ("bestbuy", "Best Buy"),
        ("newegg", "Newegg"), ("b&h", "B&H Photo"), ("bhphoto", "B&H Photo"),
        ("walmart", "Walmart"), ("micro center", "Micro Center"),
        ("adorama", "Adorama"), ("crucial.com", "Crucial"),
    ]
    for keyword, name in retailers:
        if keyword in t:
            return name
    return "Slickdeals"


class SlickdealsPoller:
    """Polls Slickdeals RSS for deals matching tracked search terms."""

    def __init__(
        self,
        redis_client: RedisClient,
        event_bus: EventBus,
        poll_interval: float = 900.0,  # 15 min default
    ):
        self._redis = redis_client
        self._bus = event_bus
        self.poll_interval = poll_interval

    def run(self):
        log.info("Slickdeals poller started")
        while True:
            terms = self._load_search_terms()
            if not terms:
                time.sleep(self.poll_interval)
                continue

            for term in terms:
                if not term.enabled:
                    continue
                try:
                    self._poll_term(term)
                except Exception as e:
                    log.error(f"Slickdeals poll error for '{term.query}': {e}")
                time.sleep(5)

            time.sleep(self.poll_interval)

    def poll_once(self, query: str) -> list[dict]:
        """Fetch and parse Slickdeals RSS for a query. Returns deal dicts."""
        with httpx.Client() as client:
            resp = client.get(
                SLICKDEALS_RSS,
                params={"searchin": "deals", "q": query, "rss": "1"},
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0 (compatible; CraneFeed/1.0)"},
            )
            resp.raise_for_status()

        root = ET.fromstring(resp.text)
        deals = []

        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            description = item.findtext("description") or ""
            pub_date = item.findtext("pubDate") or ""

            if not title or not link:
                continue

            # Extract price from title first, then description
            price = _parse_price(title) or _parse_price(description)
            retailer = _extract_retailer(title, link)

            # Use a stable ID derived from the slickdeals URL
            deal_id = "sd-" + md5(link.encode()).hexdigest()[:12]

            deals.append({
                "deal_id": deal_id,
                "title": title,
                "link": link,
                "price": price,
                "retailer": retailer,
                "pub_date": pub_date,
                "description": description[:500],
            })

        log.info(f"Slickdeals '{query}': {len(deals)} deals")
        return deals

    def _poll_term(self, term: SearchTerm):
        deals = self.poll_once(term.query)
        now = datetime.utcnow().isoformat()

        for deal in deals:
            deal_id = deal["deal_id"]
            key = f"crane:feed:deals:{deal_id}"

            # Check if we've already seen this deal
            existing = self._redis.get_model(key, EbayListing)

            listing = EbayListing(
                epid=deal_id,
                title=deal["title"],
                link=deal["link"],
                price=deal["price"],
                price_raw=f"${deal['price']:.2f}" if deal["price"] else "",
                buy_it_now=True,
                seller=SellerInfo(name=deal["retailer"]),
                search_term=term.query,
                first_seen=existing.first_seen if existing else now,
                last_seen=now,
            )

            self._redis.put_model(key, listing, ttl=30 * 86400)

            # Index under deals namespace
            self._redis.add_to_index(
                f"crane:feed:deals:index:{term.query}",
                deal_id,
            )

            # Classify and notify
            if classify_listing(term.query, listing.title):
                is_new = existing is None
                if is_new and listing.price > 0:
                    notify_listing(
                        listing,
                        reason=f"[Slickdeals] {deal['retailer']} deal: ${listing.price:.2f}",
                    )

    def _load_search_terms(self) -> list[SearchTerm]:
        term_ids = self._redis.get_index("crane:manager:terms:index")
        terms = []
        for tid in term_ids:
            t = self._redis.get_model(f"crane:manager:terms:{tid}", SearchTerm)
            if t:
                terms.append(t)
        return terms
