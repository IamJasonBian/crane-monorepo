"""Best Buy product monitor.

Tracks specific Best Buy product URLs, checks price/availability,
and sends Slack alerts with direct links.

No API key required — scrapes product pages for JSON-LD pricing data.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from typing import Optional

import httpx

from crane_shared.redis_client import RedisClient
from crane_feed.notifier import notify_listing
from crane_shared.models import EbayListing, SellerInfo

log = logging.getLogger("crane-feed.bestbuy")

# Best Buy add-to-cart URL format
BESTBUY_CART_URL = "https://www.bestbuy.com/cart/add?skuId={sku}"

# Redis key prefix for tracked products
BB_PRODUCTS_KEY = "crane:feed:bestbuy:products"
BB_PRICE_KEY = "crane:feed:bestbuy:price:{product_id}"

# Headers to mimic a real browser
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}


def _extract_sku_from_url(url: str) -> Optional[str]:
    """Extract SKU/product ID from Best Buy URL.

    Handles formats like:
      /product/.../JCQ6HQXJVH
      /site/.../1234567.p?skuId=1234567
    """
    # Try /product/{slug}/{sku} format
    m = re.search(r"/product/[^/]+/([A-Za-z0-9]+)(?:\?|$)", url)
    if m:
        return m.group(1)
    # Try skuId query param
    m = re.search(r"skuId=(\d+)", url)
    if m:
        return m.group(1)
    # Try /site/{slug}/{sku}.p format
    m = re.search(r"/site/[^/]+/(\d+)\.p", url)
    if m:
        return m.group(1)
    # Fallback: last path segment
    m = re.search(r"/([A-Za-z0-9]+)(?:\?|$)", url)
    if m:
        return m.group(1)
    return None


def _extract_numeric_sku(html: str) -> Optional[str]:
    """Try to find numeric SKU ID from page HTML for cart link."""
    # Look for skuId in various places
    m = re.search(r'"skuId"\s*:\s*"(\d+)"', html)
    if m:
        return m.group(1)
    m = re.search(r'data-sku-id="(\d+)"', html)
    if m:
        return m.group(1)
    return None


def _extract_price_from_html(html: str) -> Optional[float]:
    """Extract price from Best Buy product page HTML.

    Tries JSON-LD structured data first, then falls back to price patterns.
    """
    # Try JSON-LD (most reliable)
    for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL):
        try:
            data = json.loads(m.group(1))
            # Handle both single object and array
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") == "Product":
                    offers = item.get("offers", {})
                    if isinstance(offers, list):
                        offers = offers[0] if offers else {}
                    price = offers.get("price")
                    if price:
                        return float(price)
        except (json.JSONDecodeError, ValueError, KeyError):
            continue

    # Fallback: look for priceWithEhf or customer-price patterns
    m = re.search(r'"currentPrice"\s*:\s*([\d.]+)', html)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass

    # Last resort: dollar amount near "price" context
    m = re.search(r'class="priceView[^"]*"[^>]*>\s*\$\s*([\d,]+\.?\d*)', html)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            pass

    return None


def _check_availability(html: str) -> bool:
    """Check if the product appears to be in stock."""
    if '"ADD_TO_CART"' in html or '"Add to Cart"' in html:
        return True
    if '"SOLD_OUT"' in html or '"Sold Out"' in html:
        return False
    # Default to unknown/available
    return True


class BestBuyMonitor:
    """Monitors specific Best Buy product URLs for price changes."""

    def __init__(
        self,
        redis_client: RedisClient,
        poll_interval: float = 1800.0,  # 30 min default
    ):
        self._redis = redis_client
        self.poll_interval = poll_interval

    def add_product(self, url: str, name: str = "", target_price: float = 0.0):
        """Add a product URL to monitor."""
        product_id = _extract_sku_from_url(url)
        if not product_id:
            log.error(f"Could not extract SKU from URL: {url}")
            return

        product = {
            "product_id": product_id,
            "url": url,
            "name": name,
            "target_price": target_price,
            "added_at": datetime.utcnow().isoformat(),
        }
        self._redis.client.hset(BB_PRODUCTS_KEY, product_id, json.dumps(product))
        log.info(f"Tracking Best Buy product: {product_id} ({name})")

    def remove_product(self, product_id: str):
        """Stop monitoring a product."""
        self._redis.client.hdel(BB_PRODUCTS_KEY, product_id)
        log.info(f"Stopped tracking Best Buy product: {product_id}")

    def list_products(self) -> list[dict]:
        """List all tracked products."""
        raw = self._redis.client.hgetall(BB_PRODUCTS_KEY)
        products = []
        for pid, data in raw.items():
            try:
                p = json.loads(data)
                products.append(p)
            except json.JSONDecodeError:
                continue
        return products

    def run(self):
        """Main polling loop."""
        log.info("Best Buy monitor started")
        while True:
            products = self.list_products()
            if not products:
                log.debug("No Best Buy products to monitor")
                time.sleep(self.poll_interval)
                continue

            for product in products:
                try:
                    self._check_product(product)
                except Exception as e:
                    log.error(f"Best Buy check failed for {product.get('product_id')}: {e}")
                time.sleep(10)  # Be polite between requests

            time.sleep(self.poll_interval)

    def _check_product(self, product: dict):
        product_id = product["product_id"]
        url = product["url"]
        name = product.get("name", product_id)
        target_price = product.get("target_price", 0)

        log.info(f"Checking Best Buy product: {name} ({product_id})")

        try:
            with httpx.Client(follow_redirects=True) as client:
                resp = client.get(url, headers=HEADERS, timeout=30)
                resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            log.warning(f"Best Buy returned {e.response.status_code} for {product_id}")
            return
        except httpx.TimeoutException:
            log.warning(f"Best Buy timeout for {product_id}")
            return

        html = resp.text
        price = _extract_price_from_html(html)
        available = _check_availability(html)
        numeric_sku = _extract_numeric_sku(html)

        # Build cart link
        cart_sku = numeric_sku or product_id
        cart_link = BESTBUY_CART_URL.format(sku=cart_sku)

        price_key = BB_PRICE_KEY.format(product_id=product_id)
        previous_raw = self._redis.client.get(price_key)
        previous_price = float(previous_raw) if previous_raw else None

        now = datetime.utcnow().isoformat()

        # Store current price
        if price:
            self._redis.client.set(price_key, str(price), ex=7 * 86400)

            # Store price history
            history_key = f"crane:feed:bestbuy:history:{product_id}"
            self._redis.client.lpush(
                history_key,
                json.dumps({"price": price, "available": available, "timestamp": now}),
            )
            self._redis.client.ltrim(history_key, 0, 499)

        # Check previous availability
        avail_key = f"crane:feed:bestbuy:avail:{product_id}"
        prev_avail_raw = self._redis.client.get(avail_key)
        was_available = prev_avail_raw and prev_avail_raw.decode() == "1"
        self._redis.client.set(avail_key, "1" if available else "0", ex=7 * 86400)

        # Decide if we should alert
        should_alert = False
        reason = ""

        if available and not was_available:
            # Just came back in stock!
            should_alert = True
            price_str = f"${price:.2f}" if price else "unknown price"
            reason = f"[Best Buy] 🚨 BACK IN STOCK! {price_str}"
        elif price and previous_price is None:
            # First time seeing this product
            should_alert = True
            reason = f"[Best Buy] Now tracking: ${price:.2f}"
        elif price and previous_price and price < previous_price:
            # Price dropped
            should_alert = True
            reason = f"[Best Buy] Price drop: ${previous_price:.2f} → ${price:.2f}"
        elif price and target_price and price <= target_price:
            # Hit target price
            should_alert = True
            reason = f"[Best Buy] Target hit! ${price:.2f} (target: ${target_price:.2f})"

        if should_alert:
            # Create a listing-like object for the notifier
            listing = EbayListing(
                epid=f"bb-{product_id}",
                title=name,
                link=url,
                price=price or 0,
                price_raw=f"${price:.2f}" if price else "",
                buy_it_now=True,
                condition="Refurbished" if "refurbished" in name.lower() else "",
                seller=SellerInfo(name="Best Buy"),
                search_term="bestbuy-monitor",
                first_seen=now,
                last_seen=now,
            )
            _notify_bestbuy(listing, reason=reason, cart_link=cart_link)

        status = "in stock" if available else "sold out"
        price_str = f"${price:.2f}" if price else "unknown"
        log.info(f"Best Buy {product_id}: {price_str}, {status}")


def _notify_bestbuy(listing: EbayListing, reason: str, cart_link: str) -> bool:
    """Send a Slack notification with cart link for Best Buy products."""
    import os

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        log.debug("No SLACK_WEBHOOK_URL set, skipping notification")
        return False

    text = (
        f":shopping_trolley: *{reason}*\n"
        f"*{listing.title}*\n"
        f"Price: *${listing.price:.2f}*"
        + (f" | {listing.condition}" if listing.condition else "")
        + f"\n<{listing.link}|View on Best Buy>"
        + f"  |  <{cart_link}|:point_right: Add to Cart>"
    )

    payload = {"text": text, "unfurl_links": False}

    try:
        with httpx.Client() as client:
            resp = client.post(webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
        log.info(f"Slack notification sent for Best Buy {listing.epid}")
        return True
    except Exception as e:
        log.error(f"Slack notification failed: {e}")
        return False
