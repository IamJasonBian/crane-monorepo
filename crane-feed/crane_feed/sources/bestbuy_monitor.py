"""Best Buy product monitor.

Tracks specific Best Buy product URLs, checks price/availability
using the Best Buy Products API, and sends Slack alerts with direct cart links.

Uses the official Best Buy Developer API (api.bestbuy.com) for near real-time
pricing and availability data. Requires a BESTBUY_API_KEY env var.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from crane_shared.redis_client import RedisClient
from crane_shared.models import EbayListing, SellerInfo

log = logging.getLogger("crane-feed.bestbuy")

# Best Buy add-to-cart URL format
BESTBUY_CART_URL = "https://www.bestbuy.com/cart/add?skuId={sku}"

# Best Buy Products API
BESTBUY_API_URL = "https://api.bestbuy.com/v1/products/{sku}.json"

# Redis key prefixes
BB_PRODUCTS_KEY = "crane:feed:bestbuy:products"
BB_PRICE_KEY = "crane:feed:bestbuy:price:{product_id}"


def _extract_sku_from_url(url: str) -> Optional[str]:
    """Extract SKU/product ID from Best Buy URL."""
    m = re.search(r"/product/[^/]+/([A-Za-z0-9]+)(?:\?|$)", url)
    if m:
        return m.group(1)
    m = re.search(r"skuId=(\d+)", url)
    if m:
        return m.group(1)
    m = re.search(r"/site/[^/]+/(\d+)\.p", url)
    if m:
        return m.group(1)
    m = re.search(r"/([A-Za-z0-9]+)(?:\?|$)", url)
    if m:
        return m.group(1)
    return None


def _fetch_product_api(sku: str, api_key: str, max_retries: int = 3) -> Optional[dict]:
    """Fetch product data from the Best Buy Products API.

    Returns dict with keys: price, available, title, condition, on_sale, regular_price
    or None if all retries fail.
    """
    url = BESTBUY_API_URL.format(sku=sku)
    params = {
        "apiKey": api_key,
        "show": "sku,name,salePrice,regularPrice,onSale,orderable,inStoreAvailability,onlineAvailability,condition",
        "format": "json",
    }

    for attempt in range(1, max_retries + 1):
        try:
            with httpx.Client() as client:
                resp = client.get(url, params=params, timeout=15)
                if resp.status_code == 404:
                    log.warning(f"Product {sku} not found in Best Buy API (404)")
                    return None
                resp.raise_for_status()
                data = resp.json()

            price = data.get("salePrice") or data.get("regularPrice")
            available = (
                data.get("onlineAvailability", False)
                or data.get("inStoreAvailability", False)
            )
            orderable = data.get("orderable", "")

            return {
                "price": float(price) if price else None,
                "available": bool(available) or orderable == "Available",
                "title": data.get("name", ""),
                "condition": data.get("condition", ""),
                "on_sale": data.get("onSale", False),
                "regular_price": data.get("regularPrice"),
            }

        except httpx.HTTPStatusError as e:
            log.warning(f"Best Buy API error (attempt {attempt}/{max_retries}): {e}")
            time.sleep(2 * attempt)
        except Exception as e:
            log.warning(f"Best Buy API request failed (attempt {attempt}/{max_retries}): {e}")
            time.sleep(2 * attempt)

    return None


class BestBuyMonitor:
    """Monitors specific Best Buy product URLs for price changes."""

    def __init__(
        self,
        redis_client: RedisClient,
        poll_interval: float = 300.0,
    ):
        self._redis = redis_client
        self.poll_interval = poll_interval
        self._api_key = os.environ.get("BESTBUY_API_KEY", "")

    def add_product(self, sku: str, name: str = "", target_price: float = 0.0, url: str = ""):
        """Add a product by numeric SKU to monitor."""
        product = {
            "product_id": str(sku),
            "url": url or f"https://www.bestbuy.com/site/{sku}.p?skuId={sku}",
            "name": name,
            "target_price": target_price,
            "added_at": datetime.now(timezone.utc).isoformat(),
        }
        self._redis.client.hset(BB_PRODUCTS_KEY, str(sku), json.dumps(product))
        log.info(f"Tracking Best Buy product: {sku} ({name})")

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
        log.info("Best Buy monitor started (poll_interval=%ds)", self.poll_interval)

        if not self._api_key:
            msg = "BESTBUY_API_KEY not set — Best Buy monitor disabled."
            log.error(msg)
            _slack_log(msg)
            return

        _slack_log(f"Best Buy monitor started (API mode). Tracking {len(self.list_products())} products. Poll: {self.poll_interval}s")

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
                    msg = f"Best Buy check failed for {product.get('product_id')}: {e}"
                    log.error(msg)
                    _slack_log(msg)
                time.sleep(5)  # Small delay between API calls

            time.sleep(self.poll_interval)

    def _check_product(self, product: dict):
        product_id = product["product_id"]
        url = product["url"]
        name = product.get("name", product_id)
        target_price = product.get("target_price", 0)

        log.info(f"Checking Best Buy product: {name} ({product_id})")

        result = _fetch_product_api(product_id, self._api_key)
        if result is None:
            msg = f"Could not fetch {product_id} from API after retries"
            log.warning(msg)
            _slack_log(msg)
            return

        price = result["price"]
        available = result["available"]

        log.info(f"API response {product_id}: price=${price}, available={available}, "
                 f"on_sale={result.get('on_sale')}")

        now = datetime.now(timezone.utc).isoformat()
        cart_link = BESTBUY_CART_URL.format(sku=product_id)

        price_key = BB_PRICE_KEY.format(product_id=product_id)
        previous_raw = self._redis.client.get(price_key)
        previous_price = float(previous_raw) if previous_raw else None

        # Store current price
        if price:
            self._redis.client.set(price_key, str(price), ex=7 * 86400)

            history_key = f"crane:feed:bestbuy:history:{product_id}"
            self._redis.client.lpush(
                history_key,
                json.dumps({"price": price, "available": available, "timestamp": now}),
            )
            self._redis.client.ltrim(history_key, 0, 499)

        # Check previous availability
        avail_key = f"crane:feed:bestbuy:avail:{product_id}"
        prev_avail_raw = self._redis.client.get(avail_key)
        was_available = prev_avail_raw and (
            prev_avail_raw.decode() if isinstance(prev_avail_raw, bytes) else prev_avail_raw
        ) == "1"
        self._redis.client.set(avail_key, "1" if available else "0", ex=7 * 86400)

        # Decide if we should alert
        should_alert = False
        reason = ""

        if available and not was_available:
            should_alert = True
            price_str = f"${price:.2f}" if price else "unknown price"
            reason = f"[Best Buy] BACK IN STOCK! {price_str}"
        elif price and previous_price is None:
            should_alert = True
            reason = f"[Best Buy] Now tracking: ${price:.2f}"
        elif price and previous_price and price < previous_price:
            should_alert = True
            reason = f"[Best Buy] Price drop: ${previous_price:.2f} -> ${price:.2f}"
        elif price and target_price and price <= target_price:
            should_alert = True
            reason = f"[Best Buy] Target hit! ${price:.2f} (target: ${target_price:.2f})"

        if should_alert:
            listing = EbayListing(
                epid=f"bb-{product_id}",
                title=name,
                link=url,
                price=price or 0,
                price_raw=f"${price:.2f}" if price else "",
                buy_it_now=True,
                condition=result.get("condition", ""),
                seller=SellerInfo(name="Best Buy"),
                search_term="bestbuy-monitor",
                first_seen=now,
                last_seen=now,
            )
            _notify_bestbuy(listing, reason=reason, cart_link=cart_link, available=available)

        status = "IN STOCK" if available else "out of stock"
        price_str = f"${price:.2f}" if price else "unknown"
        log.info(f"Best Buy {product_id}: {price_str}, {status}")


def _slack_log(message: str):
    """Send a diagnostic message to Slack for remote debugging."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    try:
        httpx.post(webhook_url, json={"text": f":gear: `[BB Monitor {now}]` {message}"}, timeout=10)
    except Exception:
        pass


def _notify_bestbuy(listing: EbayListing, reason: str, cart_link: str, available: bool = False) -> bool:
    """Send a Slack notification with cart link for Best Buy products."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        log.debug("No SLACK_WEBHOOK_URL set, skipping notification")
        return False

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    stock_indicator = ":white_check_mark: In Stock" if available else ":x: Out of Stock"

    text = (
        f":shopping_trolley: *{reason}*\n"
        f"*{listing.title}*\n"
        f"Price: *${listing.price:.2f}*"
        + (f" | {listing.condition}" if listing.condition else "")
        + f"\nStatus: {stock_indicator}"
        + f"\nChecked: {now}"
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
