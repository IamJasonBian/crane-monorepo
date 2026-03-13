"""Best Buy product monitor.

Tracks specific Best Buy product URLs, checks price/availability
using the Best Buy Products API, and sends Slack alerts with direct cart links.

Uses the official Best Buy Developer API (api.bestbuy.com) for near real-time
pricing and availability data. Requires a BESTBUY_API_KEY env var.

Polls at ~2 requests/second using batch SKU queries. Heartbeat written every
30s to prove liveness. Daily full report at noon UTC. History written on
price/availability changes only.
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
BESTBUY_API_URL = "https://api.bestbuy.com/v1/products"

# Redis key prefixes
BB_PRODUCTS_KEY = "crane:feed:bestbuy:products"
BB_PRICE_KEY = "crane:feed:bestbuy:price:{product_id}"
BB_HEARTBEAT_KEY = "crane:feed:bestbuy:heartbeat"

HEARTBEAT_INTERVAL = 30  # write heartbeat every 30s
HEARTBEAT_TTL = 7 * 86400  # 7 days — keep full uptime history
DAILY_REPORT_HOUR = 12  # noon UTC


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


def _fetch_products_batch(skus: list[str], api_key: str, client: httpx.Client) -> dict[str, dict]:
    """Fetch multiple products in a single API call using sku in(...) filter.

    Returns dict mapping sku -> product data, or empty dict on failure.
    """
    sku_filter = ",".join(skus)
    url = f"{BESTBUY_API_URL}(sku in({sku_filter}))"
    params = {
        "apiKey": api_key,
        "show": "sku,name,salePrice,regularPrice,onSale,orderable,inStoreAvailability,onlineAvailability,condition",
        "format": "json",
        "pageSize": len(skus),
    }

    for attempt in range(3):
        try:
            resp = client.get(url, params=params, timeout=10)
            if resp.status_code == 403:
                # Rate limited — back off and retry
                time.sleep(1 + attempt)
                continue
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            log.warning(f"Best Buy batch API failed (attempt {attempt + 1}): {e}")
            time.sleep(1 + attempt)
    else:
        return {}

    results = {}
    for product in data.get("products", []):
        sku = str(product.get("sku", ""))
        price = product.get("salePrice") or product.get("regularPrice")
        available = (
            product.get("onlineAvailability", False)
            or product.get("inStoreAvailability", False)
        )
        orderable = product.get("orderable", "")

        results[sku] = {
            "price": float(price) if price else None,
            "available": bool(available) or orderable == "Available",
            "title": product.get("name", ""),
            "condition": product.get("condition", ""),
            "on_sale": product.get("onSale", False),
            "regular_price": product.get("regularPrice"),
        }

    return results


class BestBuyMonitor:
    """Monitors specific Best Buy product URLs for price changes at ~2 rps."""

    def __init__(
        self,
        redis_client: RedisClient,
        poll_interval: float = 1.0,  # 1 rps (BB API free tier limit)
    ):
        self._redis = redis_client
        self.poll_interval = poll_interval
        self._api_key = os.environ.get("BESTBUY_API_KEY", "")
        # In-memory counters — proof of polling frequency
        self._polls_ok = 0
        self._polls_fail = 0
        self._started_at = time.time()
        self._last_heartbeat_write: float = 0
        self._last_daily_report_date: str = ""

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

    def _maybe_write_heartbeat(self):
        """Write heartbeat every HEARTBEAT_INTERVAL seconds (not every poll)."""
        now = time.time()
        if (now - self._last_heartbeat_write) < HEARTBEAT_INTERVAL:
            return

        uptime = now - self._started_at
        total_polls = self._polls_ok + self._polls_fail
        rps = total_polls / uptime if uptime > 0 else 0

        self._redis.client.hset(BB_HEARTBEAT_KEY, mapping={
            "last_poll_ts": datetime.now(timezone.utc).isoformat(),
            "last_poll_epoch": f"{now:.3f}",
            "polls_ok": str(self._polls_ok),
            "polls_fail": str(self._polls_fail),
            "uptime_seconds": str(int(uptime)),
            "effective_rps": f"{rps:.2f}",
        })
        self._redis.client.expire(BB_HEARTBEAT_KEY, HEARTBEAT_TTL)
        self._last_heartbeat_write = now

    def _maybe_daily_report(self, results: dict[str, dict]):
        """Send a full poll report to Slack once per day at noon UTC."""
        utc_now = datetime.now(timezone.utc)
        today = utc_now.strftime("%Y-%m-%d")

        if today == self._last_daily_report_date:
            return
        if utc_now.hour != DAILY_REPORT_HOUR:
            return

        self._last_daily_report_date = today

        uptime = time.time() - self._started_at
        total = self._polls_ok + self._polls_fail
        rps = total / uptime if uptime > 0 else 0
        uptime_hrs = uptime / 3600

        lines = [
            f"*Daily BB Monitor Report* ({today})",
            f"Uptime: {uptime_hrs:.1f}h | Polls: {total:,} ({rps:.2f} rps) | "
            f"Failures: {self._polls_fail}",
        ]
        for sku, data in results.items():
            price = data.get("price")
            avail = "IN STOCK" if data.get("available") else "Sold Out"
            price_str = f"${price:.2f}" if price else "?"
            lines.append(f"  SKU {sku}: {price_str} — {avail}")

        _slack_log("\n".join(lines))

    def run(self):
        """Main polling loop — runs at ~2 rps with batch queries."""
        if not self._api_key:
            msg = "BESTBUY_API_KEY not set — Best Buy monitor disabled."
            log.error(msg)
            _slack_log(msg)
            return

        products = self.list_products()
        _slack_log(
            f"Best Buy monitor started (2 rps, batch API). "
            f"Tracking {len(products)} products."
        )
        log.info("Best Buy monitor started (poll_interval=%.1fs)", self.poll_interval)

        # Use a persistent HTTP client for connection reuse
        with httpx.Client(http2=False) as client:
            while True:
                products = self.list_products()
                if not products:
                    time.sleep(5)
                    continue

                skus = [p["product_id"] for p in products]
                results = _fetch_products_batch(skus, self._api_key, client)

                if not results:
                    self._polls_fail += 1
                    self._maybe_write_heartbeat()
                    time.sleep(5)
                    continue

                self._polls_ok += 1

                for product in products:
                    product_id = product["product_id"]
                    if product_id in results:
                        try:
                            self._process_result(product, results[product_id])
                        except Exception as e:
                            log.error(f"Error processing {product_id}: {e}")

                self._maybe_write_heartbeat()
                self._maybe_daily_report(results)
                time.sleep(self.poll_interval)

    def _process_result(self, product: dict, result: dict):
        """Process a single product result — only write to Redis on changes."""
        product_id = product["product_id"]
        url = product["url"]
        name = product.get("name", product_id)
        target_price = product.get("target_price", 0)

        price = result["price"]
        available = result["available"]
        now = datetime.now(timezone.utc).isoformat()
        cart_link = BESTBUY_CART_URL.format(sku=product_id)

        # Read previous state
        price_key = BB_PRICE_KEY.format(product_id=product_id)
        previous_raw = self._redis.client.get(price_key)
        previous_price = float(previous_raw) if previous_raw else None

        avail_key = f"crane:feed:bestbuy:avail:{product_id}"
        prev_avail_raw = self._redis.client.get(avail_key)
        was_available = prev_avail_raw and (
            prev_avail_raw.decode() if isinstance(prev_avail_raw, bytes) else prev_avail_raw
        ) == "1"

        # Detect changes
        price_changed = price and previous_price and price != previous_price
        avail_changed = available != was_available
        is_first = previous_price is None

        # Check if we have history — if so, this is a redeploy, not truly first detection
        history_key = f"crane:feed:bestbuy:history:{product_id}"
        has_history = self._redis.client.llen(history_key) > 0

        # Only write to Redis when something changed or first detection
        if price_changed or avail_changed or is_first:
            if price:
                self._redis.client.set(price_key, str(price), ex=7 * 86400)
            self._redis.client.set(avail_key, "1" if available else "0", ex=7 * 86400)

            # Write history entry on every change
            self._redis.client.lpush(
                history_key,
                json.dumps({"price": price, "available": available, "timestamp": now}),
            )
            self._redis.client.ltrim(history_key, 0, 999)

        # Decide if we should alert
        should_alert = False
        reason = ""

        if available and not was_available:
            should_alert = True
            price_str = f"${price:.2f}" if price else "unknown price"
            reason = f"[Best Buy] BACK IN STOCK! {price_str}"
        elif price and is_first and not has_history:
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
            log.info(f"ALERT {product_id}: {reason}")
        elif avail_changed or price_changed:
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
