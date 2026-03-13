"""Best Buy product monitoring endpoints.

Manages tracked Best Buy products stored in Redis by crane-feed.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from crane_manager.deps import get_redis

router = APIRouter()

BB_PRODUCTS_KEY = "crane:feed:bestbuy:products"


class AddProductRequest(BaseModel):
    url: str
    name: str = ""
    target_price: float = 0.0


def _extract_sku(url: str) -> str | None:
    m = re.search(r"skuId=(\d+)", url)
    if m:
        return m.group(1)
    m = re.search(r"/site/[^/]+/(\d+)\.p", url)
    if m:
        return m.group(1)
    m = re.search(r"/(\d+)\.p", url)
    if m:
        return m.group(1)
    # Numeric-only path segment (e.g. bestbuy.com/site/6451686.p)
    m = re.search(r"/(\d{5,})(?:\?|$)", url)
    if m:
        return m.group(1)
    return None


@router.get("/")
def list_products():
    """List all tracked Best Buy products."""
    rc = get_redis()
    raw = rc.client.hgetall(BB_PRODUCTS_KEY)
    products = []
    for pid, data in raw.items():
        try:
            p = json.loads(data)
            # Attach last known price
            price_raw = rc.client.get(f"crane:feed:bestbuy:price:{pid}")
            p["last_price"] = float(price_raw) if price_raw else None
            products.append(p)
        except (json.JSONDecodeError, ValueError):
            continue
    return products


@router.post("/")
def add_product(req: AddProductRequest):
    """Add a Best Buy product to monitor."""
    product_id = _extract_sku(req.url)
    if not product_id:
        raise HTTPException(status_code=400, detail="Could not extract SKU from URL")

    rc = get_redis()
    product = {
        "product_id": product_id,
        "url": req.url,
        "name": req.name,
        "target_price": req.target_price,
        "added_at": datetime.utcnow().isoformat(),
    }
    rc.client.hset(BB_PRODUCTS_KEY, product_id, json.dumps(product))
    return product


@router.delete("/{product_id}")
def remove_product(product_id: str):
    """Stop monitoring a Best Buy product."""
    rc = get_redis()
    removed = rc.client.hdel(BB_PRODUCTS_KEY, product_id)
    if not removed:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    return {"status": "removed", "product_id": product_id}


def _decode(val):
    return val.decode() if isinstance(val, bytes) else val


@router.get("/status")
def monitor_status():
    """Get Best Buy monitor liveness, heartbeat, and thread status."""
    rc = get_redis()
    thread_status = rc.client.get("crane:feed:bestbuy:thread_status")
    main_version = rc.client.get("crane:feed:main_version")
    heartbeat = rc.client.hgetall("crane:feed:bestbuy:heartbeat")

    alive = False
    heartbeat_age = None
    if heartbeat:
        epoch_raw = heartbeat.get(b"last_poll_epoch") or heartbeat.get("last_poll_epoch")
        if epoch_raw:
            epoch = float(epoch_raw)
            heartbeat_age = round(time.time() - epoch, 1)
            alive = heartbeat_age < 120

    def _int(key):
        raw = heartbeat.get(key.encode(), heartbeat.get(key, b"0"))
        return int(_decode(raw)) if raw else 0

    def _str(key):
        raw = heartbeat.get(key.encode(), heartbeat.get(key, b""))
        return _decode(raw) if raw else None

    return {
        "thread_status": _decode(thread_status),
        "main_version": _decode(main_version),
        "alive": alive,
        "heartbeat_age_seconds": heartbeat_age,
        "polls_ok": _int("polls_ok") if heartbeat else 0,
        "polls_fail": _int("polls_fail") if heartbeat else 0,
        "uptime_seconds": _int("uptime_seconds") if heartbeat else 0,
        "effective_rps": _str("effective_rps") if heartbeat else None,
    }


@router.get("/{product_id}/history")
def get_price_history(product_id: str, limit: int = 100):
    """Get price history for a tracked Best Buy product."""
    rc = get_redis()
    key = f"crane:feed:bestbuy:history:{product_id}"
    raw = rc.client.lrange(key, 0, limit - 1)
    points = []
    for item in reversed(raw):
        try:
            data = json.loads(item)
            points.append(data)
        except (json.JSONDecodeError, ValueError):
            continue
    return points
