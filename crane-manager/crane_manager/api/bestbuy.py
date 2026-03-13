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

    return {
        "thread_status": _decode(thread_status),
        "main_version": _decode(main_version),
        "alive": alive,
        "heartbeat_age_seconds": heartbeat_age,
        "polls_ok": int(_decode(heartbeat.get(b"polls_ok", b"0") or heartbeat.get("polls_ok", "0"))) if heartbeat else 0,
        "polls_empty": int(_decode(heartbeat.get(b"polls_empty", b"0") or heartbeat.get("polls_empty", "0"))) if heartbeat else 0,
        "sku_count": int(_decode(heartbeat.get(b"sku_count", b"0") or heartbeat.get("sku_count", "0"))) if heartbeat else 0,
    }


@router.get("/gaps")
def poll_gaps(threshold: float = 5.0):
    """Detect gaps in the BB monitor poll log. Returns gaps > threshold seconds."""
    rc = get_redis()
    raw = rc.client.lrange("crane:feed:bestbuy:poll_log", 0, -1)
    if not raw:
        return {"gaps": [], "total_entries": 0}

    entries = []
    for item in raw:
        text = item.decode() if isinstance(item, bytes) else item
        parts = text.split(":")
        if len(parts) >= 3:
            entries.append({"epoch": float(parts[0]), "skus": int(parts[1]), "status": parts[2]})

    gaps = []
    for i in range(1, len(entries)):
        delta = entries[i]["epoch"] - entries[i - 1]["epoch"]
        if delta > threshold:
            gaps.append({
                "start": datetime.utcfromtimestamp(entries[i - 1]["epoch"]).isoformat() + "Z",
                "end": datetime.utcfromtimestamp(entries[i]["epoch"]).isoformat() + "Z",
                "gap_seconds": round(delta, 1),
            })

    return {"gaps": gaps, "total_entries": len(entries)}


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
