"""Order intent endpoints.

Reads order intents written by crane-engine from Redis.
"""

from __future__ import annotations

from fastapi import APIRouter

from crane_shared.models import OrderIntent
from crane_manager.deps import get_redis

router = APIRouter()


@router.get("/")
def list_orders():
    rc = get_redis()
    ids = rc.get_index("crane:engine:intents:index")
    orders = []
    for oid in sorted(ids, reverse=True):  # newest first
        o = rc.get_model(f"crane:engine:intents:{oid}", OrderIntent)
        if o:
            orders.append(o.model_dump())
    return orders
