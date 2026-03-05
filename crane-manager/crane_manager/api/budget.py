"""Budget management endpoints.

Daily budget is the authoritative spend control consumed by crane-engine's
budget gate. Uses Redis hash for atomic increment operations.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter

from crane_shared.models import BudgetState
from crane_manager.deps import get_redis

router = APIRouter()


@router.get("/today", response_model=BudgetState)
def get_today_budget():
    rc = get_redis()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"crane:manager:budget:{today}"
    raw = rc.client.hgetall(key)
    if not raw:
        return BudgetState(date=today)
    decoded = {k.decode(): v.decode() for k, v in raw.items()}
    return BudgetState(
        date=today,
        daily_limit=float(decoded.get("daily_limit", 0)),
        spent=float(decoded.get("spent", 0)),
        remaining=float(decoded.get("daily_limit", 0)) - float(decoded.get("spent", 0)),
        trade_count=int(decoded.get("trade_count", 0)),
        max_trades=int(decoded.get("max_trades", 0)),
        frozen=decoded.get("frozen", "False") == "True",
    )


@router.post("/configure")
def configure_budget(daily_limit: float, max_trades: int = 0):
    rc = get_redis()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"crane:manager:budget:{today}"
    rc.client.hset(key, mapping={
        "daily_limit": str(daily_limit),
        "max_trades": str(max_trades),
        "date": today,
    })
    rc.client.expire(key, 86400 * 2)  # TTL 2 days
    return {"configured": today, "daily_limit": daily_limit, "max_trades": max_trades}


@router.post("/freeze")
def freeze_budget():
    rc = get_redis()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"crane:manager:budget:{today}"
    rc.client.hset(key, "frozen", "True")
    return {"frozen": True, "date": today}


@router.post("/unfreeze")
def unfreeze_budget():
    rc = get_redis()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"crane:manager:budget:{today}"
    rc.client.hset(key, "frozen", "False")
    return {"frozen": False, "date": today}
