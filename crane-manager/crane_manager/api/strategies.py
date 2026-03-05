"""Strategy CRUD endpoints.

Strategies are persisted in Redis and consumed by crane-engine.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException

from crane_shared.models import Strategy
from crane_manager.deps import get_redis

router = APIRouter()


@router.get("/", response_model=list[Strategy])
def list_strategies():
    rc = get_redis()
    ids = rc.get_index("crane:manager:strategies:index")
    strategies = []
    for sid in sorted(ids):
        s = rc.get_model(f"crane:manager:strategies:{sid}", Strategy)
        if s:
            strategies.append(s)
    return strategies


@router.get("/{strategy_id}", response_model=Strategy)
def get_strategy(strategy_id: str):
    rc = get_redis()
    s = rc.get_model(f"crane:manager:strategies:{strategy_id}", Strategy)
    if not s:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return s


@router.post("/", response_model=Strategy)
def create_strategy(strategy: Strategy):
    rc = get_redis()
    strategy.created_at = datetime.utcnow().isoformat()
    strategy.updated_at = strategy.created_at
    rc.put_model(f"crane:manager:strategies:{strategy.strategy_id}", strategy)
    rc.add_to_index("crane:manager:strategies:index", strategy.strategy_id)
    return strategy


@router.put("/{strategy_id}", response_model=Strategy)
def update_strategy(strategy_id: str, strategy: Strategy):
    rc = get_redis()
    existing = rc.get_model(f"crane:manager:strategies:{strategy_id}", Strategy)
    if not existing:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy.strategy_id = strategy_id
    strategy.updated_at = datetime.utcnow().isoformat()
    rc.put_model(f"crane:manager:strategies:{strategy_id}", strategy)
    return strategy


@router.delete("/{strategy_id}")
def delete_strategy(strategy_id: str):
    rc = get_redis()
    rc.client.delete(f"crane:manager:strategies:{strategy_id}")
    rc.client.srem("crane:manager:strategies:index", strategy_id)
    return {"deleted": strategy_id}
