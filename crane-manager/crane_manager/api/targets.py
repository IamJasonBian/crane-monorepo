"""Watch target CRUD endpoints.

Targets define what symbols to monitor and at what price thresholds.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException

from crane_shared.models import WatchTarget
from crane_manager.deps import get_redis

router = APIRouter()


@router.get("/", response_model=list[WatchTarget])
def list_targets():
    rc = get_redis()
    ids = rc.get_index("crane:manager:targets:index")
    targets = []
    for tid in sorted(ids):
        t = rc.get_model(f"crane:manager:targets:{tid}", WatchTarget)
        if t:
            targets.append(t)
    return targets


@router.get("/{target_id}", response_model=WatchTarget)
def get_target(target_id: str):
    rc = get_redis()
    t = rc.get_model(f"crane:manager:targets:{target_id}", WatchTarget)
    if not t:
        raise HTTPException(status_code=404, detail="Target not found")
    return t


@router.post("/", response_model=WatchTarget)
def create_target(target: WatchTarget):
    rc = get_redis()
    target.created_at = datetime.utcnow().isoformat()
    rc.put_model(f"crane:manager:targets:{target.target_id}", target)
    rc.add_to_index("crane:manager:targets:index", target.target_id)
    return target


@router.delete("/{target_id}")
def delete_target(target_id: str):
    rc = get_redis()
    rc.client.delete(f"crane:manager:targets:{target_id}")
    rc.client.srem("crane:manager:targets:index", target_id)
    return {"deleted": target_id}
