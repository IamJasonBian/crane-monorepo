"""Search term CRUD endpoints.

Search terms define what eBay queries to monitor via the Countdown API.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException

from crane_shared.models import SearchTerm
from crane_manager.deps import get_redis

router = APIRouter()


@router.get("/", response_model=list[SearchTerm])
def list_terms():
    rc = get_redis()
    ids = rc.get_index("crane:manager:terms:index")
    terms = []
    for tid in sorted(ids):
        t = rc.get_model(f"crane:manager:terms:{tid}", SearchTerm)
        if t:
            terms.append(t)
    return terms


@router.get("/{term_id}", response_model=SearchTerm)
def get_term(term_id: str):
    rc = get_redis()
    t = rc.get_model(f"crane:manager:terms:{term_id}", SearchTerm)
    if not t:
        raise HTTPException(status_code=404, detail="Term not found")
    return t


@router.post("/", response_model=SearchTerm)
def create_term(term: SearchTerm):
    rc = get_redis()
    term.created_at = datetime.utcnow().isoformat()
    rc.put_model(f"crane:manager:terms:{term.term_id}", term)
    rc.add_to_index("crane:manager:terms:index", term.term_id)
    return term


@router.delete("/{term_id}")
def delete_term(term_id: str):
    rc = get_redis()
    rc.client.delete(f"crane:manager:terms:{term_id}")
    rc.client.srem("crane:manager:terms:index", term_id)
    return {"deleted": term_id}
