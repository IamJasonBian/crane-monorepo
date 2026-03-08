"""eBay listing endpoints.

Reads listings written by crane-feed from Redis.
"""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException

from crane_shared.models import EbayListing, SearchTerm
from crane_shared.classifier import classify_listing
from crane_manager.deps import get_redis

router = APIRouter()


@router.get("/")
def list_all_listings(limit: int = 100):
    """Get all listings across all search terms."""
    rc = get_redis()
    epids = rc.get_index("crane:feed:listings:index:all")
    listings = []
    for epid in sorted(epids):
        listing = rc.get_model(f"crane:feed:listings:{epid}", EbayListing)
        if listing:
            listings.append(listing.model_dump())
        if len(listings) >= limit:
            break
    return listings


@router.get("/by-term/{query}")
def list_by_term(query: str, limit: int = 100, raw_search: bool = False):
    """Get listings for a specific search term.

    Args:
        raw_search: If True, skip classifier and price filters (return all raw results).
    """
    rc = get_redis()

    # Look up term's price bounds
    term_id = query.strip().lower().replace(" ", "-")
    term = rc.get_model(f"crane:manager:terms:{term_id}", SearchTerm)
    min_price = term.min_price if term and term.min_price > 0 else 0
    max_price = term.max_price if term and term.max_price > 0 else 0

    epids = rc.get_index(f"crane:feed:listings:index:{query}")
    listings = []
    for epid in sorted(epids):
        listing = rc.get_model(f"crane:feed:listings:{epid}", EbayListing)
        if listing:
            if not raw_search:
                price = listing.price
                if min_price and price < min_price:
                    continue
                if max_price and price > max_price:
                    continue
                if not classify_listing(query, listing.title):
                    continue
            listings.append(listing.model_dump())
        if len(listings) >= limit:
            break
    # Sort by price ascending
    listings.sort(key=lambda x: x.get("price", 0))
    return listings


@router.get("/{epid}")
def get_listing(epid: str):
    rc = get_redis()
    listing = rc.get_model(f"crane:feed:listings:{epid}", EbayListing)
    if not listing:
        raise HTTPException(status_code=404, detail=f"Listing {epid} not found")
    return listing.model_dump()


@router.get("/{epid}/history")
def get_listing_history(epid: str, limit: int = 100):
    """Get price history for a specific listing."""
    rc = get_redis()
    key = f"crane:feed:listings:history:{epid}"
    raw = rc.client.lrange(key, 0, limit - 1)
    points = []
    for item in reversed(raw):
        try:
            data = json.loads(item)
            points.append({
                "timestamp": data.get("last_seen", ""),
                "price": data.get("price", 0),
            })
        except Exception:
            continue
    return points
