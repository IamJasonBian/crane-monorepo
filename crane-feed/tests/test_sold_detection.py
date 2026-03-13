"""Tests that the poller marks disappeared listings as has_sales."""

from unittest.mock import MagicMock, patch

from crane_shared.models import EbayListing, SearchTerm
from crane_shared.redis_client import RedisClient
from crane_shared.events import EventBus
from crane_feed.sources.countdown_ebay import CountdownEbayPoller


def _make_listing(epid: str, price: float = 10.0, has_sales: bool = False) -> EbayListing:
    return EbayListing(
        epid=epid, title=f"Item {epid}", price=price,
        has_sales=has_sales, first_sale_at="" if not has_sales else "2025-01-01T00:00:00",
    )


def _build_poller():
    rc = MagicMock(spec=RedisClient)
    bus = MagicMock(spec=EventBus)
    poller = CountdownEbayPoller(redis_client=rc, event_bus=bus)
    return poller, rc, bus


def test_disappeared_listing_marked_has_sales():
    """A listing present in the previous poll but missing from the current poll is marked has_sales."""
    poller, rc, bus = _build_poller()
    term = SearchTerm(term_id="t1", query="test gpu")

    # Previous poll had items A, B, C
    rc.get_index.return_value = {"A", "B", "C"}

    # Current poll only returns A (B and C disappeared)
    with patch.object(poller, "poll_once", return_value=[_make_listing("A")]):
        # When the poller looks up disappeared items B and C in Redis:
        def fake_get_model(key, cls):
            if "B" in key:
                return _make_listing("B", price=200)
            if "C" in key:
                return _make_listing("C", price=300)
            if "A" in key:
                return _make_listing("A", price=10)
            return None

        rc.get_model.side_effect = fake_get_model
        poller._poll_term(term)

    # Collect all put_model calls
    saved = {}
    for call in rc.put_model.call_args_list:
        key = call[0][0]
        model = call[0][1]
        if isinstance(model, EbayListing):
            saved[model.epid] = model

    # B and C should be marked has_sales
    assert saved["B"].has_sales is True
    assert saved["B"].first_sale_at != ""
    assert saved["C"].has_sales is True
    assert saved["C"].first_sale_at != ""

    # A should NOT be marked has_sales (still active)
    assert saved["A"].has_sales is False


def test_already_has_sales_not_updated_again():
    """A listing already marked has_sales should not have its first_sale_at overwritten."""
    poller, rc, bus = _build_poller()
    term = SearchTerm(term_id="t1", query="test gpu")

    rc.get_index.return_value = {"A", "B"}

    # B was already has_sales in a previous cycle
    already_has_sales = _make_listing("B", price=200, has_sales=True)
    already_has_sales.first_sale_at = "2025-01-01T00:00:00"

    with patch.object(poller, "poll_once", return_value=[_make_listing("A")]):
        def fake_get_model(key, cls):
            if "B" in key:
                return already_has_sales
            if "A" in key:
                return _make_listing("A")
            return None

        rc.get_model.side_effect = fake_get_model
        poller._poll_term(term)

    # B should NOT have been re-saved (already has_sales)
    has_sales_saves = [
        c for c in rc.put_model.call_args_list
        if isinstance(c[0][1], EbayListing) and c[0][1].epid == "B"
    ]
    assert len(has_sales_saves) == 0


def test_no_disappearances_no_has_sales():
    """When all previous listings are still present, nothing is marked has_sales."""
    poller, rc, bus = _build_poller()
    term = SearchTerm(term_id="t1", query="test gpu")

    rc.get_index.return_value = {"A", "B"}

    with patch.object(
        poller, "poll_once",
        return_value=[_make_listing("A"), _make_listing("B")],
    ):
        rc.get_model.return_value = None
        poller._poll_term(term)

    # No listing should have has_sales=True
    for call in rc.put_model.call_args_list:
        model = call[0][1]
        if isinstance(model, EbayListing):
            assert model.has_sales is False


def test_has_sales_listing_gets_extended_ttl():
    """Sold listings should be stored with 30-day TTL (not the default 7-day)."""
    poller, rc, bus = _build_poller()
    term = SearchTerm(term_id="t1", query="test gpu")

    rc.get_index.return_value = {"A", "B"}

    with patch.object(poller, "poll_once", return_value=[_make_listing("A")]):
        rc.get_model.side_effect = lambda key, cls: (
            _make_listing("B", price=200) if "B" in key else
            _make_listing("A") if "A" in key else None
        )
        poller._poll_term(term)

    # Find the put_model call for B (the has_sales item)
    for call in rc.put_model.call_args_list:
        model = call[0][1]
        if isinstance(model, EbayListing) and model.epid == "B":
            ttl = call[1].get("ttl") if call[1] else call[0][2] if len(call[0]) > 2 else None
            assert ttl == 30 * 86400
            break
    else:
        raise AssertionError("B was not saved")
