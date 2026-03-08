"""Tests that the poller marks disappeared listings as sold."""

from unittest.mock import MagicMock, patch

from crane_shared.models import EbayListing, SearchTerm
from crane_shared.redis_client import RedisClient
from crane_shared.events import EventBus
from crane_feed.sources.countdown_ebay import CountdownEbayPoller


def _make_listing(epid: str, price: float = 10.0, sold: bool = False) -> EbayListing:
    return EbayListing(
        epid=epid, title=f"Item {epid}", price=price,
        sold=sold, sold_at="" if not sold else "2025-01-01T00:00:00",
    )


def _build_poller():
    rc = MagicMock(spec=RedisClient)
    bus = MagicMock(spec=EventBus)
    poller = CountdownEbayPoller(redis_client=rc, event_bus=bus)
    return poller, rc, bus


def test_disappeared_listing_marked_sold():
    """A listing present in the previous poll but missing from the current poll is marked sold."""
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

    # B and C should be marked sold
    assert saved["B"].sold is True
    assert saved["B"].sold_at != ""
    assert saved["C"].sold is True
    assert saved["C"].sold_at != ""

    # A should NOT be marked sold (still active)
    assert saved["A"].sold is False


def test_already_sold_not_updated_again():
    """A listing already marked sold should not have its sold_at overwritten."""
    poller, rc, bus = _build_poller()
    term = SearchTerm(term_id="t1", query="test gpu")

    rc.get_index.return_value = {"A", "B"}

    # B was already sold in a previous cycle
    already_sold = _make_listing("B", price=200, sold=True)
    already_sold.sold_at = "2025-01-01T00:00:00"

    with patch.object(poller, "poll_once", return_value=[_make_listing("A")]):
        def fake_get_model(key, cls):
            if "B" in key:
                return already_sold
            if "A" in key:
                return _make_listing("A")
            return None

        rc.get_model.side_effect = fake_get_model
        poller._poll_term(term)

    # B should NOT have been re-saved (already sold)
    sold_saves = [
        c for c in rc.put_model.call_args_list
        if isinstance(c[0][1], EbayListing) and c[0][1].epid == "B"
    ]
    assert len(sold_saves) == 0


def test_no_disappearances_no_sold():
    """When all previous listings are still present, nothing is marked sold."""
    poller, rc, bus = _build_poller()
    term = SearchTerm(term_id="t1", query="test gpu")

    rc.get_index.return_value = {"A", "B"}

    with patch.object(
        poller, "poll_once",
        return_value=[_make_listing("A"), _make_listing("B")],
    ):
        rc.get_model.return_value = None
        poller._poll_term(term)

    # No listing should have sold=True
    for call in rc.put_model.call_args_list:
        model = call[0][1]
        if isinstance(model, EbayListing):
            assert model.sold is False


def test_sold_listing_gets_extended_ttl():
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

    # Find the put_model call for B (the sold item)
    for call in rc.put_model.call_args_list:
        model = call[0][1]
        if isinstance(model, EbayListing) and model.epid == "B":
            ttl = call[1].get("ttl") if call[1] else call[0][2] if len(call[0]) > 2 else None
            assert ttl == 30 * 86400
            break
    else:
        raise AssertionError("B was not saved")
