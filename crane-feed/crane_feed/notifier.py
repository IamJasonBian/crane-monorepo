"""Slack notification for classified listings that hit price targets."""

from __future__ import annotations

import logging
import os

import httpx

from crane_shared.models import EbayListing

log = logging.getLogger("crane-feed.notifier")

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


def notify_listing(listing: EbayListing, reason: str = "New match") -> bool:
    """Send a Slack message about a listing. Returns True on success."""
    if not SLACK_WEBHOOK_URL:
        log.debug("No SLACK_WEBHOOK_URL set, skipping notification")
        return False

    condition = listing.condition or "Unknown"
    seller_name = listing.seller.name or "Unknown"
    seller_fb = listing.seller.positive_feedback_percent

    text = (
        f":rotating_light: *{reason}*\n"
        f"*{listing.title}*\n"
        f"Price: *${listing.price:.2f}* | Condition: {condition}\n"
        f"Seller: {seller_name} ({seller_fb}% positive)\n"
        f"<{listing.link}|View on eBay>"
    )

    payload = {
        "text": text,
        "unfurl_links": False,
    }

    try:
        with httpx.Client() as client:
            resp = client.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
            resp.raise_for_status()
        log.info(f"Slack notification sent for {listing.epid}")
        return True
    except Exception as e:
        log.error(f"Slack notification failed: {e}")
        return False
