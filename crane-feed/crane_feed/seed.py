"""Seed initial search terms and run a one-shot poll for each.

Usage:
    python -m crane_feed.seed          # seed terms + poll all
    python -m crane_feed.seed --only-terms   # seed terms only (no API calls)
"""

from __future__ import annotations

import logging
import os
import sys

from crane_shared import RedisClient, EventBus
from crane_shared.models import SearchTerm
from crane_feed.sources.countdown_ebay import CountdownEbayPoller

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
log = logging.getLogger("crane-feed.seed")

SEED_TERMS = [
    # GPUs
    SearchTerm(term_id="nvidia-a30", query="nvidia a30 gpu", category="gpu", max_price=300),
    SearchTerm(term_id="nvidia-a100", query="nvidia a100 gpu", category="gpu", max_price=3000),
    SearchTerm(term_id="nvidia-h100", query="nvidia h100 gpu", category="gpu", max_price=15000),
    SearchTerm(term_id="nvidia-4090", query="nvidia rtx 4090", category="gpu", max_price=1200),
    SearchTerm(term_id="nvidia-3090", query="nvidia rtx 3090", category="gpu", max_price=600),
    SearchTerm(term_id="amd-mi250", query="amd instinct mi250", category="gpu", max_price=2000),
    # DRAM / Memory
    SearchTerm(term_id="ddr5-server", query="ddr5 server memory 64gb", category="dram", max_price=100),
    SearchTerm(term_id="ddr5-ecc", query="ddr5 ecc rdimm 32gb", category="dram", max_price=60),
    SearchTerm(term_id="hbm-module", query="hbm2e memory module", category="dram", max_price=500),
    # Graphics cards (consumer)
    SearchTerm(term_id="rx-7900xtx", query="amd rx 7900 xtx", category="graphics", max_price=600),
    SearchTerm(term_id="rtx-4080", query="nvidia rtx 4080 super", category="graphics", max_price=700),
    SearchTerm(term_id="arc-a770", query="intel arc a770", category="graphics", max_price=150),
    # Networking / Datacenter
    SearchTerm(term_id="mellanox-cx6", query="mellanox connectx-6", category="networking", max_price=50),
    SearchTerm(term_id="nvidia-bluefield", query="nvidia bluefield dpu", category="networking", max_price=200),
    # Storage
    SearchTerm(term_id="crucial-t705-2tb", query="Crucial t705 2tb", category="storage", max_price=0, min_price=160),
    SearchTerm(term_id="samsung-990-pro-2tb", query="Samsung 990 pro 2tb ssd", category="storage", max_price=0, min_price=150),
    # DRAM (consumer)
    SearchTerm(term_id="ddr5-32gb-6000", query="32gb ddr5 6000", category="dram", max_price=0, min_price=160),
]


def seed_terms(redis_client: RedisClient):
    """Write seed search terms to Redis."""
    for term in SEED_TERMS:
        redis_client.put_model(f"crane:manager:terms:{term.term_id}", term)
        redis_client.add_to_index("crane:manager:terms:index", term.term_id)
        log.info(f"Seeded: {term.term_id} -> '{term.query}'")
    log.info(f"Seeded {len(SEED_TERMS)} search terms")


def poll_all(redis_client: RedisClient, event_bus: EventBus):
    """Poll all seeded terms once."""
    poller = CountdownEbayPoller(redis_client, event_bus, poll_interval=0)
    terms = poller._load_search_terms()
    total = 0
    for term in terms:
        if not term.enabled:
            continue
        try:
            poller._poll_term(term)
            total += term.result_count
            log.info(f"  '{term.query}': {term.result_count} listings")
        except Exception as e:
            log.error(f"  '{term.query}' failed: {e}")
    log.info(f"Total: {total} listings across {len(terms)} terms")


def main():
    rc = RedisClient.from_env()
    if not rc.ping():
        log.error("Redis not reachable")
        sys.exit(1)

    seed_terms(rc)

    if "--only-terms" not in sys.argv:
        bus = EventBus(rc)
        poll_all(rc, bus)
    else:
        log.info("Skipping API polling (--only-terms)")


if __name__ == "__main__":
    main()
