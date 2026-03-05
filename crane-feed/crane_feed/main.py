"""Entry point for crane-feed.

Runs the CountdownEbayPoller to poll eBay BIN listings via the Countdown API.
Seeds default search terms on first run, then polls every 5 minutes.
"""

from __future__ import annotations

import logging
import os
import time

from crane_shared import RedisClient, EventBus
from crane_feed.sources.countdown_ebay import CountdownEbayPoller

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
log = logging.getLogger("crane-feed")


def main():
    log.info("Starting crane-feed")

    redis_client = RedisClient.from_env()
    if not redis_client.ping():
        log.error("Redis not reachable")
        return

    event_bus = EventBus(redis_client)
    poller = CountdownEbayPoller(redis_client, event_bus)

    # Seed terms if none exist
    if not poller._load_search_terms():
        log.info("No search terms found, seeding defaults...")
        from crane_feed.seed import seed_terms
        seed_terms(redis_client)

    log.info("Starting eBay poller loop")
    poller.run()


if __name__ == "__main__":
    main()
