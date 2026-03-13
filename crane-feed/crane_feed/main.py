"""Entry point for crane-feed.

Runs multiple pollers concurrently:
- CountdownEbayPoller: eBay listings via paid Countdown API (primary)
- SlickdealsPoller: Free deal aggregator RSS (Best Buy, Amazon, Newegg, etc.)
- BestBuyMonitor: Direct product page monitor for specific Best Buy URLs

Seeds default search terms on first run, then polls continuously.
"""

from __future__ import annotations

import logging
import threading

from crane_shared import RedisClient, EventBus
from crane_feed.sources.countdown_ebay import CountdownEbayPoller
from crane_feed.sources.slickdeals_rss import SlickdealsPoller

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
log = logging.getLogger("crane-feed")


def main():
    log.info("Starting crane-feed")

    redis_client = RedisClient.from_env()
    if not redis_client.ping():
        log.error("Redis not reachable")
        return

    # Debug breadcrumb — confirm this code version is running
    redis_client.client.set("crane:feed:main_version", "fix-dockerfile-path", ex=3600)

    event_bus = EventBus(redis_client)
    countdown_poller = CountdownEbayPoller(redis_client, event_bus)

    # Seed terms if none exist
    if not countdown_poller._load_search_terms():
        log.info("No search terms found, seeding defaults...")
        from crane_feed.seed import seed_terms
        seed_terms(redis_client)

    # Start Slickdeals poller in background (free, covers Best Buy/Amazon/Newegg)
    sd_poller = SlickdealsPoller(redis_client, event_bus, poll_interval=900)
    sd_thread = threading.Thread(target=sd_poller.run, daemon=True, name="slickdeals-poller")
    sd_thread.start()
    log.info("Slickdeals poller started in background thread")

    # Start Best Buy product monitor in background (5 min intervals)
    try:
        from crane_feed.sources.bestbuy_monitor import BestBuyMonitor
        bb_monitor = BestBuyMonitor(redis_client, poll_interval=300)
        bb_products = bb_monitor.list_products()
        tracked_ids = {p.get("product_id") for p in bb_products}
        if "JCQ6HQXJVH" not in tracked_ids:
            bb_monitor.add_product(
                url="https://www.bestbuy.com/product/samsung-geek-squad-certified-refurbished-980-pro-2tb-internal-ssd-pcie-gen-4-x4-nvme/JCQ6HQXJVH",
                name="Samsung 980 Pro 2TB (GS Certified Refurbished)",
                target_price=80.0,
            )
        if "JCQ6HRHVGC" not in tracked_ids:
            bb_monitor.add_product(
                url="https://www.bestbuy.com/product/samsung-geek-squad-certified-refurbished-980-pro-1tb-internal-ssd-pcie-gen-4-x4-nvme/JCQ6HRHVGC",
                name="Samsung 980 Pro 1TB (GS Certified Refurbished)",
                target_price=60.0,
            )
        redis_client.client.set("crane:feed:bestbuy:thread_status", "seeded", ex=3600)

        def _bb_thread_wrapper():
            import traceback as _tb
            try:
                redis_client.client.set("crane:feed:bestbuy:thread_status",
                                         "running", ex=3600)
                bb_monitor.run()
            except Exception as e:
                err = _tb.format_exc()
                log.error(f"Best Buy monitor thread crashed: {err}")
                redis_client.client.set("crane:feed:bestbuy:thread_status",
                                         f"crashed: {err[:500]}", ex=3600)
                try:
                    from crane_feed.sources.bestbuy_monitor import _slack_log
                    _slack_log(f"THREAD CRASHED: {err[:300]}")
                except Exception:
                    pass

        bb_thread = threading.Thread(target=_bb_thread_wrapper, daemon=True, name="bestbuy-monitor")
        bb_thread.start()
        log.info("Best Buy monitor started in background thread")
    except Exception as e:
        log.error(f"Failed to start Best Buy monitor: {e}", exc_info=True)
        redis_client.client.set("crane:feed:bestbuy:thread_status",
                                 f"init_failed: {e}", ex=3600)

    # Run Countdown poller in main thread
    log.info("Starting Countdown API poller loop")
    countdown_poller.run()


if __name__ == "__main__":
    main()
