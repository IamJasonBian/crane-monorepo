"""Entry point for crane-feed.

Spawns data source pollers/streams and publishes to Redis + event bus.
"""

from __future__ import annotations

import logging
import os
import threading
import time

from crane_shared import RedisClient, EventBus
from crane_feed.sources.alpaca_quotes import AlpacaQuotePoller
from crane_feed.sources.alpaca_options import AlpacaOptionsPoller
from crane_feed.store.redis_writer import FeedRedisWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
log = logging.getLogger("crane-feed")


def main():
    log.info("Starting crane-feed")

    redis_client = RedisClient.from_env()
    if not redis_client.ping():
        log.warning("Redis not reachable — running in offline mode")

    event_bus = EventBus(redis_client)
    writer = FeedRedisWriter(redis_client, event_bus)

    # Config from env
    symbols = os.environ.get("SYMBOLS", "AAPL,MSFT,GOOGL").split(",")
    crypto_symbols = os.environ.get("CRYPTO_SYMBOLS", "BTC/USD,ETH/USD").split(",")
    options_symbols = os.environ.get("OPTIONS_SYMBOLS", "").split(",")
    options_symbols = [s for s in options_symbols if s]
    poll_interval = int(os.environ.get("POLL_INTERVAL_MS", "3000")) / 1000.0

    # Quote poller
    quote_poller = AlpacaQuotePoller(
        symbols=symbols,
        crypto_symbols=crypto_symbols,
        writer=writer,
        poll_interval=poll_interval,
    )

    threads = []

    # Main thread: stock/crypto quotes
    t_quotes = threading.Thread(target=quote_poller.run, name="quote-poller", daemon=True)
    t_quotes.start()
    threads.append(t_quotes)

    # Options poller (if configured)
    if options_symbols:
        options_poller = AlpacaOptionsPoller(
            underlyings=options_symbols,
            writer=writer,
        )
        t_options = threading.Thread(target=options_poller.run, name="options-poller", daemon=True)
        t_options.start()
        threads.append(t_options)

    log.info(f"Feed running — symbols={symbols}, crypto={crypto_symbols}, options={options_symbols}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down crane-feed")


if __name__ == "__main__":
    main()
