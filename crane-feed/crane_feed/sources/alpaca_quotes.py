"""Alpaca stock and crypto quote poller.

Fetches latest quotes via REST API on a configurable interval.
"""

from __future__ import annotations

import logging
import os
import time

import httpx

from crane_shared.models import MarketQuote
from crane_feed.store.redis_writer import FeedRedisWriter

log = logging.getLogger("crane-feed.quotes")

ALPACA_DATA_URL = "https://data.alpaca.markets"
ALPACA_CRYPTO_URL = "https://data.alpaca.markets"


class AlpacaQuotePoller:
    def __init__(
        self,
        symbols: list[str],
        crypto_symbols: list[str],
        writer: FeedRedisWriter,
        poll_interval: float = 3.0,
    ):
        self.symbols = symbols
        self.crypto_symbols = crypto_symbols
        self.writer = writer
        self.poll_interval = poll_interval
        self._headers = {
            "APCA-API-KEY-ID": os.environ.get("ALPACA_KEY_ID", ""),
            "APCA-API-SECRET-KEY": os.environ.get("ALPACA_SECRET_KEY", ""),
        }

    def run(self):
        log.info(f"Quote poller started — {len(self.symbols)} stocks, {len(self.crypto_symbols)} crypto")
        while True:
            try:
                self._poll_stocks()
                self._poll_crypto()
            except Exception as e:
                log.error(f"Poll error: {e}")
            time.sleep(self.poll_interval)

    def _poll_stocks(self):
        if not self.symbols:
            return
        params = {"symbols": ",".join(self.symbols)}
        with httpx.Client() as client:
            resp = client.get(
                f"{ALPACA_DATA_URL}/v2/stocks/quotes/latest",
                headers=self._headers,
                params=params,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json().get("quotes", {})

        for symbol, q in data.items():
            quote = MarketQuote(
                symbol=symbol,
                bid=q.get("bp", 0),
                ask=q.get("ap", 0),
                mid=(q.get("bp", 0) + q.get("ap", 0)) / 2,
                last=q.get("ap", 0),
                volume=q.get("as", 0) + q.get("bs", 0),
                timestamp=q.get("t", ""),
            )
            self.writer.write_quote(quote)

    def _poll_crypto(self):
        if not self.crypto_symbols:
            return
        params = {"symbols": ",".join(self.crypto_symbols)}
        with httpx.Client() as client:
            resp = client.get(
                f"{ALPACA_CRYPTO_URL}/v1beta3/crypto/us/latest/quotes",
                headers=self._headers,
                params=params,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json().get("quotes", {})

        for symbol, q in data.items():
            quote = MarketQuote(
                symbol=symbol,
                bid=q.get("bp", 0),
                ask=q.get("ap", 0),
                mid=(q.get("bp", 0) + q.get("ap", 0)) / 2,
                last=q.get("ap", 0),
                volume=0,
                timestamp=q.get("t", ""),
            )
            self.writer.write_quote(quote)
