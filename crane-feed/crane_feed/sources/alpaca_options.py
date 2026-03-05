"""Alpaca options chain poller.

Fetches option chain snapshots and writes OptionsRecords to Redis.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta

import httpx

from crane_shared.models import OptionsRecord, Pricing, Greeks, Sizing, Bar
from crane_feed.store.redis_writer import FeedRedisWriter

log = logging.getLogger("crane-feed.options")

ALPACA_DATA_URL = "https://data.alpaca.markets"


class AlpacaOptionsPoller:
    def __init__(
        self,
        underlyings: list[str],
        writer: FeedRedisWriter,
        poll_interval: float = 30.0,
    ):
        self.underlyings = underlyings
        self.writer = writer
        self.poll_interval = poll_interval
        self._headers = {
            "APCA-API-KEY-ID": os.environ.get("ALPACA_KEY_ID", ""),
            "APCA-API-SECRET-KEY": os.environ.get("ALPACA_SECRET_KEY", ""),
        }

    def run(self):
        log.info(f"Options poller started — underlyings={self.underlyings}")
        while True:
            for underlying in self.underlyings:
                try:
                    self._poll_chain(underlying)
                except Exception as e:
                    log.error(f"Options poll error for {underlying}: {e}")
            time.sleep(self.poll_interval)

    def _poll_chain(self, underlying: str):
        exp_from = datetime.utcnow().strftime("%Y-%m-%d")
        exp_to = (datetime.utcnow() + timedelta(days=45)).strftime("%Y-%m-%d")

        params = {
            "underlying_symbols": underlying,
            "expiration_date_gte": exp_from,
            "expiration_date_lte": exp_to,
            "limit": 100,
            "feed": "indicative",
        }

        with httpx.Client() as client:
            resp = client.get(
                f"{ALPACA_DATA_URL}/v1beta1/options/snapshots",
                headers=self._headers,
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            snapshots = resp.json().get("snapshots", {})

        now = datetime.utcnow().isoformat()
        for symbol, snap in snapshots.items():
            quote = snap.get("latestQuote", {})
            greeks_data = snap.get("greeks", {})

            record = OptionsRecord(
                symbol=symbol,
                underlying=underlying,
                expiration=snap.get("expiration", ""),
                strike=snap.get("strikePrice", 0),
                option_type=snap.get("type", ""),
                pricing=Pricing(
                    bid=quote.get("bp", 0),
                    ask=quote.get("ap", 0),
                    mid=(quote.get("bp", 0) + quote.get("ap", 0)) / 2,
                ),
                greeks=Greeks(
                    delta=greeks_data.get("delta", 0),
                    gamma=greeks_data.get("gamma", 0),
                    theta=greeks_data.get("theta", 0),
                    vega=greeks_data.get("vega", 0),
                    rho=greeks_data.get("rho", 0),
                    iv=greeks_data.get("impliedVolatility", 0),
                ),
                sizing=Sizing(
                    volume=snap.get("dailyBar", {}).get("v", 0),
                    open_interest=snap.get("openInterest", 0),
                ),
                updated_at=now,
                created_at=now,
            )
            self.writer.write_option(record)
