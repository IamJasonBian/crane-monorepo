"""Alpaca options WebSocket streaming client (Python prototype).

Connects to Alpaca's binary (MessagePack) WebSocket feed and
publishes real-time trades/quotes to Redis + event bus.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime

import msgpack
import websockets

from crane_shared.models import MarketQuote
from crane_feed.store.redis_writer import FeedRedisWriter

log = logging.getLogger("crane-feed.stream")

WS_URL = "wss://stream.data.alpaca.markets/v1beta1/indicative"


class AlpacaOptionsStream:
    def __init__(self, symbols: list[str], writer: FeedRedisWriter):
        self.symbols = symbols
        self.writer = writer
        self._key_id = os.environ.get("ALPACA_KEY_ID", "")
        self._secret = os.environ.get("ALPACA_SECRET_KEY", "")

    async def connect(self):
        while True:
            try:
                await self._run()
            except Exception as e:
                log.error(f"WebSocket error: {e}, reconnecting in 5s")
                await asyncio.sleep(5)

    async def _run(self):
        async with websockets.connect(WS_URL) as ws:
            # Auth
            auth_msg = json.dumps({"action": "auth", "key": self._key_id, "secret": self._secret})
            await ws.send(auth_msg)
            auth_resp = await ws.recv()
            log.info(f"Auth response: {auth_resp}")

            # Subscribe
            sub_msg = json.dumps({"action": "subscribe", "quotes": self.symbols, "trades": self.symbols})
            await ws.send(sub_msg)
            sub_resp = await ws.recv()
            log.info(f"Subscribe response: {sub_resp}")

            # Consume
            async for raw in ws:
                try:
                    if isinstance(raw, bytes):
                        messages = msgpack.unpackb(raw, raw=False)
                    else:
                        messages = json.loads(raw)

                    if not isinstance(messages, list):
                        messages = [messages]

                    for msg in messages:
                        self._handle_message(msg)
                except Exception as e:
                    log.warning(f"Message parse error: {e}")

    def _handle_message(self, msg: dict):
        msg_type = msg.get("T", "")
        symbol = msg.get("S", "")

        if msg_type == "q":  # quote
            quote = MarketQuote(
                symbol=symbol,
                bid=msg.get("bp", 0),
                ask=msg.get("ap", 0),
                mid=(msg.get("bp", 0) + msg.get("ap", 0)) / 2,
                timestamp=msg.get("t", datetime.utcnow().isoformat()),
            )
            self.writer.write_quote(quote)
        elif msg_type == "t":  # trade
            quote = MarketQuote(
                symbol=symbol,
                last=msg.get("p", 0),
                volume=msg.get("s", 0),
                timestamp=msg.get("t", datetime.utcnow().isoformat()),
            )
            self.writer.write_quote(quote)
