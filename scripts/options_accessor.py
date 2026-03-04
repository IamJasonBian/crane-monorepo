"""
OptionsAccessor — Python interface to the unified OptionsRecord in Redis.

Schema matches proto/options_contract.proto and the Scala RedisOptionsAccessor.
One Redis hash per contract: options:{OCC_SYMBOL}

Sub-attributes (pricing, greeks, sizing, pnl) stored as JSON strings.
Identity and order fields stored as flat strings.

Usage:
    accessor = RedisOptionsAccessor.from_env()

    # Read
    record = accessor.get("AAPL250620C00150000")
    print(record.pricing.bid, record.greeks.delta, record.sizing.qty)

    # Write a quote
    record = OptionsRecord.quote("AAPL250620C00150000", "AAPL", "2025-06-20", 150.0, "C",
        pricing=Pricing(bid=2.80, ask=2.90), greeks=Greeks(delta=0.45, iv=0.32))
    accessor.put_quote(record)

    # Write a position
    record = OptionsRecord.position("AAPL250620C00150000", "AAPL", "2025-06-20", 150.0, "C",
        pricing=Pricing(avg_entry=2.50), sizing=Sizing(qty=10), pnl=PnL(unrealized_pl=350.0))
    accessor.put_position(record)
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import redis


# ── Sub-attribute types ──────────────────────────────


@dataclass
class Pricing:
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    spread: Optional[float] = None
    last_price: Optional[float] = None
    limit_price: Optional[float] = None    # order limit
    stop_price: Optional[float] = None     # order stop
    avg_entry: Optional[float] = None      # position entry

    def to_dict(self) -> dict:
        return {k: v for k, v in {
            "bid": self.bid, "ask": self.ask, "mid": self.mid,
            "spread": self.spread, "last_price": self.last_price,
            "limit_price": self.limit_price, "stop_price": self.stop_price,
            "avg_entry": self.avg_entry,
        }.items() if v is not None}

    @classmethod
    def from_dict(cls, d: dict) -> "Pricing":
        return cls(**{k: d.get(k) for k in cls.__dataclass_fields__})


@dataclass
class Greeks:
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    rho: Optional[float] = None
    iv: Optional[float] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in {
            "delta": self.delta, "gamma": self.gamma, "theta": self.theta,
            "vega": self.vega, "rho": self.rho, "iv": self.iv,
        }.items() if v is not None}

    @classmethod
    def from_dict(cls, d: dict) -> "Greeks":
        return cls(**{k: d.get(k) for k in cls.__dataclass_fields__})


@dataclass
class Sizing:
    qty: Optional[int] = None              # position qty
    filled_qty: Optional[int] = None       # order filled qty
    volume: Optional[int] = None           # market volume
    open_interest: Optional[int] = None    # market OI

    def to_dict(self) -> dict:
        return {k: v for k, v in {
            "qty": self.qty, "filled_qty": self.filled_qty,
            "volume": self.volume, "open_interest": self.open_interest,
        }.items() if v is not None}

    @classmethod
    def from_dict(cls, d: dict) -> "Sizing":
        return cls(
            qty=_opt_int(d, "qty"), filled_qty=_opt_int(d, "filled_qty"),
            volume=_opt_int(d, "volume"), open_interest=_opt_int(d, "open_interest"),
        )


@dataclass
class PnL:
    unrealized_pl: Optional[float] = None
    unrealized_pl_pct: Optional[float] = None
    market_value: Optional[float] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in {
            "unrealized_pl": self.unrealized_pl,
            "unrealized_pl_pct": self.unrealized_pl_pct,
            "market_value": self.market_value,
        }.items() if v is not None}

    @classmethod
    def from_dict(cls, d: dict) -> "PnL":
        return cls(**{k: d.get(k) for k in cls.__dataclass_fields__})


# ── Order and Bar types ──────────────────────────────


@dataclass
class OrderEntry:
    id: str = ""
    side: str = ""
    order_type: str = ""
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    qty: int = 0
    status: str = ""
    filled_qty: Optional[int] = None
    filled_avg_price: Optional[float] = None
    created_at: str = ""

    def to_dict(self) -> dict:
        d = {"id": self.id, "side": self.side, "order_type": self.order_type,
             "qty": self.qty, "status": self.status, "created_at": self.created_at}
        if self.limit_price is not None: d["limit_price"] = self.limit_price
        if self.stop_price is not None: d["stop_price"] = self.stop_price
        if self.filled_qty is not None: d["filled_qty"] = self.filled_qty
        if self.filled_avg_price is not None: d["filled_avg_price"] = self.filled_avg_price
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "OrderEntry":
        return cls(
            id=d.get("id", ""), side=d.get("side", ""),
            order_type=d.get("order_type", ""),
            limit_price=d.get("limit_price"), stop_price=d.get("stop_price"),
            qty=int(d.get("qty", 0)), status=d.get("status", ""),
            filled_qty=d.get("filled_qty"), filled_avg_price=d.get("filled_avg_price"),
            created_at=d.get("created_at", ""),
        )


@dataclass
class Bar:
    timestamp: str = ""
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: int = 0

    def to_dict(self) -> dict:
        return {"timestamp": self.timestamp, "open": self.open, "high": self.high,
                "low": self.low, "close": self.close, "volume": self.volume}

    @classmethod
    def from_dict(cls, d: dict) -> "Bar":
        return cls(timestamp=d.get("timestamp", ""), open=float(d.get("open", 0)),
                   high=float(d.get("high", 0)), low=float(d.get("low", 0)),
                   close=float(d.get("close", 0)), volume=int(d.get("volume", 0)))


# ── The unified record ───────────────────────────────


@dataclass
class OptionsRecord:
    """A single options record. Contract, Position, and Order are the same
    type at different lifecycle stages. Schema matches proto/options_contract.proto.
    """
    # Identity
    symbol: str = ""
    underlying: str = ""
    expiration: str = ""
    strike: float = 0.0
    option_type: str = ""          # C or P

    # Sub-attributes
    pricing: Pricing = field(default_factory=Pricing)
    greeks: Greeks = field(default_factory=Greeks)
    sizing: Sizing = field(default_factory=Sizing)
    pnl: PnL = field(default_factory=PnL)

    # Order fields
    side: Optional[str] = None             # buy / sell
    order_type: Optional[str] = None       # limit / stop / stop_limit / market
    status: Optional[str] = None           # open / filled / partial / cancelled
    order_id: Optional[str] = None

    # Multiple orders
    orders: list = field(default_factory=list)

    # History
    bars: list = field(default_factory=list)

    # Meta
    updated_at: str = ""
    created_at: str = ""

    @classmethod
    def quote(cls, symbol, underlying, expiration, strike, option_type,
              pricing=None, greeks=None, sizing=None) -> "OptionsRecord":
        return cls(symbol=symbol, underlying=underlying, expiration=expiration,
                   strike=strike, option_type=option_type,
                   pricing=pricing or Pricing(), greeks=greeks or Greeks(),
                   sizing=sizing or Sizing())

    @classmethod
    def position(cls, symbol, underlying, expiration, strike, option_type,
                 pricing=None, sizing=None, pnl=None) -> "OptionsRecord":
        return cls(symbol=symbol, underlying=underlying, expiration=expiration,
                   strike=strike, option_type=option_type,
                   pricing=pricing or Pricing(), sizing=sizing or Sizing(),
                   pnl=pnl or PnL())

    @classmethod
    def order(cls, symbol, underlying, expiration, strike, option_type,
              side, order_type, pricing=None, sizing=None,
              status="open", order_id="") -> "OptionsRecord":
        return cls(symbol=symbol, underlying=underlying, expiration=expiration,
                   strike=strike, option_type=option_type,
                   side=side, order_type=order_type, status=status, order_id=order_id,
                   pricing=pricing or Pricing(), sizing=sizing or Sizing())


# ── Helpers ──────────────────────────────────────────


def _opt_float(m: dict, key: str) -> Optional[float]:
    v = m.get(key)
    if v is None: return None
    try: return float(v)
    except (ValueError, TypeError): return None


def _opt_int(m: dict, key: str) -> Optional[int]:
    v = m.get(key)
    if v is None: return None
    try: return int(float(v))
    except (ValueError, TypeError): return None


def _parse_sub(raw: dict, key: str, cls):
    """Parse a JSON sub-attribute from Redis hash."""
    val = raw.get(key)
    if not val:
        return cls()
    try:
        return cls.from_dict(json.loads(val))
    except (json.JSONDecodeError, TypeError):
        return cls()


def _parse_list(raw: dict, key: str, cls):
    val = raw.get(key)
    if not val:
        return []
    try:
        return [cls.from_dict(item) for item in json.loads(val)]
    except (json.JSONDecodeError, TypeError):
        return []


def _from_redis_hash(raw: dict) -> Optional[OptionsRecord]:
    if not raw or "symbol" not in raw:
        return None

    return OptionsRecord(
        symbol=raw.get("symbol", ""),
        underlying=raw.get("underlying", ""),
        expiration=raw.get("expiration", ""),
        strike=float(raw.get("strike", 0)),
        option_type=raw.get("option_type", ""),
        pricing=_parse_sub(raw, "pricing", Pricing),
        greeks=_parse_sub(raw, "greeks", Greeks),
        sizing=_parse_sub(raw, "sizing", Sizing),
        pnl=_parse_sub(raw, "pnl", PnL),
        side=raw.get("side"),
        order_type=raw.get("order_type"),
        status=raw.get("status"),
        order_id=raw.get("order_id"),
        orders=_parse_list(raw, "orders", OrderEntry),
        bars=_parse_list(raw, "bars", Bar),
        updated_at=raw.get("updated_at", ""),
        created_at=raw.get("created_at", ""),
    )


# ── Redis Accessor ───────────────────────────────────


class RedisOptionsAccessor:
    """Read/write OptionsRecord to Redis.
    Same key layout as the Scala RedisOptionsAccessor.
    """

    KEY_PREFIX = "options"
    INDEX_PREFIX = "options:index"
    ALL_INDEX = "options:index:all"

    def __init__(self, client: redis.Redis):
        self._r = client

    @classmethod
    def from_env(cls) -> "RedisOptionsAccessor":
        host = os.getenv("OPTIONS_REDIS_HOST", os.getenv("REDIS_HOST", "localhost"))
        password = os.getenv("OPTIONS_REDIS_PASSWORD", os.getenv("REDIS_PASSWORD"))
        port = 6379
        if ":" in host:
            host, port_str = host.rsplit(":", 1)
            try: port = int(port_str)
            except ValueError: pass
        client = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        return cls(client)

    def _key(self, symbol: str) -> str:
        return f"{self.KEY_PREFIX}:{symbol}"

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def get(self, symbol: str) -> Optional[OptionsRecord]:
        raw = self._r.hgetall(self._key(symbol))
        return _from_redis_hash(raw)

    def get_by_underlying(self, underlying: str) -> list[OptionsRecord]:
        symbols = self._r.smembers(f"{self.INDEX_PREFIX}:{underlying}")
        if not symbols: return []
        return [c for s in sorted(symbols) if (c := self.get(s)) is not None]

    def list_symbols(self) -> list[str]:
        members = self._r.smembers(self.ALL_INDEX)
        return sorted(members) if members else []

    def _write_identity(self, pipe, key: str, record: OptionsRecord):
        pipe.hset(key, "symbol", record.symbol)
        pipe.hset(key, "underlying", record.underlying)
        pipe.hset(key, "expiration", record.expiration)
        pipe.hset(key, "strike", str(record.strike))
        pipe.hset(key, "option_type", record.option_type)

    def put_quote(self, record: OptionsRecord, ttl: int = 604800) -> None:
        """Write quote data. Default TTL: 7 days (604800 seconds)"""
        pipe = self._r.pipeline()
        key = self._key(record.symbol)
        self._write_identity(pipe, key, record)
        pipe.hset(key, "pricing", json.dumps(record.pricing.to_dict()))
        pipe.hset(key, "greeks", json.dumps(record.greeks.to_dict()))
        pipe.hset(key, "sizing", json.dumps(record.sizing.to_dict()))
        pipe.hset(key, "updated_at", self._now())
        pipe.expire(key, ttl)
        pipe.sadd(self.ALL_INDEX, record.symbol)
        pipe.sadd(f"{self.INDEX_PREFIX}:{record.underlying}", record.symbol)
        pipe.execute()

    def put_position(self, record: OptionsRecord, ttl: int = 2592000) -> None:
        """Write position data. Default TTL: 30 days (2592000 seconds)"""
        pipe = self._r.pipeline()
        key = self._key(record.symbol)
        self._write_identity(pipe, key, record)
        pipe.hset(key, "pricing", json.dumps(record.pricing.to_dict()))
        pipe.hset(key, "sizing", json.dumps(record.sizing.to_dict()))
        pipe.hset(key, "pnl", json.dumps(record.pnl.to_dict()))
        pipe.hset(key, "updated_at", self._now())
        pipe.expire(key, ttl)
        pipe.sadd(self.ALL_INDEX, record.symbol)
        pipe.sadd(f"{self.INDEX_PREFIX}:{record.underlying}", record.symbol)
        pipe.execute()

    def put_orders(self, symbol: str, orders: list[OrderEntry], ttl: int = 2592000) -> None:
        """Update orders field. Default TTL: 30 days (2592000 seconds)"""
        key = self._key(symbol)
        self._r.hset(key, "orders", json.dumps([o.to_dict() for o in orders]))
        self._r.hset(key, "updated_at", self._now())
        self._r.expire(key, ttl)

    def put_bars(self, symbol: str, bars: list[Bar], ttl: int = 604800) -> None:
        """Update bars field. Default TTL: 7 days (604800 seconds)"""
        key = self._key(symbol)
        self._r.hset(key, "bars", json.dumps([b.to_dict() for b in bars]))
        self._r.hset(key, "updated_at", self._now())
        self._r.expire(key, ttl)

    def put(self, record: OptionsRecord, ttl: int = 2592000) -> None:
        """Write full record. Default TTL: 30 days (2592000 seconds)"""
        pipe = self._r.pipeline()
        key = self._key(record.symbol)
        self._write_identity(pipe, key, record)
        pipe.hset(key, "pricing", json.dumps(record.pricing.to_dict()))
        pipe.hset(key, "greeks", json.dumps(record.greeks.to_dict()))
        pipe.hset(key, "sizing", json.dumps(record.sizing.to_dict()))
        pipe.hset(key, "pnl", json.dumps(record.pnl.to_dict()))
        if record.side: pipe.hset(key, "side", record.side)
        if record.order_type: pipe.hset(key, "order_type", record.order_type)
        if record.status: pipe.hset(key, "status", record.status)
        if record.order_id: pipe.hset(key, "order_id", record.order_id)
        if record.orders:
            pipe.hset(key, "orders", json.dumps([o.to_dict() for o in record.orders]))
        if record.bars:
            pipe.hset(key, "bars", json.dumps([b.to_dict() for b in record.bars]))
        pipe.hset(key, "updated_at", self._now())
        if record.created_at:
            pipe.hset(key, "created_at", record.created_at)
        pipe.expire(key, ttl)
        pipe.sadd(self.ALL_INDEX, record.symbol)
        pipe.sadd(f"{self.INDEX_PREFIX}:{record.underlying}", record.symbol)
        pipe.execute()

    def close(self) -> None:
        self._r.close()
