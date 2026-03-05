"""Shared data models used across all Crane services.

These models are the contract between crane-feed, crane-engine, and crane-manager.
All inter-service data flows through these types via Redis.
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Market Data (crane-feed → Redis → crane-engine)
# ---------------------------------------------------------------------------


class MarketQuote(BaseModel):
    """Real-time stock/crypto quote from data feed."""

    symbol: str
    bid: float = 0.0
    ask: float = 0.0
    mid: float = 0.0
    last: float = 0.0
    volume: int = 0
    timestamp: str = ""

    @property
    def spread(self) -> float:
        return self.ask - self.bid


class SellerInfo(BaseModel):
    name: str = ""
    review_count: str = ""
    positive_feedback_percent: float = 0.0


class EbayListing(BaseModel):
    """An eBay product listing from Countdown API search results."""

    epid: str  # eBay product ID
    title: str = ""
    link: str = ""
    image: str = ""
    condition: str = ""
    price: float = 0.0
    price_raw: str = ""
    is_auction: bool = False
    buy_it_now: bool = False
    free_returns: bool = False
    best_offer: bool = False
    sponsored: bool = False
    item_location: str = ""
    seller: SellerInfo = Field(default_factory=SellerInfo)
    search_term: str = ""  # which query found this
    first_seen: str = ""
    last_seen: str = ""


class SearchTerm(BaseModel):
    """A tracked search term for eBay feed polling."""

    term_id: str
    query: str
    category: str = ""
    enabled: bool = True
    threshold_price: float = 0.0  # alert if price below this
    sort_by: str = "price_low_to_high"
    listing_type: str = "buy_it_now"
    last_polled: str = ""
    result_count: int = 0
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class Pricing(BaseModel):
    bid: float = 0.0
    ask: float = 0.0
    mid: float = 0.0
    last: float = 0.0
    spread: float = 0.0
    limit_price: float = 0.0
    stop_price: float = 0.0
    avg_entry: float = 0.0


class Greeks(BaseModel):
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    rho: float = 0.0
    iv: float = 0.0


class Sizing(BaseModel):
    qty: float = 0.0
    filled_qty: float = 0.0
    volume: int = 0
    open_interest: int = 0


class PnL(BaseModel):
    unrealized_pl: float = 0.0
    unrealized_pl_pct: float = 0.0
    market_value: float = 0.0


class OrderEntry(BaseModel):
    id: str = ""
    side: str = ""
    order_type: str = ""
    limit_price: float = 0.0
    stop_price: float = 0.0
    qty: float = 0.0
    status: str = ""
    filled_qty: float = 0.0
    filled_avg_price: float = 0.0
    submitted_at: str = ""
    filled_at: str = ""


class Bar(BaseModel):
    timestamp: str = ""
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: int = 0


class OptionsRecord(BaseModel):
    """Unified options record — compatible with the existing Scala/protobuf schema."""

    symbol: str  # OCC symbol e.g. AAPL250620C00150000
    underlying: str = ""
    expiration: str = ""
    strike: float = 0.0
    option_type: str = ""  # C or P

    pricing: Pricing = Field(default_factory=Pricing)
    greeks: Greeks = Field(default_factory=Greeks)
    sizing: Sizing = Field(default_factory=Sizing)
    pnl: PnL = Field(default_factory=PnL)

    side: Optional[str] = None
    order_type: Optional[str] = None
    status: Optional[str] = None
    order_id: Optional[str] = None

    orders: list[OrderEntry] = Field(default_factory=list)
    bars: list[Bar] = Field(default_factory=list)

    updated_at: str = ""
    created_at: str = ""


# ---------------------------------------------------------------------------
# Allocation Signals (crane-engine internal + Redis)
# ---------------------------------------------------------------------------


class SignalType(str, Enum):
    PRICE_THRESHOLD = "price_threshold"
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    VOLATILITY = "volatility"
    CUSTOM = "custom"


class AllocationSignal(BaseModel):
    """Signal emitted by crane-engine when an allocation opportunity is detected."""

    signal_id: str
    signal_type: SignalType = SignalType.PRICE_THRESHOLD
    symbol: str
    underlying: str = ""
    score: float = 0.0  # -1.0 to 1.0 confidence
    target_price: float = 0.0
    current_price: float = 0.0
    metadata: dict = Field(default_factory=dict)
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    PARTIAL = "partial"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class OrderIntent(BaseModel):
    """Order intent produced by crane-engine, consumed by execution layer."""

    intent_id: str
    signal_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType = OrderType.LIMIT
    qty: float = 0.0
    limit_price: float = 0.0
    stop_price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    strategy_id: str = ""
    dry_run: bool = True
    metadata: dict = Field(default_factory=dict)
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = ""


# ---------------------------------------------------------------------------
# Configuration (crane-manager → Redis → crane-engine)
# ---------------------------------------------------------------------------


class Strategy(BaseModel):
    """Allocation strategy configuration managed by crane-manager."""

    strategy_id: str
    name: str
    enabled: bool = True
    dry_run: bool = True
    symbols: list[str] = Field(default_factory=list)
    signal_type: SignalType = SignalType.PRICE_THRESHOLD
    params: dict = Field(default_factory=dict)  # strategy-specific config
    max_position_size: float = 0.0
    max_daily_trades: int = 0
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = ""


class WatchTarget(BaseModel):
    """A symbol/query to monitor, with threshold and constraints."""

    target_id: str
    symbol: str
    underlying: str = ""
    threshold_price: float = 0.0
    max_qty: float = 0.0
    strategy_id: str = ""
    dry_run: bool = True
    enabled: bool = True
    filters: dict = Field(default_factory=dict)
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class BudgetState(BaseModel):
    """Daily budget tracking — authoritative state in Redis."""

    date: str  # YYYY-MM-DD
    daily_limit: float = 0.0
    spent: float = 0.0
    remaining: float = 0.0
    trade_count: int = 0
    max_trades: int = 0
    frozen: bool = False
