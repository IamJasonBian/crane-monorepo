// Mirrors crane-shared models

export interface MarketQuote {
  symbol: string;
  bid: number;
  ask: number;
  mid: number;
  last: number;
  volume: number;
  timestamp: string;
}

export interface PricePoint {
  timestamp: number;
  price: number;
}

export interface OptionsRecord {
  symbol: string;
  underlying: string;
  expiration: string;
  strike: number;
  option_type: string;
  pricing: {
    bid: number;
    ask: number;
    mid: number;
    last: number;
    spread: number;
  };
  greeks: {
    delta: number;
    gamma: number;
    theta: number;
    vega: number;
    iv: number;
  };
  sizing: {
    volume: number;
    open_interest: number;
  };
  updated_at: string;
}

export interface WatchTarget {
  target_id: string;
  symbol: string;
  underlying: string;
  threshold_price: number;
  max_qty: number;
  strategy_id: string;
  dry_run: boolean;
  enabled: boolean;
  filters: Record<string, unknown>;
  created_at: string;
}

export interface Strategy {
  strategy_id: string;
  name: string;
  enabled: boolean;
  dry_run: boolean;
  symbols: string[];
  signal_type: string;
  params: Record<string, unknown>;
  max_position_size: number;
  max_daily_trades: number;
  created_at: string;
  updated_at: string;
}

export interface OrderIntent {
  intent_id: string;
  signal_id: string;
  symbol: string;
  side: string;
  order_type: string;
  qty: number;
  limit_price: number;
  status: string;
  strategy_id: string;
  dry_run: boolean;
  created_at: string;
}

export interface HealthStatus {
  status: string;
  redis: boolean;
  streams: Record<string, { length: number; last_entry_id: string }>;
  circuit_breaker: { state: string; failures: number };
  timestamp: string;
}

export interface FeedHealth {
  tracked_quotes: number;
  tracked_options: number;
  quote_symbols: string[];
}
