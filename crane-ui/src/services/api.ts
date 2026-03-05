import type {
  MarketQuote,
  PricePoint,
  OptionsRecord,
  WatchTarget,
  Strategy,
  OrderIntent,
  HealthStatus,
  FeedHealth,
} from './types';

const BASE = '/api';

async function get<T>(path: string): Promise<T> {
  const resp = await fetch(`${BASE}${path}`);
  if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
  return resp.json();
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const resp = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
  return resp.json();
}

async function del(path: string): Promise<void> {
  const resp = await fetch(`${BASE}${path}`, { method: 'DELETE' });
  if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
}

// ── Market Data ─────────────────────────────────────────────────────────

export async function getQuotes(): Promise<MarketQuote[]> {
  return get<MarketQuote[]>('/market/quotes');
}

export async function getQuote(symbol: string): Promise<MarketQuote> {
  return get<MarketQuote>(`/market/quotes/${symbol}`);
}

export async function getPriceHistory(symbol: string): Promise<PricePoint[]> {
  return get<PricePoint[]>(`/market/quotes/${symbol}/history`);
}

export async function getOptions(underlying: string): Promise<OptionsRecord[]> {
  return get<OptionsRecord[]>(`/market/options/${underlying}`);
}

// ── Watch Targets ───────────────────────────────────────────────────────

export async function getTargets(): Promise<WatchTarget[]> {
  return get<WatchTarget[]>('/targets/');
}

export async function createTarget(target: WatchTarget): Promise<WatchTarget> {
  return post<WatchTarget>('/targets/', target);
}

export async function deleteTarget(targetId: string): Promise<void> {
  return del(`/targets/${targetId}`);
}

// ── Strategies ──────────────────────────────────────────────────────────

export async function getStrategies(): Promise<Strategy[]> {
  return get<Strategy[]>('/strategies/');
}

export async function createStrategy(strategy: Strategy): Promise<Strategy> {
  return post<Strategy>('/strategies/', strategy);
}

// ── Orders ──────────────────────────────────────────────────────────────

export async function getOrders(): Promise<OrderIntent[]> {
  return get<OrderIntent[]>('/orders/');
}

// ── Health ──────────────────────────────────────────────────────────────

export async function getHealth(): Promise<HealthStatus> {
  return get<HealthStatus>('/health/');
}

export async function getFeedHealth(): Promise<FeedHealth> {
  return get<FeedHealth>('/health/feed');
}
