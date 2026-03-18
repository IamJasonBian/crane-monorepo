import { useState, useEffect } from 'react';
import { RefreshCw } from 'lucide-react';
import { getOrderEvents, getEventDates } from '../services/api';
import { formatCurrency, timeAgo } from '../utils/formatters';
import type { OrderEvent } from '../services/types';

const stateColors: Record<string, string> = {
  queued: 'bg-[#332d1a] text-[#da3]',
  confirmed: 'bg-[#1a2233] text-[#6cf]',
  partially_filled: 'bg-[#1a2233] text-[#6cf]',
  filled: 'bg-[#1a331a] text-[#4a4]',
  cancelled: 'bg-[#1a1a1a] text-[#555]',
  rejected: 'bg-[#331a1a] text-[#a44]',
  failed: 'bg-[#331a1a] text-[#a44]',
};

export default function EventsPage() {
  const [events, setEvents] = useState<OrderEvent[]>([]);
  const [dates, setDates] = useState<string[]>([]);
  const [selectedDate, setSelectedDate] = useState<string>('');
  const [assetFilter, setAssetFilter] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadDates = async () => {
    try {
      const resp = await getEventDates(30);
      setDates(resp.dates);
    } catch {
      // dates not available yet
    }
  };

  const loadEvents = async (date?: string, asset?: string) => {
    setLoading(true);
    setError(null);
    try {
      const resp = await getOrderEvents(date || undefined, asset || undefined);
      setEvents(resp.events);
    } catch (err) {
      setError(String(err));
      setEvents([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadDates();
    loadEvents();
  }, []);

  const handleDateChange = (date: string) => {
    setSelectedDate(date);
    loadEvents(date, assetFilter);
  };

  const handleFilterChange = (asset: string) => {
    setAssetFilter(asset);
    loadEvents(selectedDate, asset);
  };

  const equityCount = events.filter(e => e.asset_type === 'equity').length;
  const optionCount = events.filter(e => e.asset_type === 'option').length;
  const filledCount = events.filter(e => e.state === 'filled').length;
  const openCount = events.filter(e => ['queued', 'confirmed', 'partially_filled'].includes(e.state)).length;

  return (
    <div className="max-w-7xl mx-auto px-4 py-4">
      {/* Header */}
      <div className="flex items-center justify-between mb-3 border-b border-[#222] pb-2">
        <div>
          <h1 className="text-sm font-medium text-white uppercase tracking-wider">Order Events</h1>
          <p className="text-[9px] text-[#555] uppercase tracking-wide mt-0.5">
            Equity & option orders from allocation-engine
          </p>
        </div>
        <div className="flex items-center gap-2">
          {/* Date selector */}
          <select
            value={selectedDate}
            onChange={(e) => handleDateChange(e.target.value)}
            className="bg-[#0a0a0a] border border-[#333] text-[10px] text-[#ccc] px-2 py-1 focus:outline-none focus:border-[#555]"
          >
            <option value="">Today</option>
            {dates.map(d => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>

          {/* Asset type filter */}
          <select
            value={assetFilter}
            onChange={(e) => handleFilterChange(e.target.value)}
            className="bg-[#0a0a0a] border border-[#333] text-[10px] text-[#ccc] px-2 py-1 focus:outline-none focus:border-[#555]"
          >
            <option value="">All Types</option>
            <option value="equity">Equity</option>
            <option value="option">Option</option>
          </select>

          <button
            onClick={() => loadEvents(selectedDate, assetFilter)}
            className="flex items-center gap-1 px-2 py-1 border border-[#333] text-[10px] text-[#666] hover:text-[#ccc] hover:border-[#555]"
          >
            <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} />
            refresh
          </button>
        </div>
      </div>

      {/* Stats bar */}
      <div className="flex gap-4 mb-3 text-[10px]">
        <span className="text-[#555]">
          Total: <span className="text-[#ccc]">{events.length}</span>
        </span>
        <span className="text-[#555]">
          Equity: <span className="text-[#6cf]">{equityCount}</span>
        </span>
        <span className="text-[#555]">
          Options: <span className="text-[#da3]">{optionCount}</span>
        </span>
        <span className="text-[#555]">
          Filled: <span className="text-[#4a4]">{filledCount}</span>
        </span>
        {openCount > 0 && (
          <span className="text-[#555]">
            Open: <span className="text-[#6cf]">{openCount}</span>
          </span>
        )}
      </div>

      {error && (
        <div className="border border-[#331a1a] bg-[#1a0a0a] text-[#a44] text-[10px] px-3 py-2 mb-3">
          {error}
        </div>
      )}

      {/* Events table */}
      {events.length === 0 && !loading ? (
        <div className="text-center text-[#333] text-[10px] py-6">
          {error ? 'Failed to load events' : 'No events for this date'}
        </div>
      ) : (
        <table className="w-full border-collapse text-[11px]">
          <thead>
            <tr>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Symbol</th>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Type</th>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Side</th>
              <th className="text-right text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Qty</th>
              <th className="text-right text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Price</th>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Order</th>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">State</th>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Time</th>
            </tr>
          </thead>
          <tbody>
            {events.map((e, i) => (
              <tr key={e.id || i} className="hover:bg-[#0a0a0a] border-b border-[#111]">
                <td className="px-2 py-1.5 text-white font-medium">
                  {e.symbol}
                  {e.asset_type === 'option' && e.legs && e.legs.length > 0 && (
                    <span className="text-[9px] text-[#555] ml-1">
                      {e.legs[0].strike} {e.legs[0].option_type?.charAt(0).toUpperCase()} {e.legs[0].expiration}
                    </span>
                  )}
                </td>
                <td className="px-2 py-1.5">
                  <span className={`text-[9px] px-1 py-px ${
                    e.asset_type === 'equity' ? 'bg-[#1a2233] text-[#6cf]' : 'bg-[#332d1a] text-[#da3]'
                  }`}>
                    {e.asset_type}
                  </span>
                </td>
                <td className="px-2 py-1.5">
                  <span className={
                    e.side === 'BUY' || e.side === 'DEBIT' ? 'text-[#4a4] font-medium' : 'text-[#a44] font-medium'
                  }>
                    {e.side}
                  </span>
                </td>
                <td className="px-2 py-1.5 text-right text-[#ccc] tabular-nums">{e.quantity}</td>
                <td className="px-2 py-1.5 text-right text-[#ccc] tabular-nums">
                  {e.price ? formatCurrency(e.price) : e.limit_price ? formatCurrency(e.limit_price) : '—'}
                </td>
                <td className="px-2 py-1.5 text-[#666]">
                  {e.order_type}
                  {e.opening_strategy && (
                    <span className="text-[9px] text-[#555] ml-1">({e.opening_strategy})</span>
                  )}
                </td>
                <td className="px-2 py-1.5">
                  <span className={`text-[9px] px-1 py-px ${stateColors[e.state] || 'bg-[#1a1a1a] text-[#555]'}`}>
                    {e.state}
                  </span>
                </td>
                <td className="px-2 py-1.5 text-[10px] text-[#555]">
                  {e.created_at ? timeAgo(e.created_at) : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
