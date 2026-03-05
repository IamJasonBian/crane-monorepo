import { useState, useEffect } from 'react';
import { Plus, Trash2, Eye, EyeOff } from 'lucide-react';
import { getTargets, createTarget, deleteTarget, getOrders } from '../services/api';
import { formatCurrency, timeAgo } from '../utils/formatters';
import type { WatchTarget, OrderIntent } from '../services/types';

const statusColors: Record<string, string> = {
  pending: 'bg-[#332d1a] text-[#da3]',
  submitted: 'bg-[#1a2233] text-[#6cf]',
  filled: 'bg-[#1a331a] text-[#4a4]',
  partial: 'bg-[#1a2233] text-[#6cf]',
  cancelled: 'bg-[#1a1a1a] text-[#555]',
  rejected: 'bg-[#331a1a] text-[#a44]',
};

export default function TargetsPage() {
  const [targets, setTargets] = useState<WatchTarget[]>([]);
  const [orders, setOrders] = useState<OrderIntent[]>([]);
  const [showAdd, setShowAdd] = useState(false);
  const [form, setForm] = useState({ symbol: '', threshold: '', maxQty: '' });

  const load = async () => {
    try {
      const [t, o] = await Promise.all([getTargets(), getOrders()]);
      setTargets(t);
      setOrders(o);
    } catch {}
  };

  useEffect(() => { load(); }, []);

  const handleAdd = async () => {
    if (!form.symbol) return;
    const target: WatchTarget = {
      target_id: `t-${Date.now()}`,
      symbol: form.symbol.toUpperCase(),
      underlying: form.symbol.toUpperCase(),
      threshold_price: parseFloat(form.threshold) || 0,
      max_qty: parseFloat(form.maxQty) || 0,
      strategy_id: '',
      dry_run: true,
      enabled: true,
      filters: {},
      created_at: new Date().toISOString(),
    };
    try {
      await createTarget(target);
      setForm({ symbol: '', threshold: '', maxQty: '' });
      setShowAdd(false);
      load();
    } catch (err) {
      console.error('Failed to create target:', err);
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteTarget(id);
      load();
    } catch (err) {
      console.error('Failed to delete target:', err);
    }
  };

  return (
    <div className="max-w-7xl mx-auto px-4 py-4">
      {/* Tracked Targets */}
      <div className="mb-6">
        <div className="flex items-center justify-between mb-3 border-b border-[#222] pb-2">
          <div>
            <h1 className="text-sm font-medium text-white uppercase tracking-wider">Targets</h1>
            <p className="text-[9px] text-[#555] uppercase tracking-wide mt-0.5">Symbols monitored by crane-engine</p>
          </div>
          <button
            onClick={() => setShowAdd(!showAdd)}
            className="flex items-center gap-1 px-2 py-1 border border-[#333] text-[10px] text-[#666] hover:text-[#ccc] hover:border-[#555]"
          >
            <Plus className="w-3 h-3" />
            add
          </button>
        </div>

        {showAdd && (
          <div className="border border-[#1a1a1a] p-3 mb-3">
            <div className="flex gap-2 items-end">
              <div className="flex-1">
                <label className="block text-[9px] font-medium text-[#555] uppercase tracking-wide mb-1">Symbol</label>
                <input type="text" value={form.symbol} onChange={(e) => setForm({ ...form, symbol: e.target.value })} placeholder="AAPL"
                  className="w-full px-2 py-1 bg-[#0a0a0a] border border-[#333] text-[11px] text-white focus:outline-none focus:border-[#555]" />
              </div>
              <div className="flex-1">
                <label className="block text-[9px] font-medium text-[#555] uppercase tracking-wide mb-1">Threshold</label>
                <input type="number" value={form.threshold} onChange={(e) => setForm({ ...form, threshold: e.target.value })} placeholder="150.00"
                  className="w-full px-2 py-1 bg-[#0a0a0a] border border-[#333] text-[11px] text-white focus:outline-none focus:border-[#555]" />
              </div>
              <div className="flex-1">
                <label className="block text-[9px] font-medium text-[#555] uppercase tracking-wide mb-1">Max Qty</label>
                <input type="number" value={form.maxQty} onChange={(e) => setForm({ ...form, maxQty: e.target.value })} placeholder="10"
                  className="w-full px-2 py-1 bg-[#0a0a0a] border border-[#333] text-[11px] text-white focus:outline-none focus:border-[#555]" />
              </div>
              <button onClick={handleAdd} className="px-3 py-1 border border-[#4a4] text-[10px] text-[#4a4] hover:bg-[#1a331a]">save</button>
              <button onClick={() => setShowAdd(false)} className="px-3 py-1 border border-[#333] text-[10px] text-[#666] hover:text-[#ccc]">cancel</button>
            </div>
          </div>
        )}

        {targets.length === 0 ? (
          <div className="text-center text-[#333] text-[10px] py-6">No tracked targets</div>
        ) : (
          <div className="space-y-0.5">
            {targets.map((t) => (
              <div key={t.target_id} className="border border-[#1a1a1a] p-2 flex items-center justify-between hover:bg-[#0a0a0a]">
                <div className="flex items-center gap-3">
                  <div className={`w-1.5 h-1.5 rounded-full ${t.enabled ? 'bg-[#4a4]' : 'bg-[#333]'}`} />
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="text-[11px] font-medium text-white">{t.symbol}</span>
                      {t.underlying && t.underlying !== t.symbol && <span className="text-[10px] text-[#555]">({t.underlying})</span>}
                      {t.dry_run && <span className="text-[9px] px-1 py-px bg-[#332d1a] text-[#da3]">dry</span>}
                    </div>
                    <p className="text-[10px] text-[#666]">{formatCurrency(t.threshold_price)}{t.max_qty > 0 && ` / qty ${t.max_qty}`}</p>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-[10px] text-[#333]">{timeAgo(t.created_at)}</span>
                  {t.enabled ? <Eye className="w-3 h-3 text-[#4a4]" /> : <EyeOff className="w-3 h-3 text-[#333]" />}
                  <button onClick={() => handleDelete(t.target_id)} className="text-[#333] hover:text-[#a44] transition-colors">
                    <Trash2 className="w-3 h-3" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Orders */}
      <div>
        <div className="border-b border-[#222] pb-2 mb-3">
          <h2 className="text-sm font-medium text-white uppercase tracking-wider">Orders</h2>
          <p className="text-[9px] text-[#555] uppercase tracking-wide mt-0.5">Intents from crane-engine</p>
        </div>

        {orders.length === 0 ? (
          <div className="text-center text-[#333] text-[10px] py-6">No orders</div>
        ) : (
          <table className="w-full border-collapse text-[11px]">
            <thead>
              <tr>
                <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Symbol</th>
                <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Side</th>
                <th className="text-right text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Qty</th>
                <th className="text-right text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Price</th>
                <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Type</th>
                <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Status</th>
                <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Time</th>
              </tr>
            </thead>
            <tbody>
              {orders.map((o) => (
                <tr key={o.intent_id} className="hover:bg-[#0a0a0a] border-b border-[#111]">
                  <td className="px-2 py-1.5 text-white font-medium">{o.symbol}</td>
                  <td className="px-2 py-1.5">
                    <span className={o.side === 'buy' ? 'text-[#4a4] font-medium' : 'text-[#a44] font-medium'}>{o.side.toUpperCase()}</span>
                  </td>
                  <td className="px-2 py-1.5 text-right text-[#ccc] tabular-nums">{o.qty}</td>
                  <td className="px-2 py-1.5 text-right text-[#ccc] tabular-nums">{formatCurrency(o.limit_price)}</td>
                  <td className="px-2 py-1.5 text-[#666]">{o.order_type}</td>
                  <td className="px-2 py-1.5">
                    <span className={`text-[9px] px-1 py-px ${statusColors[o.status] || 'bg-[#1a1a1a] text-[#555]'}`}>
                      {o.status}{o.dry_run && ' (dry)'}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 text-[10px] text-[#555]">{timeAgo(o.created_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
