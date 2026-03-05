import { useState, useEffect } from 'react';
import { Plus } from 'lucide-react';
import TargetsList from '../components/TargetsList';
import OrdersList from '../components/OrdersList';
import { getTargets, createTarget, deleteTarget, getOrders } from '../services/api';
import type { WatchTarget, OrderIntent } from '../services/types';

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
    } catch {
      // API may not have these endpoints yet
    }
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
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Tracked Targets */}
      <div className="mb-10">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Tracked Targets</h1>
            <p className="text-sm text-gray-500 mt-1">Symbols monitored by crane-engine</p>
          </div>
          <button
            onClick={() => setShowAdd(!showAdd)}
            className="flex items-center gap-2 px-4 py-2 bg-blue-500 text-white text-sm rounded-lg hover:bg-blue-600"
          >
            <Plus className="w-4 h-4" />
            Add Target
          </button>
        </div>

        {/* Add form */}
        {showAdd && (
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 mb-4">
            <div className="flex gap-3 items-end">
              <div className="flex-1">
                <label className="block text-xs font-medium text-gray-500 mb-1">Symbol</label>
                <input
                  type="text"
                  value={form.symbol}
                  onChange={(e) => setForm({ ...form, symbol: e.target.value })}
                  placeholder="AAPL"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div className="flex-1">
                <label className="block text-xs font-medium text-gray-500 mb-1">Threshold Price</label>
                <input
                  type="number"
                  value={form.threshold}
                  onChange={(e) => setForm({ ...form, threshold: e.target.value })}
                  placeholder="150.00"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div className="flex-1">
                <label className="block text-xs font-medium text-gray-500 mb-1">Max Qty</label>
                <input
                  type="number"
                  value={form.maxQty}
                  onChange={(e) => setForm({ ...form, maxQty: e.target.value })}
                  placeholder="10"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <button
                onClick={handleAdd}
                className="px-4 py-2 bg-green-500 text-white text-sm rounded-lg hover:bg-green-600"
              >
                Save
              </button>
              <button
                onClick={() => setShowAdd(false)}
                className="px-4 py-2 bg-gray-100 text-gray-600 text-sm rounded-lg hover:bg-gray-200"
              >
                Cancel
              </button>
            </div>
          </div>
        )}

        <TargetsList targets={targets} onDelete={handleDelete} />
      </div>

      {/* Orders */}
      <div>
        <h2 className="text-xl font-bold text-gray-900 mb-3">Orders</h2>
        <p className="text-sm text-gray-500 mb-4">Order intents generated by crane-engine</p>
        <OrdersList orders={orders} />
      </div>
    </div>
  );
}
