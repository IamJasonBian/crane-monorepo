import { formatCurrency, timeAgo } from '../utils/formatters';
import type { OrderIntent } from '../services/types';

interface OrdersListProps {
  orders: OrderIntent[];
}

const statusColors: Record<string, string> = {
  pending: 'bg-yellow-100 text-yellow-700',
  submitted: 'bg-blue-100 text-blue-700',
  filled: 'bg-green-100 text-green-700',
  partial: 'bg-blue-100 text-blue-700',
  cancelled: 'bg-gray-100 text-gray-500',
  rejected: 'bg-red-100 text-red-700',
};

export default function OrdersList({ orders }: OrdersListProps) {
  if (orders.length === 0) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8 text-center text-gray-400">
        No orders yet
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Symbol</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Side</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Qty</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Price</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Time</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {orders.map((o) => (
              <tr key={o.intent_id} className="hover:bg-gray-50">
                <td className="px-4 py-2 text-sm font-medium text-gray-900">{o.symbol}</td>
                <td className="px-4 py-2 text-sm">
                  <span className={o.side === 'buy' ? 'text-green-600 font-medium' : 'text-red-600 font-medium'}>
                    {o.side.toUpperCase()}
                  </span>
                </td>
                <td className="px-4 py-2 text-sm text-right">{o.qty}</td>
                <td className="px-4 py-2 text-sm text-right">{formatCurrency(o.limit_price)}</td>
                <td className="px-4 py-2 text-sm text-gray-500">{o.order_type}</td>
                <td className="px-4 py-2 text-sm">
                  <span className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${statusColors[o.status] || 'bg-gray-100 text-gray-500'}`}>
                    {o.status}
                    {o.dry_run && ' (dry)'}
                  </span>
                </td>
                <td className="px-4 py-2 text-xs text-gray-400">{timeAgo(o.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
