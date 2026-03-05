import { formatCurrency } from '../utils/formatters';
import type { OptionsRecord } from '../services/types';

interface OptionsTableProps {
  options: OptionsRecord[];
}

export default function OptionsTable({ options }: OptionsTableProps) {
  if (options.length === 0) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8 text-center text-gray-400">
        No options listings found
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Contract</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Strike</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Exp</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Bid</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Ask</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">IV</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Delta</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Vol</th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">OI</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {options.map((opt) => (
              <tr key={opt.symbol} className="hover:bg-gray-50">
                <td className="px-4 py-2 text-sm font-mono text-gray-900">{opt.symbol}</td>
                <td className="px-4 py-2 text-sm">
                  <span className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${
                    opt.option_type === 'C'
                      ? 'bg-green-100 text-green-700'
                      : 'bg-red-100 text-red-700'
                  }`}>
                    {opt.option_type === 'C' ? 'Call' : 'Put'}
                  </span>
                </td>
                <td className="px-4 py-2 text-sm text-right font-medium">{formatCurrency(opt.strike)}</td>
                <td className="px-4 py-2 text-sm text-gray-500">{opt.expiration}</td>
                <td className="px-4 py-2 text-sm text-right">{formatCurrency(opt.pricing.bid)}</td>
                <td className="px-4 py-2 text-sm text-right">{formatCurrency(opt.pricing.ask)}</td>
                <td className="px-4 py-2 text-sm text-right">{(opt.greeks.iv * 100).toFixed(1)}%</td>
                <td className="px-4 py-2 text-sm text-right">{opt.greeks.delta.toFixed(3)}</td>
                <td className="px-4 py-2 text-sm text-right text-gray-500">{opt.sizing.volume.toLocaleString()}</td>
                <td className="px-4 py-2 text-sm text-right text-gray-500">{opt.sizing.open_interest.toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
