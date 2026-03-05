import { TrendingUp, TrendingDown } from 'lucide-react';
import { formatCurrency, timeAgo } from '../utils/formatters';
import type { MarketQuote } from '../services/types';

interface PriceCardProps {
  quote: MarketQuote;
  onClick?: () => void;
}

export default function PriceCard({ quote, onClick }: PriceCardProps) {
  const spread = quote.ask - quote.bid;
  const spreadPct = quote.mid > 0 ? (spread / quote.mid) * 100 : 0;
  // We don't have prior close from the API, so show spread-based info
  const isPositive = quote.mid > 0;

  return (
    <div
      onClick={onClick}
      className={`bg-white rounded-xl shadow-sm border border-gray-200 p-5 hover:shadow-md transition-shadow ${onClick ? 'cursor-pointer' : ''}`}
    >
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-full bg-blue-100 flex items-center justify-center">
            <span className="text-blue-600 font-bold text-sm">{quote.symbol.slice(0, 2)}</span>
          </div>
          <div>
            <h3 className="text-base font-semibold text-gray-900">{quote.symbol}</h3>
            {quote.timestamp && (
              <p className="text-xs text-gray-400">{timeAgo(quote.timestamp)}</p>
            )}
          </div>
        </div>
        {isPositive ? (
          <TrendingUp className="w-4 h-4 text-green-500" />
        ) : (
          <TrendingDown className="w-4 h-4 text-red-500" />
        )}
      </div>

      <p className="text-2xl font-bold text-gray-900 mb-2">{formatCurrency(quote.mid)}</p>

      <div className="grid grid-cols-3 gap-3 pt-3 border-t border-gray-100 text-sm">
        <div>
          <p className="text-gray-400">Bid</p>
          <p className="font-medium text-gray-700">{formatCurrency(quote.bid)}</p>
        </div>
        <div>
          <p className="text-gray-400">Ask</p>
          <p className="font-medium text-gray-700">{formatCurrency(quote.ask)}</p>
        </div>
        <div>
          <p className="text-gray-400">Spread</p>
          <p className="font-medium text-gray-700">{spreadPct.toFixed(2)}%</p>
        </div>
      </div>
    </div>
  );
}
