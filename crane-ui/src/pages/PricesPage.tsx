import { useState, useEffect } from 'react';
import { RefreshCw } from 'lucide-react';
import { getTerms, getListingsByTerm } from '../services/api';
import { formatCurrency } from '../utils/formatters';
import type { SearchTerm, EbayListing } from '../services/types';

interface TermSummary {
  term: SearchTerm;
  listings: EbayListing[];
  low: number;
  median: number;
  high: number;
}

export default function PricesPage() {
  const [summaries, setSummaries] = useState<TermSummary[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const fetchAll = async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    else setLoading(true);
    setError(null);

    try {
      const terms = await getTerms();
      const enabled = terms.filter((t) => t.enabled && t.result_count > 0);

      const results = await Promise.all(
        enabled.map(async (term) => {
          try {
            const listings = await getListingsByTerm(term.query);
            const prices = listings.map((l) => l.price).filter((p) => p > 0).sort((a, b) => a - b);
            return {
              term,
              listings,
              low: prices[0] ?? 0,
              median: prices[Math.floor(prices.length / 2)] ?? 0,
              high: prices[prices.length - 1] ?? 0,
            };
          } catch {
            return { term, listings: [], low: 0, median: 0, high: 0 };
          }
        })
      );

      results.sort((a, b) => {
        if (a.term.category !== b.term.category) return a.term.category.localeCompare(b.term.category);
        return a.term.query.localeCompare(b.term.query);
      });

      setSummaries(results);
      if (!selected && results.length > 0) setSelected(results[0].term.query);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => { fetchAll(); }, []);

  const selectedSummary = summaries.find((s) => s.term.query === selected);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="text-center">
          <RefreshCw className="w-6 h-6 text-[#555] animate-spin mx-auto mb-2" />
          <p className="text-[#555] text-xs">loading</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="text-center">
          <p className="text-[#a44] text-sm mb-3">{error}</p>
          <button onClick={() => fetchAll()} className="px-3 py-1 border border-[#333] text-xs text-[#ccc] hover:border-[#555]">
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-4">
      <div className="flex items-center justify-between mb-4 border-b border-[#222] pb-2">
        <div>
          <h1 className="text-sm font-medium text-white uppercase tracking-wider">Prices</h1>
          <p className="text-[9px] text-[#555] uppercase tracking-wide mt-0.5">eBay listing price ranges by search term</p>
        </div>
        <button
          onClick={() => fetchAll(true)}
          disabled={refreshing}
          className="flex items-center gap-1.5 px-2 py-1 border border-[#333] text-[10px] text-[#666] hover:text-[#ccc] hover:border-[#555] disabled:opacity-50"
        >
          <RefreshCw className={`w-3 h-3 ${refreshing ? 'animate-spin' : ''}`} />
          refresh
        </button>
      </div>

      {/* Summary cards grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-1 mb-4">
        {summaries.map((s) => (
          <div
            key={s.term.term_id}
            onClick={() => setSelected(s.term.query)}
            className={`border p-3 cursor-pointer transition-colors ${
              selected === s.term.query
                ? 'border-[#4a4] bg-[#0a0a0a]'
                : 'border-[#1a1a1a] hover:border-[#333] hover:bg-[#0a0a0a]'
            }`}
          >
            <div className="flex items-start justify-between mb-2">
              <div>
                <span className="text-[9px] font-medium uppercase text-[#555] tracking-wide">{s.term.category}</span>
                <h3 className="text-xs font-medium text-white leading-tight">{s.term.query}</h3>
              </div>
              <span className="text-[10px] text-[#555]">
                {s.listings.length}
              </span>
            </div>

            <p className="text-lg font-bold text-white mb-2">{formatCurrency(s.low)}</p>

            <div className="grid grid-cols-3 gap-2 pt-2 border-t border-[#111] text-[10px]">
              <div>
                <p className="text-[#555]">Low</p>
                <p className="font-medium text-[#4a4]">{formatCurrency(s.low)}</p>
              </div>
              <div>
                <p className="text-[#555]">Med</p>
                <p className="font-medium text-[#ccc]">{formatCurrency(s.median)}</p>
              </div>
              <div>
                <p className="text-[#555]">High</p>
                <p className="font-medium text-[#a44]">{formatCurrency(s.high)}</p>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Price distribution for selected term */}
      {selectedSummary && selectedSummary.listings.length > 0 && (
        <PriceDistribution summary={selectedSummary} />
      )}
    </div>
  );
}

function PriceDistribution({ summary }: { summary: TermSummary }) {
  const prices = summary.listings
    .map((l) => l.price)
    .filter((p) => p > 0)
    .sort((a, b) => a - b);

  const bucketCount = Math.min(20, Math.max(5, Math.floor(prices.length / 3)));
  const min = prices[0];
  const max = prices[prices.length - 1];
  const range = max - min || 1;
  const bucketSize = range / bucketCount;

  const buckets: { label: string; count: number }[] = [];
  for (let i = 0; i < bucketCount; i++) {
    const from = min + i * bucketSize;
    const to = from + bucketSize;
    const count = prices.filter((p) => p >= from && (i === bucketCount - 1 ? p <= to : p < to)).length;
    buckets.push({ label: formatCurrency(from), count });
  }

  const maxCount = Math.max(...buckets.map((b) => b.count));

  return (
    <div className="border border-[#1a1a1a] p-4">
      <h3 className="text-xs font-medium text-white mb-0.5">{summary.term.query}</h3>
      <p className="text-[9px] text-[#555] mb-3">
        Price distribution across {prices.length} listings
      </p>

      <div className="space-y-0.5">
        {buckets.map((b, i) => (
          <div key={i} className="flex items-center gap-2">
            <span className="text-[10px] text-[#555] w-16 text-right">{b.label}</span>
            <div className="flex-1 h-4 bg-[#111] overflow-hidden">
              <div
                className="h-full bg-[#4a4] transition-all"
                style={{ width: `${maxCount > 0 ? (b.count / maxCount) * 100 : 0}%` }}
              />
            </div>
            <span className="text-[10px] text-[#555] w-5 text-right">{b.count}</span>
          </div>
        ))}
      </div>

      {/* Top 5 cheapest */}
      <div className="mt-4 pt-3 border-t border-[#1a1a1a]">
        <h4 className="text-[9px] font-medium text-[#555] uppercase tracking-wide mb-2">Cheapest listings</h4>
        <div className="space-y-1">
          {summary.listings
            .filter((l) => l.price > 0)
            .slice(0, 5)
            .map((l) => (
              <div key={l.epid} className="flex items-center justify-between text-[11px] border-b border-[#111] pb-1">
                <a
                  href={l.link}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#888] hover:text-white truncate flex-1 mr-4"
                >
                  {l.title}
                </a>
                <span className="font-medium text-white flex-shrink-0">
                  {formatCurrency(l.price)}
                </span>
              </div>
            ))}
        </div>
      </div>
    </div>
  );
}
