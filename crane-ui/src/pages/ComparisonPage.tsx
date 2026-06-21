import { useState, useEffect } from 'react';
import { RefreshCw, ExternalLink } from 'lucide-react';
import { getListingsByTerm } from '../services/api';
import { formatCurrency, timeAgo } from '../utils/formatters';
import type { EbayListing } from '../services/types';

interface ProductGroup {
  label: string;
  query: string;
  classifier: boolean;
  color: string;
  accent: string;
}

const PRODUCT_GROUPS: ProductGroup[] = [
  {
    label: 'A30 Chips',
    query: 'amd a30',
    classifier: true,
    color: '#1a1a2e',
    accent: '#6cf',
  },
  {
    label: 'Elitebook (Any)',
    query: 'hp elitebook laptop',
    classifier: true,
    color: '#1a2218',
    accent: '#4a4',
  },
  {
    label: 'Elitebook 120Hz+',
    query: 'hp elitebook 120hz',
    classifier: true,
    color: '#1a1f12',
    accent: '#9d4',
  },
  {
    label: 'Wireless Mouse',
    query: 'wireless mouse',
    classifier: true,
    color: '#221a2e',
    accent: '#c9f',
  },
];

interface GroupData {
  group: ProductGroup;
  listings: EbayListing[];
  low: number;
  median: number;
  high: number;
  loading: boolean;
  error: string | null;
}

export default function ComparisonPage() {
  const [groups, setGroups] = useState<GroupData[]>(
    PRODUCT_GROUPS.map((g) => ({
      group: g,
      listings: [],
      low: 0,
      median: 0,
      high: 0,
      loading: true,
      error: null,
    }))
  );
  const [activeGroup, setActiveGroup] = useState<string>(PRODUCT_GROUPS[0].query);
  const [refreshing, setRefreshing] = useState(false);

  const fetchGroup = async (g: ProductGroup): Promise<GroupData> => {
    try {
      const listings = await getListingsByTerm(g.query, { classifier: g.classifier, limit: 50 });
      const prices = listings.map((l) => l.price).filter((p) => p > 0).sort((a, b) => a - b);
      return {
        group: g,
        listings,
        low: prices[0] ?? 0,
        median: prices[Math.floor(prices.length / 2)] ?? 0,
        high: prices[prices.length - 1] ?? 0,
        loading: false,
        error: null,
      };
    } catch (err) {
      return {
        group: g,
        listings: [],
        low: 0,
        median: 0,
        high: 0,
        loading: false,
        error: err instanceof Error ? err.message : 'Failed to fetch',
      };
    }
  };

  const fetchAll = async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    setGroups((prev) => prev.map((g) => ({ ...g, loading: true, error: null })));

    const results = await Promise.all(PRODUCT_GROUPS.map(fetchGroup));
    setGroups(results);
    setRefreshing(false);
  };

  useEffect(() => { fetchAll(); }, []);

  const active = groups.find((g) => g.group.query === activeGroup);

  return (
    <div className="max-w-7xl mx-auto px-4 py-4">
      <div className="flex items-center justify-between mb-4 border-b border-[#222] pb-2">
        <div>
          <h1 className="text-sm font-medium text-white uppercase tracking-wider">Comparison</h1>
          <p className="text-[9px] text-[#555] uppercase tracking-wide mt-0.5">
            A30 chips · Elitebook laptops · Wireless mice — eBay &amp; Best Buy
          </p>
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

      {/* Summary cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-1 mb-4">
        {groups.map((g) => (
          <div
            key={g.group.query}
            onClick={() => setActiveGroup(g.group.query)}
            style={{ backgroundColor: activeGroup === g.group.query ? g.group.color : undefined }}
            className={`border p-3 cursor-pointer transition-colors ${
              activeGroup === g.group.query
                ? 'border-[#333]'
                : 'border-[#1a1a1a] hover:border-[#2a2a2a] hover:bg-[#0a0a0a]'
            }`}
          >
            <div className="flex items-center justify-between mb-2">
              <span
                className="text-[9px] font-medium uppercase tracking-wide"
                style={{ color: g.group.accent }}
              >
                {g.group.label}
              </span>
              {g.loading && <RefreshCw className="w-3 h-3 text-[#555] animate-spin" />}
              {!g.loading && (
                <span className="text-[10px] text-[#555]">{g.listings.length}</span>
              )}
            </div>

            {g.loading ? (
              <div className="h-8 flex items-center">
                <span className="text-[10px] text-[#333]">loading…</span>
              </div>
            ) : g.error ? (
              <p className="text-[10px] text-[#a44]">{g.error}</p>
            ) : g.listings.length === 0 ? (
              <p className="text-[10px] text-[#555]">no listings</p>
            ) : (
              <>
                <p className="text-lg font-bold text-white mb-1">{formatCurrency(g.low)}</p>
                <div className="grid grid-cols-3 gap-1 text-[10px] pt-2 border-t border-[#111]">
                  <div>
                    <p className="text-[#555]">Low</p>
                    <p style={{ color: g.group.accent }}>{formatCurrency(g.low)}</p>
                  </div>
                  <div>
                    <p className="text-[#555]">Med</p>
                    <p className="text-[#ccc]">{formatCurrency(g.median)}</p>
                  </div>
                  <div>
                    <p className="text-[#555]">High</p>
                    <p className="text-[#a44]">{formatCurrency(g.high)}</p>
                  </div>
                </div>
              </>
            )}
          </div>
        ))}
      </div>

      {/* Detail panel for active group */}
      {active && !active.loading && (
        <DetailPanel data={active} />
      )}
    </div>
  );
}

function DetailPanel({ data }: { data: GroupData }) {
  const { group, listings } = data;

  if (data.error) {
    return (
      <div className="border border-[#333] p-4 text-[11px] text-[#a44]">
        Error loading {group.label}: {data.error}
      </div>
    );
  }

  if (listings.length === 0) {
    return (
      <div className="border border-[#1a1a1a] p-6 text-center text-[11px] text-[#555]">
        No listings found for "{group.query}" — add this search term in the Terms page to start tracking
      </div>
    );
  }

  const sorted = [...listings].filter((l) => l.price > 0).sort((a, b) => a.price - b.price);

  return (
    <div className="border border-[#1a1a1a]">
      <div className="px-4 py-2 border-b border-[#1a1a1a] flex items-center justify-between">
        <div>
          <span className="text-xs font-medium text-white">{group.label}</span>
          <span className="ml-2 text-[9px] text-[#555]">{listings.length} listings · sorted by price</span>
        </div>
        <span className="text-[9px] uppercase tracking-wide" style={{ color: group.accent }}>
          eBay
        </span>
      </div>

      {/* Top 3 best deals highlight */}
      {sorted.length >= 3 && (
        <div className="grid grid-cols-3 gap-px border-b border-[#111] bg-[#111]">
          {sorted.slice(0, 3).map((l, i) => (
            <a
              key={l.epid}
              href={l.link}
              target="_blank"
              rel="noopener noreferrer"
              className="block p-3 bg-[#0a0a0a] hover:bg-[#111] transition-colors"
            >
              <div
                className="text-[9px] font-medium uppercase tracking-wide mb-1"
                style={{ color: i === 0 ? group.accent : '#555' }}
              >
                {i === 0 ? 'Best deal' : `#${i + 1}`}
              </div>
              <div className="text-sm font-bold text-white mb-1">{formatCurrency(l.price)}</div>
              <div className="text-[10px] text-[#888] truncate">{l.title}</div>
              {l.condition && (
                <div className="text-[9px] text-[#555] mt-1">{l.condition}</div>
              )}
            </a>
          ))}
        </div>
      )}

      {/* Full listing table */}
      <table className="w-full border-collapse text-[11px]">
        <thead>
          <tr>
            <th className="text-left text-[#555] font-medium px-3 py-1.5 border-b border-[#1a1a1a] text-[9px] uppercase tracking-wide">Title</th>
            <th className="text-right text-[#555] font-medium px-3 py-1.5 border-b border-[#1a1a1a] text-[9px] uppercase tracking-wide">Price</th>
            <th className="text-left text-[#555] font-medium px-3 py-1.5 border-b border-[#1a1a1a] text-[9px] uppercase tracking-wide">Condition</th>
            <th className="text-left text-[#555] font-medium px-3 py-1.5 border-b border-[#1a1a1a] text-[9px] uppercase tracking-wide">Seller</th>
            <th className="text-left text-[#555] font-medium px-3 py-1.5 border-b border-[#1a1a1a] text-[9px] uppercase tracking-wide">Seen</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((item) => (
            <tr
              key={item.epid}
              className={`hover:bg-[#0a0a0a] border-b border-[#111] ${item.has_sales ? 'opacity-40' : ''}`}
            >
              <td className="px-3 py-1.5 max-w-xs">
                <a
                  href={item.link}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#ccc] hover:text-white truncate block"
                >
                  {item.title}
                  <ExternalLink className="inline w-2.5 h-2.5 ml-1 text-[#333]" />
                </a>
              </td>
              <td className="px-3 py-1.5 text-right font-medium text-white tabular-nums whitespace-nowrap">
                {formatCurrency(item.price)}
              </td>
              <td className="px-3 py-1.5 text-[#666]">{item.condition || '—'}</td>
              <td className="px-3 py-1.5 text-[#666]">
                {item.seller?.name || '—'}
                {item.seller?.positive_feedback_percent > 0 && (
                  <span className="text-[#444] ml-1">({item.seller.positive_feedback_percent}%)</span>
                )}
              </td>
              <td className="px-3 py-1.5 text-[#555] whitespace-nowrap">
                {item.last_seen ? timeAgo(item.last_seen) : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
