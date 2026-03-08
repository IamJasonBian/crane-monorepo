import { useState, useEffect } from 'react';
import { RefreshCw, ExternalLink } from 'lucide-react';
import { getTerms, getListingsByTerm } from '../services/api';
import { formatCurrency, timeAgo } from '../utils/formatters';
import type { EbayListing, SearchTerm } from '../services/types';

export default function ListingsPage() {
  const [terms, setTerms] = useState<SearchTerm[]>([]);
  const [activeTerm, setActiveTerm] = useState<string>('');
  const [listings, setListings] = useState<EbayListing[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rawSearch, setRawSearch] = useState(false);

  useEffect(() => {
    getTerms()
      .then((t) => { setTerms(t.filter((x) => x.enabled)); })
      .catch(() => {});
  }, []);

  const handleSearch = async (query: string, raw?: boolean) => {
    if (!query) return;
    setActiveTerm(query);
    setLoading(true);
    setError(null);
    const useRaw = raw !== undefined ? raw : rawSearch;
    try {
      const data = await getListingsByTerm(query, { raw_search: useRaw });
      setListings(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch listings');
      setListings([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (terms.length > 0 && !activeTerm) {
      const first = terms.find((t) => t.result_count > 0) || terms[0];
      handleSearch(first.query);
    }
  }, [terms]);

  return (
    <div className="max-w-7xl mx-auto px-4 py-4">
      <div className="mb-3 border-b border-[#222] pb-2">
        <h1 className="text-sm font-medium text-white uppercase tracking-wider">Listings</h1>
        <p className="text-[9px] text-[#555] uppercase tracking-wide mt-0.5">Live eBay listings from Countdown API</p>
      </div>

      {/* Term selector + raw toggle */}
      {terms.length > 0 && (
        <div className="flex flex-wrap items-center gap-1 mb-3">
          {terms.map((t) => (
            <button
              key={t.term_id}
              onClick={() => handleSearch(t.query)}
              className={`px-2 py-0.5 text-[10px] border transition-colors ${
                t.query === activeTerm
                  ? 'bg-white text-black border-white'
                  : 'text-[#666] border-[#333] hover:border-[#555] hover:text-[#ccc]'
              }`}
            >
              {t.query}
              {t.result_count > 0 && (
                <span className="ml-1 opacity-60">{t.result_count}</span>
              )}
            </button>
          ))}
          <span className="mx-1 text-[#222]">|</span>
          <button
            onClick={() => {
              const next = !rawSearch;
              setRawSearch(next);
              if (activeTerm) handleSearch(activeTerm, next);
            }}
            className={`px-2 py-0.5 text-[10px] border transition-colors ${
              rawSearch
                ? 'bg-[#332a1a] text-[#fa3] border-[#554422]'
                : 'text-[#555] border-[#333] hover:border-[#555] hover:text-[#ccc]'
            }`}
          >
            raw search
          </button>
        </div>
      )}

      {error && (
        <div className="border border-[#a44] bg-[rgba(170,68,68,0.1)] p-2 mb-3 text-[11px] text-[#f66]">
          {error}
        </div>
      )}

      {loading && (
        <div className="flex items-center justify-center py-8">
          <RefreshCw className="w-4 h-4 text-[#555] animate-spin" />
          <span className="ml-2 text-[#555] text-xs">loading</span>
        </div>
      )}

      {activeTerm && !loading && (
        <div className="mb-2">
          <span className="text-[9px] text-[#555] uppercase tracking-wide">
            {activeTerm} &mdash; {listings.length} listings
          </span>
        </div>
      )}

      {/* Listing table */}
      {!loading && listings.length > 0 && (
        <table className="w-full border-collapse text-[11px]">
          <thead>
            <tr>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Title</th>
              <th className="text-right text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Price</th>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Condition</th>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Seller</th>
              <th className="text-center text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Tags</th>
              <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Seen</th>
            </tr>
          </thead>
          <tbody>
            {listings.map((item) => (
              <tr key={item.epid} className={`hover:bg-[#0a0a0a] border-b border-[#111] ${item.has_sales ? 'opacity-40' : ''}`}>
                <td className="px-2 py-1.5 max-w-md">
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
                <td className="px-2 py-1.5 text-right text-white font-medium tabular-nums">
                  {formatCurrency(item.price)}
                </td>
                <td className="px-2 py-1.5 text-[#666]">{item.condition || '\u2014'}</td>
                <td className="px-2 py-1.5 text-[#666]">
                  {item.seller?.name || '\u2014'}
                  {item.seller?.positive_feedback_percent > 0 && (
                    <span className="text-[#555] ml-1">({item.seller.positive_feedback_percent}%)</span>
                  )}
                </td>
                <td className="px-2 py-1.5 text-center">
                  <span className="inline-flex gap-1">
                    {item.has_sales && <span className="text-[9px] px-1 py-px bg-[#332a1a] text-[#fa3]">HAS SALES</span>}
                    {item.buy_it_now && <span className="text-[9px] px-1 py-px bg-[#1a331a] text-[#4a4]">BIN</span>}
                    {item.free_returns && <span className="text-[9px] px-1 py-px bg-[#1a2233] text-[#6cf]">RET</span>}
                    {item.best_offer && <span className="text-[9px] px-1 py-px bg-[#2a1a33] text-[#c9f]">BO</span>}
                  </span>
                </td>
                <td className="px-2 py-1.5 text-[#555]">
                  {item.last_seen ? timeAgo(item.last_seen) : '\u2014'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {!loading && activeTerm && listings.length === 0 && !error && (
        <div className="text-center text-[#333] text-[10px] py-8">
          No listings found for "{activeTerm}"
        </div>
      )}
    </div>
  );
}
