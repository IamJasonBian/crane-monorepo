import { useState, useEffect } from 'react';
import { Search, RefreshCw } from 'lucide-react';
import OptionsTable from '../components/OptionsTable';
import { getOptions, getFeedHealth } from '../services/api';
import type { OptionsRecord, FeedHealth } from '../services/types';

export default function ListingsPage() {
  const [underlying, setUnderlying] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [options, setOptions] = useState<OptionsRecord[]>([]);
  const [feedHealth, setFeedHealth] = useState<FeedHealth | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load feed health to know which symbols have data
  useEffect(() => {
    getFeedHealth()
      .then(setFeedHealth)
      .catch(() => { /* non-fatal */ });
  }, []);

  const handleSearch = async (symbol?: string) => {
    const target = symbol || searchInput.trim().toUpperCase();
    if (!target) return;
    setUnderlying(target);
    setLoading(true);
    setError(null);

    try {
      const data = await getOptions(target);
      setOptions(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch options');
      setOptions([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Listings</h1>
        <p className="text-sm text-gray-500 mt-1">Options chains and discovered contracts</p>
      </div>

      {/* Search bar */}
      <div className="flex gap-3 mb-6">
        <div className="relative flex-1 max-w-md">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
            placeholder="Search underlying (e.g. AAPL, IWN, CRWD)"
            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
        <button
          onClick={() => handleSearch()}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 bg-blue-500 text-white text-sm rounded-lg hover:bg-blue-600 disabled:opacity-50"
        >
          {loading ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
          Search
        </button>
      </div>

      {/* Quick-access tracked symbols */}
      {feedHealth && feedHealth.quote_symbols.length > 0 && (
        <div className="flex flex-wrap gap-2 mb-6">
          <span className="text-xs text-gray-400 self-center mr-1">Tracked:</span>
          {feedHealth.quote_symbols.map((sym) => (
            <button
              key={sym}
              onClick={() => { setSearchInput(sym); handleSearch(sym); }}
              className={`px-3 py-1 text-xs font-medium rounded-full border transition-colors ${
                sym === underlying
                  ? 'bg-blue-500 text-white border-blue-500'
                  : 'bg-white text-gray-600 border-gray-300 hover:border-blue-300'
              }`}
            >
              {sym}
            </button>
          ))}
        </div>
      )}

      {/* Results */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6 text-sm text-red-700">
          {error}
        </div>
      )}

      {underlying && !loading && (
        <div>
          <h2 className="text-lg font-semibold text-gray-900 mb-3">
            {underlying} Options Chain
            <span className="text-sm font-normal text-gray-400 ml-2">
              {options.length} contracts
            </span>
          </h2>
          <OptionsTable options={options} />
        </div>
      )}
    </div>
  );
}
