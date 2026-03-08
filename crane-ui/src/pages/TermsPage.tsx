import { useState, useEffect } from 'react';
import { Plus, Trash2, RefreshCw } from 'lucide-react';
import { getTerms, createTerm, deleteTerm, updateTerm } from '../services/api';
import { timeAgo } from '../utils/formatters';
import type { SearchTerm } from '../services/types';

export default function TermsPage() {
  const [terms, setTerms] = useState<SearchTerm[]>([]);
  const [showAdd, setShowAdd] = useState(false);
  const [form, setForm] = useState({ query: '', category: '', threshold: '', min: '' });
  const [loading, setLoading] = useState(true);

  const load = async () => {
    setLoading(true);
    try {
      const t = await getTerms();
      setTerms(t);
    } catch {
      // API may not be up
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const handleAdd = async () => {
    if (!form.query.trim()) return;
    const termId = form.query.trim().toLowerCase().replace(/\s+/g, '-');
    const term: SearchTerm = {
      term_id: termId,
      query: form.query.trim(),
      category: form.category || 'custom',
      enabled: true,
      threshold_price: parseFloat(form.threshold) || 0,
      min_price: parseFloat(form.min) || 0,
      sort_by: 'price_low_to_high',
      listing_type: 'buy_it_now',
      last_polled: '',
      result_count: 0,
      created_at: new Date().toISOString(),
    };
    try {
      await createTerm(term);
      setForm({ query: '', category: '', threshold: '', min: '' });
      setShowAdd(false);
      load();
    } catch (err) {
      console.error('Failed to create term:', err);
    }
  };

  const handleDelete = async (termId: string) => {
    try {
      await deleteTerm(termId);
      load();
    } catch (err) {
      console.error('Failed to delete term:', err);
    }
  };

  const handleUpdate = async (termId: string, field: string, value: string) => {
    const num = parseFloat(value) || 0;
    try {
      await updateTerm(termId, { [field]: num });
      load();
    } catch (err) {
      console.error('Failed to update term:', err);
    }
  };

  const categories = [...new Set(terms.map((t) => t.category).filter(Boolean))];

  return (
    <div className="max-w-7xl mx-auto px-4 py-4">
      <div className="flex items-center justify-between mb-3 border-b border-[#222] pb-2">
        <div>
          <h1 className="text-sm font-medium text-white uppercase tracking-wider">Search Terms</h1>
          <p className="text-[9px] text-[#555] uppercase tracking-wide mt-0.5">eBay queries monitored by crane-feed</p>
        </div>
        <div className="flex gap-1">
          <button
            onClick={load}
            className="flex items-center px-2 py-1 border border-[#333] text-[#666] hover:text-[#ccc] hover:border-[#555]"
          >
            <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} />
          </button>
          <button
            onClick={() => setShowAdd(!showAdd)}
            className="flex items-center gap-1 px-2 py-1 border border-[#333] text-[10px] text-[#666] hover:text-[#ccc] hover:border-[#555]"
          >
            <Plus className="w-3 h-3" />
            add
          </button>
        </div>
      </div>

      {/* Add form */}
      {showAdd && (
        <div className="border border-[#1a1a1a] p-3 mb-3">
          <div className="flex gap-2 items-end">
            <div className="flex-1">
              <label className="block text-[9px] font-medium text-[#555] uppercase tracking-wide mb-1">Query</label>
              <input
                type="text"
                value={form.query}
                onChange={(e) => setForm({ ...form, query: e.target.value })}
                onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
                placeholder="nvidia a100 gpu"
                className="w-full px-2 py-1 bg-[#0a0a0a] border border-[#333] text-[11px] text-white focus:outline-none focus:border-[#555]"
              />
            </div>
            <div className="w-28">
              <label className="block text-[9px] font-medium text-[#555] uppercase tracking-wide mb-1">Category</label>
              <input
                type="text"
                value={form.category}
                onChange={(e) => setForm({ ...form, category: e.target.value })}
                placeholder="gpu"
                className="w-full px-2 py-1 bg-[#0a0a0a] border border-[#333] text-[11px] text-white focus:outline-none focus:border-[#555]"
              />
            </div>
            <div className="w-24">
              <label className="block text-[9px] font-medium text-[#555] uppercase tracking-wide mb-1">Min $</label>
              <input
                type="number"
                value={form.min}
                onChange={(e) => setForm({ ...form, min: e.target.value })}
                placeholder="0"
                className="w-full px-2 py-1 bg-[#0a0a0a] border border-[#333] text-[11px] text-white focus:outline-none focus:border-[#555]"
              />
            </div>
            <div className="w-24">
              <label className="block text-[9px] font-medium text-[#555] uppercase tracking-wide mb-1">Max $</label>
              <input
                type="number"
                value={form.threshold}
                onChange={(e) => setForm({ ...form, threshold: e.target.value })}
                placeholder="5000"
                className="w-full px-2 py-1 bg-[#0a0a0a] border border-[#333] text-[11px] text-white focus:outline-none focus:border-[#555]"
              />
            </div>
            <button
              onClick={handleAdd}
              className="px-3 py-1 border border-[#4a4] text-[10px] text-[#4a4] hover:bg-[#1a331a]"
            >
              save
            </button>
            <button
              onClick={() => setShowAdd(false)}
              className="px-3 py-1 border border-[#333] text-[10px] text-[#666] hover:text-[#ccc]"
            >
              cancel
            </button>
          </div>
        </div>
      )}

      {/* Category groups */}
      {categories.length > 0 ? (
        categories.map((cat) => {
          const catTerms = terms.filter((t) => t.category === cat);
          return (
            <div key={cat} className="mb-4">
              <h2 className="text-[9px] font-medium text-[#555] uppercase tracking-wide mb-1">
                {cat} <span className="text-[#333]">({catTerms.length})</span>
              </h2>
              <table className="w-full border-collapse text-[11px]">
                <thead>
                  <tr>
                    <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Query</th>
                    <th className="text-right text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Results</th>
                    <th className="text-right text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Min $</th>
                    <th className="text-right text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Max $</th>
                    <th className="text-left text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Polled</th>
                    <th className="text-center text-[#555] font-medium px-2 py-1 border-b border-[#222] uppercase text-[9px] tracking-wide">Status</th>
                    <th className="px-2 py-1 border-b border-[#222] w-8"></th>
                  </tr>
                </thead>
                <tbody>
                  {catTerms.map((t) => (
                    <tr key={t.term_id} className="hover:bg-[#0a0a0a] border-b border-[#111]">
                      <td className="px-2 py-1.5 text-white font-medium">{t.query}</td>
                      <td className="px-2 py-1.5 text-right text-[#888] tabular-nums">{t.result_count || '\u2014'}</td>
                      <td className="px-2 py-1.5 text-right">
                        <input
                          type="number"
                          defaultValue={t.min_price || ''}
                          placeholder="—"
                          onBlur={(e) => handleUpdate(t.term_id, 'min_price', e.target.value)}
                          onKeyDown={(e) => e.key === 'Enter' && (e.target as HTMLInputElement).blur()}
                          className="w-16 px-1 py-0.5 bg-transparent border border-transparent hover:border-[#333] focus:border-[#555] text-[11px] text-[#888] text-right tabular-nums focus:outline-none focus:text-white"
                        />
                      </td>
                      <td className="px-2 py-1.5 text-right">
                        <input
                          type="number"
                          defaultValue={t.threshold_price || ''}
                          placeholder="—"
                          onBlur={(e) => handleUpdate(t.term_id, 'threshold_price', e.target.value)}
                          onKeyDown={(e) => e.key === 'Enter' && (e.target as HTMLInputElement).blur()}
                          className="w-16 px-1 py-0.5 bg-transparent border border-transparent hover:border-[#333] focus:border-[#555] text-[11px] text-[#888] text-right tabular-nums focus:outline-none focus:text-white"
                        />
                      </td>
                      <td className="px-2 py-1.5 text-[#666]">
                        {t.last_polled ? timeAgo(t.last_polled) : 'never'}
                      </td>
                      <td className="px-2 py-1.5 text-center">
                        <span className={`text-[9px] px-1 py-px ${
                          t.enabled
                            ? 'bg-[#1a331a] text-[#4a4]'
                            : 'bg-[#1a1a1a] text-[#555]'
                        }`}>
                          {t.enabled ? 'ACTIVE' : 'OFF'}
                        </span>
                      </td>
                      <td className="px-2 py-1.5">
                        <button
                          onClick={() => handleDelete(t.term_id)}
                          className="text-[#333] hover:text-[#a44] transition-colors"
                        >
                          <Trash2 className="w-3 h-3" />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          );
        })
      ) : (
        !loading && (
          <div className="text-center text-[#333] text-[10px] py-8">
            No search terms configured
          </div>
        )
      )}
    </div>
  );
}
