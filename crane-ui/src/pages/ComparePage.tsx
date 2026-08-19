import { useState, useEffect } from 'react';
import { RefreshCw, Server } from 'lucide-react';
import {
  AreaChart,
  Area,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { getBoms, getBomHistory } from '../services/api';
import { formatCurrency } from '../utils/formatters';
import type { BomSpec, BomHistory, PriceSnapshot } from '../services/types';

function pctChange(series: { low?: number; total?: number }[], key: 'low' | 'total'): number | null {
  const vals = series.map((s) => (s as any)[key] as number).filter((v) => v > 0);
  if (vals.length < 2) return null;
  return ((vals[vals.length - 1] - vals[0]) / vals[0]) * 100;
}

function latest<T>(arr: T[]): T | undefined {
  return arr.length ? arr[arr.length - 1] : undefined;
}

export default function ComparePage() {
  const [boms, setBoms] = useState<BomSpec[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [history, setHistory] = useState<BomHistory | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    getBoms()
      .then((bs) => {
        setBoms(bs);
        if (bs.length > 0) setSelected(bs[0].bom_id);
        else setLoading(false);
      })
      .catch((e) => {
        setError(e.message);
        setLoading(false);
      });
  }, []);

  const load = async (bomId: string, isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    else setLoading(true);
    setError(null);
    try {
      setHistory(await getBomHistory(bomId));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    if (selected) load(selected);
  }, [selected]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <RefreshCw className="w-6 h-6 text-[#555] animate-spin" />
      </div>
    );
  }

  const chassisChange = history ? pctChange(history.chassis, 'low') : null;
  const totalChange = history ? pctChange(history.total, 'total') : null;
  const chassisNow = history ? latest(history.chassis) : undefined;
  const totalNow = history ? latest(history.total) : undefined;

  return (
    <div className="max-w-7xl mx-auto px-4 py-4">
      <div className="flex items-center justify-between mb-4 border-b border-[#222] pb-2">
        <div>
          <h1 className="text-sm font-medium text-white uppercase tracking-wider flex items-center gap-2">
            <Server className="w-4 h-4 text-[#4a4]" /> Compare
          </h1>
          <p className="text-[9px] text-[#555] uppercase tracking-wide mt-0.5">
            historical chassis &amp; whole-build price tracking
          </p>
        </div>
        <div className="flex items-center gap-2">
          {boms.length > 1 && (
            <select
              value={selected ?? ''}
              onChange={(e) => setSelected(e.target.value)}
              className="bg-black border border-[#333] text-[10px] text-[#ccc] px-2 py-1"
            >
              {boms.map((b) => (
                <option key={b.bom_id} value={b.bom_id}>{b.name}</option>
              ))}
            </select>
          )}
          <button
            onClick={() => selected && load(selected, true)}
            disabled={refreshing}
            className="flex items-center gap-1.5 px-2 py-1 border border-[#333] text-[10px] text-[#666] hover:text-[#ccc] hover:border-[#555] disabled:opacity-50"
          >
            <RefreshCw className={`w-3 h-3 ${refreshing ? 'animate-spin' : ''}`} />
            refresh
          </button>
        </div>
      </div>

      {error && <p className="text-[#a44] text-sm mb-3">{error}</p>}

      {history && (
        <>
          {/* Headline stats */}
          <div className="grid grid-cols-2 gap-1 mb-4">
            <StatCard
              label="Chassis (cheapest)"
              sub={history.chassis_term_id}
              value={chassisNow ? formatCurrency(chassisNow.low) : '—'}
              change={chassisChange}
              count={chassisNow?.sample_count}
            />
            <StatCard
              label="Whole build total"
              sub={history.name}
              value={totalNow ? formatCurrency(totalNow.total) : '—'}
              change={totalChange}
              count={history.lines.length}
              countLabel="lines"
            />
          </div>

          {/* Chassis price over time */}
          <ChartCard
            title="Chassis price over time"
            subtitle="cheapest qualifying listing per day"
            data={history.chassis.map((p) => ({ date: p.date, value: p.low }))}
            kind="area"
            change={chassisChange}
          />

          {/* Build total over time */}
          <ChartCard
            title="Whole-build total over time"
            subtitle="Σ cheapest component × qty (carried forward)"
            data={history.total.map((p) => ({ date: p.date, value: p.total }))}
            kind="line"
            change={totalChange}
          />

          {/* BOM table */}
          <BomTable history={history} />
        </>
      )}
    </div>
  );
}

function StatCard({
  label, sub, value, change, count, countLabel = 'listings',
}: {
  label: string; sub: string; value: string; change: number | null; count?: number; countLabel?: string;
}) {
  return (
    <div className="border border-[#1a1a1a] p-3">
      <div className="flex items-start justify-between mb-1">
        <span className="text-[9px] font-medium uppercase text-[#555] tracking-wide">{label}</span>
        {count != null && <span className="text-[10px] text-[#555]">{count} {countLabel}</span>}
      </div>
      <p className="text-[11px] text-[#888] mb-1 truncate">{sub}</p>
      <div className="flex items-baseline gap-2">
        <p className="text-2xl font-bold text-white">{value}</p>
        {change != null && (
          <span className={`text-xs font-medium ${change <= 0 ? 'text-[#4a4]' : 'text-[#a44]'}`}>
            {change >= 0 ? '+' : ''}{change.toFixed(1)}%
          </span>
        )}
      </div>
    </div>
  );
}

function ChartCard({
  title, subtitle, data, kind, change,
}: {
  title: string; subtitle: string; data: { date: string; value: number }[];
  kind: 'area' | 'line'; change: number | null;
}) {
  const color = change != null && change > 0 ? '#a44' : '#4a4';
  const pts = data.filter((d) => d.value > 0);

  return (
    <div className="border border-[#1a1a1a] p-4 mb-4">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-xs font-medium text-white">{title}</h3>
          <p className="text-[9px] text-[#555]">{subtitle}</p>
        </div>
      </div>
      {pts.length < 2 ? (
        <div className="h-[220px] flex items-center justify-center text-center">
          <p className="text-[11px] text-[#555]">
            Collecting history — one point is written per day.<br />
            Chart fills in as snapshots accrue.
          </p>
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          {kind === 'area' ? (
            <AreaChart data={pts}>
              <defs>
                <linearGradient id={`g-${title}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={color} stopOpacity={0.25} />
                  <stop offset="95%" stopColor={color} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1a1a1a" />
              <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#555' }} axisLine={{ stroke: '#222' }} />
              <YAxis tickFormatter={(v) => formatCurrency(v)} tick={{ fontSize: 10, fill: '#555' }} axisLine={{ stroke: '#222' }} width={64} domain={['auto', 'auto']} />
              <Tooltip content={<DarkTooltip />} />
              <Area type="monotone" dataKey="value" stroke={color} strokeWidth={2} fill={`url(#g-${title})`} />
            </AreaChart>
          ) : (
            <LineChart data={pts}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1a1a1a" />
              <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#555' }} axisLine={{ stroke: '#222' }} />
              <YAxis tickFormatter={(v) => formatCurrency(v)} tick={{ fontSize: 10, fill: '#555' }} axisLine={{ stroke: '#222' }} width={64} domain={['auto', 'auto']} />
              <Tooltip content={<DarkTooltip />} />
              <Line type="monotone" dataKey="value" stroke={color} strokeWidth={2} dot={false} />
            </LineChart>
          )}
        </ResponsiveContainer>
      )}
    </div>
  );
}

function DarkTooltip({ active, payload }: any) {
  if (active && payload && payload.length) {
    const p = payload[0].payload;
    return (
      <div className="bg-black border border-[#333] p-2 text-[10px]">
        <p className="text-[#888]">{p.date}</p>
        <p className="text-white font-medium">{formatCurrency(payload[0].value)}</p>
      </div>
    );
  }
  return null;
}

function BomTable({ history }: { history: BomHistory }) {
  return (
    <div className="border border-[#1a1a1a] p-4">
      <h3 className="text-xs font-medium text-white mb-3">Bill of materials</h3>
      <table className="w-full text-[11px]">
        <thead>
          <tr className="text-[9px] uppercase tracking-wide text-[#555] border-b border-[#1a1a1a]">
            <th className="text-left font-medium pb-1">Role</th>
            <th className="text-left font-medium pb-1">Component</th>
            <th className="text-right font-medium pb-1">Qty</th>
            <th className="text-right font-medium pb-1">Latest low</th>
            <th className="text-right font-medium pb-1">30d Δ</th>
            <th className="text-right font-medium pb-1">Line total</th>
          </tr>
        </thead>
        <tbody>
          {history.lines.map((line) => {
            const cur = latest(line.series) as PriceSnapshot | undefined;
            const change = pctChange(line.series as any, 'low');
            const lineTotal = cur ? cur.low * line.qty : 0;
            const isChassis = line.term_id === history.chassis_term_id;
            return (
              <tr key={line.term_id} className={`border-b border-[#111] ${isChassis ? 'text-white' : 'text-[#aaa]'}`}>
                <td className="py-1.5">
                  <span className={`text-[9px] uppercase tracking-wide ${isChassis ? 'text-[#4a4]' : 'text-[#555]'}`}>
                    {line.role}
                  </span>
                </td>
                <td className="py-1.5 truncate max-w-[220px]">{line.label}</td>
                <td className="py-1.5 text-right">{line.qty}</td>
                <td className="py-1.5 text-right">{cur ? formatCurrency(cur.low) : '—'}</td>
                <td className={`py-1.5 text-right ${change == null ? 'text-[#555]' : change <= 0 ? 'text-[#4a4]' : 'text-[#a44]'}`}>
                  {change == null ? '—' : `${change >= 0 ? '+' : ''}${change.toFixed(1)}%`}
                </td>
                <td className="py-1.5 text-right text-white">{cur ? formatCurrency(lineTotal) : '—'}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
