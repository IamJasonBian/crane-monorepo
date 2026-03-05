import { Trash2, Eye, EyeOff } from 'lucide-react';
import { formatCurrency, timeAgo } from '../utils/formatters';
import type { WatchTarget } from '../services/types';

interface TargetsListProps {
  targets: WatchTarget[];
  onDelete: (id: string) => void;
}

export default function TargetsList({ targets, onDelete }: TargetsListProps) {
  if (targets.length === 0) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8 text-center text-gray-400">
        No tracked targets yet. Add a symbol to start watching.
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {targets.map((t) => (
        <div
          key={t.target_id}
          className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 flex items-center justify-between hover:shadow-md transition-shadow"
        >
          <div className="flex items-center gap-4">
            <div className={`w-2 h-2 rounded-full ${t.enabled ? 'bg-green-500' : 'bg-gray-300'}`} />
            <div>
              <div className="flex items-center gap-2">
                <span className="font-semibold text-gray-900">{t.symbol}</span>
                {t.underlying && t.underlying !== t.symbol && (
                  <span className="text-xs text-gray-400">({t.underlying})</span>
                )}
                {t.dry_run && (
                  <span className="text-xs bg-yellow-100 text-yellow-700 px-1.5 py-0.5 rounded">
                    dry-run
                  </span>
                )}
              </div>
              <p className="text-sm text-gray-500">
                Threshold: {formatCurrency(t.threshold_price)}
                {t.max_qty > 0 && ` / Max qty: ${t.max_qty}`}
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-400">{timeAgo(t.created_at)}</span>
            {t.enabled ? (
              <Eye className="w-4 h-4 text-green-500" />
            ) : (
              <EyeOff className="w-4 h-4 text-gray-400" />
            )}
            <button
              onClick={() => onDelete(t.target_id)}
              className="p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition-colors"
            >
              <Trash2 className="w-4 h-4" />
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}
