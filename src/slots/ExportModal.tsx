/**
 * ExportModal — Phase 4. Analytical export (CSV blob or Parquet presigned URL).
 * Uses GlobalTimeContext and panel.series for POST /api/datahub/export.
 */

import React, { useState } from 'react';
import { X } from 'lucide-react';
import type { DashboardPanel, GlobalTimeContext } from '../types/dashboard';
import {
  requestExport,
  type ExportAggregation,
} from '../services/datahubApi';

export interface ExportModalProps {
  panel: DashboardPanel;
  timeContext: GlobalTimeContext;
  onClose: () => void;
}

function triggerCsvDownload(blob: Blob, startTime: string, endTime: string): void {
  const start = startTime.slice(0, 10);
  const end = endTime.slice(0, 10);
  const name = `datahub-export-${start}_${end}.csv`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

export const ExportModal: React.FC<ExportModalProps> = ({
  panel,
  timeContext,
  onClose,
}) => {
  const [format, setFormat] = useState<'csv' | 'parquet'>('csv');
  const [aggregation, setAggregation] = useState<ExportAggregation>('1 hour');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const canExport = panel.series.length > 0;

  const handleExport = async () => {
    if (!canExport) return;
    setLoading(true);
    setError(null);
    try {
      const payload = {
        start_time: timeContext.startTime,
        end_time: timeContext.endTime,
        series: panel.series.map((s) => ({
          entity_id: s.entityId,
          attribute: s.attribute,
        })),
        format,
        aggregation,
      };
      const result = await requestExport(payload);
      if (result.format === 'csv') {
        triggerCsvDownload(result.blob, timeContext.startTime, timeContext.endTime);
      } else {
        window.open(result.data.download_url, '_blank');
      }
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60"
      role="dialog"
      aria-modal="true"
      aria-labelledby="export-modal-title"
    >
      <div className="bg-slate-900 border border-slate-700 rounded-lg shadow-xl w-full max-w-md mx-4 p-4">
        <div className="flex justify-between items-center mb-4">
          <h2 id="export-modal-title" className="text-sm font-semibold text-slate-200">
            Exportar datos
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="text-slate-400 hover:text-slate-200 p-1"
            aria-label="Cerrar"
          >
            <X size={18} />
          </button>
        </div>

        {!canExport ? (
          <p className="text-slate-400 text-sm mb-4">
            Este panel no tiene series. Añade al menos una para exportar.
          </p>
        ) : (
          <>
            <div className="space-y-4 mb-4">
              <div>
                <label className="block text-xs text-slate-400 mb-1">Formato</label>
                <select
                  value={format}
                  onChange={(e) => setFormat(e.target.value as 'csv' | 'parquet')}
                  className="w-full bg-slate-800 border border-slate-600 rounded px-3 py-2 text-sm text-slate-200"
                >
                  <option value="csv">CSV (texto)</option>
                  <option value="parquet">Parquet (binario)</option>
                </select>
              </div>
              <div>
                <label className="block text-xs text-slate-400 mb-1">Granularidad</label>
                <select
                  value={aggregation}
                  onChange={(e) => setAggregation(e.target.value as ExportAggregation)}
                  className="w-full bg-slate-800 border border-slate-600 rounded px-3 py-2 text-sm text-slate-200"
                >
                  <option value="raw">Raw (alta frecuencia)</option>
                  <option value="1 hour">1 hora</option>
                  <option value="1 day">1 día</option>
                </select>
              </div>
            </div>
            {error && (
              <p className="text-red-400 text-xs mb-4" role="alert">
                {error}
              </p>
            )}
          </>
        )}

        <div className="flex justify-end gap-2">
          <button
            type="button"
            onClick={onClose}
            className="px-3 py-1.5 text-sm text-slate-300 hover:text-slate-100 border border-slate-600 rounded"
          >
            Cancelar
          </button>
          <button
            type="button"
            onClick={handleExport}
            disabled={!canExport || loading}
            className="px-3 py-1.5 text-sm bg-blue-600 text-white rounded hover:bg-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? 'Exportando…' : 'Exportar'}
          </button>
        </div>
      </div>
    </div>
  );
};
