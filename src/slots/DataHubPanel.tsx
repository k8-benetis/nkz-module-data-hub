import React, { useState, useMemo, useRef, useEffect, useCallback } from 'react';
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query';
import UPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { fetchEventSource } from '@microsoft/fetch-event-source';
import {
  fetchDataHubEntities,
  fetchTimeseriesArrow,
  fetchTimeseriesAlign,
  getAuthToken,
  getIntelligenceStreamUrl,
  requestExport,
  submitPredictJob,
  type DataHubEntity,
  type PredictionResult,
} from '../services/datahubApi';

export interface CanvasSeriesItem {
  entityId: string;
  attribute: string;
  label: string;
  /** Data source from entity (e.g. timescale, odoo). Used for hybrid align. */
  source?: string;
}

/** Unit of measure per attribute: scales are grouped by this so same unit shares one Y axis. */
const ATTRIBUTE_UNIT: Record<string, string> = {
  temp_avg: '°C',
  temp_min: '°C',
  temp_max: '°C',
  humidity_avg: '%',
  precip_mm: 'mm',
  solar_rad_w_m2: 'W/m²',
  eto_mm: 'mm',
  soil_moisture_0_10cm: '%',
  wind_speed_ms: 'm/s',
  pressure_hpa: 'hPa',
};

const queryClient = new QueryClient({ defaultOptions: { queries: { staleTime: 60_000 } } });

const RESOLUTION = 1000;
const BRUSH_DEBOUNCE_MS = 350;

/**
 * Matrix padding for uPlot: overlay prediction on historical without mutating original arrays.
 * Builds new X (N_hist + N_pred), padded historical (tail = null), padded prediction (head = null).
 * First prediction point shares the same time index as last historical for visual continuity.
 */
export function renderPrediction(
  uplotInstance: UPlot,
  histTimestamps: Float64Array,
  histValues: Float64Array,
  result: PredictionResult
): void {
  const N_hist = histTimestamps.length;
  const predictions = result.predictions ?? [];
  const N_pred = predictions.length;
  if (N_pred === 0) return;

  const totalLen = N_hist + N_pred;
  const newX = new Float64Array(totalLen);
  newX.set(histTimestamps, 0);
  for (let i = 0; i < N_pred; i++) {
    newX[N_hist + i] = new Date(predictions[i].timestamp).getTime() / 1000;
  }

  const paddedHist: (number | null)[] = new Array(totalLen);
  for (let i = 0; i < N_hist; i++) paddedHist[i] = histValues[i];
  for (let i = N_hist; i < totalLen; i++) paddedHist[i] = null;

  const paddedPred: (number | null)[] = new Array(totalLen);
  for (let i = 0; i < N_hist; i++) paddedPred[i] = null;
  for (let i = 0; i < N_pred; i++) paddedPred[N_hist + i] = predictions[i].value;

  const seriesCount = uplotInstance.series.length;
  if (seriesCount === 2) {
    uplotInstance.addSeries(
      {
        label: 'Predicción (IA)',
        stroke: 'orange',
        dash: [10, 5],
        width: 2,
        scale: 'y',
      },
      2
    );
  }
  uplotInstance.setData([newX, paddedHist, paddedPred]);
}

function defaultTimeRange(): { start: string; end: string } {
  const end = new Date();
  const start = new Date(end.getTime() - 7 * 24 * 60 * 60 * 1000);
  return { start: start.toISOString(), end: end.toISOString() };
}

function useDebouncedCallback<T extends (...args: unknown[]) => void>(fn: T, ms: number): T {
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const fnRef = useRef(fn);
  fnRef.current = fn;
  return useCallback(
    ((...args: unknown[]) => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      timeoutRef.current = setTimeout(() => {
        timeoutRef.current = null;
        fnRef.current(...args);
      }, ms);
    }) as T,
    [ms]
  );
}

const DataTree: React.FC<{
  selectedEntity: DataHubEntity | null;
  selectedAttribute: string | null;
  onSelect: (entity: DataHubEntity, attribute: string) => void;
  onAddToCanvas?: (entity: DataHubEntity, attribute: string) => void;
}> = ({ selectedEntity, selectedAttribute, onSelect, onAddToCanvas }) => {
  const [search, setSearch] = useState('');
  const { data, isLoading, error } = useQuery({
    queryKey: ['datahub', 'entities', search || null],
    queryFn: () => fetchDataHubEntities(search || undefined),
    placeholderData: (prev) => prev,
  });

  const entities = useMemo(() => data?.entities ?? [], [data?.entities]);

  return (
    <div className="flex flex-col h-full min-h-0">
      <div className="p-2 border-b border-slate-200 dark:border-slate-700">
        <input
          type="search"
          placeholder="Search entities (e.g. Parcela 4)"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full px-3 py-2 text-sm border border-slate-300 dark:border-slate-600 rounded bg-white dark:bg-slate-800 text-slate-900 dark:text-slate-100 placeholder-slate-500"
        />
      </div>
      <div className="flex-1 overflow-auto p-2">
        {isLoading && <p className="text-sm text-slate-500">Loading…</p>}
        {error && <p className="text-sm text-red-600">{String(error)}</p>}
        {!isLoading && !error && entities.length === 0 && (
          <p className="text-sm text-slate-500">No entities. Set PLATFORM_API_URL on the BFF to list from the platform.</p>
        )}
        {!isLoading && !error && entities.length > 0 && (
          <ul className="space-y-1 text-sm">
            {entities.map((e: DataHubEntity) => (
              <li key={e.id} className="space-y-0.5">
                <div
                  role="button"
                  tabIndex={0}
                  onClick={() => {
                    const attr = e.attributes[0] ?? '';
                    if (attr) {
                      onSelect(e, attr);
                      onAddToCanvas?.(e, attr);
                    }
                  }}
                  onKeyDown={(ev) => {
                    if (ev.key === 'Enter' || ev.key === ' ') {
                      const attr = e.attributes[0] ?? '';
                      if (attr) {
                        onSelect(e, attr);
                        onAddToCanvas?.(e, attr);
                      }
                    }
                  }}
                  className={`flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer ${
                    selectedEntity?.id === e.id
                      ? 'bg-slate-200 dark:bg-slate-600'
                      : 'hover:bg-slate-100 dark:hover:bg-slate-700/50'
                  }`}
                >
                  <span className="font-medium text-slate-800 dark:text-slate-200 truncate">{e.name}</span>
                  <span className="text-slate-500 shrink-0">{e.type}</span>
                </div>
                {selectedEntity?.id === e.id && e.attributes.length > 0 && (
                  <ul className="pl-3 text-xs">
                    {e.attributes.map((attr) => (
                      <li
                        key={attr}
                        role="button"
                        tabIndex={0}
                        onClick={() => {
                          onSelect(e, attr);
                          onAddToCanvas?.(e, attr);
                        }}
                        onKeyDown={(ev) => {
                          if (ev.key === 'Enter' || ev.key === ' ') {
                            onSelect(e, attr);
                            onAddToCanvas?.(e, attr);
                          }
                        }}
                        className={`py-1 rounded px-1 cursor-pointer ${
                          selectedAttribute === attr
                            ? 'bg-slate-300 dark:bg-slate-500 text-slate-900 dark:text-slate-100'
                            : 'text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700/50'
                        }`}
                      >
                        {attr}
                      </li>
                    ))}
                  </ul>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
};

const DataCanvas: React.FC<{
  series: CanvasSeriesItem[];
  startTime: string;
  endTime: string;
  onBrushSelect?: (startIso: string, endIso: string) => void;
  predictionResult?: PredictionResult | null;
}> = ({ series, startTime, endTime, onBrushSelect, predictionResult }) => {
  const chartRef = useRef<HTMLDivElement>(null);
  const uplotRef = useRef<UPlot | null>(null);

  const singleSeries = series.length === 1 ? series[0] : null;
  const alignSeries = series.length >= 2 ? series : [];

  const singleQuery = useQuery({
    queryKey: ['timeseries', singleSeries?.entityId, singleSeries?.attribute, startTime, endTime, RESOLUTION],
    queryFn: ({ signal }) =>
      fetchTimeseriesArrow(
        singleSeries!.entityId,
        singleSeries!.attribute,
        startTime,
        endTime,
        RESOLUTION,
        signal
      ),
    structuralSharing: false,
    enabled: singleSeries != null && !!startTime && !!endTime,
  });

  const alignQuery = useQuery({
    queryKey: [
      'timeseries-align',
      alignSeries.map((s) => `${s.entityId}:${s.attribute}:${s.source ?? 'timescale'}`).join(','),
      startTime,
      endTime,
      RESOLUTION,
    ],
    queryFn: ({ signal }) =>
      fetchTimeseriesAlign(
        alignSeries.map((s) => ({
          entity_id: s.entityId,
          attribute: s.attribute,
          ...(s.source && { source: s.source }),
        })),
        startTime,
        endTime,
        RESOLUTION,
        signal
      ),
    structuralSharing: false,
    enabled: alignSeries.length >= 2 && !!startTime && !!endTime,
  });

  const isLoading = singleSeries ? singleQuery.isLoading : alignQuery.isLoading;
  const error = singleSeries ? singleQuery.error : alignQuery.error;
  const dataSingle = singleQuery.data;
  const dataAlign = alignQuery.data;

  useEffect(() => {
    return () => {
      if (uplotRef.current) {
        uplotRef.current.destroy();
        uplotRef.current = null;
      }
    };
  }, [series.length, series.map((s) => s.entityId + s.attribute).join(',')]);

  useEffect(() => {
    if (!chartRef.current) return;
    if (singleSeries && dataSingle) {
      const opts: UPlot.Options = {
        width: chartRef.current.offsetWidth,
        height: 280,
        series: [{}, { label: singleSeries.label }],
        scales: { x: { time: true }, y: {} },
        axes: [{}, { label: singleSeries.attribute }],
        cursor: { drag: { x: true, y: false } },
        hooks: onBrushSelect
          ? {
              setSelect: [
                (u, min, max) => {
                  if (min != null && max != null && Number.isFinite(min) && Number.isFinite(max) && min < max) {
                    onBrushSelect(new Date(min * 1000).toISOString(), new Date(max * 1000).toISOString());
                  }
                },
              ],
            }
          : undefined,
      };
      if (!uplotRef.current) {
        uplotRef.current = new UPlot(opts, [dataSingle.timestamps, dataSingle.values], chartRef.current);
      } else {
        uplotRef.current.setData([dataSingle.timestamps, dataSingle.values]);
      }
      return;
    }
    if (alignSeries.length >= 2 && dataAlign) {
      // One scale per unique unit of measure (e.g. °C, %) so same-magnitude series share one Y axis.
      const uniqueUnits = Array.from(
        new Set(alignSeries.map((s) => ATTRIBUTE_UNIT[s.attribute] ?? '')).filter(Boolean)
      );
      if (uniqueUnits.length === 0) uniqueUnits.push('');
      const unitToScale: Record<string, string> = {};
      uniqueUnits.forEach((u, i) => {
        unitToScale[u] = i === 0 ? 'y' : `y${i + 1}`;
      });
      const scales: Record<string, { min?: number; max?: number }> = { x: { time: true } };
      uniqueUnits.forEach((_, i) => (scales[unitToScale[uniqueUnits[i]]] = {}));
      const uplotSeries: UPlot.Series[] = [{}];
      alignSeries.forEach((s) => {
        const unit = ATTRIBUTE_UNIT[s.attribute] ?? '';
        uplotSeries.push({ scale: unitToScale[unit] ?? 'y', label: s.label });
      });
      const axes: UPlot.Axis[] = [{}];
      uniqueUnits.forEach((unit, i) => {
        axes.push({
          scale: unitToScale[unit],
          label: unit || 'value',
          side: i % 2 === 0 ? 1 : 2,
        });
      });
      const opts: UPlot.Options = {
        width: chartRef.current.offsetWidth,
        height: 280,
        series: uplotSeries,
        scales,
        axes,
        cursor: { drag: { x: true, y: false } },
        hooks: onBrushSelect
          ? {
              setSelect: [
                (u, min, max) => {
                  if (min != null && max != null && Number.isFinite(min) && Number.isFinite(max) && min < max) {
                    onBrushSelect(new Date(min * 1000).toISOString(), new Date(max * 1000).toISOString());
                  }
                },
              ],
            }
          : undefined,
      };
      const dataRows: (Float64Array | number[])[] = [dataAlign.timestamps, ...dataAlign.valueArrays];
      if (!uplotRef.current) {
        uplotRef.current = new UPlot(opts, dataRows, chartRef.current);
      } else {
        uplotRef.current.setData(dataRows);
      }
    }
  }, [singleSeries, dataSingle, alignSeries, dataAlign, onBrushSelect]);

  useEffect(() => {
    if (!predictionResult || !singleSeries || !dataSingle || !uplotRef.current) return;
    renderPrediction(uplotRef.current, dataSingle.timestamps, dataSingle.values, predictionResult);
  }, [predictionResult, singleSeries, dataSingle]);

  if (isLoading) return <p className="text-sm text-slate-500 p-4">Loading series…</p>;
  if (error) return <p className="text-sm text-red-600 p-4">{String(error)}</p>;
  if (series.length === 0) return null;
  if (singleSeries && !dataSingle) return null;
  if (alignSeries.length >= 2 && !dataAlign) return null;
  return <div ref={chartRef} className="w-full" />;
};

/**
 * DataHub bottom-panel: Data Tree (selector) + Data Canvas (uPlot) with Arrow, resolution, Brush, multi-series align.
 */
const DataHubPanelInner: React.FC = () => {
  const [selectedEntity, setSelectedEntity] = useState<DataHubEntity | null>(null);
  const [selectedAttribute, setSelectedAttribute] = useState<string | null>(null);
  const [canvasSeries, setCanvasSeries] = useState<CanvasSeriesItem[]>([]);
  const [timeRange, setTimeRangeState] = useState(defaultTimeRange);
  const [predictionResult, setPredictionResult] = useState<PredictionResult | null>(null);
  const [predictStatus, setPredictStatus] = useState<string | null>(null);
  const [exportStatus, setExportStatus] = useState<string | null>(null);
  const [exportAggregation, setExportAggregation] = useState<'raw' | '1 hour' | '1 day'>('1 hour');

  const handleSelect = (entity: DataHubEntity, attribute: string) => {
    setSelectedEntity(entity);
    setSelectedAttribute(attribute);
  };

  const handleAddToCanvas = useCallback((entity: DataHubEntity, attribute: string) => {
    setCanvasSeries((prev) => {
      if (prev.some((s) => s.entityId === entity.id && s.attribute === attribute)) return prev;
      return [
        ...prev,
        {
          entityId: entity.id,
          attribute,
          label: `${entity.name} — ${attribute}`,
          ...(entity.source && { source: entity.source }),
        },
      ];
    });
    setPredictionResult(null);
  }, []);

  const removeFromCanvas = useCallback((entityId: string, attribute: string) => {
    setCanvasSeries((prev) => prev.filter((s) => s.entityId !== entityId || s.attribute !== attribute));
    setPredictionResult(null);
  }, []);

  const setTimeRange = useCallback((start: string, end: string) => {
    setTimeRangeState({ start, end });
  }, []);
  const debouncedSetTimeRange = useDebouncedCallback(setTimeRange, BRUSH_DEBOUNCE_MS);

  const runPredict = useCallback(() => {
    const single = canvasSeries.length === 1 ? canvasSeries[0] : null;
    if (!single) return;
    const token = getAuthToken();
    if (!token) {
      setPredictStatus('auth_required');
      return;
    }
    setPredictionResult(null);
    setPredictStatus('submitting');
    submitPredictJob(
      single.entityId,
      single.attribute,
      timeRange.start,
      timeRange.end,
      24
    )
      .then((jobId) => {
        setPredictStatus('streaming');
        const streamUrl = getIntelligenceStreamUrl(jobId);
        const ctrl = new AbortController();
        fetchEventSource(streamUrl, {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${token}`,
            Accept: 'text/event-stream',
          },
          signal: ctrl.signal,
          onmessage(ev) {
            try {
              const data = JSON.parse(ev.data);
              if (data.status === 'completed') {
                ctrl.abort();
                setPredictStatus(null);
                if (data.result) setPredictionResult(data.result);
              } else if (data.status === 'failed' || data.status === 'cancelled') {
                ctrl.abort();
                setPredictStatus(data.error ?? data.status ?? 'failed');
              }
            } catch {
              ctrl.abort();
              setPredictStatus('parse_error');
            }
          },
          onerror(err) {
            ctrl.abort();
            setPredictStatus(err?.message ?? 'stream_error');
            throw err;
          },
        });
      })
      .catch((err) => {
        setPredictStatus(err?.message ?? 'submit_error');
      });
  }, [canvasSeries, timeRange.start, timeRange.end]);

  const runExport = useCallback(
    (format: 'csv' | 'parquet') => {
      if (canvasSeries.length === 0) return;
      setExportStatus(`Exporting ${format}…`);
      const payload = {
        start_time: timeRange.start,
        end_time: timeRange.end,
        resolution: RESOLUTION,
        series: canvasSeries.map((s) => ({ entity_id: s.entityId, attribute: s.attribute })),
        format,
      };
      requestExport(payload)
        .then((result) => {
          setExportStatus(null);
          if (result.format === 'csv') {
            const url = URL.createObjectURL(result.blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `timeseries_export_${Date.now()}.csv`;
            a.click();
            URL.revokeObjectURL(url);
          } else {
            window.open(result.data.download_url, '_blank', 'noopener');
          }
        })
        .catch((err) => {
          setExportStatus(err?.message ?? 'export_error');
        });
    },
    [canvasSeries, timeRange.start, timeRange.end, exportAggregation]
  );

  return (
    <div className="flex flex-col h-full text-slate-700 dark:text-slate-300">
      <div className="shrink-0 px-3 py-2 border-b border-slate-200 dark:border-slate-700 font-medium">
        DataHub — Data Tree
      </div>
      <div className="flex-1 min-h-0 flex">
        <div className="w-64 shrink-0 border-r border-slate-200 dark:border-slate-700">
          <DataTree
            selectedEntity={selectedEntity}
            selectedAttribute={selectedAttribute}
            onSelect={handleSelect}
            onAddToCanvas={handleAddToCanvas}
          />
        </div>
        <div className="flex-1 flex flex-col min-w-0 p-4">
          {canvasSeries.length > 0 ? (
            <>
              <div className="flex flex-wrap items-center gap-2 mb-2">
                {canvasSeries.length === 1 && (
                  <button
                    type="button"
                    onClick={runPredict}
                    disabled={!!predictStatus && predictStatus !== 'stream_error' && predictStatus !== 'submit_error' && predictStatus !== 'parse_error' && predictStatus !== 'auth_required'}
                    className="px-3 py-1.5 text-sm font-medium rounded bg-amber-500 text-white hover:bg-amber-600 disabled:opacity-50"
                  >
                    {predictStatus === 'submitting' || predictStatus === 'streaming'
                      ? 'Predicting…'
                      : 'Predicción (IA)'}
                  </button>
                )}
                {predictStatus && predictStatus !== 'submitting' && predictStatus !== 'streaming' && (
                  <span className="text-xs text-slate-500">{predictStatus}</span>
                )}
                {canvasSeries.length >= 1 && (
                  <>
                    <label className="flex items-center gap-1.5 text-xs text-slate-600 dark:text-slate-400">
                      <span>Export granularity:</span>
                      <select
                        value={exportAggregation}
                        onChange={(e) => setExportAggregation(e.target.value as 'raw' | '1 hour' | '1 day')}
                        className="rounded border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-800 text-slate-900 dark:text-slate-100 text-xs py-1 px-2"
                      >
                        <option value="raw">Raw (1 s)</option>
                        <option value="1 hour">1 hour</option>
                        <option value="1 day">1 day</option>
                      </select>
                    </label>
                    <button
                      type="button"
                      onClick={() => runExport('csv')}
                      disabled={!!exportStatus}
                      className="px-3 py-1.5 text-sm font-medium rounded bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50"
                    >
                      Export CSV
                    </button>
                    <button
                      type="button"
                      onClick={() => runExport('parquet')}
                      disabled={!!exportStatus}
                      className="px-3 py-1.5 text-sm font-medium rounded bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50"
                    >
                      Export Parquet
                    </button>
                  </>
                )}
                {exportStatus && (
                  <span className="text-xs text-slate-500">{exportStatus}</span>
                )}
                {canvasSeries.map((s) => (
                  <span
                    key={`${s.entityId}:${s.attribute}`}
                    className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs bg-slate-200 dark:bg-slate-600 text-slate-800 dark:text-slate-200"
                  >
                    {s.label}
                    <button
                      type="button"
                      aria-label="Remove"
                      onClick={() => removeFromCanvas(s.entityId, s.attribute)}
                      className="hover:bg-slate-300 dark:hover:bg-slate-500 rounded px-1"
                    >
                      ×
                    </button>
                  </span>
                ))}
              </div>
              <DataCanvas
                series={canvasSeries}
                startTime={timeRange.start}
                endTime={timeRange.end}
                onBrushSelect={debouncedSetTimeRange}
                predictionResult={predictionResult}
              />
            </>
          ) : (
            <div className="flex items-center justify-center flex-1 text-slate-500 text-sm">
              Click an entity and attribute in the tree to add series to the canvas (single or multi). Drag on chart to zoom.
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const DataHubPanel: React.FC = () => (
  <QueryClientProvider client={queryClient}>
    <DataHubPanelInner />
  </QueryClientProvider>
);

export default DataHubPanel;
