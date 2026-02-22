/**
 * DataCanvasPanel — Single or multi-series chart (Phase 1 + 3 + 4.5).
 * Single: GET /data. Multi: POST /align. With prediction: merge hist+pred in worker, 3 series (hist + pred dashed).
 */

import React, { useEffect, useMemo, useRef, useState } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';

import ArrowWorker from '../workers/arrow-decoder.worker?worker';
import { useUPlotCesiumSync } from '../hooks/useUPlotCesiumSync';
import { getAuthToken, getBaseUrl } from '../services/datahubApi';
import type { ChartSeriesDef, PredictionPayload } from '../types/dashboard';

const COLORS = ['#10B981', '#8B5CF6', '#F59E0B', '#3B82F6', '#EF4444'];
const PREDICTION_STROKE = '#F59E0B';

export interface DataCanvasPanelProps {
  panelId: string;
  series: ChartSeriesDef[];
  startTime: string;
  endTime: string;
  resolution: number;
  /** When set (SSE completed), merge with historical in worker and show Histórico + Predicción (IA). */
  prediction?: PredictionPayload | null;
}

type WorkerResponse =
  | { action: 'DECODE_ARROW_DONE'; jobId: string; uPlotData?: uPlot.AlignedData; error?: string }
  | { action: 'MERGE_PREDICTION_DONE'; jobId: string; uPlotData: uPlot.AlignedData };

export const DataCanvasPanel: React.FC<DataCanvasPanelProps> = ({
  panelId,
  series,
  startTime,
  endTime,
  resolution,
  prediction = null,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const workerRef = useRef<Worker | null>(null);

  const [plotData, setPlotData] = useState<uPlot.AlignedData | null>(null);
  const [mergedPlotData, setMergedPlotData] = useState<uPlot.AlignedData | null>(null);
  const [status, setStatus] = useState<'loading' | 'error' | 'ready' | 'empty'>('loading');

  useEffect(() => {
    const worker = new ArrowWorker();
    worker.onmessage = (e: MessageEvent<WorkerResponse & { uPlotData?: uPlot.AlignedData; error?: string }>) => {
      const d = e.data;
      if (d.error) {
        setStatus('error');
        return;
      }
      if (d.action === 'MERGE_PREDICTION_DONE' && d.uPlotData?.length === 3) {
        setMergedPlotData(d.uPlotData);
        return;
      }
      if ((d.action === 'DECODE_ARROW_DONE' || !d.action) && d.uPlotData) {
        setPlotData(d.uPlotData);
        setStatus('ready');
      }
    };
    workerRef.current = worker;
    return () => {
      worker.terminate();
      workerRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (series.length === 0) {
      setStatus('empty');
      return;
    }

    const ac = new AbortController();
    const headers: HeadersInit = { Accept: 'application/vnd.apache.arrow.stream' };
    const token = getAuthToken();
    if (token) headers['Authorization'] = `Bearer ${token}`;
    const base = getBaseUrl().replace(/\/$/, '');

    const fetchAndDecode = async () => {
      setStatus('loading');
      try {
        let response: Response;
        if (series.length === 1) {
          const s = series[0];
          const params = new URLSearchParams({
            start_time: startTime,
            end_time: endTime,
            resolution: String(resolution),
            attribute: s.attribute,
            format: 'arrow',
          });
          const path = `/api/datahub/timeseries/entities/${encodeURIComponent(s.entityId)}/data?${params}`;
          const url = base ? `${base}${path}` : path;
          response = await fetch(url, { headers, signal: ac.signal });
        } else {
          const path = '/api/datahub/timeseries/align';
          const url = base ? `${base}${path}` : path;
          response = await fetch(url, {
            method: 'POST',
            headers: { ...headers, 'Content-Type': 'application/json' },
            body: JSON.stringify({
              start_time: startTime,
              end_time: endTime,
              resolution,
              series: series.map((s) => ({
                entity_id: s.entityId,
                attribute: s.attribute,
                source: s.source ?? 'timescale',
              })),
            }),
            signal: ac.signal,
          });
        }

        if (response.status === 204) {
          setStatus('empty');
          return;
        }
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const arrayBuffer = await response.arrayBuffer();

        workerRef.current!.postMessage(
          { action: 'DECODE_ARROW', jobId: panelId, buffer: arrayBuffer },
          [arrayBuffer]
        );
      } catch (err) {
        if ((err as Error).name === 'AbortError') return;
        setStatus('error');
      }
    };

    fetchAndDecode();
    return () => ac.abort();
  }, [panelId, series, startTime, endTime, resolution]);

  useEffect(() => {
    if (!prediction) {
      setMergedPlotData(null);
      return;
    }
    if (series.length !== 1 || !plotData || plotData.length !== 2) return;
    const histTimes = plotData[0];
    const histValues = plotData[1];
    if (histTimes.length === 0) return;
    const jobId = `${panelId}-merge`;
    workerRef.current?.postMessage({
      action: 'MERGE_PREDICTION',
      jobId,
      histTimes,
      histValues,
      predTimes: prediction.timestamps,
      predValues: prediction.values,
    });
  }, [panelId, series.length, prediction, plotData]);

  const hasPrediction = mergedPlotData != null && mergedPlotData.length === 3;

  const uPlotOptions = useMemo(() => {
    if (hasPrediction && series.length === 1) {
      return {
        width: 800,
        height: 300,
        title: `${series[0].entityId} — ${series[0].attribute}`,
        series: [
          {},
          {
            label: 'Histórico',
            stroke: COLORS[0],
            width: 2,
            paths: uPlot.paths.linear?.(),
            spanGaps: false,
          },
          {
            label: 'Predicción (IA)',
            stroke: PREDICTION_STROKE,
            width: 2,
            dash: [10, 5],
            paths: uPlot.paths.linear?.(),
            spanGaps: false,
          },
        ],
        axes: [
          { grid: { show: false } },
          { grid: { stroke: '#334155' } },
        ],
      } as uPlot.Options;
    }
    const dynamicSeries: uPlot.Series[] = [{}];
    series.forEach((s, idx) => {
      dynamicSeries.push({
        stroke: COLORS[idx % COLORS.length],
        width: 2,
        paths: uPlot.paths.linear?.(),
        label: s.attribute,
      });
    });
    return {
      width: 800,
      height: 300,
      title: series.length === 1 ? `${series[0].entityId} — ${series[0].attribute}` : `Multi-Serie (${series.length})`,
      series: dynamicSeries,
      axes: [
        { grid: { show: false } },
        { grid: { stroke: '#334155' } },
      ],
    } as uPlot.Options;
  }, [series, hasPrediction]);

  const chartData = hasPrediction ? mergedPlotData : plotData;

  useUPlotCesiumSync({
    chartContainerRef: containerRef,
    options: uPlotOptions,
    data: chartData,
  });

  if (series.length === 0) {
    return (
      <div className="relative w-full h-full bg-slate-900 border border-slate-800 rounded-lg p-4 flex items-center justify-center text-slate-400 font-mono">
        [ ARRASTRA UNA SERIE AQUÍ ]
      </div>
    );
  }

  return (
    <div className="relative w-full h-full bg-slate-900 border border-slate-800 rounded-lg p-4">
      {status === 'loading' && (
        <div className="absolute inset-0 flex items-center justify-center bg-slate-900/80 z-10 text-emerald-500 font-mono">
          [ DECODING ARROW IPC... ]
        </div>
      )}
      {status === 'error' && (
        <div className="absolute inset-0 flex items-center justify-center bg-slate-900/80 z-10 text-red-500 font-mono">
          [ SIGNAL LOST ]
        </div>
      )}
      {status === 'empty' && (
        <div className="absolute inset-0 flex items-center justify-center bg-slate-900/80 z-10 text-slate-400 font-mono">
          [ NO DATA ]
        </div>
      )}
      <div ref={containerRef} className="uplot-container" />
    </div>
  );
};

/** Memoized for grid; re-renders when panelId, series, or time/resolution change. */
export const DataCanvasPanelMemo = React.memo(DataCanvasPanel);
