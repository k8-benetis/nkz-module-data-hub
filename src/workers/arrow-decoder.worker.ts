/**
 * Web Worker: command router for Arrow decode and prediction merge (Phase 4.5).
 * DECODE_ARROW: Arrow IPC → uPlot. MERGE_PREDICTION: hist + pred → 3-axis matrix (NaN padding).
 */

import * as arrow from 'apache-arrow';

export type AlignedData = Float64Array[];

const ctx: Worker = self as unknown as Worker;

function getFloat64Values(col: unknown): Float64Array {
  const v = (col as { values?: Float64Array }).values;
  if (v && v instanceof Float64Array) return v;
  throw new Error('Column is not Float64');
}

/**
 * Merges historical (Arrow) and prediction (SSE) into one uPlot-compatible matrix.
 * Uses NaN for gaps so uPlot lifts the pen; prediction series anchored at last historical point.
 */
function mergeHistoryAndPrediction(
  histTimes: Float64Array,
  histValues: Float64Array,
  predTimes: number[] | Float64Array,
  predValues: number[] | Float64Array
): Float64Array[] {
  const N = histTimes.length;
  const pt = predTimes instanceof Float64Array ? predTimes : new Float64Array(predTimes);
  const pv = predValues instanceof Float64Array ? predValues : new Float64Array(predValues);
  const M = pt.length;
  const totalLen = N + M;

  const mergedTimes = new Float64Array(totalLen);
  const mergedHist = new Float64Array(totalLen);
  const mergedPred = new Float64Array(totalLen);

  mergedTimes.set(histTimes, 0);
  mergedHist.set(histValues, 0);
  mergedPred.fill(NaN, 0, N);

  if (N > 0) {
    mergedPred[N - 1] = histValues[N - 1];
  }

  mergedTimes.set(pt, N);
  mergedHist.fill(NaN, N, totalLen);
  mergedPred.set(pv, N);

  return [mergedTimes, mergedHist, mergedPred];
}

type DecodePayload = { action?: 'DECODE_ARROW'; jobId: string; buffer: ArrayBuffer };
type MergePayload = {
  action: 'MERGE_PREDICTION';
  jobId: string;
  histTimes: Float64Array;
  histValues: Float64Array;
  predTimes: number[] | Float64Array;
  predValues: number[] | Float64Array;
};

function handleDecodeArrow(jobId: string, buffer: ArrayBuffer): void {
  const table = arrow.tableFromIPC(new Uint8Array(buffer));
  const tsCol = table.getChild('timestamp');
  if (!tsCol) {
    ctx.postMessage({ action: 'DECODE_ARROW_DONE', error: 'Missing timestamp column', jobId });
    return;
  }
  const timestamps = getFloat64Values(tsCol);

  const valueCol = table.getChild('value');
  if (valueCol) {
    const values = getFloat64Values(valueCol);
    const uPlotData: AlignedData = [timestamps, values];
    ctx.postMessage(
      { action: 'DECODE_ARROW_DONE', jobId, uPlotData },
      [timestamps.buffer, values.buffer] as Transferable[]
    );
    return;
  }

  const valueColumns: Float64Array[] = [timestamps];
  const transfer: ArrayBuffer[] = [timestamps.buffer];
  let idx = 0;
  while (true) {
    const col = table.getChild(`value_${idx}`);
    if (!col) break;
    const arr = getFloat64Values(col);
    valueColumns.push(arr);
    transfer.push(arr.buffer);
    idx += 1;
  }
  if (valueColumns.length < 2) {
    ctx.postMessage({ action: 'DECODE_ARROW_DONE', error: 'Missing value or value_0 column', jobId });
    return;
  }
  ctx.postMessage(
    { action: 'DECODE_ARROW_DONE', jobId, uPlotData: valueColumns },
    transfer as Transferable[]
  );
}

ctx.onmessage = (e: MessageEvent<DecodePayload | MergePayload>) => {
  const data = e.data;
  const action = data?.action;

  try {
    if (action === 'MERGE_PREDICTION') {
      const { jobId, histTimes, histValues, predTimes, predValues } = data as MergePayload;
      const mergedData = mergeHistoryAndPrediction(histTimes, histValues, predTimes, predValues);
      ctx.postMessage(
        {
          action: 'MERGE_PREDICTION_DONE',
          jobId,
          uPlotData: mergedData,
        },
        [mergedData[0].buffer, mergedData[1].buffer, mergedData[2].buffer] as Transferable[]
      );
      return;
    }

    if (action === 'DECODE_ARROW' || (data && 'buffer' in data)) {
      const { jobId, buffer } = data as DecodePayload;
      handleDecodeArrow(jobId, buffer);
      return;
    }

    ctx.postMessage({ error: 'Unknown action or missing buffer', jobId: (data as { jobId?: string })?.jobId });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    ctx.postMessage({ error: message, jobId: (data as { jobId?: string })?.jobId });
  }
};
