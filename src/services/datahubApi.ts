/**
 * DataHub BFF API client.
 * Uses same origin /api and token from host (Keycloak / __nekazariAuth).
 */

/** A single timeseries attribute on a DataHub entity. */
export interface DataHubEntityAttribute {
  /** Attribute name as it appears in NGSI-LD (e.g. "ndviMean", "temperature"). */
  name: string;
  /**
   * Data source for this specific attribute. Maps to the BFF adapter env var
   * TIMESERIES_ADAPTER_{SOURCE}_URL (upper-cased). Examples:
   *   "timescale"         → platform TimescaleDB (default)
   *   "vegetation_health" → vegetation-health adapter (Arrow IPC)
   *   "carbon"            → carbon module adapter
   */
  source: string;
}

export interface DataHubEntity {
  id: string;
  type: string;
  name: string;
  /** Per-attribute source metadata. Each attribute routes to its own adapter. */
  attributes: DataHubEntityAttribute[];
  /** Entity-level default source (fallback when attribute has no explicit source). */
  source?: string;
}

export interface DataHubEntitiesResponse {
  entities: DataHubEntity[];
}

/** Token for Authorization header. Never pass JWT in URL (CWE-316). */
export function getAuthToken(): string | null {
  if (typeof window === 'undefined') return null;
  const w = window as unknown as { keycloak?: { token?: string }; __nekazariAuth?: { token?: string } };
  return w.keycloak?.token ?? w.__nekazariAuth?.token ?? null;
}

export function getBaseUrl(): string {
  if (typeof window === 'undefined') return '';
  const w = window as unknown as { __ENV__?: { VITE_API_URL?: string } };
  return w.__ENV__?.VITE_API_URL ?? '';
}

export async function fetchDataHubEntities(search?: string): Promise<DataHubEntitiesResponse> {
  const base = getBaseUrl().replace(/\/$/, '');
  const path = '/api/datahub/entities' + (search ? `?search=${encodeURIComponent(search)}` : '');
  const url = base ? `${base}${path}` : path;
  const token = getAuthToken();
  const headers: HeadersInit = { Accept: 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`DataHub entities: ${res.status} ${await res.text()}`);
  return res.json();
}

export interface TimeseriesArrowResult {
  timestamps: Float64Array;
  values: Float64Array;
}

/**
 * Fetch timeseries as Apache Arrow IPC; returns typed arrays for uPlot.
 * Pass AbortSignal to cancel on zoom/brush (TanStack Query provides it).
 */
export async function fetchTimeseriesArrow(
  entityId: string,
  attribute: string,
  startTime: string,
  endTime: string,
  resolution: number,
  signal: AbortSignal
): Promise<TimeseriesArrowResult> {
  const base = getBaseUrl().replace(/\/$/, '');
  const params = new URLSearchParams({
    start_time: startTime,
    end_time: endTime,
    resolution: String(resolution),
    attribute,
    format: 'arrow',
  });
  const path = `/api/datahub/timeseries/entities/${entityId}/data?${params}`;
  const url = base ? `${base}${path}` : path;
  const token = getAuthToken();
  const headers: HeadersInit = { Accept: 'application/vnd.apache.arrow.stream' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(url, { headers, signal });
  if (!res.ok) throw new Error(await res.text());
  const buffer = await res.arrayBuffer();

  const arrow = await import('apache-arrow');
  const table = arrow.tableFromIPC(buffer);
  const tsCol = table.getChild('timestamp');
  const valueCol = table.getChild('value');
  if (!tsCol || !valueCol) throw new Error('Missing timestamp or value column');

  const timestamps = (tsCol as arrow.FloatVector).values as Float64Array;
  const values = (valueCol as arrow.FloatVector).values as Float64Array;
  return { timestamps, values };
}

export interface AlignSeriesSpec {
  entity_id: string;
  attribute: string;
  /** Data source (e.g. timescale, odoo). Enables hybrid alignment when mixing sources. */
  source?: string;
}

export interface TimeseriesAlignResult {
  timestamps: Float64Array;
  valueArrays: Float64Array[];
}

/**
 * Fetch aligned multi-series as Apache Arrow IPC (POST /api/timeseries/align).
 * Returns timestamp + value_0, value_1, ... as typed arrays for uPlot multi-axis.
 */
export async function fetchTimeseriesAlign(
  series: AlignSeriesSpec[],
  startTime: string,
  endTime: string,
  resolution: number,
  signal: AbortSignal
): Promise<TimeseriesAlignResult> {
  const base = getBaseUrl().replace(/\/$/, '');
  const url = base ? `${base}/api/datahub/timeseries/align` : '/api/datahub/timeseries/align';
  const token = getAuthToken();
  const headers: HeadersInit = {
    'Content-Type': 'application/json',
    Accept: 'application/vnd.apache.arrow.stream',
  };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      start_time: startTime,
      end_time: endTime,
      resolution,
      series,
    }),
    signal,
  });
  if (!res.ok) throw new Error(await res.text());
  const buffer = await res.arrayBuffer();

  const arrow = await import('apache-arrow');
  const table = arrow.tableFromIPC(buffer);
  const tsCol = table.getChild('timestamp');
  if (!tsCol) throw new Error('Missing timestamp column');

  const timestamps = (tsCol as arrow.FloatVector).values as Float64Array;
  const valueArrays: Float64Array[] = [];
  for (let i = 0; ; i++) {
    const col = table.getChild(`value_${i}`);
    if (!col) break;
    valueArrays.push((col as arrow.FloatVector).values as Float64Array);
  }
  return { timestamps, valueArrays };
}

// ---------------------------------------------------------------------------
// Intelligence /predict job + SSE stream (Phase 3.5)
// ---------------------------------------------------------------------------

export interface PredictJobResponse {
  job_id: string;
  status: string;
  message?: string;
}

export interface PredictionPoint {
  timestamp: string; // ISO 8601
  value: number;
}

export interface PredictionResult {
  predictions: PredictionPoint[];
  confidence?: number;
  model?: string;
  metadata?: Record<string, unknown>;
}

/** Submit metadata-only predict job; returns job_id for SSE stream. */
export async function submitPredictJob(
  entityId: string,
  attribute: string,
  startTime: string,
  endTime: string,
  predictionHorizonHours: number = 24
): Promise<string> {
  const base = getBaseUrl().replace(/\/$/, '');
  const url = base ? `${base}/api/intelligence/predict` : '/api/intelligence/predict';
  const token = getAuthToken();
  const headers: HeadersInit = { 'Content-Type': 'application/json', Accept: 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      entity_id: entityId,
      attribute,
      start_time: startTime,
      end_time: endTime,
      prediction_horizon: predictionHorizonHours,
    }),
  });
  if (!res.ok) throw new Error(`Predict: ${res.status} ${await res.text()}`);
  const data = (await res.json()) as PredictJobResponse;
  return data.job_id;
}

/** SSE stream URL for job status. Use fetchEventSource with Authorization header (never JWT in URL). */
export function getIntelligenceStreamUrl(jobId: string): string {
  const base = getBaseUrl().replace(/\/$/, '');
  const path = `/api/intelligence/jobs/${encodeURIComponent(jobId)}/stream`;
  return base ? `${base}${path}` : path;
}

// ---------------------------------------------------------------------------
// Export (Phase 3.1 CSV, 3.2 Parquet) — metadata-only POST
// ---------------------------------------------------------------------------

export interface ExportSeriesSpec {
  entity_id: string;
  attribute: string;
}

/** Analytical granularity for export (not screen resolution). */
export type ExportAggregation = 'raw' | '1 hour' | '1 day';

export interface ExportRequest {
  start_time: string;
  end_time: string;
  series: ExportSeriesSpec[];
  format: 'csv' | 'parquet';
  /** Aggregation interval: raw (finest), 1 hour, 1 day. Default 1 hour. */
  aggregation: ExportAggregation;
}

export interface ExportParquetResponse {
  download_url: string;
  expires_in: number;
  format: 'parquet';
}

/**
 * POST /api/datahub/export with view state only. Returns CSV blob or JSON with presigned URL.
 */
export async function requestExport(
  payload: ExportRequest
): Promise<{ format: 'csv'; blob: Blob } | { format: 'parquet'; data: ExportParquetResponse }> {
  const base = getBaseUrl().replace(/\/$/, '');
  const url = base ? `${base}/api/datahub/export` : '/api/datahub/export';
  const token = getAuthToken();
  const headers: HeadersInit = { 'Content-Type': 'application/json', Accept: 'text/csv, application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(payload) });
  if (!res.ok) throw new Error(`Export: ${res.status} ${await res.text()}`);

  const contentType = res.headers.get('content-type') ?? '';
  if (contentType.includes('text/csv')) {
    const blob = await res.blob();
    return { format: 'csv', blob };
  }
  const data = (await res.json()) as ExportParquetResponse;
  return { format: 'parquet', data };
}

// ---------------------------------------------------------------------------
// Workspaces (Phase 5) — NGSI-LD DataHubWorkspace persist/load
// ---------------------------------------------------------------------------

export interface WorkspaceLayoutPanel {
  panelId: string;
  grid: { x: number; y: number; w: number; h: number };
  type: 'timeseries_chart';
  title?: string;
  series: Array<{ entityId: string; attribute: string; source: string }>;
}

export interface DataHubWorkspacePayload {
  id: string;
  type: 'DataHubWorkspace';
  name: { type: 'Property'; value: string };
  timeContext: { type: 'Property'; value: { startTime: string; endTime: string; resolution: number } };
  layout: { type: 'Property'; value: WorkspaceLayoutPanel[] };
}

export interface DataHubWorkspaceStored {
  id: string;
  type: string;
  name?: { type: string; value: string };
  timeContext?: { type: string; value: { startTime: string; endTime: string; resolution: number } };
  layout?: { type: string; value: WorkspaceLayoutPanel[] };
}

/** POST /api/datahub/workspaces — create or update workspace (BFF → Context Broker). */
export async function saveWorkspace(payload: DataHubWorkspacePayload): Promise<void> {
  const base = getBaseUrl().replace(/\/$/, '');
  const url = base ? `${base}/api/datahub/workspaces` : '/api/datahub/workspaces';
  const token = getAuthToken();
  const headers: HeadersInit = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(payload) });
  if (!res.ok) throw new Error(await res.text());
}

/** GET /api/datahub/workspaces — list workspaces for current tenant. */
export async function listWorkspaces(): Promise<DataHubWorkspaceStored[]> {
  const base = getBaseUrl().replace(/\/$/, '');
  const path = '/api/datahub/workspaces';
  const url = base ? `${base}${path}` : path;
  const token = getAuthToken();
  const headers: HeadersInit = { Accept: 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(await res.text());
  const data = await res.json();
  return Array.isArray(data) ? data : data.workspaces ?? [];
}
