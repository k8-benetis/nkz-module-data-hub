/**
 * Dashboard workspace state (subset of NGSI-LD DataHubWorkspace for persistence).
 * Phase 3: panels hold an array of series. Phase 4.5: panel can hold SSE prediction for canvas injection.
 */

export interface ChartSeriesDef {
  entityId: string;
  attribute: string;
  source: string;
}

/** Result of SSE prediction stream (epoch seconds + values) for merge in worker. */
export interface PredictionPayload {
  timestamps: number[];
  values: number[];
}

export interface DashboardPanel {
  id: string;
  grid: { x: number; y: number; w: number; h: number };
  type: 'timeseries_chart';
  title?: string;
  series: ChartSeriesDef[];
  /** When set, canvas merges with historical and renders Histórico + Predicción (IA). */
  prediction?: PredictionPayload;
}

export interface GlobalTimeContext {
  startTime: string;
  endTime: string;
  resolution: number;
}
