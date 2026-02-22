/**
 * Hook: create uPlot instance, feed data, and sync via DOM CustomEvents only.
 * DataHub is agnostic: it does not access Cesium, ViewerContext, or any host globals.
 * - Emits: when the user selects a time range (brush), dispatches a CustomEvent so the Host
 *   can move its 3D viewer clock.
 * - Listens: for a CustomEvent from the Host to set the chart's visible X range (e.g. when
 *   the user moves the timeline in the viewer).
 */

import { useEffect, useRef, RefObject } from 'react';
import uPlot from 'uplot';

/** CustomEvent detail: time range in Unix epoch seconds (uPlot X axis). */
export interface DataHubTimeRangeDetail {
  min: number;
  max: number;
}

/** Event emitted by DataHub when user selects a time range (e.g. brush). Host should listen and update its clock. */
export const DATAHUB_EVENT_TIME_SELECT = 'nekazari:datahub:timeSelect';

/** Event the Host can dispatch to set the chart's visible X range. DataHub listens and calls u.setScale('x', { min, max }). */
export const DATAHUB_EVENT_SET_TIME_RANGE = 'nekazari:datahub:setTimeRange';

export interface UseUPlotCesiumSyncProps {
  chartContainerRef: RefObject<HTMLDivElement | null>;
  options: uPlot.Options;
  data: uPlot.AlignedData | null;
}

function emptyDataForSeriesCount(seriesCount: number): uPlot.AlignedData {
  return Array.from({ length: seriesCount }, () => new Float64Array(0)) as uPlot.AlignedData;
}

function isTimeRangeDetail(d: unknown): d is DataHubTimeRangeDetail {
  return (
    typeof d === 'object' &&
    d !== null &&
    typeof (d as DataHubTimeRangeDetail).min === 'number' &&
    typeof (d as DataHubTimeRangeDetail).max === 'number'
  );
}

export function useUPlotCesiumSync({
  chartContainerRef,
  options,
  data,
}: UseUPlotCesiumSyncProps): void {
  const uplotRef = useRef<uPlot | null>(null);

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container) return;

    const opts = { ...options };
    if (!opts.width) opts.width = container.offsetWidth || 800;
    if (!opts.height) opts.height = 300;

    const existingSetSelect = opts.hooks?.setSelect ?? [];
    const setSelectHandlers = Array.isArray(existingSetSelect) ? [...existingSetSelect] : [existingSetSelect];
    setSelectHandlers.push(((_u: uPlot, min: number, max: number) => {
      if (min !== max) {
        window.dispatchEvent(
          new CustomEvent<DataHubTimeRangeDetail>(DATAHUB_EVENT_TIME_SELECT, {
            detail: { min, max },
          })
        );
      }
    }) as (self: uPlot) => void);
    opts.hooks = { ...opts.hooks, setSelect: setSelectHandlers };

    const emptyData = emptyDataForSeriesCount(opts.series?.length ?? 2);
    const u = new uPlot(opts, emptyData, container);
    uplotRef.current = u;

    const ro = new ResizeObserver(() => {
      if (uplotRef.current && container) {
        const w = container.offsetWidth || opts.width || 800;
        const h = container.offsetHeight || opts.height || 300;
        uplotRef.current.setSize({ width: w, height: h });
      }
    });
    ro.observe(container);

    const onSetTimeRange = (e: Event) => {
      const d = (e as CustomEvent).detail;
      if (!isTimeRangeDetail(d) || !uplotRef.current) return;
      uplotRef.current.setScale('x', { min: d.min, max: d.max });
    };
    window.addEventListener(DATAHUB_EVENT_SET_TIME_RANGE, onSetTimeRange);

    return () => {
      ro.disconnect();
      window.removeEventListener(DATAHUB_EVENT_SET_TIME_RANGE, onSetTimeRange);
      u.destroy();
      uplotRef.current = null;
    };
  }, [chartContainerRef, options]);

  useEffect(() => {
    if (uplotRef.current && data && data.length >= 2) {
      uplotRef.current.setData(data);
    }
  }, [data]);
}
