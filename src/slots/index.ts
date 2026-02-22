import type { ModuleViewerSlots } from '@nekazari/sdk';
import DataHubPanel from './DataHubPanel';
import { DataCanvasPanel, DataCanvasPanelMemo } from './DataCanvasPanel';
import { DataHubDashboard } from './DataHubDashboard';

const MODULE_ID = 'datahub';

export const moduleSlots: ModuleViewerSlots = {
  'map-layer': [],
  'layer-toggle': [],
  'context-panel': [],
  'bottom-panel': [
    {
      id: 'datahub-canvas',
      moduleId: MODULE_ID,
      component: 'DataHubPanel',
      localComponent: DataHubPanel,
      priority: 50,
    },
  ],
  'entity-tree': [],
};

export { DataCanvasPanel, DataCanvasPanelMemo, DataHubDashboard };
