/**
 * Main view for the /datahub route.
 * Renders the same DataHub panel as the bottom-panel slot, in full-page layout.
 */
import React from 'react';
import DataHubPanel from './slots/DataHubPanel';

const DataHubPage: React.FC = () => (
  <div className="flex flex-col h-full min-h-[70vh] p-4">
    <header className="mb-4">
      <h1 className="text-xl font-semibold text-slate-800">DataHub</h1>
      <p className="text-sm text-slate-500">
        Analytical canvas: add series from the tree, visualize and export.
      </p>
    </header>
    <main className="flex-1 min-h-0 rounded-lg border border-slate-200 bg-white shadow-sm overflow-hidden">
      <DataHubPanel />
    </main>
  </div>
);

export default DataHubPage;
