import { moduleSlots } from './slots';
import DataHubPage from './DataHubPage';
import pkg from '../package.json';

const MODULE_ID = 'datahub';

if (typeof window !== 'undefined' && window.__NKZ__) {
  window.__NKZ__.register({
    id: MODULE_ID,
    viewerSlots: moduleSlots,
    main: DataHubPage,
    version: pkg.version,
  });
}
