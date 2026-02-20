import { defineConfig } from 'vite';
import { nkzModulePreset } from '@nekazari/module-builder';
import path from 'path';

const MODULE_ID = 'datahub';

export default defineConfig(nkzModulePreset({
  moduleId: MODULE_ID,
  entry: 'src/moduleEntry.ts',
  viteConfig: {
    resolve: {
      alias: { '@': path.resolve(__dirname, './src') },
    },
    server: {
      port: 5004,
      proxy: {
        // Use VITE_PROXY_TARGET for local dev (e.g. https://your-api-domain). No default to avoid hardcoded URLs.
        '/api': {
          target: process.env.VITE_PROXY_TARGET || 'http://localhost:8000',
          changeOrigin: true,
          secure: process.env.VITE_PROXY_TARGET?.startsWith('https') ?? false,
        },
      },
    },
  },
}));
