-- Register DataHub module in marketplace_modules (run once per environment)
-- Adjust id, name, remote_entry_url to match deployment (MinIO path).

INSERT INTO marketplace_modules (
  id, name, display_name, version, description, short_description,
  icon, remote_entry_url, route_path, label, required_roles, category,
  module_type, required_plan_type, pricing_tier, author, build_config
) VALUES (
  'datahub',
  'datahub',
  'DataHub',
  '1.0.0',
  'High-performance analytical canvas (Data Canvas) to cross variables from any source, export ranges, and run predictive models via Intelligence.',
  'Analytical canvas and data export',
  'line-chart',
  '/modules/datahub/nkz-module.js',
  '/datahub',
  'DataHub',
  ARRAY['Farmer', 'TenantAdmin', 'PlatformAdmin'],
  'analytics',
  'ADDON_FREE',
  'basic',
  'FREE',
  '{"name": "Nekazari Team", "organization": "k8-benetis"}'::jsonb,
  '{"type": "iife", "remote_entry_url": "/modules/datahub/nkz-module.js"}'::jsonb
) ON CONFLICT (id) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  version = EXCLUDED.version,
  remote_entry_url = EXCLUDED.remote_entry_url,
  description = EXCLUDED.description;
