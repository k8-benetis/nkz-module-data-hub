-- Register DataHub module (schema matches platform marketplace_modules)
INSERT INTO marketplace_modules (
  id, name, display_name, description,
  is_local, remote_entry_url, scope, exposed_module,
  route_path, label, version, author, category,
  module_type, required_plan_type, pricing_tier,
  is_active, required_roles, metadata
) VALUES (
  'datahub',
  'datahub',
  'DataHub',
  'High-performance analytical canvas (Data Canvas) to cross variables from any source, export ranges, and run predictive models via Intelligence.',
  false,
  '/modules/datahub/nkz-module.js',
  NULL, NULL,
  '/datahub',
  'DataHub',
  '1.0.0',
  'Nekazari Team',
  'analytics',
  'ADDON_FREE',
  'basic',
  'FREE',
  true,
  ARRAY['Farmer', 'TenantAdmin', 'PlatformAdmin'],
  '{"icon": "line-chart", "shortDescription": "Analytical canvas and data export", "features": ["Data Canvas", "Multi-source timeseries", "Export CSV/Parquet", "Intelligence predict"]}'::jsonb
) ON CONFLICT (id) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  remote_entry_url = EXCLUDED.remote_entry_url,
  route_path = EXCLUDED.route_path,
  label = EXCLUDED.label,
  version = EXCLUDED.version,
  metadata = EXCLUDED.metadata,
  updated_at = NOW();
