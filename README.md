# NKZ DataHub Module

**DataHub** is a Nekazari platform module that provides a high-performance analytical canvas (Data Canvas) to cross variables from multiple timeseries sources, export time ranges as CSV or Parquet, and run predictive models via the Intelligence module.

|  |  |
|--|--|
| **Module ID** | `datahub` |
| **Repository** | [k8-benetis/nkz-module-data-hub](https://github.com/k8-benetis/nkz-module-data-hub) |
| **License** | AGPL-3.0 |

---

## Features

- **Multi-source timeseries**: Query and align series from NGSI-LD (Orion), TimescaleDB (platform), and pluggable adapters.
- **Aligned time grid**: Server-side alignment with configurable resolution (Polars `join_asof`, LOCF); responses in Apache Arrow IPC for zero-copy frontend consumption.
- **Data Canvas UI**: Entity tree, variable selection, uPlot charts, granularity selector (raw / 1h / 1d) for export.
- **Export**: CSV (streaming) or Parquet (upload to MinIO + presigned URL). Route A: proxy to platform; Route B: multi-source gather + align in BFF.
- **Predictions**: SSE stream to Intelligence module for AI-based forecasts.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Nekazari Host (your host domain)                                │
│  Loads IIFE: /modules/datahub/nkz-module.js from MinIO          │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│  API (your API domain)                                           │
│  /api/datahub/* → DataHub BFF (this module)                      │
│  /api/timeseries/* → Platform timeseries-reader (proxy optional)  │
└─────────────────────────────────────────────────────────────────┘
                                    │
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
┌───────────────┐         ┌─────────────────┐         ┌──────────────┐
│  DataHub BFF  │         │ Platform APIs   │         │  MinIO (S3)  │
│  (FastAPI)    │────────▶│ (entities,      │         │  Parquet     │
│  align/export │         │  timeseries)     │         │  exports     │
└───────────────┘         └─────────────────┘         └──────────────┘
```

- **Backend (BFF)**: FastAPI service. Proxies to platform when a single source is used; performs multi-source fetch + Polars alignment and returns Arrow IPC or CSV/Parquet.
- **Frontend**: Single IIFE bundle (`dist/nkz-module.js`), slot `bottom-panel`. Uses host-provided React, `@nekazari/sdk`, `@nekazari/ui-kit`; builds with `@nekazari/module-builder`.

---

## Requirements

- **Node**: 18+ (pnpm recommended)
- **Python**: 3.11+ (backend)
- **Platform**: Nekazari host and API; optional Intelligence module for predictions

---

## Repository structure

```
nkz-module-datahub/
├── backend/                 # BFF (FastAPI)
│   ├── app/
│   │   ├── api/
│   │   │   ├── entities.py   # GET /api/datahub/entities
│   │   │   └── timeseries.py  # align, entity data, export
│   │   └── main.py
│   ├── Dockerfile
│   └── requirements.txt
├── src/
│   ├── slots/
│   │   └── DataHubPanel.tsx  # bottom-panel slot
│   ├── services/
│   │   └── datahubApi.ts
│   └── moduleEntry.ts       # window.__NKZ__.register()
├── k8s/
│   └── backend-deployment.yaml
├── manifest.json
├── package.json
└── README.md
```

---

## API summary (BFF)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (200 OK) |
| GET | `/metrics` | Prometheus placeholder |
| GET | `/api/datahub/entities` | List entities with timeseries (optional `source`, default `timescale`) |
| POST | `/api/datahub/timeseries/align` | Multi-series alignment; body: `{ series: [{ entityId, attribute, source? }], start, end, resolution }`; returns Arrow IPC or JSON |
| GET | `/api/datahub/timeseries/entities/{id}/data` | Proxy to platform timeseries for single entity |
| POST | `/api/datahub/export` | Export aligned data: CSV (streaming) or Parquet (MinIO + presigned URL) |

All `/api/datahub/*` endpoints forward `Authorization` and `X-Tenant-ID` to the platform or adapters.

---

## Environment variables (BFF)

| Variable | Description |
|----------|-------------|
| `PLATFORM_API_URL` | Base URL of the Nekazari API (e.g. `https://your-api-domain`). Required for proxy and Timescale adapter. Set per environment; do not commit. |
| `TIMESERIES_ADAPTER_<NAME>_URL` | Optional adapter base URL for non-timescale sources (e.g. `TIMESERIES_ADAPTER_CUSTOM_URL`) |
| `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` | MinIO/S3 for Parquet export (Route B). If unset, Parquet export may be disabled or fallback to platform. |

---

## Local development

### Backend

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
export PLATFORM_API_URL=https://your-api-domain
uvicorn app.main:app --reload --port 8000
```

### Frontend (standalone)

```bash
pnpm install
pnpm run dev
# Vite dev server; full integration requires the host app.
# Proxy targets local BFF (http://localhost:8000) by default. To use a remote API, set:
#   VITE_PROXY_TARGET=https://your-api-domain
```

### Build IIFE bundle

```bash
pnpm run build:module
# Output: dist/nkz-module.js
```

---

## Deployment

### Backend (Kubernetes)

1. **Build and push image** (from this repo):

   ```bash
   docker build -t ghcr.io/k8-benetis/nkz-module-data-hub/datahub-backend:latest ./backend
   docker push ghcr.io/k8-benetis/nkz-module-data-hub/datahub-backend:latest
   ```

2. **Apply manifests** (namespace `nekazari`):

   ```bash
   kubectl apply -f k8s/backend-configmap.yaml -n nekazari
   kubectl apply -f k8s/backend-deployment.yaml -n nekazari
   ```

3. **Ingress**: The platform ingress must route `/api/datahub` to `datahub-api-service:8000`. Add a path rule before the generic `/api` catch-all, e.g.:

   ```yaml
   - path: /api/datahub
     pathType: Prefix
     backend:
       service:
         name: datahub-api-service
         port:
           number: 8000
   ```

### Frontend (MinIO)

1. Build: `pnpm run build:module` → `dist/nkz-module.js`
2. Upload to MinIO at `nekazari-frontend/modules/datahub/nkz-module.js` (use S3 API or `mc`, never write directly to MinIO filesystem).
3. Ensure `marketplace_modules.remote_entry_url` for `datahub` is `/modules/datahub/nkz-module.js`.

### Marketplace registration

Run the SQL in `k8s/registration.sql` once per environment (or insert/update the `datahub` row in `marketplace_modules`). Tenants then enable the module via the UI (`tenant_installed_modules`).

### Production server checklist

1. **Platform ingress**: Ensure `/api/datahub` is routed to `datahub-api-service:8000`. The main platform repo (`nkz`) already includes this rule in `k8s/core/networking/ingress.yaml`; deploy it with the rest of the platform (e.g. `kubectl apply -f k8s/core/networking/ingress.yaml -n nekazari`).
2. **Backend image**: Build and push from this repo (or use CI). No secrets or hardcoded URLs in the image. The repo ships a ConfigMap with `PLATFORM_API_URL` empty; **override it per environment** (e.g. `kubectl create configmap datahub-api-config -n nekazari --from-literal=PLATFORM_API_URL=https://your-api-domain ...`) so the same manifests work for any domain. For Parquet export to MinIO, add `S3_*` env vars via a Secret.
3. **Frontend bundle**: Upload `dist/nkz-module.js` to MinIO at `nekazari-frontend/modules/datahub/nkz-module.js` using the S3 API or `mc` (never write directly to MinIO filesystem).
4. **Database**: Run `k8s/registration.sql` if the module is not yet in `marketplace_modules`.

---

## References

- **Arrow IPC spec**: See `NKZ_DATAHUB_ARROW_SPEC.md` (in workspace) for binary format and schema (Float64 timestamp epoch seconds).
- **Adapters**: Optional multi-source adapters and env vars — see `ADAPTER_SPEC.md` in this repo.

---

## License

AGPL-3.0. See [LICENSE](LICENSE) in the repository.
