# Nekazari DataHub — Adapter Contract

**Version**: 1.1
**Status**: Required standard for data-producing addons
**Scope**: Any Nekazari module that stores timeseries data outside the platform's TimescaleDB

---

## 1. Overview

The DataHub BFF reads timeseries data through a two-path routing model:

| Path | When | What the module needs to do |
|------|------|-----------------------------|
| **Route A** — `timescale` (default) | The entity attribute is stored in the platform's TimescaleDB via the telemetry or weather worker | Nothing. DataHub proxies to the platform `timeseries-reader` automatically. |
| **Route B** — custom adapter | The entity attribute lives in an external system (Odoo, ROS2, ISOBUS, a third-party DB, etc.) | Implement the adapter API described in this document and declare `source` in the NGSI-LD entity. |

Only Route B requires any work from a module developer.

---

## 2. Declaring a custom source in NGSI-LD

For DataHub to route queries to your adapter, each attribute that has external timeseries data must include a `source` sub-property in its NGSI-LD entity:

```json
{
  "id": "urn:ngsi-ld:AgriRobot:001",
  "type": "AgriRobot",
  "batteryLevel": {
    "type": "Property",
    "value": 85,
    "source": { "type": "Property", "value": "ros2-adapter" }
  }
}
```

The `source` value (e.g. `ros2-adapter`) maps directly to a BFF environment variable:

```
TIMESERIES_ADAPTER_ROS2_ADAPTER_URL=https://your-ros2-service
```

Pattern: `TIMESERIES_ADAPTER_{SOURCE_UPPER}_URL` where `SOURCE_UPPER` is the source string uppercased with `-` replaced by `_`.

If `source` is absent or equals `timescale`, Route A applies.

---

## 3. Required endpoint

Your adapter must expose this endpoint:

```
GET /api/timeseries/entities/{entity_id}/data
```

### Query parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `attribute` | string | yes | Attribute name to fetch |
| `start_time` | ISO 8601 | yes | Range start (inclusive) |
| `end_time` | ISO 8601 | yes | Range end (exclusive) |
| `resolution` | integer | no | Target number of data points; apply downsampling if provided |
| `format` | string | yes | `arrow` (required) or `json` (optional) |

### Request headers forwarded by the BFF

| Header | Description |
|--------|-------------|
| `Authorization: Bearer <token>` | JWT from the original user request |
| `Fiware-Service: <tenant_id>` | Tenant identifier; **must** be used to scope data |

### Responses

| Status | Condition |
|--------|-----------|
| `200` | Data found; body is Arrow IPC stream (see §4) |
| `204 No Content` | No data for the requested range (empty body) |
| `400` | Invalid parameters |
| `401` | Invalid or missing JWT |

---

## 4. Transport format — Apache Arrow IPC

DataHub rejects JSON for timeseries volumes. The `200` response **must** be an Arrow IPC stream.

**Required headers:**
```
Content-Type: application/vnd.apache.arrow.stream
```

**Schema:**

| Column | Arrow type | Description |
|--------|------------|-------------|
| `timestamp` | `float64` | Unix epoch **seconds** (not milliseconds, not Arrow Timestamp type) |
| `value` | `float64` | Observation value |

Rows must be **sorted ascending** by `timestamp`.

> **Why `float64` seconds?** The frontend renders with uPlot which expects float64 epoch seconds for zero-copy aligned data. Native Arrow `Timestamp[us]` requires an extra conversion pass and breaks the zero-copy guarantee.

### Python example (PyArrow)

```python
import pyarrow as pa
import pyarrow.ipc as ipc
from fastapi.responses import Response

def serialize_to_arrow(timestamps_sec: list[float], values: list[float]) -> bytes:
    table = pa.table({
        "timestamp": pa.array(timestamps_sec, type=pa.float64()),
        "value":     pa.array(values,          type=pa.float64()),
    })
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()

@app.get("/api/timeseries/entities/{entity_id}/data")
async def get_entity_data(
    entity_id: str,
    attribute: str,
    start_time: str,
    end_time: str,
    resolution: int | None = None,
    format: str = "arrow",
    fiware_service: str = Header("", alias="Fiware-Service"),
):
    rows = await db.fetch(entity_id, attribute, start_time, end_time,
                          tenant=fiware_service, limit=resolution)
    if not rows:
        return Response(status_code=204)

    timestamps = [r.time.timestamp() for r in rows]   # float seconds
    values     = [float(r.value) for r in rows]

    body = serialize_to_arrow(timestamps, values)
    return Response(content=body,
                    media_type="application/vnd.apache.arrow.stream")
```

---

## 5. Robustness requirements

**Abort handling**: The BFF cancels in-flight requests when the user pans or zooms. Your endpoint must release database connections and threads as soon as the HTTP connection drops. In FastAPI/Starlette, use `request.is_disconnected()` or `anyio.CancelScope`.

**Downsampling**: If `resolution` is provided, return at most that many points. Apply time-bucket aggregation (e.g. average per bucket) rather than sending all raw rows. Sending 500 000 raw rows when `resolution=500` is requested is a contract violation.

**Tenant isolation**: Always filter data by `Fiware-Service`. Never return data across tenant boundaries.

**Response time**: Target under 200 ms for up to 10 000 points after downsampling.

---

## 6. Multi-series alignment

When the DataHub UI combines multiple attributes (possibly from different adapters), the BFF performs the alignment server-side using Polars `join_asof` (LOCF). **Individual adapters do not need to implement any alignment logic** — they only need to serve the single-attribute `/data` endpoint correctly.

---

## 7. Pre-publication checklist

Before submitting your module to the Nekazari Marketplace, verify:

- [ ] `GET /api/timeseries/entities/{id}/data` responds with `Content-Type: application/vnd.apache.arrow.stream`
- [ ] `timestamp` column is `float64`, values are Unix epoch **seconds**
- [ ] `value` column is `float64`
- [ ] Rows are sorted ascending by timestamp
- [ ] Returns `204 No Content` (not `200` with empty body) for empty ranges
- [ ] Respects `Fiware-Service` header for tenant isolation
- [ ] Applies downsampling when `resolution` is provided
- [ ] NGSI-LD entity declares `source` sub-property matching `TIMESERIES_ADAPTER_*_URL` env var
- [ ] Response time under 200 ms for ≤ 10 000 points
