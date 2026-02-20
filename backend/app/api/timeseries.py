"""
Timeseries: transparent proxy (single-source) and hybrid alignment (multi-source).
GET /data and POST /align with conditional routing: Route A = proxy to platform;
Route B = fetch per-source /data, align in BFF with Polars join_asof, return Arrow IPC.
"""

import asyncio
import io
import os
from typing import Any, Optional

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import polars as pl
from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

router = APIRouter(prefix="/api/datahub", tags=["datahub"])

PLATFORM_API_URL = os.getenv("PLATFORM_API_URL", "").rstrip("/")
ARROW_STREAM_TYPE = "application/vnd.apache.arrow.stream"


def _auth_headers(authorization: Optional[str], x_tenant_id: Optional[str]) -> dict:
    h: dict = {}
    if authorization:
        h["Authorization"] = authorization
    if x_tenant_id:
        h["X-Tenant-ID"] = x_tenant_id
    return h


def _get_adapter_base_url(source: str) -> Optional[str]:
    """Adapter base URL for a given source. timescale uses PLATFORM_API_URL; others from env."""
    s = (source or "timescale").strip().lower()
    if s == "timescale":
        return PLATFORM_API_URL or None
    key = f"TIMESERIES_ADAPTER_{s.upper()}_URL"
    return (os.getenv(key) or "").rstrip("/") or None


def _parse_arrow_stream(body: bytes) -> pa.Table:
    """Read Arrow IPC stream from bytes into a single table."""
    return ipc.open_stream(body).read_all()


def _align_multi_source_to_df_sync(
    arrow_bodies: list[bytes],
    start_ts: float,
    end_ts: float,
    resolution: int,
) -> pl.DataFrame:
    """
    CPU-bound: parse Arrow streams, build time grid, join_asof each series (LOCF).
    Run in thread pool via asyncio.to_thread. Returns aligned Polars DataFrame.
    """
    resolution = max(2, min(resolution, 10000))
    grid_ts = pl.Series(
        "timestamp",
        [start_ts + (end_ts - start_ts) * i / (resolution - 1) for i in range(resolution)],
    )
    grid_df = pl.DataFrame({"timestamp": grid_ts})
    result_df = grid_df
    for idx, body in enumerate(arrow_bodies):
        try:
            table = _parse_arrow_stream(body)
            df_series = pl.from_arrow(table)
        except Exception:
            result_df = result_df.with_columns(pl.lit(None).cast(pl.Float64).alias(f"value_{idx}"))
            continue
        if df_series.height == 0:
            result_df = result_df.with_columns(pl.lit(None).cast(pl.Float64).alias(f"value_{idx}"))
            continue
        if "timestamp" not in df_series.columns or "value" not in df_series.columns:
            result_df = result_df.with_columns(pl.lit(None).cast(pl.Float64).alias(f"value_{idx}"))
            continue
        df_series = df_series.sort("timestamp")
        joined = result_df.join_asof(
            df_series.select(["timestamp", "value"]),
            left_on="timestamp",
            right_on="timestamp",
            strategy="backward",
        )
        result_df = result_df.with_columns(joined.get_column("value").alias(f"value_{idx}"))
    return result_df


def _align_multi_source_to_arrow_ipc_sync(
    arrow_bodies: list[bytes],
    start_ts: float,
    end_ts: float,
    resolution: int,
) -> bytes:
    """CPU-bound: align to DataFrame then serialize to Arrow IPC bytes. Run in thread pool."""
    df = _align_multi_source_to_df_sync(arrow_bodies, start_ts, end_ts, resolution)
    out_table = df.to_arrow()
    sink = io.BytesIO()
    with ipc.new_stream(sink, out_table.schema) as writer:
        writer.write_table(out_table)
    return sink.getvalue()


def _batch_to_csv_bytes_sync(batch: pl.DataFrame, include_header: bool) -> bytes:
    """CPU-bound: write a single DataFrame slice to CSV bytes. Run in thread pool."""
    buf = io.BytesIO()
    batch.write_csv(buf, include_header=include_header)
    return buf.getvalue()


async def _stream_polars_csv(df: pl.DataFrame, chunk_rows: int = 10000):
    """
    Async generator: yield CSV chunks via iter_slices to avoid holding the full CSV in RAM.
    First chunk includes header; subsequent chunks do not.
    """
    first = True
    for batch in df.iter_slices(chunk_rows):
        chunk_bytes = await asyncio.to_thread(_batch_to_csv_bytes_sync, batch, include_header=first)
        first = False
        yield chunk_bytes


def _dataframe_to_parquet_minio_sync(df: pl.DataFrame, tenant_id: str) -> str:
    """
    CPU-bound: write DataFrame to SpooledTemporaryFile, upload to MinIO, return presigned URL.
    Run in thread pool. Requires S3_* env vars.
    """
    import tempfile
    import uuid
    spool_max = 25 * 1024 * 1024
    bucket = os.getenv("S3_BUCKET", "nekazari-frontend")
    prefix = "exports/"
    key = f"{prefix}{tenant_id}/{uuid.uuid4().hex}.parquet"
    # Default is in-cluster MinIO service name; set S3_ENDPOINT_URL per environment for other setups
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://minio-service:9000")
    access = os.getenv("S3_ACCESS_KEY")
    secret = os.getenv("S3_SECRET_KEY")
    if not access or not secret:
        raise ValueError("S3_ACCESS_KEY and S3_SECRET_KEY required for Parquet export")
    import boto3
    from botocore.config import Config
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
    )
    with tempfile.SpooledTemporaryFile(max_size=spool_max, mode="wb") as spool:
        df.write_parquet(spool, compression="snappy")
        spool.seek(0)
        client.upload_fileobj(
            spool,
            bucket,
            key,
            ExtraArgs={"ContentType": "application/vnd.apache.parquet"},
        )
    url = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=3600,
    )
    return url


async def _fetch_entity_data_raw(
    client: httpx.AsyncClient,
    base_url: str,
    entity_id: str,
    attribute: str,
    start_time: str,
    end_time: str,
    resolution: int,
    headers: dict,
) -> bytes:
    """Fetch one entity's timeseries as Arrow bytes from adapter /data."""
    url = f"{base_url}/api/timeseries/entities/{entity_id}/data"
    params = {
        "start_time": start_time,
        "end_time": end_time,
        "resolution": resolution,
        "attribute": attribute,
        "format": "arrow",
    }
    r = await client.get(url, params=params, headers={**headers, "Accept": ARROW_STREAM_TYPE})
    r.raise_for_status()
    return r.content


@router.post("/timeseries/align")
async def proxy_timeseries_align(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_tenant_id: Optional[str] = Header(None),
):
    """
    Hybrid alignment. Body: start_time, end_time, resolution, series: [{ entity_id, attribute, source? }].
    - Route A (single source, timescale): transparent proxy to platform POST /api/timeseries/align.
    - Route B (multi-source or non-timescale): fetch each series from adapter /data, align in BFF with
      Polars join_asof on a common time grid, return Arrow IPC.
    """
    try:
        body: Any = await request.json()
    except Exception:
        return JSONResponse(content={"error": "Invalid JSON body"}, status_code=400)

    start_time = body.get("start_time")
    end_time = body.get("end_time")
    resolution = int(body.get("resolution", 1000))
    raw_series = body.get("series") or []
    if not start_time or not end_time:
        return JSONResponse(content={"error": "start_time and end_time required"}, status_code=400)
    if not isinstance(raw_series, list) or len(raw_series) < 2:
        return JSONResponse(content={"error": "series must be an array of at least 2 items"}, status_code=400)

    # Normalize series: { entity_id, attribute, source } with source default "timescale"
    series: list[dict] = []
    for i, item in enumerate(raw_series):
        if not isinstance(item, dict):
            return JSONResponse(content={"error": f"series[{i}] must be an object"}, status_code=400)
        eid = item.get("entity_id")
        attr = item.get("attribute")
        if not eid or not attr:
            return JSONResponse(content={"error": f"series[{i}] must have entity_id and attribute"}, status_code=400)
        source = (item.get("source") or "timescale")
        if hasattr(source, "strip"):
            source = str(source).strip().lower() or "timescale"
        else:
            source = "timescale"
        series.append({"entity_id": str(eid), "attribute": str(attr), "source": source})

    sources = {s["source"] for s in series}
    single_timescale = sources == {"timescale"} and len(sources) == 1

    # Route A: all series from timescale -> proxy to platform align (pure SQL)
    if single_timescale and PLATFORM_API_URL:
        url = f"{PLATFORM_API_URL}/api/timeseries/align"
        headers = {"Content-Type": "application/json", **_auth_headers(authorization, x_tenant_id)}
        proxy_body = {
            "start_time": start_time,
            "end_time": end_time,
            "resolution": min(max(resolution, 100), 10000),
            "series": [{"entity_id": s["entity_id"], "attribute": s["attribute"]} for s in series],
        }
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(url, json=proxy_body, headers=headers)
            r.raise_for_status()
        return Response(content=r.content, media_type=ARROW_STREAM_TYPE)

    # Route B: fetch Arrow bytes per series, then offload Polars (join_asof) to thread pool
    resolution = min(max(resolution, 100), 10000)
    try:
        from datetime import datetime
        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
    except Exception:
        return JSONResponse(content={"error": "Invalid start_time or end_time format"}, status_code=400)
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()
    if start_ts >= end_ts:
        return JSONResponse(content={"error": "start_time must be before end_time"}, status_code=400)

    headers = _auth_headers(authorization, x_tenant_id)

    async def fetch_one(idx: int, s: dict) -> tuple[int, bytes]:
        base = _get_adapter_base_url(s["source"])
        if not base:
            raise ValueError(f"No adapter URL for source: {s['source']}")
        async with httpx.AsyncClient(timeout=60.0) as client:
            raw = await _fetch_entity_data_raw(
                client, base, s["entity_id"], s["attribute"],
                start_time, end_time, resolution, headers,
            )
        return idx, raw

    try:
        results = await asyncio.gather(*[fetch_one(i, s) for i, s in enumerate(series)])
    except Exception as e:
        return JSONResponse(content={"error": f"Adapter fetch failed: {e!s}"}, status_code=502)

    results.sort(key=lambda x: x[0])
    arrow_bodies = [b for _, b in results]
    result_bytes = await asyncio.to_thread(
        _align_multi_source_to_arrow_ipc_sync,
        arrow_bodies,
        start_ts,
        end_ts,
        resolution,
    )
    return Response(
        content=result_bytes,
        media_type=ARROW_STREAM_TYPE,
        headers={"Content-Length": str(len(result_bytes))},
    )


@router.get("/timeseries/entities/{entity_id}/data")
async def proxy_timeseries_data(
    entity_id: str,
    request: Request,
    authorization: Optional[str] = Header(None),
    x_tenant_id: Optional[str] = Header(None),
):
    """
    Transparent proxy to platform GET /api/timeseries/entities/<id>/data.
    Forwards query string and auth headers; streams response body (no parse).
    """
    if not PLATFORM_API_URL:
        return JSONResponse(
            content={"error": "PLATFORM_API_URL not configured"},
            status_code=503,
        )

    url = f"{PLATFORM_API_URL}/api/timeseries/entities/{entity_id}/data"
    headers = _auth_headers(authorization, x_tenant_id)
    params = dict(request.query_params)

    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.get(url, params=params, headers=headers or None)
        r.raise_for_status()
        content_type = r.headers.get("content-type", "application/octet-stream")
        return Response(content=r.content, media_type=content_type)


def _resolution_from_aggregation(start_ts: float, end_ts: float, aggregation: str) -> int:
    """Compute resolution (number of points) from aggregation and time range."""
    delta = end_ts - start_ts
    if delta <= 0:
        return 1000
    agg = (aggregation or "1 hour").strip().lower()
    if agg == "raw":
        return min(10000, max(1000, int(delta / 60)))
    if agg == "1 day":
        return min(10000, max(100, int(delta / 86400)))
    if agg == "1 hour":
        return min(10000, max(100, int(delta / 3600)))
    return min(10000, max(100, int(delta / 3600)))


@router.post("/export")
async def proxy_export(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_tenant_id: Optional[str] = Header(None),
):
    """
    Hybrid export. Body: start_time, end_time, series: [{ entity_id, attribute, source? }], format, aggregation.
    - Route A (single source timescale): proxy to platform POST /api/timeseries/export.
    - Route B (multi-source): BFF fetches from adapters, align with Polars in thread, then CSV stream or Parquet to MinIO + presigned URL.
    """
    try:
        body: Any = await request.json()
    except Exception:
        return JSONResponse(content={"error": "Invalid JSON body"}, status_code=400)

    start_time = body.get("start_time")
    end_time = body.get("end_time")
    raw_series = body.get("series") or []
    fmt = (body.get("format") or "csv").strip().lower()
    aggregation = (body.get("aggregation") or "1 hour").strip().lower()
    if fmt not in ("csv", "parquet"):
        return JSONResponse(content={"error": "format must be csv or parquet"}, status_code=400)
    if not start_time or not end_time:
        return JSONResponse(content={"error": "start_time and end_time required"}, status_code=400)
    if not isinstance(raw_series, list) or len(raw_series) == 0:
        return JSONResponse(content={"error": "series must be a non-empty array"}, status_code=400)

    series: list[dict] = []
    for i, item in enumerate(raw_series):
        if not isinstance(item, dict):
            return JSONResponse(content={"error": f"series[{i}] must be an object"}, status_code=400)
        eid = item.get("entity_id")
        attr = item.get("attribute")
        if not eid or not attr:
            return JSONResponse(content={"error": f"series[{i}] must have entity_id and attribute"}, status_code=400)
        source = (item.get("source") or "timescale")
        if hasattr(source, "strip"):
            source = str(source).strip().lower() or "timescale"
        else:
            source = "timescale"
        series.append({"entity_id": str(eid), "attribute": str(attr), "source": source})

    sources = {s["source"] for s in series}
    single_timescale = sources == {"timescale"} and len(sources) == 1

    # Route A: single source timescale -> proxy to platform
    if single_timescale and PLATFORM_API_URL:
        url = f"{PLATFORM_API_URL}/api/timeseries/export"
        headers = {"Content-Type": "application/json", **_auth_headers(authorization, x_tenant_id)}
        proxy_body = {
            "start_time": start_time,
            "end_time": end_time,
            "series": [{"entity_id": s["entity_id"], "attribute": s["attribute"]} for s in series],
            "format": fmt,
            "aggregation": aggregation,
        }
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(url, json=proxy_body, headers=headers)
            r.raise_for_status()
        if r.headers.get("content-type", "").startswith("text/csv"):
            return Response(content=r.content, media_type="text/csv", headers=dict(r.headers))
        return JSONResponse(content=r.json())

    # Route B: multi-source or non-timescale -> BFF orchestrates: fetch, align in thread, then CSV or Parquet
    try:
        from datetime import datetime
        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
    except Exception:
        return JSONResponse(content={"error": "Invalid start_time or end_time format"}, status_code=400)
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()
    if start_ts >= end_ts:
        return JSONResponse(content={"error": "start_time must be before end_time"}, status_code=400)
    resolution = _resolution_from_aggregation(start_ts, end_ts, aggregation)
    headers = _auth_headers(authorization, x_tenant_id)

    async def fetch_one(idx: int, s: dict) -> tuple[int, bytes]:
        base = _get_adapter_base_url(s["source"])
        if not base:
            raise ValueError(f"No adapter URL for source: {s['source']}")
        async with httpx.AsyncClient(timeout=60.0) as client:
            raw = await _fetch_entity_data_raw(
                client, base, s["entity_id"], s["attribute"],
                start_time, end_time, resolution, headers,
            )
        return idx, raw

    try:
        results = await asyncio.gather(*[fetch_one(i, s) for i, s in enumerate(series)])
    except Exception as e:
        return JSONResponse(content={"error": f"Adapter fetch failed: {e!s}"}, status_code=502)

    results.sort(key=lambda x: x[0])
    arrow_bodies = [b for _, b in results]
    df = await asyncio.to_thread(
        _align_multi_source_to_df_sync,
        arrow_bodies,
        start_ts,
        end_ts,
        resolution,
    )

    if fmt == "csv":
        return StreamingResponse(
            _stream_polars_csv(df),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="hybrid_export.csv"'},
        )
    else:
        tenant_id = (x_tenant_id or "default").strip() or "default"
        try:
            download_url = await asyncio.to_thread(_dataframe_to_parquet_minio_sync, df, tenant_id)
        except ValueError as e:
            return JSONResponse(content={"error": str(e)}, status_code=503)
        return JSONResponse(
            content={"download_url": download_url, "expires_in": 3600, "format": "parquet"},
        )
