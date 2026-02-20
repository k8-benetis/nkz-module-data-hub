"""
GET /api/datahub/entities — list entities that have timeseries data.
Proxies to platform NGSI-LD / entity APIs when PLATFORM_API_URL is set.
"""

import os
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, Header, Query

router = APIRouter(prefix="/api/datahub", tags=["datahub"])

PLATFORM_API_URL = os.getenv("PLATFORM_API_URL", "").rstrip("/")

# Entity types that typically have timeseries; NGSI-LD types
ENTITY_TYPES_WITH_DATA = [
    "AgriParcel",
    "WeatherObserved",
    "Device",
    "AgriSensor",
]


def _get_value(obj: Any) -> Any:
    """Extract value from NGSI-LD property (normalized or simplified)."""
    if obj is None:
        return None
    if isinstance(obj, dict) and "value" in obj:
        return obj["value"]
    return obj


def _norm_entity(e: dict, etype: str) -> dict:
    """Normalize NGSI-LD entity to { id, type, name, attributes, source }.
    source: data origin (provider/source in NGSI-LD); default 'timescale' for telemetry."""
    o = e.get("id") or ""
    if isinstance(o, dict):
        o = o.get("value", o) or ""
    o = str(o)
    name = "Unknown"
    raw_name = _get_value(e.get("name"))
    if isinstance(raw_name, str):
        name = raw_name
    elif raw_name is not None:
        name = str(raw_name)
    source = _get_value(e.get("source")) or _get_value(e.get("provider"))
    if not isinstance(source, str) or not source.strip():
        source = "timescale"
    source = str(source).strip().lower()
    # Common attributes that may have timeseries
    attrs = []
    for key in ("temperature", "humidity", "pressure", "windSpeed", "soilMoisture", "precipitation", "location"):
        if key in e and e[key] is not None:
            attrs.append(key)
    return {"id": o, "type": etype, "name": name, "attributes": attrs, "source": source}


async def _fetch_ngsi_entities(
    platform_base: str,
    etype: str,
    authorization: Optional[str],
    x_tenant_id: Optional[str],
) -> list[dict]:
    """Fetch entities by type from platform NGSI-LD."""
    url = f"{platform_base}/ngsi-ld/v1/entities"
    headers = {"Accept": "application/ld+json"}
    if authorization:
        headers["Authorization"] = authorization
    if x_tenant_id:
        headers["X-Tenant-ID"] = x_tenant_id
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(url, params={"type": etype}, headers=headers)
        r.raise_for_status()
        data = r.json()
    return data if isinstance(data, list) else []


@router.get("/entities")
async def get_entities(
    search: Optional[str] = Query(None, description="Filter by name or id"),
    authorization: Optional[str] = Header(None),
    x_tenant_id: Optional[str] = Header(None),
):
    """
    List entities that have timeseries data (parcels, weather stations, sensors, etc.).
    When PLATFORM_API_URL is set, aggregates from platform NGSI-LD; otherwise returns empty list.
    """
    if not PLATFORM_API_URL:
        return {"entities": []}

    all_entities: list[dict] = []
    for etype in ENTITY_TYPES_WITH_DATA:
        try:
            raw = await _fetch_ngsi_entities(
                PLATFORM_API_URL, etype, authorization, x_tenant_id
            )
            for e in raw:
                rec = _norm_entity(e, etype)
                if search:
                    q = search.lower()
                    if q not in rec["name"].lower() and q not in rec["id"].lower():
                        continue
                all_entities.append(rec)
        except Exception:
            continue

    return {"entities": all_entities}
