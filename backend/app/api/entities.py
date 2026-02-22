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


# NGSI-LD system keys that are never timeseries attributes
_NGSI_SYSTEM_KEYS = frozenset({
    "id", "type", "@context", "location", "name", "description",
    "address", "source", "provider", "dateCreated", "dateModified",
    "refAgriParcel", "refDevice", "refWeatherStation",
})


def _attr_source(attr_val: Any) -> str | None:
    """Extract the nested source from an NGSI-LD attribute value, if present.

    NGSI-LD attribute-level metadata looks like:
        "ndviMean": { "type": "Property", "value": 0.72, "source": {"type": "Property", "value": "vegetation_health"} }

    Returns the source string (lower-cased) or None if absent.
    """
    if not isinstance(attr_val, dict):
        return None
    raw = attr_val.get("source")
    if raw is None:
        return None
    val = _get_value(raw)
    if isinstance(val, str) and val.strip():
        return val.strip().lower()
    return None


def _norm_entity(e: dict, etype: str) -> dict:
    """Normalize an NGSI-LD entity for the DataHub entity tree.

    Returns:
        {
          id, type, name,
          source: str,           # entity-level default source (fallback)
          attributes: [          # all discovered timeseries-capable attributes
            { name: str, source: str }   # source may differ per attribute
          ]
        }

    Discovery rules:
    1. Skip NGSI-LD system keys (id, type, @context, location, etc.)
    2. Skip keys whose value is not a dict (scalars, nulls)
    3. For each remaining NGSI-LD Property, read its nested "source" metadata.
       If absent, fall back to the entity-level source (defaults to "timescale").
    4. Skip attributes whose value is None or clearly non-numeric
       (Relationship, GeoProperty, array of strings without numeric value).

    This makes the entity tree self-describing: any module that PATCHes an
    attribute with source="my_module" onto an entity will automatically appear
    in the DataHub UI without code changes.
    """
    entity_id = e.get("id") or ""
    if isinstance(entity_id, dict):
        entity_id = entity_id.get("value", entity_id) or ""
    entity_id = str(entity_id)

    raw_name = _get_value(e.get("name"))
    name = str(raw_name) if raw_name is not None else "Unknown"

    # Entity-level default source (used when an attribute has no explicit source)
    entity_source_raw = _get_value(e.get("source")) or _get_value(e.get("provider"))
    entity_source = (
        str(entity_source_raw).strip().lower()
        if isinstance(entity_source_raw, str) and entity_source_raw.strip()
        else "timescale"
    )

    attributes: list[dict] = []
    for key, val in e.items():
        if key in _NGSI_SYSTEM_KEYS:
            continue
        if not isinstance(val, dict):
            continue
        # Skip Relationships and GeoProperties — they are not timeseries
        prop_type = val.get("type", "")
        if prop_type in ("Relationship", "GeoProperty"):
            continue
        # Require a non-None value so we only expose attributes with actual data
        if _get_value(val) is None:
            continue

        per_attr_source = _attr_source(val) or entity_source
        attributes.append({"name": key, "source": per_attr_source})

    return {
        "id": entity_id,
        "type": etype,
        "name": name,
        "source": entity_source,
        "attributes": attributes,
    }


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
