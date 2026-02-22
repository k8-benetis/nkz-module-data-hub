"""
POST/GET /api/datahub/workspaces — persist and list DataHubWorkspace (NGSI-LD) via Orion-LD.
Phase 5: BFF receives JSON from frontend, injects tenant from headers, POST/PATCH to Orion; GET filters by type and tenant.
"""

import os
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/datahub", tags=["datahub"])

ORION_URL = os.getenv("ORION_URL", "").rstrip("/")
# Fallback: if no direct Orion, use platform (api-gateway) which proxies to Orion
PLATFORM_API_URL = os.getenv("PLATFORM_API_URL", "").rstrip("/")


def _orion_base() -> str:
    if ORION_URL:
        return ORION_URL
    if PLATFORM_API_URL:
        return PLATFORM_API_URL
    return ""


def _headers_orion(authorization: Optional[str], fiware_service: Optional[str]) -> dict:
    h: dict = {"Content-Type": "application/ld+json", "Accept": "application/ld+json"}
    if authorization:
        h["Authorization"] = authorization
    if fiware_service:
        h["Fiware-Service"] = fiware_service
        h["Fiware-ServicePath"] = "/"
    return h


@router.post("/workspaces")
async def post_workspace(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_tenant_id: Optional[str] = Header(None),
    fiware_service: Optional[str] = Header(None, alias="Fiware-Service"),
):
    """
    Create or update a DataHubWorkspace in Orion-LD.
    Frontend sends NGSI-LD payload (id, type, name, timeContext, layout).
    Tenant is taken from Fiware-Service or X-Tenant-ID. On 409 Conflict, PATCH the entity.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(content={"error": "Invalid JSON body"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse(content={"error": "Body must be a JSON object"}, status_code=400)

    base = _orion_base()
    if not base:
        return JSONResponse(
            content={"error": "ORION_URL or PLATFORM_API_URL not configured"},
            status_code=503,
        )
    tenant = (fiware_service or x_tenant_id or "").strip() or None
    if not tenant:
        return JSONResponse(
            content={"error": "Fiware-Service or X-Tenant-ID required for multitenancy"},
            status_code=400,
        )

    entity_id = body.get("id")
    if not entity_id or not isinstance(entity_id, str):
        return JSONResponse(content={"error": "id required"}, status_code=400)
    if body.get("type") != "DataHubWorkspace":
        return JSONResponse(content={"error": "type must be DataHubWorkspace"}, status_code=400)

    url = f"{base}/ngsi-ld/v1/entities"
    headers = _headers_orion(authorization, tenant)

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            r = await client.post(url, json=body, headers=headers)
        except Exception as e:
            return JSONResponse(
                content={"error": f"Orion request failed: {e!s}"},
                status_code=502,
            )

        if r.status_code == 201:
            return JSONResponse(content={"id": entity_id, "status": "created"}, status_code=201)
        if r.status_code == 409:
            patch_url = f"{base}/ngsi-ld/v1/entities/{entity_id}/attrs"
            patch_body: dict = {}
            if "name" in body:
                patch_body["name"] = body["name"]
            if "timeContext" in body:
                patch_body["timeContext"] = body["timeContext"]
            if "layout" in body:
                patch_body["layout"] = body["layout"]
            if not patch_body:
                return JSONResponse(content={"id": entity_id, "status": "exists"}, status_code=200)
            try:
                r2 = await client.patch(patch_url, json=patch_body, headers=headers)
                if r2.status_code in (200, 204):
                    return JSONResponse(content={"id": entity_id, "status": "updated"}, status_code=200)
                return JSONResponse(
                    content={"error": r2.text or "PATCH failed", "status": r2.status_code},
                    status_code=502,
                )
            except Exception as e:
                return JSONResponse(
                    content={"error": f"Orion PATCH failed: {e!s}"},
                    status_code=502,
                )
        return JSONResponse(
            content={"error": r.text or "Orion rejected the request"},
            status_code=r.status_code if 400 <= r.status_code < 600 else 502,
        )


@router.get("/workspaces")
async def get_workspaces(
    authorization: Optional[str] = Header(None),
    x_tenant_id: Optional[str] = Header(None),
    fiware_service: Optional[str] = Header(None, alias="Fiware-Service"),
):
    """
    List DataHubWorkspace entities for the current tenant.
    GET Orion-LD with type=DataHubWorkspace; Fiware-Service / X-Tenant-ID restricts to tenant.
    Returns a JSON array of workspace entities.
    """
    base = _orion_base()
    if not base:
        return JSONResponse(
            content={"error": "ORION_URL or PLATFORM_API_URL not configured"},
            status_code=503,
        )
    tenant = (fiware_service or x_tenant_id or "").strip() or None
    if not tenant:
        return JSONResponse(
            content={"error": "Fiware-Service or X-Tenant-ID required for multitenancy"},
            status_code=400,
        )

    url = f"{base}/ngsi-ld/v1/entities"
    headers = _headers_orion(authorization, tenant)
    params: dict = {"type": "DataHubWorkspace"}

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            r = await client.get(url, params=params, headers=headers)
        except Exception as e:
            return JSONResponse(
                content={"error": f"Orion request failed: {e!s}"},
                status_code=502,
            )

        if r.status_code != 200:
            return JSONResponse(
                content={"error": r.text or "Orion error"},
                status_code=r.status_code if 400 <= r.status_code < 600 else 502,
            )

        data = r.json()
        return data if isinstance(data, list) else []
