"""
DataHub Module Backend (BFF).
Health, metrics, and /api/datahub/* (entities; timeseries/export to be added per plan).
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.entities import router as entities_router
from app.api.timeseries import router as timeseries_router
from app.api.workspaces import router as workspaces_router

app = FastAPI(
    title="DataHub BFF",
    description="Backend For Frontend for NKZ-DataHub module. Proxies/adapts platform APIs; no duplicate domain logic.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(entities_router)
app.include_router(timeseries_router)
app.include_router(workspaces_router)


@app.get("/health")
def health():
    return {"status": "healthy", "service": "datahub-bff"}


@app.get("/metrics")
def metrics():
    return "# DataHub BFF metrics placeholder\n"
