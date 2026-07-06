from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from src.config.settings import Settings, get_settings
from src.services.local_dataset_import_service import LocalDatasetImportService
from src.types.local_import import (
    LocalDatasetImportListResponse,
    LocalDatasetImportRequest,
    LocalDatasetImportResponse,
)


router = APIRouter(prefix="/api/source-discovery/imports", tags=["source-discovery-imports"])


@router.post("/local", response_model=LocalDatasetImportResponse)
def import_local_dataset(
    request: LocalDatasetImportRequest,
    settings: Settings = Depends(get_settings),
) -> LocalDatasetImportResponse:
    try:
        return LocalDatasetImportService(settings).import_dataset(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/local/runs", response_model=LocalDatasetImportListResponse)
def list_local_dataset_import_runs(
    limit: int = Query(default=25, ge=1, le=100),
    source_id: str | None = None,
    settings: Settings = Depends(get_settings),
) -> LocalDatasetImportListResponse:
    return LocalDatasetImportService(settings).list_import_runs(limit=limit, source_id=source_id)
