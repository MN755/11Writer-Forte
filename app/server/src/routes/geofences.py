from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from src.config.settings import Settings, get_settings
from src.services.geofence_service import GeofenceService
from src.types.geofence import (
    GeofenceCreateRequest,
    GeofenceDetail,
    GeofenceEvaluationListResponse,
    GeofenceEvaluationRequest,
    GeofenceEvaluationResponse,
    GeofenceListResponse,
)


router = APIRouter(prefix="/api/geofences", tags=["geofences"])


def _service(settings: Settings) -> GeofenceService:
    return GeofenceService(settings)


@router.get("", response_model=GeofenceListResponse)
async def list_geofences(
    enabled_only: bool = Query(default=False),
    settings: Settings = Depends(get_settings),
) -> GeofenceListResponse:
    return _service(settings).list_geofences(enabled_only=enabled_only)


@router.post("", response_model=GeofenceDetail)
async def create_geofence(
    request: GeofenceCreateRequest,
    settings: Settings = Depends(get_settings),
) -> GeofenceDetail:
    try:
        return _service(settings).create_geofence(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{geofence_id}", response_model=GeofenceDetail)
async def get_geofence(
    geofence_id: str,
    settings: Settings = Depends(get_settings),
) -> GeofenceDetail:
    try:
        return _service(settings).get_geofence(geofence_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{geofence_id}/evaluate", response_model=GeofenceEvaluationResponse)
async def evaluate_geofence(
    geofence_id: str,
    request: GeofenceEvaluationRequest,
    settings: Settings = Depends(get_settings),
) -> GeofenceEvaluationResponse:
    try:
        return _service(settings).evaluate_point(geofence_id, request)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/{geofence_id}/evaluations", response_model=GeofenceEvaluationListResponse)
async def list_geofence_evaluations(
    geofence_id: str,
    limit: int = Query(default=25, ge=1, le=100),
    settings: Settings = Depends(get_settings),
) -> GeofenceEvaluationListResponse:
    try:
        return _service(settings).list_evaluations(geofence_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
