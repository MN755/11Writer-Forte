from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import (
    StorageLifecycleSweepResultRead,
    StorageObjectCreate,
    StorageObjectPromoteRequest,
    StorageObjectRead,
    StorageObjectTransitionRequest,
    StorageReportRead,
)
from src.services.storage_service import (
    build_storage_report,
    create_storage_object,
    list_storage_objects,
    promote_storage_object,
    sweep_expired_storage_objects,
    transition_storage_object,
)

router = APIRouter(prefix="/storage", tags=["storage"])


@router.get("/objects", response_model=list[StorageObjectRead])
def list_storage_objects_route(
    owner_type: str | None = None,
    owner_id: str | None = None,
    object_kind: str | None = None,
    lifecycle_status: str | None = None,
    retention_class: str | None = None,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_storage_objects(
        session,
        owner_type=owner_type,
        owner_id=owner_id,
        object_kind=object_kind,
        lifecycle_status=lifecycle_status,
        retention_class=retention_class,
        limit=limit,
    )


@router.post("/objects", response_model=StorageObjectRead)
def create_storage_object_route(
    payload: StorageObjectCreate,
    session: Session = Depends(get_db),
) -> object:
    return create_storage_object(session, payload)


@router.get("/report", response_model=StorageReportRead)
def get_storage_report_route(
    limit: int = Query(default=25, ge=1, le=250),
    session: Session = Depends(get_db),
) -> object:
    return build_storage_report(session, limit=limit)


@router.post("/sweep", response_model=StorageLifecycleSweepResultRead)
def run_storage_sweep_route(
    retention_class: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    dry_run: bool = False,
    session: Session = Depends(get_db),
) -> object:
    return sweep_expired_storage_objects(
        session,
        retention_class=retention_class,
        limit=limit,
        dry_run=dry_run,
    )


@router.patch("/objects/{storage_object_id}/promote", response_model=StorageObjectRead)
def promote_storage_object_route(
    storage_object_id: int,
    payload: StorageObjectPromoteRequest,
    session: Session = Depends(get_db),
) -> object:
    try:
        return promote_storage_object(session, storage_object_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.patch("/objects/{storage_object_id}/transition", response_model=StorageObjectRead)
def transition_storage_object_route(
    storage_object_id: int,
    payload: StorageObjectTransitionRequest,
    session: Session = Depends(get_db),
) -> object:
    try:
        return transition_storage_object(session, storage_object_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
