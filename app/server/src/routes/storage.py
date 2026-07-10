from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import (
    StorageActionResultRead,
    StorageArchiveRequest,
    StorageLifecycleSweepResultRead,
    StorageManifestRead,
    StorageObjectCreate,
    StorageObjectPromoteRequest,
    StorageObjectRead,
    StorageObjectTransitionRequest,
    StorageQuarantineRequest,
    StorageRehydrateRequest,
    StorageReportRead,
    StorageUnquarantineRequest,
)
from src.services.storage_service import (
    archive_storage_object,
    build_storage_report,
    create_storage_object,
    get_storage_manifest_for_object,
    list_storage_objects,
    promote_storage_object,
    prune_storage_object,
    quarantine_storage_object,
    rehydrate_storage_object,
    request_storage_object_rehydration,
    sweep_expired_storage_objects,
    transition_storage_object,
    unquarantine_storage_object,
    verify_storage_object,
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
    try:
        return create_storage_object(session, payload)
    except ValueError as exc:
        raise translate_storage_error(exc, 0, "create") from exc


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
    operations: list[str] | None = Query(default=None),
    session: Session = Depends(get_db),
) -> object:
    return sweep_expired_storage_objects(
        session,
        retention_class=retention_class,
        limit=limit,
        dry_run=dry_run,
        operations=operations,
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
        raise translate_storage_error(exc, storage_object_id, "promote") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "promote") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "promote") from exc


@router.patch("/objects/{storage_object_id}/transition", response_model=StorageObjectRead)
def transition_storage_object_route(
    storage_object_id: int,
    payload: StorageObjectTransitionRequest,
    session: Session = Depends(get_db),
) -> object:
    try:
        return transition_storage_object(session, storage_object_id, payload)
    except ValueError as exc:
        raise translate_storage_error(exc, storage_object_id, "transition") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "transition") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "transition") from exc


@router.get("/objects/{storage_object_id}/manifest", response_model=StorageManifestRead)
def get_storage_manifest_route(storage_object_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return get_storage_manifest_for_object(session, storage_object_id)
    except ValueError as exc:
        raise translate_storage_error(exc, storage_object_id, "manifest") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "manifest") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "manifest") from exc


@router.post("/objects/{storage_object_id}/archive", response_model=StorageActionResultRead)
def archive_storage_object_route(
    storage_object_id: int,
    payload: StorageArchiveRequest | None = None,
    session: Session = Depends(get_db),
) -> object:
    try:
        return archive_storage_object(
            session,
            storage_object_id,
            prune_local=(payload.prune_local if payload is not None else False),
        )
    except ValueError as exc:
        raise translate_storage_error(exc, storage_object_id, "archive") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "archive") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "archive") from exc


@router.post("/objects/{storage_object_id}/verify", response_model=StorageActionResultRead)
def verify_storage_object_route(storage_object_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return verify_storage_object(session, storage_object_id)
    except ValueError as exc:
        raise translate_storage_error(exc, storage_object_id, "verify") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "verify") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "verify") from exc


@router.post("/objects/{storage_object_id}/request-rehydrate", response_model=StorageActionResultRead)
def request_storage_object_rehydration_route(
    storage_object_id: int,
    session: Session = Depends(get_db),
) -> object:
    try:
        return request_storage_object_rehydration(session, storage_object_id)
    except ValueError as exc:
        raise translate_storage_error(exc, storage_object_id, "request_rehydrate") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "request_rehydrate") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "request_rehydrate") from exc


@router.post("/objects/{storage_object_id}/rehydrate", response_model=StorageActionResultRead)
def rehydrate_storage_object_route(
    storage_object_id: int,
    payload: StorageRehydrateRequest | None = None,
    session: Session = Depends(get_db),
) -> object:
    try:
        request_payload = payload or StorageRehydrateRequest()
        return rehydrate_storage_object(
            session,
            storage_object_id,
            target_path=request_payload.target_path,
            replace_existing=request_payload.replace_existing,
        )
    except ValueError as exc:
        raise translate_storage_error(exc, storage_object_id, "rehydrate") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "rehydrate") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "rehydrate") from exc


@router.post("/objects/{storage_object_id}/prune", response_model=StorageActionResultRead)
def prune_storage_object_route(storage_object_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return prune_storage_object(session, storage_object_id)
    except ValueError as exc:
        raise translate_storage_error(exc, storage_object_id, "prune") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "prune") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "prune") from exc


@router.post("/objects/{storage_object_id}/quarantine", response_model=StorageActionResultRead)
def quarantine_storage_object_route(
    storage_object_id: int,
    payload: StorageQuarantineRequest,
    session: Session = Depends(get_db),
) -> object:
    try:
        return quarantine_storage_object(session, storage_object_id, reason=payload.reason)
    except ValueError as exc:
        raise translate_storage_error(exc, storage_object_id, "quarantine") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "quarantine") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "quarantine") from exc


@router.post("/objects/{storage_object_id}/unquarantine", response_model=StorageActionResultRead)
def unquarantine_storage_object_route(
    storage_object_id: int,
    payload: StorageUnquarantineRequest | None = None,
    session: Session = Depends(get_db),
) -> object:
    try:
        return unquarantine_storage_object(
            session,
            storage_object_id,
            note=payload.note if payload is not None else None,
        )
    except ValueError as exc:
        raise translate_storage_error(exc, storage_object_id, "unquarantine") from exc
    except RuntimeError as exc:
        raise translate_storage_error(exc, storage_object_id, "unquarantine") from exc
    except OSError as exc:
        raise translate_storage_error(exc, storage_object_id, "unquarantine") from exc


def translate_storage_error(exc: Exception, storage_object_id: int, action: str) -> HTTPException:
    detail = str(exc)
    if isinstance(exc, FileNotFoundError) or "does not exist" in detail:
        status_code = 404
    elif isinstance(exc, ValueError):
        status_code = 409
    elif isinstance(exc, (RuntimeError, OSError)):
        status_code = 502
    else:
        status_code = 500
    return HTTPException(
        status_code=status_code,
        detail={
            "message": detail,
            "storage_object_id": storage_object_id,
            "action": action,
            "error_type": exc.__class__.__name__,
        },
    )
