from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import (
    ClickHouseArchiveResultRead,
    ClickHouseDiagnosticsRead,
    ClickHouseProvisionResultRead,
    ClickHouseR2ConfigRead,
    ClickHouseSyncResultRead,
)
from src.services.clickhouse_service import (
    archive_clickhouse_observations_to_r2,
    build_clickhouse_diagnostics,
    build_clickhouse_r2_config_preview,
    provision_clickhouse_backend,
    sync_runtime_to_clickhouse,
)

router = APIRouter(prefix="/operations/clickhouse", tags=["clickhouse"])


@router.get("", response_model=ClickHouseDiagnosticsRead)
def clickhouse_status() -> dict[str, object]:
    return build_clickhouse_diagnostics()


@router.post("/provision", response_model=ClickHouseProvisionResultRead)
def provision_clickhouse(session: Session = Depends(get_db)) -> dict[str, object]:
    try:
        return provision_clickhouse_backend(session, actor="api_clickhouse")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/sync", response_model=ClickHouseSyncResultRead)
def sync_clickhouse(
    layer_key: str | None = None,
    source_domain: str | None = None,
    limit: int = Query(default=1000, ge=1, le=20000),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return sync_runtime_to_clickhouse(
            session,
            layer_key=layer_key,
            source_domain=source_domain,
            limit=limit,
            actor="api_clickhouse",
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/archive", response_model=ClickHouseArchiveResultRead)
def archive_clickhouse(
    layer_key: str | None = None,
    source_domain: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=500000),
    session: Session = Depends(get_db),
) -> dict[str, object]:
    try:
        return archive_clickhouse_observations_to_r2(
            session,
            layer_key=layer_key,
            source_domain=source_domain,
            limit=limit,
            actor="api_clickhouse",
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/r2-config", response_model=ClickHouseR2ConfigRead)
def clickhouse_r2_config() -> dict[str, object]:
    try:
        return build_clickhouse_r2_config_preview()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
