from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from src.db import get_db
from src.models import LocalImportRunORM
from src.schemas import LocalImportRequest, LocalImportRunRead
from src.services.import_service import import_local_path

router = APIRouter(prefix="/imports", tags=["imports"])


@router.get("/runs", response_model=list[LocalImportRunRead])
def list_import_runs(session: Session = Depends(get_db)) -> list[LocalImportRunORM]:
    statement = (
        select(LocalImportRunORM)
        .options(selectinload(LocalImportRunORM.observations))
        .order_by(LocalImportRunORM.created_at.desc())
    )
    return list(session.scalars(statement))


@router.post("/local", response_model=LocalImportRunRead)
def create_local_import(
    payload: LocalImportRequest,
    session: Session = Depends(get_db),
) -> LocalImportRunORM:
    try:
        run = import_local_path(session, payload.source_path, payload.layer_key, payload.notes)
        statement = select(LocalImportRunORM).options(selectinload(LocalImportRunORM.observations))
        return session.scalar(statement.where(LocalImportRunORM.import_run_id == run.import_run_id))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
