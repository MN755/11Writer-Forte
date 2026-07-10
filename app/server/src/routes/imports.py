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
        .order_by(LocalImportRunORM.created_at.desc(), LocalImportRunORM.import_run_id.desc())
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
        raise translate_import_error(exc, payload.source_path, payload.layer_key) from exc
    except ValueError as exc:
        raise translate_import_error(exc, payload.source_path, payload.layer_key) from exc
    except RuntimeError as exc:
        raise translate_import_error(exc, payload.source_path, payload.layer_key) from exc


def translate_import_error(exc: Exception, source_path: str, layer_key: str) -> HTTPException:
    if isinstance(exc, FileNotFoundError):
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
            "message": str(exc),
            "source_path": source_path,
            "layer_key": layer_key,
            "error_type": exc.__class__.__name__,
        },
    )
