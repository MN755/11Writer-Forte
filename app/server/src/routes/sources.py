from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import SourceDefinitionCreate, SourceDefinitionRead, SourceRunRead
from src.services.source_service import (
    create_source_definition,
    list_source_definitions,
    list_source_runs,
    run_source_definition,
)

router = APIRouter(prefix="/sources", tags=["sources"])


@router.get("", response_model=list[SourceDefinitionRead])
def list_sources(session: Session = Depends(get_db)) -> list[object]:
    return list_source_definitions(session)


@router.post("", response_model=SourceDefinitionRead)
def create_source(payload: SourceDefinitionCreate, session: Session = Depends(get_db)) -> object:
    return create_source_definition(session, payload)


@router.get("/runs", response_model=list[SourceRunRead])
def list_runs(session: Session = Depends(get_db)) -> list[object]:
    return list_source_runs(session)


@router.post("/{source_id}/run", response_model=SourceRunRead)
def run_source(source_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return run_source_definition(session, source_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
