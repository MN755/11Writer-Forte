from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import EntityCitationORM, EntityORM, EntityObservationLinkORM
from src.schemas import (
    EntityObservationLinkRead,
    EntityRead,
    EntityAliasCreate,
    EntityAliasRead,
    EntityCandidateCreate,
    EntityCandidateRead,
    EntityCitationCreate,
    EntityCitationRead,
    EntityIdentityAssertionCreate,
    EntityIdentityAssertionRead,
    EntityRelationshipAssertionCreate,
    EntityRelationshipAssertionRead,
    EntityRelationshipTypeCreate,
    EntityRelationshipTypeRead,
    EntityResolutionRequest,
    EntityResolutionResponse,
    EntityResolutionResultRead,
)
from src.services.entity_resolution_service import materialize_entities
from src.services.entity_graph_service import (
    create_alias,
    create_candidate,
    create_citation,
    create_identity_assertion,
    create_relationship_assertion,
    create_relationship_type,
    get_entity_profile,
    query_network_slice,
)

router = APIRouter(prefix="/entities", tags=["entities"])


@router.get("", response_model=list[EntityRead])
def list_entities(session: Session = Depends(get_db)) -> list[EntityORM]:
    statement = select(EntityORM).order_by(EntityORM.confidence_score.desc(), EntityORM.created_at.desc())
    return list(session.scalars(statement))


@router.post("/resolve", response_model=EntityResolutionResponse)
def resolve_entities(
    payload: EntityResolutionRequest,
    session: Session = Depends(get_db),
) -> EntityResolutionResponse:
    results = materialize_entities(session, payload)
    return EntityResolutionResponse(
        created_entity_count=sum(1 for result in results if result.created_new),
        entity_results=[
            EntityResolutionResultRead(
                entity_id=result.entity.entity_id,
                slug=result.entity.slug,
                entity_type=result.entity.entity_type,
                canonical_name=result.entity.canonical_name,
                observation_count=result.observation_count,
                signal_count=result.signal_count,
                confidence_score=result.confidence_score,
                created_new=result.created_new,
            )
            for result in results
        ],
    )


@router.get("/{entity_id}/observations", response_model=list[EntityObservationLinkRead])
def list_entity_observations(
    entity_id: int,
    session: Session = Depends(get_db),
) -> list[EntityObservationLinkORM]:
    entity = session.get(EntityORM, entity_id)
    if entity is None:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} does not exist.")
    statement = select(EntityObservationLinkORM).where(EntityObservationLinkORM.entity_id == entity_id)
    return list(session.scalars(statement.order_by(EntityObservationLinkORM.entity_observation_link_id.asc())))


@router.post("/citations", response_model=EntityCitationRead)
def add_entity_citation(payload: EntityCitationCreate, session: Session = Depends(get_db)) -> EntityCitationORM:
    return translate_graph_write(lambda: create_citation(session, payload))


@router.post("/relationship-types", response_model=EntityRelationshipTypeRead)
def add_relationship_type(payload: EntityRelationshipTypeCreate, session: Session = Depends(get_db)) -> object:
    return translate_graph_write(lambda: create_relationship_type(session, payload))


@router.post("/candidates", response_model=EntityCandidateRead)
def add_entity_candidate(payload: EntityCandidateCreate, session: Session = Depends(get_db)) -> object:
    return translate_graph_write(lambda: create_candidate(session, payload))


@router.post("/aliases", response_model=EntityAliasRead)
def add_entity_alias(payload: EntityAliasCreate, session: Session = Depends(get_db)) -> object:
    return translate_graph_write(lambda: create_alias(session, payload))


@router.post("/identity-assertions", response_model=EntityIdentityAssertionRead)
def add_identity_assertion(payload: EntityIdentityAssertionCreate, session: Session = Depends(get_db)) -> object:
    return translate_graph_write(lambda: create_identity_assertion(session, payload))


@router.post("/relationships", response_model=EntityRelationshipAssertionRead)
def add_relationship_assertion(
    payload: EntityRelationshipAssertionCreate, session: Session = Depends(get_db)
) -> object:
    return translate_graph_write(lambda: create_relationship_assertion(session, payload))


@router.get("/{entity_id}/profile")
def entity_profile(
    entity_id: int,
    max_redaction_level: str = Query(default="public"),
    session: Session = Depends(get_db),
) -> JSONResponse:
    return JSONResponse(content=jsonable_encoder(translate_graph_read(lambda: get_entity_profile(session, entity_id, max_redaction_level=max_redaction_level))))


@router.get("/{entity_id}/network")
def entity_network(
    entity_id: int,
    max_depth: int = Query(default=2, ge=0, le=6),
    jurisdiction: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    source_reliability: list[str] = Query(default=[]),
    max_redaction_level: str = Query(default="public"),
    session: Session = Depends(get_db),
) -> JSONResponse:
    return JSONResponse(content=jsonable_encoder(translate_graph_read(
        lambda: query_network_slice(
            session,
            entity_id=entity_id,
            max_depth=max_depth,
            jurisdiction=jurisdiction,
            since=since,
            until=until,
            min_confidence=min_confidence,
            source_reliability=set(source_reliability) or None,
            max_redaction_level=max_redaction_level,
        )
    )))


def translate_graph_write(operation: object) -> object:
    try:
        return operation()  # type: ignore[operator]
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


def translate_graph_read(operation: object) -> dict[str, object]:
    try:
        return operation()  # type: ignore[operator]
    except ValueError as exc:
        status_code = 404 if "does not exist" in str(exc) else 403
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
