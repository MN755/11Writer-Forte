from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import EntityORM, EntityObservationLinkORM
from src.schemas import (
    EntityObservationLinkRead,
    EntityRead,
    EntityResolutionRequest,
    EntityResolutionResponse,
    EntityResolutionResultRead,
)
from src.services.entity_resolution_service import materialize_entities

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
