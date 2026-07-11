from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import SourceTrustProfileORM
from src.schemas import (
    IntegritySeedResponse,
    SourceTrustProfileCreate,
    SourceTrustProfileRead,
    SourceTrustProfileUpdate,
)
from src.services.trust_service import (
    create_source_trust_profile,
    seed_default_integrity_sources,
    update_source_trust_profile,
)

router = APIRouter(prefix="/source-trust", tags=["source-trust"])


@router.get("/profiles", response_model=list[SourceTrustProfileRead])
def list_profiles(session: Session = Depends(get_db)) -> list[SourceTrustProfileORM]:
    return list(
        session.scalars(select(SourceTrustProfileORM).order_by(SourceTrustProfileORM.domain.asc()))
    )


@router.post("/profiles", response_model=SourceTrustProfileRead)
def create_profile(
    payload: SourceTrustProfileCreate,
    session: Session = Depends(get_db),
) -> SourceTrustProfileORM:
    try:
        return create_source_trust_profile(session, payload, actor="api_source_trust")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.patch("/profiles/{trust_profile_id}", response_model=SourceTrustProfileRead)
def patch_profile(
    trust_profile_id: int,
    payload: SourceTrustProfileUpdate,
    session: Session = Depends(get_db),
) -> SourceTrustProfileORM:
    try:
        return update_source_trust_profile(
            session,
            trust_profile_id,
            payload,
            actor="api_source_trust",
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "does not exist" in detail else 409
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/seed-defaults", response_model=IntegritySeedResponse)
def seed_profiles(session: Session = Depends(get_db)) -> IntegritySeedResponse:
    created = seed_default_integrity_sources(session, actor="api_source_trust")
    return IntegritySeedResponse(created=len(created), domains=created)
