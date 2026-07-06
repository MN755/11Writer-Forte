from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db import get_db
from src.models import SourceTrustProfileORM
from src.schemas import IntegritySeedResponse, SourceTrustProfileCreate, SourceTrustProfileRead
from src.services.trust_service import normalize_domain, seed_default_integrity_sources

router = APIRouter(prefix="/source-trust", tags=["source-trust"])


@router.get("/profiles", response_model=list[SourceTrustProfileRead])
def list_profiles(session: Session = Depends(get_db)) -> list[SourceTrustProfileORM]:
    return list(session.scalars(select(SourceTrustProfileORM).order_by(SourceTrustProfileORM.domain.asc())))


@router.post("/profiles", response_model=SourceTrustProfileRead)
def create_profile(
    payload: SourceTrustProfileCreate,
    session: Session = Depends(get_db),
) -> SourceTrustProfileORM:
    record = SourceTrustProfileORM(
        **payload.model_dump(exclude={"domain"}),
        domain=normalize_domain(payload.domain) or payload.domain.lower(),
    )
    session.add(record)
    session.commit()
    session.refresh(record)
    return record


@router.post("/seed-defaults", response_model=IntegritySeedResponse)
def seed_profiles(session: Session = Depends(get_db)) -> IntegritySeedResponse:
    created = seed_default_integrity_sources(session)
    return IntegritySeedResponse(created=len(created), domains=created)

