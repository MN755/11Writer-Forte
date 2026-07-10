from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.routes.error_helpers import translate_service_error
from src.schemas import CrossVerificationSummaryRead, ObservationRead, ObservationSearchResultRead
from src.services.observation_service import (
    build_cross_verification_summaries,
    query_observations,
    search_observations,
)

router = APIRouter(prefix="/observations", tags=["observations"])


@router.get("", response_model=list[ObservationRead])
def list_observations(
    layer_key: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(default=200, ge=1, le=1000),
    backend: str = Query(default="runtime"),
    archive_glob_url: str | None = Query(default=None),
    session: Session = Depends(get_db),
) -> list[object]:
    try:
        return query_observations(
            session,
            layer_key=layer_key,
            source_domain=source_domain,
            trust_level=trust_level,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            since=since,
            until=until,
            limit=limit,
            backend=backend,
            archive_glob_url=archive_glob_url,
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="list_observations",
            context={"backend": backend, "layer_key": layer_key},
        ) from exc
    except RuntimeError as exc:
        raise translate_service_error(
            exc,
            action="list_observations",
            context={"backend": backend, "layer_key": layer_key},
        ) from exc


@router.get("/search", response_model=list[ObservationSearchResultRead])
def search_observation_corpus(
    q: str = Query(min_length=1),
    layer_key: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    candidate_limit: int = Query(default=500, ge=1, le=5000),
    backend: str = Query(default="runtime"),
    archive_glob_url: str | None = Query(default=None),
    session: Session = Depends(get_db),
) -> list[object]:
    try:
        return search_observations(
            session,
            query_text=q,
            layer_key=layer_key,
            source_domain=source_domain,
            trust_level=trust_level,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            since=since,
            until=until,
            limit=limit,
            candidate_limit=candidate_limit,
            backend=backend,
            archive_glob_url=archive_glob_url,
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="search_observations",
            context={"backend": backend, "layer_key": layer_key, "query_text": q},
        ) from exc
    except RuntimeError as exc:
        raise translate_service_error(
            exc,
            action="search_observations",
            context={"backend": backend, "layer_key": layer_key, "query_text": q},
        ) from exc


@router.get("/cross-verify", response_model=list[CrossVerificationSummaryRead])
def cross_verify_observations(
    layer_key: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(default=500, ge=1, le=2000),
    time_window_minutes: int = Query(default=60, ge=1, le=1440),
    distance_km: float = Query(default=25.0, gt=0.0, le=500.0),
    min_independent_signals: int = Query(default=2, ge=2, le=10),
    backend: str = Query(default="runtime"),
    archive_glob_url: str | None = Query(default=None),
    session: Session = Depends(get_db),
) -> list[dict[str, object]]:
    try:
        observations = query_observations(
            session,
            layer_key=layer_key,
            source_domain=source_domain,
            trust_level=trust_level,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            since=since,
            until=until,
            limit=limit,
            backend=backend,
            archive_glob_url=archive_glob_url,
        )
        return build_cross_verification_summaries(
            session,
            observations,
            time_window_minutes=time_window_minutes,
            distance_km=distance_km,
            min_independent_signals=min_independent_signals,
        )
    except ValueError as exc:
        raise translate_service_error(
            exc,
            action="cross_verify_observations",
            context={"backend": backend, "layer_key": layer_key},
        ) from exc
    except RuntimeError as exc:
        raise translate_service_error(
            exc,
            action="cross_verify_observations",
            context={"backend": backend, "layer_key": layer_key},
        ) from exc
