from datetime import datetime

<<<<<<< HEAD
from fastapi import APIRouter, Depends, Query
=======
from fastapi import APIRouter, Depends, HTTPException, Query
>>>>>>> 05aeee6 (chore: initialize repository)
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import CrossVerificationSummaryRead, ObservationRead
from src.services.observation_service import build_cross_verification_summaries, query_observations

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
<<<<<<< HEAD
    session: Session = Depends(get_db),
) -> list[object]:
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
    )
=======
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
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
>>>>>>> 05aeee6 (chore: initialize repository)


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
<<<<<<< HEAD
    session: Session = Depends(get_db),
) -> list[dict[str, object]]:
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
    )
    return build_cross_verification_summaries(
        session,
        observations,
        time_window_minutes=time_window_minutes,
        distance_km=distance_km,
        min_independent_signals=min_independent_signals,
    )
=======
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
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
>>>>>>> 05aeee6 (chore: initialize repository)
