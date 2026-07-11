from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import (
    AlertRead,
    InvestigationWatchCandidateCreate,
    InvestigationWatchCompilation,
    InvestigationWatchCompileRequest,
    ScheduledTaskRead,
    StorageObjectRead,
    WatchCreate,
    WatchRead,
    WatchRunRead,
    WatchScheduleCreate,
    WatchUpdate,
)
from src.services.watch_service import (
    attach_watch_schedule,
    compile_investigation_watch_instruction,
    create_investigation_watch_candidate,
    create_watch,
    evaluate_watch,
    get_watch,
    list_watch_alerts,
    list_watch_evidence,
    list_watch_runs,
    list_watches,
    pause_watch,
    render_watch_alert_rss,
    resume_watch,
    update_watch,
)

router = APIRouter(prefix="/watches", tags=["watches"])


@router.post("/compile", response_model=InvestigationWatchCompilation)
def compile_investigation_watch(
    payload: InvestigationWatchCompileRequest,
) -> object:
    """Preview the deterministic enforced scope before any watch is enabled."""
    return compile_investigation_watch_instruction(payload)


@router.post("/investigation-candidates", response_model=WatchRead)
def create_investigation_candidate(
    payload: InvestigationWatchCandidateCreate,
    session: Session = Depends(get_db),
) -> object:
    try:
        watch, _ = create_investigation_watch_candidate(
            session,
            InvestigationWatchCompileRequest(
                **payload.model_dump(exclude={"name", "slug", "description", "severity"})
            ),
            name=payload.name,
            slug=payload.slug,
            description=payload.description,
            severity=payload.severity,
            actor="api",
        )
        return watch
    except ValueError as exc:
        raise translate_watch_value_error(exc, action="create_investigation_candidate") from exc


@router.get("", response_model=list[WatchRead])
def list_watch_records(
    state: str | None = None,
    watch_type: str | None = None,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_watches(session, state=state, watch_type=watch_type, limit=limit)


@router.post("", response_model=WatchRead)
def create_watch_record(
    payload: WatchCreate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return create_watch(session, payload, actor="api")
    except ValueError as exc:
        raise translate_watch_value_error(exc, action="create_watch") from exc


# Keep collection-level static routes above /{watch_id}; FastAPI matches in declaration order.
@router.get("/runs", response_model=list[WatchRunRead])
def list_watch_run_records(
    watch_id: int | None = None,
    status: str | None = None,
    outcome: str | None = None,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_watch_runs(
        session,
        watch_id=watch_id,
        status=status,
        outcome=outcome,
        limit=limit,
    )


@router.get("/alerts", response_model=list[AlertRead])
def list_watch_alert_records(
    watch_id: int | None = None,
    status: str | None = None,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_watch_alerts(session, watch_id=watch_id, status=status, limit=limit)


@router.get("/feed.rss", response_class=Response)
def watch_alert_feed(
    request: Request,
    watch_id: int | None = None,
    status: str | None = "open",
    limit: int = Query(default=50, ge=1, le=500),
    session: Session = Depends(get_db),
) -> Response:
    try:
        content = render_watch_alert_rss(
            session,
            base_url=str(request.base_url).rstrip("/"),
            watch_id=watch_id,
            status=status,
            limit=limit,
        )
    except ValueError as exc:
        raise translate_watch_value_error(exc, action="render_watch_alert_rss") from exc
    return Response(content=content, media_type="application/rss+xml")


@router.get("/{watch_id}", response_model=WatchRead)
def get_watch_record(watch_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return get_watch(session, watch_id)
    except ValueError as exc:
        raise translate_watch_value_error(
            exc,
            action="get_watch",
            watch_id=watch_id,
        ) from exc


@router.patch("/{watch_id}", response_model=WatchRead)
def patch_watch_record(
    watch_id: int,
    payload: WatchUpdate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return update_watch(session, watch_id, payload, actor="api")
    except ValueError as exc:
        raise translate_watch_value_error(
            exc,
            action="update_watch",
            watch_id=watch_id,
        ) from exc


@router.post("/{watch_id}/pause", response_model=WatchRead)
def pause_watch_record(watch_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return pause_watch(session, watch_id, actor="api")
    except ValueError as exc:
        raise translate_watch_value_error(
            exc,
            action="pause_watch",
            watch_id=watch_id,
        ) from exc


@router.post("/{watch_id}/resume", response_model=WatchRead)
def resume_watch_record(watch_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return resume_watch(session, watch_id, actor="api")
    except ValueError as exc:
        raise translate_watch_value_error(
            exc,
            action="resume_watch",
            watch_id=watch_id,
        ) from exc


@router.post("/{watch_id}/run", response_model=WatchRunRead)
def run_watch_record(
    watch_id: int,
    force: bool = False,
    session: Session = Depends(get_db),
) -> object:
    try:
        return evaluate_watch(session, watch_id, actor="api", force=force)
    except ValueError as exc:
        raise translate_watch_value_error(
            exc,
            action="evaluate_watch",
            watch_id=watch_id,
        ) from exc
    except Exception as exc:
        raise translate_watch_execution_error(exc, watch_id=watch_id) from exc


@router.post("/{watch_id}/schedule", response_model=ScheduledTaskRead)
def schedule_watch_record(
    watch_id: int,
    payload: WatchScheduleCreate,
    session: Session = Depends(get_db),
) -> object:
    try:
        return attach_watch_schedule(session, watch_id, payload, actor="api")
    except ValueError as exc:
        raise translate_watch_value_error(
            exc,
            action="attach_watch_schedule",
            watch_id=watch_id,
        ) from exc


@router.get("/{watch_id}/runs", response_model=list[WatchRunRead])
def list_runs_for_watch(
    watch_id: int,
    status: str | None = None,
    outcome: str | None = None,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    ensure_watch_exists(session, watch_id, action="list_watch_runs")
    return list_watch_runs(
        session,
        watch_id=watch_id,
        status=status,
        outcome=outcome,
        limit=limit,
    )


@router.get("/{watch_id}/alerts", response_model=list[AlertRead])
def list_alerts_for_watch(
    watch_id: int,
    status: str | None = None,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    ensure_watch_exists(session, watch_id, action="list_watch_alerts")
    return list_watch_alerts(session, watch_id=watch_id, status=status, limit=limit)


@router.get("/{watch_id}/evidence", response_model=list[StorageObjectRead])
def list_evidence_for_watch(
    watch_id: int,
    limit: int = Query(default=200, ge=1, le=2000),
    session: Session = Depends(get_db),
) -> list[object]:
    try:
        return list_watch_evidence(session, watch_id, limit=limit)
    except ValueError as exc:
        raise translate_watch_value_error(
            exc,
            action="list_watch_evidence",
            watch_id=watch_id,
        ) from exc


def ensure_watch_exists(session: Session, watch_id: int, *, action: str) -> None:
    try:
        get_watch(session, watch_id)
    except ValueError as exc:
        raise translate_watch_value_error(exc, action=action, watch_id=watch_id) from exc


def translate_watch_value_error(
    exc: ValueError,
    *,
    action: str,
    watch_id: int | None = None,
) -> HTTPException:
    return HTTPException(status_code=404 if "does not exist" in str(exc) else 409, detail=str(exc))


def translate_watch_execution_error(exc: Exception, *, watch_id: int) -> HTTPException:
    cause = getattr(exc, "cause", exc)
    watch_run_id = getattr(exc, "watch_run_id", None)
    if watch_run_id is None:
        watch_run_id = getattr(cause, "watch_run_id", None)
    return HTTPException(status_code=502, detail=str(cause))
