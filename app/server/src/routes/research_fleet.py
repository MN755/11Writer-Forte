from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.db import get_db
from src.schemas import (
    ResearchProviderCreate,
    ResearchProviderRead,
    ResearchProviderRunCreate,
    ResearchProviderRunRead,
    ResearchProviderUpdate,
)
from src.services.discovery_fetch import DiscoveryFetchError
from src.services.research_fleet_service import (
    DEFAULT_PUBLIC_PROVIDER_REGISTRY,
    admit_static_collection_url,
    configured_public_provider_registry,
    deduplicate_candidate_urls,
    plan_research_fleet,
    source_health_and_gaps,
)
from src.services.research_provider_service import (
    bootstrap_official_public_providers,
    cancel_provider_run,
    claim_provider_run,
    create_provider,
    enqueue_provider_run,
    execute_provider_run,
    heartbeat_provider_run,
    list_provider_runs,
    list_providers as list_configured_providers,
    update_provider,
)


router = APIRouter(prefix="/research-fleet", tags=["research-fleet"])


@router.get("/configured-providers", response_model=list[ResearchProviderRead])
def configured_providers(
    limit: int = Query(default=500, ge=1, le=5000), session: Session = Depends(get_db)
) -> list[object]:
    return list_configured_providers(session, limit=limit)


@router.post("/configured-providers", response_model=ResearchProviderRead)
def configure_provider(payload: ResearchProviderCreate, session: Session = Depends(get_db)) -> object:
    try:
        return create_provider(session, payload)
    except ValueError as exc:
        raise _fleet_error(exc) from exc


@router.post("/configured-providers/bootstrap-official")
def bootstrap_official_providers(session: Session = Depends(get_db)) -> dict[str, object]:
    """Add only reviewed official APIs that do not require a secret or login."""

    result = bootstrap_official_public_providers(session)
    return {
        "created": [ResearchProviderRead.model_validate(row).model_dump(mode="python") for row in result["created"]],
        "existing": [ResearchProviderRead.model_validate(row).model_dump(mode="python") for row in result["existing"]],
        "skipped": result["skipped"],
    }


@router.patch("/configured-providers/{provider_id}", response_model=ResearchProviderRead)
def patch_provider(
    provider_id: int, payload: ResearchProviderUpdate, session: Session = Depends(get_db)
) -> object:
    try:
        return update_provider(session, provider_id, payload)
    except ValueError as exc:
        raise _fleet_error(exc) from exc


@router.get("/runs", response_model=list[ResearchProviderRunRead])
def durable_runs(
    provider_id: int | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
    session: Session = Depends(get_db),
) -> list[object]:
    return list_provider_runs(session, provider_id=provider_id, limit=limit)


@router.post("/runs", response_model=ResearchProviderRunRead)
def queue_durable_run(payload: ResearchProviderRunCreate, session: Session = Depends(get_db)) -> object:
    try:
        return enqueue_provider_run(session, payload)
    except ValueError as exc:
        raise _fleet_error(exc) from exc


@router.post("/runs/claim", response_model=ResearchProviderRunRead | None)
def claim_durable_run(payload: dict[str, Any] = Body(...), session: Session = Depends(get_db)) -> object:
    worker_id = _optional_string(payload.get("worker_id"))
    if not worker_id:
        raise HTTPException(status_code=422, detail="worker_id is required.")
    return claim_provider_run(
        session, worker_id=worker_id, lease_seconds=int(payload.get("lease_seconds", 60))
    )


@router.post("/runs/{run_id}/heartbeat", response_model=ResearchProviderRunRead)
def heartbeat_durable_run(
    run_id: int, payload: dict[str, Any] = Body(...), session: Session = Depends(get_db)
) -> object:
    worker_id = _optional_string(payload.get("worker_id"))
    if not worker_id:
        raise HTTPException(status_code=422, detail="worker_id is required.")
    try:
        return heartbeat_provider_run(
            session, run_id, worker_id=worker_id, lease_seconds=int(payload.get("lease_seconds", 60))
        )
    except ValueError as exc:
        raise _fleet_error(exc) from exc


@router.post("/runs/{run_id}/execute", response_model=ResearchProviderRunRead)
def execute_durable_run(
    run_id: int, payload: dict[str, Any] = Body(...), session: Session = Depends(get_db)
) -> object:
    worker_id = _optional_string(payload.get("worker_id"))
    if not worker_id:
        raise HTTPException(status_code=422, detail="worker_id is required.")
    try:
        return execute_provider_run(session, run_id, worker_id=worker_id)
    except ValueError as exc:
        raise _fleet_error(exc) from exc


@router.post("/runs/{run_id}/cancel", response_model=ResearchProviderRunRead)
def cancel_durable_run(run_id: int, session: Session = Depends(get_db)) -> object:
    try:
        return cancel_provider_run(session, run_id)
    except ValueError as exc:
        raise _fleet_error(exc) from exc


@router.get("/providers")
def list_providers() -> list[dict[str, object]]:
    """List the reviewed public-provider contracts; secrets and endpoints are absent."""

    return [provider.to_public_dict() for provider in configured_public_provider_registry()]


@router.post("/plan")
def create_plan(payload: dict[str, Any] = Body(...)) -> dict[str, object]:
    try:
        question = str(payload.get("question", ""))
        aliases = _string_list(payload.get("aliases"))
        languages = _string_list(payload.get("languages")) or ("en",)
        source_kinds = _string_list(payload.get("source_kinds"))
        return plan_research_fleet(
            question,
            aliases=aliases,
            languages=languages,
            place=_optional_string(payload.get("place")),
            date_range=_optional_string(payload.get("date_range")),
            source_kinds=source_kinds,
            evidence_requirement=_optional_string(payload.get("evidence_requirement")) or "citation_required",
        ).to_dict()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/candidates/deduplicate")
def deduplicate_candidates(payload: dict[str, Any] = Body(...)) -> dict[str, object]:
    urls = _string_list(payload.get("urls"))
    return {"candidates": deduplicate_candidate_urls(urls), "input_count": len(urls)}


@router.post("/static-collection/admit")
def admit_static_collection(payload: dict[str, Any] = Body(...)) -> dict[str, object]:
    provider_id = _optional_string(payload.get("provider_id"))
    provider = next((item for item in DEFAULT_PUBLIC_PROVIDER_REGISTRY if item.provider_id == provider_id), None)
    if provider is None:
        raise HTTPException(status_code=404, detail="Configured provider does not exist.")
    try:
        return admit_static_collection_url(str(payload.get("url", "")), provider=provider)
    except (ValueError, DiscoveryFetchError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/health")
def research_fleet_health(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, object]:
    ledger = payload.get("coverage_ledger")
    if ledger is not None and not isinstance(ledger, dict):
        raise HTTPException(status_code=422, detail="coverage_ledger must be an object.")
    return source_health_and_gaps(ledger_payload=ledger)


def _string_list(value: object) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value if str(item).strip())
    return ()


def _optional_string(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _fleet_error(exc: ValueError) -> HTTPException:
    message = str(exc)
    return HTTPException(status_code=404 if "does not exist" in message else 409, detail=message)
