"""Durable, policy-bound execution for configured public research providers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.models import CustodyLogORM, ResearchProviderORM, ResearchProviderRunORM
from src.schemas import (
    ResearchProviderCreate,
    ResearchProviderRunCreate,
    ResearchProviderUpdate,
)
from src.services.discovery_analysis import canonicalize_url
from src.services.discovery_fetch import FetchPolicy, fetch_url


# These records are intentionally narrow.  GovInfo is not included because its API
# requires an api.data.gov key, which must be operator-supplied and must never be
# written into a provider record, run snapshot, custody log, or repository.
OFFICIAL_NO_SECRET_PROVIDER_BOOTSTRAP: tuple[dict[str, object], ...] = (
    {
        "provider_key": "federal_register_api",
        "name": "Federal Register API",
        "source_kind": "official_public_record",
        "enabled": True,
        "health_state": "unknown",
        "schema_version": "v1",
        "license_notes": (
            "Public FederalRegister.gov API metadata. FederalRegister.gov is an unofficial "
            "informational edition; legal research must verify against the official GPO "
            "GovInfo edition. Preserve publication and capture metadata."
        ),
        "jurisdiction": "US-federal",
        "languages_json": ["en"],
        "freshness_hours": 24,
        "cost": "free",
        "access_requirement": "none",
        "robots_supported": True,
        "evidence_capture_method": "response_manifest",
        "capabilities_json": ["search", "structured_api", "document"],
        "default_budget_json": {
            "max_requests": 10,
            "max_response_bytes": 5 * 1024 * 1024,
            "max_concurrency": 1,
            "retry_attempts": 2,
            "timeout_seconds": 20,
            "retention_days": 30,
        },
        "coverage_gaps_json": {},
        "metadata_json": {
            "bootstrap_managed": True,
            "endpoint_url": "https://www.federalregister.gov/api/v1/documents.json?per_page=20&order=newest",
            "provider_homepage": "https://www.federalregister.gov/",
            "api_documentation_url": "https://www.federalregister.gov/developers/documentation/api/v1",
            "legal_status_url": "https://www.federalregister.gov/developers/documentation/api/v1",
            "endpoint_configuration": "fixed_public_metadata_feed",
        },
    },
)

OFFICIAL_PROVIDER_BOOTSTRAP_SKIPS: tuple[dict[str, str], ...] = (
    {
        "provider_key": "govinfo_api",
        "reason": "api_key_required",
        "detail": "GovInfo API requires an operator-supplied api.data.gov key; no secret-free bootstrap is permitted.",
        "documentation_url": "https://www.govinfo.gov/developers",
    },
)


def now() -> datetime:
    return datetime.now(timezone.utc)


def create_provider(
    session: Session, payload: ResearchProviderCreate, *, actor: str = "api"
) -> ResearchProviderORM:
    if session.scalar(
        select(ResearchProviderORM).where(ResearchProviderORM.provider_key == payload.provider_key)
    ):
        raise ValueError(f"Research provider {payload.provider_key!r} already exists.")
    provider = ResearchProviderORM(**payload.model_dump(mode="python"))
    session.add(provider)
    session.flush()
    _custody(session, provider, "research_provider_created", actor, {})
    session.commit()
    session.refresh(provider)
    return provider


def bootstrap_official_public_providers(
    session: Session, *, actor: str = "api_research_fleet_bootstrap"
) -> dict[str, object]:
    """Idempotently add reviewed no-secret public providers and report safe skips.

    This function does not probe providers, make network calls, add keys, or modify an
    existing provider.  An existing record may be locally tailored by an operator, so
    treating it as a no-op is both idempotent and avoids clobbering local controls.
    """

    created: list[ResearchProviderORM] = []
    existing: list[ResearchProviderORM] = []
    for definition in OFFICIAL_NO_SECRET_PROVIDER_BOOTSTRAP:
        provider_key = str(definition["provider_key"])
        record = session.scalar(
            select(ResearchProviderORM).where(ResearchProviderORM.provider_key == provider_key)
        )
        if record is not None:
            existing.append(record)
            continue
        record = ResearchProviderORM(**definition)
        session.add(record)
        try:
            session.flush()
        except IntegrityError:
            # A second operator/process may have performed the same bootstrap after
            # our preflight query.  Re-read the winner; never overwrite it.
            session.rollback()
            record = session.scalar(
                select(ResearchProviderORM).where(ResearchProviderORM.provider_key == provider_key)
            )
            if record is None:
                raise
            existing.append(record)
            continue
        _custody(
            session,
            record,
            "research_provider_bootstrapped",
            actor,
            {
                "provider_key": record.provider_key,
                "bootstrap_kind": "official_no_secret",
                "endpoint_configuration": "fixed_public_metadata_feed",
            },
        )
        created.append(record)
    if created:
        session.commit()
        for record in created:
            session.refresh(record)
    return {
        "created": created,
        "existing": existing,
        "skipped": [dict(item) for item in OFFICIAL_PROVIDER_BOOTSTRAP_SKIPS],
    }


def list_providers(session: Session, *, limit: int = 500) -> list[ResearchProviderORM]:
    return list(
        session.scalars(
            select(ResearchProviderORM)
            .order_by(ResearchProviderORM.provider_key.asc())
            .limit(limit)
        )
    )


def require_provider(session: Session, provider_id: int) -> ResearchProviderORM:
    provider = session.get(ResearchProviderORM, provider_id)
    if provider is None:
        raise ValueError(f"Research provider {provider_id} does not exist.")
    return provider


def update_provider(
    session: Session, provider_id: int, payload: ResearchProviderUpdate, *, actor: str = "api"
) -> ResearchProviderORM:
    provider = require_provider(session, provider_id)
    for key, value in payload.model_dump(exclude_unset=True, mode="python").items():
        setattr(provider, key, value)
    _custody(session, provider, "research_provider_updated", actor, {})
    session.commit()
    session.refresh(provider)
    return provider


def enqueue_provider_run(
    session: Session, payload: ResearchProviderRunCreate, *, actor: str = "api"
) -> ResearchProviderRunORM:
    require_provider(session, payload.research_provider_id)
    existing = session.scalar(
        select(ResearchProviderRunORM).where(
            ResearchProviderRunORM.idempotency_key == payload.idempotency_key
        )
    )
    if existing:
        return existing
    run = ResearchProviderRunORM(**payload.model_dump(mode="python"))
    session.add(run)
    session.flush()
    session.add(
        CustodyLogORM(
            object_type="research_provider_run",
            object_id=str(run.research_provider_run_id),
            action="research_provider_run_enqueued",
            actor=actor,
            details_json={"provider_id": run.research_provider_id, "idempotency_key": run.idempotency_key},
        )
    )
    session.commit()
    session.refresh(run)
    return run


def list_provider_runs(
    session: Session, *, provider_id: int | None = None, limit: int = 500
) -> list[ResearchProviderRunORM]:
    statement = select(ResearchProviderRunORM).order_by(
        ResearchProviderRunORM.priority.desc(), ResearchProviderRunORM.research_provider_run_id.asc()
    )
    if provider_id is not None:
        statement = statement.where(ResearchProviderRunORM.research_provider_id == provider_id)
    return list(session.scalars(statement.limit(limit)))


def require_provider_run(session: Session, run_id: int) -> ResearchProviderRunORM:
    run = session.get(ResearchProviderRunORM, run_id)
    if run is None:
        raise ValueError(f"Research provider run {run_id} does not exist.")
    return run


def claim_provider_run(
    session: Session, *, worker_id: str, lease_seconds: int = 60, actor: str = "worker"
) -> ResearchProviderRunORM | None:
    """Lease one eligible job; stale leases are deliberately recoverable."""
    moment = now()
    lease_until = moment + timedelta(seconds=max(10, min(lease_seconds, 3600)))
    candidate = session.scalar(
        select(ResearchProviderRunORM)
        .where(
            ResearchProviderRunORM.cancellation_requested.is_(False),
            or_(
                ResearchProviderRunORM.status == "queued",
                (ResearchProviderRunORM.status == "running")
                & (ResearchProviderRunORM.lease_expires_at < moment),
            ),
            or_(ResearchProviderRunORM.retry_at.is_(None), ResearchProviderRunORM.retry_at <= moment),
        )
        .order_by(ResearchProviderRunORM.priority.desc(), ResearchProviderRunORM.created_at.asc())
        .limit(1)
    )
    if candidate is None:
        return None
    candidate.status = "running"
    candidate.lease_owner = worker_id
    candidate.lease_acquired_at = moment
    candidate.lease_expires_at = lease_until
    candidate.heartbeat_at = moment
    candidate.started_at = candidate.started_at or moment
    candidate.attempt_count += 1
    session.add(
        CustodyLogORM(
            object_type="research_provider_run",
            object_id=str(candidate.research_provider_run_id),
            action="research_provider_run_leased",
            actor=actor,
            details_json={"worker_id": worker_id, "lease_expires_at": lease_until.isoformat()},
        )
    )
    session.commit()
    session.refresh(candidate)
    return candidate


def heartbeat_provider_run(
    session: Session, run_id: int, *, worker_id: str, lease_seconds: int = 60
) -> ResearchProviderRunORM:
    run = require_provider_run(session, run_id)
    if run.status != "running" or run.lease_owner != worker_id:
        raise ValueError("Research provider run is not leased by this worker.")
    run.heartbeat_at = now()
    run.lease_expires_at = run.heartbeat_at + timedelta(seconds=max(10, min(lease_seconds, 3600)))
    session.commit()
    session.refresh(run)
    return run


def cancel_provider_run(session: Session, run_id: int, *, actor: str = "api") -> ResearchProviderRunORM:
    run = require_provider_run(session, run_id)
    if run.status in {"completed", "failed", "cancelled"}:
        raise ValueError(f"Research provider run {run_id} is already terminal.")
    run.cancellation_requested = True
    run.status = "cancelled"
    run.finished_at = now()
    run.lease_owner = None
    run.lease_expires_at = None
    session.add(
        CustodyLogORM(
            object_type="research_provider_run",
            object_id=str(run_id),
            action="research_provider_run_cancelled",
            actor=actor,
            details_json={},
        )
    )
    session.commit()
    session.refresh(run)
    return run


def execute_provider_run(
    session: Session, run_id: int, *, worker_id: str, actor: str = "worker"
) -> ResearchProviderRunORM:
    """Collect exactly one operator-configured public endpoint through the safe fetcher."""
    run = require_provider_run(session, run_id)
    if run.status != "running" or run.lease_owner != worker_id:
        raise ValueError("Research provider run must be leased by the executing worker.")
    provider = require_provider(session, run.research_provider_id)
    try:
        endpoint = _provider_endpoint(provider)
        budget = dict(provider.default_budget_json or {})
        response = fetch_url(
            endpoint,
            policy=FetchPolicy(
                timeout_seconds=float(budget.get("timeout_seconds", 20)),
                retry_attempts=int(budget.get("retry_attempts", 2)),
                max_response_bytes=int(budget.get("max_response_bytes", 5 * 1024 * 1024)),
            ),
        )
        run.response_hash = hashlib.sha256(response.payload).hexdigest()
        run.candidate_urls_json = _candidate_urls(response.payload)
        run.bytes_collected = len(response.payload)
        run.request_count += response.attempt_count
        run.output_json = {
            "final_url": response.final_url,
            "status_code": response.status_code,
            "headers": response.headers,
            "candidate_count": len(run.candidate_urls_json),
        }
        run.status, run.finished_at = "completed", now()
        provider.health_state, provider.last_success_at, provider.last_error_text = "healthy", now(), None
    except Exception as exc:
        _fail_or_retry(run, provider, str(exc))
    finally:
        run.lease_owner, run.lease_expires_at = None, None
        session.add(
            CustodyLogORM(
                object_type="research_provider_run",
                object_id=str(run.research_provider_run_id),
                action="research_provider_run_finished",
                actor=actor,
                details_json={"status": run.status, "response_hash": run.response_hash},
            )
        )
        session.commit()
        session.refresh(run)
    return run


def _provider_endpoint(provider: ResearchProviderORM) -> str:
    if not provider.enabled or provider.cost == "disabled":
        raise ValueError("Research provider is disabled.")
    if provider.access_requirement not in {"none", "operator_supplied"} or not provider.robots_supported:
        raise ValueError("Research provider does not permit policy-bound public collection.")
    endpoint = str((provider.metadata_json or {}).get("endpoint_url", "")).strip()
    if not endpoint:
        raise ValueError("Research provider has no operator-configured endpoint_url.")
    return endpoint


def _candidate_urls(payload: bytes) -> list[str]:
    """Extract candidate URLs from a bounded JSON/line payload without trusting it as control data."""
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return []
    values: list[str] = []
    stack = [decoded]
    while stack and len(values) < 500:
        item = stack.pop()
        if isinstance(item, dict):
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, str) and item.startswith(("https://", "http://")):
            try:
                values.append(canonicalize_url(item))
            except ValueError:
                pass
    return sorted(set(values))


def _fail_or_retry(run: ResearchProviderRunORM, provider: ResearchProviderORM, error: str) -> None:
    provider.health_state, provider.last_health_at, provider.last_error_text = "degraded", now(), error
    run.error_text = error
    run.coverage_gaps_json = {"provider": provider.provider_key, "reason": "collection_failed"}
    if run.attempt_count < run.max_attempts and not run.cancellation_requested:
        run.status = "queued"
        run.retry_at = now() + timedelta(seconds=min(300, 2 ** max(0, run.attempt_count - 1)))
    else:
        run.status, run.finished_at = "failed", now()


def _custody(
    session: Session, provider: ResearchProviderORM, action: str, actor: str, details: dict[str, object]
) -> None:
    session.add(
        CustodyLogORM(
            object_type="research_provider",
            object_id=str(provider.research_provider_id),
            action=action,
            actor=actor,
            details_json=details,
        )
    )
