"""Deterministic investigation readiness and tightly bounded model-use controls."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    InvestigationDiscoveryAttemptORM,
    InvestigationEvidencePromotionORM,
    SourceDefinitionORM,
)
from src.services.investigation_planner import InvestigationPlanner, SourceCatalogEntry
from src.services.investigation_service import require_investigation

APPROVED_MODEL_ROUTE = (
    "gpt-5.3-codex-spark",
    "gpt-5.4-mini",
    "gpt-5.6-luna",
)
MAX_LLM_WORK_SHARE = 0.10
DEFAULT_QUOTA_RESERVE = 0.60


def build_investigation_plan(session: Session, investigation_id: int) -> dict[str, object]:
    """Build a stable plan from operator-approved managed sources only."""
    investigation = require_investigation(session, investigation_id)
    source_rows = list(
        session.scalars(
            select(SourceDefinitionORM)
            .where(SourceDefinitionORM.enabled.is_(True))
            .order_by(SourceDefinitionORM.source_id.asc())
        )
    )
    entries = [
        SourceCatalogEntry(
            source_id=str(row.source_id),
            name=row.name,
            target_uri=row.target_uri,
            access_policy="public",
            robots_allowed=True,
            tags=tuple(str(item) for item in (row.metadata_json or {}).get("tags", [])),
        )
        for row in source_rows
    ]
    plan = InvestigationPlanner().build_initial_plan(investigation.question, entries)
    return {
        "plan_version": "investigation-control/v1",
        "investigation_id": investigation_id,
        "question": plan.question,
        "leads": [lead.__dict__ for lead in plan.leads],
        "work_items": [
            {
                "item_id": item.item_id,
                "source_id": item.source_id,
                "source_name": item.source_name,
                "target_uri": item.target_uri,
                "source_class": item.source_class.value,
                "priority": item.priority,
            }
            for item in plan.work_items
        ],
        "coverage_ledger": plan.audit(),
    }


def assess_investigation_readiness(session: Session, investigation_id: int) -> dict[str, object]:
    """Derive progress solely from persisted coverage and promoted evidence."""
    investigation = require_investigation(session, investigation_id)
    attempts = list(
        session.scalars(
            select(InvestigationDiscoveryAttemptORM)
            .where(InvestigationDiscoveryAttemptORM.investigation_id == investigation_id)
            .order_by(InvestigationDiscoveryAttemptORM.investigation_attempt_id.asc())
        )
    )
    evidence = list(
        session.scalars(
            select(InvestigationEvidencePromotionORM).where(
                InvestigationEvidencePromotionORM.investigation_id == investigation_id,
                InvestigationEvidencePromotionORM.disposition == "promoted",
            )
        )
    )
    threshold = dict(investigation.evidence_threshold_json or {})
    min_evidence = max(1, int(threshold.get("min_promoted_evidence", 1)))
    min_sources = max(1, int(threshold.get("min_independent_sources", 2)))
    completed = [row for row in attempts if row.status == "completed"]
    pending = [row for row in attempts if row.status in {"queued", "running"}]
    independent_sources = {
        row.source_uri.strip().lower() for row in completed if (row.source_uri or "").strip()
    }
    gaps = [
        {
            "lead_key": row.lead_key,
            "reason": row.reason,
            "source_uri": row.source_uri,
            "alternate_queued": row.alternate_queued,
        }
        for row in attempts
        if row.status != "completed"
    ]
    ready = len(evidence) >= min_evidence and len(independent_sources) >= min_sources
    exhausted = bool(attempts) and not pending and not ready
    return {
        "investigation_id": investigation_id,
        "state": investigation.state,
        "evidence_count": len(evidence),
        "independent_source_count": len(independent_sources),
        "threshold": {
            "min_promoted_evidence": min_evidence,
            "min_independent_sources": min_sources,
        },
        "coverage_gaps": gaps,
        "next_outcome": "ready" if ready else ("insufficient_evidence" if exhausted else "continue"),
        "stop_reason": "coverage exhausted below evidence threshold" if exhausted else None,
    }


def evaluate_llm_gate(
    *,
    telemetry_at: datetime | None,
    eligible_work_units: int,
    llm_work_units: int,
    requested_llm_work_units: int,
    quota_limit_tokens: int,
    used_tokens: int,
    estimated_tokens: int,
    model: str,
    reserve_fraction: float = DEFAULT_QUOTA_RESERVE,
    now: datetime | None = None,
) -> dict[str, object]:
    """Fail closed before a model can consume quota or control-plane authority."""
    clock = now or datetime.now(timezone.utc)
    if model not in APPROVED_MODEL_ROUTE:
        return {"allowed": False, "reason": "unapproved_model", "model": model}
    if telemetry_at is None or telemetry_at < clock - timedelta(minutes=15):
        return {"allowed": False, "reason": "stale_quota_telemetry", "model": model}
    eligible_after = max(1, eligible_work_units + requested_llm_work_units)
    share = (llm_work_units + requested_llm_work_units) / eligible_after
    if share > MAX_LLM_WORK_SHARE:
        return {"allowed": False, "reason": "llm_work_share_limit", "share": share}
    reserve = int(quota_limit_tokens * reserve_fraction)
    if used_tokens + estimated_tokens > quota_limit_tokens - reserve:
        return {"allowed": False, "reason": "quota_reserve", "reserve_tokens": reserve}
    return {"allowed": True, "reason": "approved", "share": share, "model": model}


def validate_model_assistance(
    response: dict[str, Any], *, evidence_ids: set[str]
) -> dict[str, object]:
    """Reject uncited narrative and every attempt to influence deterministic control work."""
    if set(response) - {"ambiguities", "aliases", "research_gaps", "visual_assessment", "citations"}:
        raise ValueError("Model response contains unsupported or control-plane fields.")
    citations = response.get("citations", [])
    if not isinstance(citations, list) or any(str(item) not in evidence_ids for item in citations):
        raise ValueError("Model response cites an unknown evidence ID.")
    for key in ("ambiguities", "aliases", "research_gaps"):
        if key in response and not isinstance(response[key], list):
            raise ValueError(f"Model response field {key!r} must be a list.")
    return {"accepted": True, "citation_count": len(citations)}
