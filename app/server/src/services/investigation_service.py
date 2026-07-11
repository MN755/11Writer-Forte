from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.models import (
    CustodyLogORM,
    InvestigationDiscoveryAttemptORM,
    InvestigationEvidencePromotionORM,
    InvestigationORM,
    InvestigationReportVersionORM,
    StorageObjectORM,
)
from src.schemas import (
    InvestigationArchiveResultRead,
    InvestigationCreate,
    InvestigationDiscoveryAttemptCreate,
    InvestigationEvidencePromotionCreate,
    InvestigationReportVersionCreate,
    InvestigationTransitionRequest,
    StorageObjectPromoteRequest,
)
from src.services.storage_service import apply_storage_promotion, apply_storage_transition


TERMINAL_STATES = {"archived", "insufficient_evidence", "failed"}
ACTIVE_STATES = {
    "draft",
    "planning",
    "collecting",
    "normalizing",
    "corroborating",
    "reporting",
    "ready",
    "monitoring",
}
STATE_TRANSITIONS: dict[str, set[str]] = {
    "draft": {"planning", "archived", "failed"},
    "planning": {"collecting", "insufficient_evidence", "failed", "archived"},
    "collecting": {"normalizing", "insufficient_evidence", "failed", "archived"},
    "normalizing": {"corroborating", "insufficient_evidence", "failed", "archived"},
    "corroborating": {"reporting", "insufficient_evidence", "failed", "archived"},
    "reporting": {"ready", "insufficient_evidence", "failed", "archived"},
    "ready": {"monitoring", "archived"},
    "monitoring": {"archived"},
    "archived": set(),
    "insufficient_evidence": {"archived"},
    "failed": {"archived"},
}
ALTERNATE_ELIGIBLE_REASONS = {"unavailable", "blocked_by_policy", "dead_end"}


def now() -> datetime:
    return datetime.now(timezone.utc)


def require_investigation(session: Session, investigation_id: int) -> InvestigationORM:
    investigation = session.get(InvestigationORM, investigation_id)
    if investigation is None:
        raise ValueError(f"Investigation {investigation_id} does not exist.")
    return investigation


def create_investigation(
    session: Session, payload: InvestigationCreate, *, actor: str = "api"
) -> InvestigationORM:
    existing = session.scalar(select(InvestigationORM).where(InvestigationORM.slug == payload.slug))
    if existing is not None:
        raise ValueError(f"Investigation slug {payload.slug!r} already exists.")
    investigation = InvestigationORM(**payload.model_dump(mode="python"))
    session.add(investigation)
    session.flush()
    add_custody(
        session,
        investigation,
        action="investigation_created",
        actor=actor,
        details={"state": investigation.state, "question": investigation.question},
    )
    session.commit()
    session.refresh(investigation)
    return investigation


def list_investigations(
    session: Session, *, state: str | None = None, limit: int = 200
) -> list[InvestigationORM]:
    statement = select(InvestigationORM).order_by(InvestigationORM.investigation_id.desc())
    if state:
        statement = statement.where(InvestigationORM.state == state)
    return list(session.scalars(statement.limit(limit)))


def transition_investigation(
    session: Session,
    investigation_id: int,
    payload: InvestigationTransitionRequest,
    *,
    actor: str = "api",
) -> InvestigationORM:
    investigation = require_investigation(session, investigation_id)
    target = payload.state
    if target == investigation.state:
        raise ValueError(f"Investigation {investigation_id} is already {target}.")
    if target not in STATE_TRANSITIONS[investigation.state]:
        raise ValueError(
            f"Cannot transition investigation from {investigation.state!r} to {target!r}."
        )
    if target in {"failed", "insufficient_evidence"} and not (payload.stop_reason or "").strip():
        raise ValueError(
            f"A stop_reason is required when transitioning an investigation to {target}."
        )

    previous_state = investigation.state
    investigation.state = target
    if target in TERMINAL_STATES:
        investigation.stop_reason = (payload.stop_reason or "archived by operator").strip()
        investigation.stopped_at = now()
    else:
        investigation.stop_reason = None
        investigation.stopped_at = None
    add_custody(
        session,
        investigation,
        action="investigation_state_changed",
        actor=actor,
        details={
            "previous_state": previous_state,
            "state": target,
            "stop_reason": investigation.stop_reason,
        },
    )
    session.commit()
    session.refresh(investigation)
    return investigation


def record_discovery_attempt(
    session: Session,
    investigation_id: int,
    payload: InvestigationDiscoveryAttemptCreate,
    *,
    actor: str = "api",
) -> InvestigationDiscoveryAttemptORM:
    investigation = require_investigation(session, investigation_id)
    if investigation.state in TERMINAL_STATES:
        raise ValueError(
            f"Cannot record discovery activity for terminal investigation {investigation_id}."
        )
    if payload.status == "queued" and payload.alternate_discovery_path:
        raise ValueError("Queued attempts cannot queue another alternate discovery path.")

    attempt = InvestigationDiscoveryAttemptORM(
        investigation_id=investigation_id,
        lead_key=payload.lead_key,
        discovery_path=payload.discovery_path,
        source_uri=payload.source_uri,
        status=payload.status,
        reason=payload.reason,
        attempted_at=now() if payload.status == "completed" else None,
        alternate_eligible=payload.alternate_eligible,
        error_text=payload.error_text,
        details_json=payload.details_json,
    )
    session.add(attempt)
    session.flush()

    queued_attempt: InvestigationDiscoveryAttemptORM | None = None
    wants_alternate = bool(payload.alternate_discovery_path or payload.alternate_source_uri)
    if wants_alternate and not payload.alternate_eligible:
        raise ValueError("An alternate discovery path requires alternate_eligible=true.")
    if wants_alternate and payload.reason not in ALTERNATE_ELIGIBLE_REASONS:
        raise ValueError(f"Reason {payload.reason!r} is not eligible for alternate discovery work.")
    if wants_alternate:
        queued_attempt = InvestigationDiscoveryAttemptORM(
            investigation_id=investigation_id,
            lead_key=payload.lead_key,
            discovery_path=payload.alternate_discovery_path or "public_web",
            source_uri=payload.alternate_source_uri,
            status="queued",
            reason="dead_end",
            alternate_eligible=False,
            details_json={"alternate_of_attempt_id": attempt.investigation_attempt_id},
        )
        attempt.alternate_queued = True
        session.add(queued_attempt)

    add_custody(
        session,
        investigation,
        action="investigation_discovery_attempt_recorded",
        actor=actor,
        details={
            "investigation_attempt_id": attempt.investigation_attempt_id,
            "lead_key": attempt.lead_key,
            "reason": attempt.reason,
            "alternate_queued": attempt.alternate_queued,
            "queued_attempt_id": queued_attempt.investigation_attempt_id
            if queued_attempt
            else None,
        },
    )
    session.commit()
    session.refresh(attempt)
    return attempt


def list_discovery_attempts(
    session: Session, investigation_id: int, *, limit: int = 200
) -> list[InvestigationDiscoveryAttemptORM]:
    require_investigation(session, investigation_id)
    statement = (
        select(InvestigationDiscoveryAttemptORM)
        .where(InvestigationDiscoveryAttemptORM.investigation_id == investigation_id)
        .order_by(InvestigationDiscoveryAttemptORM.investigation_attempt_id.asc())
        .limit(limit)
    )
    return list(session.scalars(statement))


def add_report_version(
    session: Session,
    investigation_id: int,
    payload: InvestigationReportVersionCreate,
    *,
    actor: str = "api",
) -> InvestigationReportVersionORM:
    investigation = require_investigation(session, investigation_id)
    if investigation.state in {"archived", "failed"}:
        raise ValueError(
            f"Cannot add a report to {investigation.state} investigation {investigation_id}."
        )
    if (
        payload.storage_object_id is not None
        and session.get(StorageObjectORM, payload.storage_object_id) is None
    ):
        raise ValueError(f"Storage object {payload.storage_object_id} does not exist.")
    version_number = (
        int(
            session.scalar(
                select(InvestigationReportVersionORM.version_number)
                .where(InvestigationReportVersionORM.investigation_id == investigation_id)
                .order_by(InvestigationReportVersionORM.version_number.desc())
                .limit(1)
            )
            or 0
        )
        + 1
    )
    report = InvestigationReportVersionORM(
        investigation_id=investigation_id,
        version_number=version_number,
        **payload.model_dump(mode="python"),
    )
    session.add(report)
    session.flush()
    add_custody(
        session,
        investigation,
        action="investigation_report_version_created",
        actor=actor,
        details={
            "report_version_id": report.investigation_report_version_id,
            "version_number": version_number,
        },
    )
    session.commit()
    session.refresh(report)
    return report


def list_report_versions(
    session: Session, investigation_id: int, *, limit: int = 200
) -> list[InvestigationReportVersionORM]:
    require_investigation(session, investigation_id)
    return list(
        session.scalars(
            select(InvestigationReportVersionORM)
            .where(InvestigationReportVersionORM.investigation_id == investigation_id)
            .order_by(InvestigationReportVersionORM.version_number.desc())
            .limit(limit)
        )
    )


def promote_evidence(
    session: Session,
    investigation_id: int,
    payload: InvestigationEvidencePromotionCreate,
    *,
    actor: str = "api",
) -> InvestigationEvidencePromotionORM:
    investigation = require_investigation(session, investigation_id)
    if investigation.state == "archived":
        raise ValueError(f"Cannot promote evidence for archived investigation {investigation_id}.")
    storage_object = session.get(StorageObjectORM, payload.storage_object_id)
    if storage_object is None:
        raise ValueError(f"Storage object {payload.storage_object_id} does not exist.")
    existing = session.scalar(
        select(InvestigationEvidencePromotionORM).where(
            InvestigationEvidencePromotionORM.investigation_id == investigation_id,
            InvestigationEvidencePromotionORM.storage_object_id == payload.storage_object_id,
        )
    )
    if existing is not None:
        raise ValueError(
            f"Storage object {payload.storage_object_id} is already recorded for this investigation."
        )
    if payload.disposition == "promoted":
        apply_storage_promotion(
            session,
            storage_object,
            StorageObjectPromoteRequest(
                storage_tier="warm",
                retention_class="permanent",
                promoted_by_type="investigation",
                promoted_by_id=str(investigation_id),
                metadata_json={"data_role": "evidence", "investigation_id": investigation_id},
            ),
            actor=actor,
        )
    promotion = InvestigationEvidencePromotionORM(
        investigation_id=investigation_id,
        **payload.model_dump(mode="python"),
    )
    session.add(promotion)
    session.flush()
    add_custody(
        session,
        investigation,
        action="investigation_evidence_promoted",
        actor=actor,
        details={
            "promotion_id": promotion.investigation_evidence_promotion_id,
            "storage_object_id": promotion.storage_object_id,
            "disposition": promotion.disposition,
        },
    )
    session.commit()
    session.refresh(promotion)
    return promotion


def list_evidence_promotions(
    session: Session, investigation_id: int, *, limit: int = 200
) -> list[InvestigationEvidencePromotionORM]:
    require_investigation(session, investigation_id)
    return list(
        session.scalars(
            select(InvestigationEvidencePromotionORM)
            .where(InvestigationEvidencePromotionORM.investigation_id == investigation_id)
            .order_by(InvestigationEvidencePromotionORM.investigation_evidence_promotion_id.desc())
            .limit(limit)
        )
    )


def build_monitor_contract(session: Session, investigation_id: int) -> dict[str, object]:
    """Publish data only; watch scheduling and delivery remain outside this service."""
    investigation = require_investigation(session, investigation_id)
    if investigation.state != "monitoring":
        raise ValueError(
            "An investigation must be in monitoring state before a monitor contract is published."
        )
    latest_report = session.scalar(
        select(InvestigationReportVersionORM)
        .where(InvestigationReportVersionORM.investigation_id == investigation_id)
        .order_by(InvestigationReportVersionORM.version_number.desc())
        .limit(1)
    )
    promotions = list_evidence_promotions(session, investigation_id)
    return {
        "contract_version": "investigation-monitor/v1",
        "investigation_id": investigation.investigation_id,
        "question": investigation.question,
        "operator_scope": investigation.operator_scope_json,
        "time_window": investigation.time_window_json,
        "geography": investigation.geography_json,
        "source_policy": investigation.source_policy_snapshot_json,
        "evidence_threshold": investigation.evidence_threshold_json,
        "latest_report_version": latest_report.version_number if latest_report else None,
        "latest_report_storage_object_id": latest_report.storage_object_id
        if latest_report
        else None,
        "promoted_evidence_storage_object_ids": sorted(
            promotion.storage_object_id
            for promotion in promotions
            if promotion.disposition == "promoted"
        ),
    }


def archive_investigation(
    session: Session, investigation_id: int, *, stop_reason: str | None, actor: str = "api"
) -> dict[str, object]:
    investigation = require_investigation(session, investigation_id)
    if investigation.state != "archived":
        transition_investigation(
            session,
            investigation_id,
            InvestigationTransitionRequest(state="archived", stop_reason=stop_reason),
            actor=actor,
        )
        investigation = require_investigation(session, investigation_id)

    promoted_storage_ids = set(
        session.scalars(
            select(InvestigationEvidencePromotionORM.storage_object_id).where(
                InvestigationEvidencePromotionORM.investigation_id == investigation_id,
                InvestigationEvidencePromotionORM.disposition == "promoted",
            )
        )
    )
    raw_records = list(
        session.scalars(
            select(StorageObjectORM).where(
                StorageObjectORM.owner_type == "investigation",
                StorageObjectORM.owner_id == str(investigation_id),
            )
        )
    )
    archived_raw_ids: list[int] = []
    for storage_object in raw_records:
        data_role = str((storage_object.metadata_json or {}).get("data_role", ""))
        is_raw = data_role == "raw" or storage_object.object_kind.startswith("raw_")
        if not is_raw or storage_object.storage_object_id in promoted_storage_ids:
            continue
        if storage_object.lifecycle_status not in {"archived", "expired"}:
            apply_storage_transition(
                session,
                storage_object,
                lifecycle_status="archived",
                actor=actor,
                action="investigation_raw_artifact_archived",
                extra_details={"investigation_id": investigation_id},
            )
        archived_raw_ids.append(storage_object.storage_object_id)
    add_custody(
        session,
        investigation,
        action="investigation_archived",
        actor=actor,
        details={"archived_raw_storage_object_ids": archived_raw_ids},
    )
    session.commit()
    session.refresh(investigation)
    return InvestigationArchiveResultRead.model_validate(
        {
            "investigation": investigation,
            "retained_report_count": int(
                session.scalar(
                    select(func.count())
                    .select_from(InvestigationReportVersionORM)
                    .where(InvestigationReportVersionORM.investigation_id == investigation_id)
                )
                or 0
            ),
            "retained_evidence_count": len(promoted_storage_ids),
            "archived_raw_storage_object_ids": archived_raw_ids,
        }
    ).model_dump(mode="python")


def add_custody(
    session: Session,
    investigation: InvestigationORM,
    *,
    action: str,
    actor: str,
    details: dict[str, object],
) -> None:
    session.add(
        CustodyLogORM(
            object_type="investigation",
            object_id=str(investigation.investigation_id),
            action=action,
            actor=actor,
            details_json=details,
        )
    )
