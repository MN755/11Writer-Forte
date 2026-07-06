from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

from src.config.settings import Settings
from src.reference.db import get_engine
from src.source_discovery.db import session_scope
from src.source_discovery.models import BackendAlertRecordORM, BackendProvenanceEventORM
from src.types.api import (
    BackendAlertRecord,
    BackendAlertResponse,
    BackendProvenanceEvent,
    BackendProvenanceResponse,
)


def record_provenance_event(
    settings: Settings,
    *,
    subsystem: str,
    event_kind: str,
    operation: str,
    status: str,
    actor: str,
    subject_type: str,
    subject_id: str,
    summary: str,
    correlation_id: str | None = None,
    parent_event_id: str | None = None,
    source_uri: str | None = None,
    input_refs: list[str] | None = None,
    output_refs: list[str] | None = None,
    evidence_refs: list[str] | None = None,
    chain_of_custody: list[str] | None = None,
    metadata: dict[str, object] | None = None,
    session: Session | None = None,
) -> BackendProvenanceEvent:
    if session is not None:
        row = _record_provenance_event_row(
            session,
            subsystem=subsystem,
            event_kind=event_kind,
            operation=operation,
            status=status,
            actor=actor,
            subject_type=subject_type,
            subject_id=subject_id,
            summary=summary,
            correlation_id=correlation_id,
            parent_event_id=parent_event_id,
            source_uri=source_uri,
            input_refs=input_refs,
            output_refs=output_refs,
            evidence_refs=evidence_refs,
            chain_of_custody=chain_of_custody,
            metadata=metadata,
        )
        session.flush()
        return _serialize_provenance_event(row)

    init_ops_audit_db(settings.source_discovery_database_url)
    with session_scope(settings.source_discovery_database_url) as owned_session:
        row = _record_provenance_event_row(
            owned_session,
            subsystem=subsystem,
            event_kind=event_kind,
            operation=operation,
            status=status,
            actor=actor,
            subject_type=subject_type,
            subject_id=subject_id,
            summary=summary,
            correlation_id=correlation_id,
            parent_event_id=parent_event_id,
            source_uri=source_uri,
            input_refs=input_refs,
            output_refs=output_refs,
            evidence_refs=evidence_refs,
            chain_of_custody=chain_of_custody,
            metadata=metadata,
        )
        owned_session.flush()
        return _serialize_provenance_event(row)


def upsert_alert_record(
    settings: Settings,
    *,
    dedupe_key: str,
    subsystem: str,
    alert_type: str,
    severity: str,
    status: str,
    title: str,
    summary: str,
    subject_type: str,
    subject_id: str,
    source_event_id: str | None = None,
    evidence_refs: list[str] | None = None,
    metadata: dict[str, object] | None = None,
    caveats: list[str] | None = None,
    session: Session | None = None,
) -> BackendAlertRecord:
    if session is not None:
        row = _upsert_alert_record_row(
            session,
            dedupe_key=dedupe_key,
            subsystem=subsystem,
            alert_type=alert_type,
            severity=severity,
            status=status,
            title=title,
            summary=summary,
            subject_type=subject_type,
            subject_id=subject_id,
            source_event_id=source_event_id,
            evidence_refs=evidence_refs,
            metadata=metadata,
            caveats=caveats,
        )
        session.flush()
        return _serialize_alert_record(row)

    init_ops_audit_db(settings.source_discovery_database_url)
    with session_scope(settings.source_discovery_database_url) as owned_session:
        row = _upsert_alert_record_row(
            owned_session,
            dedupe_key=dedupe_key,
            subsystem=subsystem,
            alert_type=alert_type,
            severity=severity,
            status=status,
            title=title,
            summary=summary,
            subject_type=subject_type,
            subject_id=subject_id,
            source_event_id=source_event_id,
            evidence_refs=evidence_refs,
            metadata=metadata,
            caveats=caveats,
        )
        owned_session.flush()
        return _serialize_alert_record(row)


def list_provenance_events(
    settings: Settings,
    *,
    limit: int = 50,
    subsystem: str | None = None,
    subject_type: str | None = None,
    subject_id: str | None = None,
) -> BackendProvenanceResponse:
    init_ops_audit_db(settings.source_discovery_database_url)
    with session_scope(settings.source_discovery_database_url) as session:
        stmt = select(BackendProvenanceEventORM).order_by(BackendProvenanceEventORM.occurred_at.desc())
        if subsystem:
            stmt = stmt.where(BackendProvenanceEventORM.subsystem == subsystem)
        if subject_type:
            stmt = stmt.where(BackendProvenanceEventORM.subject_type == subject_type)
        if subject_id:
            stmt = stmt.where(BackendProvenanceEventORM.subject_id == subject_id)
        rows = list(session.scalars(stmt.limit(max(1, limit))))
        return BackendProvenanceResponse(
            count=len(rows),
            events=[_serialize_provenance_event(row) for row in rows],
        )


def list_alert_records(
    settings: Settings,
    *,
    limit: int = 50,
    subsystem: str | None = None,
    status: str | None = None,
    severity: str | None = None,
) -> BackendAlertResponse:
    init_ops_audit_db(settings.source_discovery_database_url)
    with session_scope(settings.source_discovery_database_url) as session:
        stmt = select(BackendAlertRecordORM).order_by(BackendAlertRecordORM.last_observed_at.desc())
        if subsystem:
            stmt = stmt.where(BackendAlertRecordORM.subsystem == subsystem)
        if status:
            stmt = stmt.where(BackendAlertRecordORM.status == status)
        if severity:
            stmt = stmt.where(BackendAlertRecordORM.severity == severity)
        rows = list(session.scalars(stmt.limit(max(1, limit))))
        return BackendAlertResponse(
            count=len(rows),
            alerts=[_serialize_alert_record(row) for row in rows],
        )


def _record_provenance_event_row(
    session: Session,
    *,
    subsystem: str,
    event_kind: str,
    operation: str,
    status: str,
    actor: str,
    subject_type: str,
    subject_id: str,
    summary: str,
    correlation_id: str | None,
    parent_event_id: str | None,
    source_uri: str | None,
    input_refs: list[str] | None,
    output_refs: list[str] | None,
    evidence_refs: list[str] | None,
    chain_of_custody: list[str] | None,
    metadata: dict[str, object] | None,
) -> BackendProvenanceEventORM:
    row = BackendProvenanceEventORM(
        provenance_event_id=f"prov:{subsystem}:{_compact_timestamp(_utc_now())}:{uuid4().hex[:8]}",
        subsystem=subsystem,
        event_kind=event_kind,
        operation=operation,
        status=status,
        actor=actor,
        subject_type=subject_type,
        subject_id=subject_id,
        correlation_id=correlation_id,
        parent_event_id=parent_event_id,
        source_uri=source_uri,
        input_refs_json=_json_dumps(input_refs or []),
        output_refs_json=_json_dumps(output_refs or []),
        evidence_refs_json=_json_dumps(evidence_refs or []),
        chain_of_custody_json=_json_dumps(chain_of_custody or []),
        metadata_json=_json_dumps(metadata or {}),
        summary=summary,
        occurred_at=_utc_now(),
    )
    session.add(row)
    return row


def _upsert_alert_record_row(
    session: Session,
    *,
    dedupe_key: str,
    subsystem: str,
    alert_type: str,
    severity: str,
    status: str,
    title: str,
    summary: str,
    subject_type: str,
    subject_id: str,
    source_event_id: str | None,
    evidence_refs: list[str] | None,
    metadata: dict[str, object] | None,
    caveats: list[str] | None,
) -> BackendAlertRecordORM:
    now = _utc_now()
    row = session.scalar(
        select(BackendAlertRecordORM).where(BackendAlertRecordORM.dedupe_key == dedupe_key)
    )
    if row is None:
        row = BackendAlertRecordORM(
            alert_id=f"alert:{subsystem}:{_compact_timestamp(now)}:{uuid4().hex[:8]}",
            dedupe_key=dedupe_key,
            subsystem=subsystem,
            alert_type=alert_type,
            severity=severity,
            status=status,
            title=title,
            summary=summary,
            subject_type=subject_type,
            subject_id=subject_id,
            source_event_id=source_event_id,
            first_observed_at=now,
            last_observed_at=now,
            occurrence_count=1,
            evidence_refs_json=_json_dumps(evidence_refs or []),
            metadata_json=_json_dumps(metadata or {}),
            caveats_json=_json_dumps(caveats or []),
        )
        session.add(row)
        return row

    row.severity = severity
    row.status = status
    row.title = title
    row.summary = summary
    row.subject_type = subject_type
    row.subject_id = subject_id
    row.source_event_id = source_event_id
    row.last_observed_at = now
    row.occurrence_count += 1
    row.evidence_refs_json = _json_dumps(evidence_refs or [])
    row.metadata_json = _json_dumps(metadata or {})
    row.caveats_json = _json_dumps(caveats or [])
    return row


def _serialize_provenance_event(row: BackendProvenanceEventORM) -> BackendProvenanceEvent:
    return BackendProvenanceEvent(
        provenance_event_id=row.provenance_event_id,
        subsystem=row.subsystem,
        event_kind=row.event_kind,
        operation=row.operation,
        status=row.status,
        actor=row.actor,
        subject_type=row.subject_type,
        subject_id=row.subject_id,
        correlation_id=row.correlation_id,
        parent_event_id=row.parent_event_id,
        source_uri=row.source_uri,
        input_refs=_loads_json_list(row.input_refs_json),
        output_refs=_loads_json_list(row.output_refs_json),
        evidence_refs=_loads_json_list(row.evidence_refs_json),
        chain_of_custody=_loads_json_list(row.chain_of_custody_json),
        metadata=_loads_json_dict(row.metadata_json),
        summary=row.summary,
        occurred_at=row.occurred_at,
    )


def _serialize_alert_record(row: BackendAlertRecordORM) -> BackendAlertRecord:
    return BackendAlertRecord(
        alert_id=row.alert_id,
        dedupe_key=row.dedupe_key,
        subsystem=row.subsystem,
        alert_type=row.alert_type,
        severity=row.severity,  # type: ignore[arg-type]
        status=row.status,  # type: ignore[arg-type]
        title=row.title,
        summary=row.summary,
        subject_type=row.subject_type,
        subject_id=row.subject_id,
        source_event_id=row.source_event_id,
        first_observed_at=row.first_observed_at,
        last_observed_at=row.last_observed_at,
        occurrence_count=row.occurrence_count,
        evidence_refs=_loads_json_list(row.evidence_refs_json),
        metadata=_loads_json_dict(row.metadata_json),
        caveats=_loads_json_list(row.caveats_json),
    )


def _loads_json_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _loads_json_dict(raw: str | None) -> dict[str, object]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(value, dict):
        return value
    return {}


def _json_dumps(value: object) -> str:
    return json.dumps(value)


def init_ops_audit_db(database_url: str) -> None:
    _ensure_ops_tables(get_engine(database_url))


def _ensure_ops_tables(bind: Engine) -> None:
    BackendProvenanceEventORM.metadata.create_all(
        bind,
        tables=[BackendProvenanceEventORM.__table__, BackendAlertRecordORM.__table__],
    )


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _compact_timestamp(value: str) -> str:
    return re.sub(r"[^0-9]", "", value)[:20]
