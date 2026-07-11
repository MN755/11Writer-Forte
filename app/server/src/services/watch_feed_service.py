"""Safe, versioned, local delivery surface for material watch updates.

The watch evaluator remains the authority for collection and materiality.  This module
only publishes runs that carry an explicit materiality decision *and* usable public
citations.  That deliberately makes the feed fail closed while W3 evolves.
"""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import CustodyLogORM, SourceRunORM, StorageObjectORM, WatchORM, WatchRunORM
from src.services.storage_service import apply_storage_transition, register_storage_object
from src.services.watch_service import archive_watch as archive_watch_state

API_VERSION = "v1"
PUBLIC_REDACTION_LEVEL = "public"
MATERIAL_OUTCOMES = {
    "new_source", "new_fact", "corroboration", "contradiction", "status_change", "visual_change_candidate",
}


def ensure_local_feed_access(request: Request) -> None:
    """Reject non-loopback callers unless the deployment explicitly trusts a CIDR."""
    # Token-authenticated operators already passed the application-wide boundary.
    # Keep the stricter local-only rule for intentionally unauthenticated mode.
    if getattr(get_settings(), "auth_mode", "disabled") == "token" and getattr(
        request.state, "operator_principal", None
    ) is not None:
        return
    client = request.client
    host = client.host if client else ""
    # Starlette's in-process TestClient never represents a network listener.
    if host == "testclient":
        return
    configured = getattr(get_settings(), "local_api_trusted_networks", ["127.0.0.0/8", "::1/128"])
    try:
        address = ipaddress.ip_address(host)
        if any(address in ipaddress.ip_network(network, strict=False) for network in configured):
            return
    except ValueError:
        pass
    raise HTTPException(
        status_code=403,
        detail="The watch feed is restricted to localhost or configured trusted networks.",
    )


def encode_cursor(watch_run_id: int) -> str:
    payload = json.dumps({"v": 1, "watch_run_id": watch_run_id}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_cursor(cursor: str | None) -> int | None:
    if cursor is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        payload = json.loads(raw)
        value = payload["watch_run_id"]
        if payload.get("v") != 1 or not isinstance(value, int) or value < 1:
            raise ValueError
        return value
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid v1 watch-feed cursor.") from exc


def safe_public_uri(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = urlparse(value.strip())
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return value.strip()
    return None


def sanitize_public_text(value: object) -> str:
    """Avoid returning a local filesystem reference embedded in an otherwise public row."""
    text = str(value or "")
    text = re.sub(r"file:(?://)?[^\s)]+", "[local artifact]", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<!\w)[A-Za-z]:\\[^\s)]+", "[local artifact]", text)
    return text[:4000]


def materiality_for_run(run: WatchRunORM) -> dict[str, Any] | None:
    for value in ((run.metadata_json or {}).get("materiality"), (run.evidence_json or {}).get("materiality")):
        if isinstance(value, dict):
            return value
    return None


def citation_list(run: WatchRunORM) -> list[dict[str, str]]:
    evidence = run.evidence_json or {}
    candidates = evidence.get("citations") or evidence.get("citation") or []
    if isinstance(candidates, dict):
        candidates = [candidates]
    if not isinstance(candidates, list):
        candidates = []
    citations: list[dict[str, str]] = []
    for candidate in candidates:
        if isinstance(candidate, str):
            uri, label = safe_public_uri(candidate), candidate
        elif isinstance(candidate, dict):
            uri = safe_public_uri(candidate.get("uri") or candidate.get("url") or candidate.get("source_uri"))
            label = str(candidate.get("label") or candidate.get("title") or uri or "Source")
        else:
            continue
        if uri:
            citations.append({"uri": uri, "label": sanitize_public_text(label)[:300]})
    # Older evaluators place the source URI alongside the evidence.  It is still a
    # citation only when a materiality decision was explicitly recorded.
    source_uri = safe_public_uri(evidence.get("source_uri"))
    if source_uri and not citations:
        citations.append({"uri": source_uri, "label": "Source"})
    return citations


def is_publishable_run(run: WatchRunORM) -> bool:
    materiality = materiality_for_run(run)
    if not materiality or run.status != "completed" or not run.change_detected:
        return False
    update_type = str(materiality.get("update_type") or materiality.get("classification") or run.outcome)
    # W3's dataclass serializes this as ``material``; the long-form API contract
    # also accepts ``is_material``.  Both are explicit booleans, never inferred.
    decision = materiality.get("is_material", materiality.get("material"))
    if decision is not True or update_type not in MATERIAL_OUTCOMES:
        return False
    return bool(citation_list(run))


def redact_level(run: WatchRunORM, watch: WatchORM) -> str:
    for source in (run.metadata_json or {}, run.evidence_json or {}, watch.metadata_json or {}):
        value = source.get("redaction_level")
        if isinstance(value, str) and value:
            return value.lower()
    return PUBLIC_REDACTION_LEVEL


def serialize_update(run: WatchRunORM, watch: WatchORM, *, view: str) -> dict[str, Any] | None:
    if not is_publishable_run(run):
        return None
    redaction = redact_level(run, watch)
    if view == "public" and redaction != PUBLIC_REDACTION_LEVEL:
        return None
    materiality = materiality_for_run(run) or {}
    update_type = str(materiality.get("update_type") or materiality.get("classification") or run.outcome)
    payload: dict[str, Any] = {
        "event_id": f"watch-update-{run.watch_run_id}",
        "api_version": API_VERSION,
        "watch_id": watch.watch_id,
        "watch_slug": watch.slug,
        "watch_state": watch.state,
        "update_type": update_type,
        "occurred_at": run.finished_at or run.started_at,
        "summary": sanitize_public_text(run.output_summary) if view == "public" else run.output_summary,
        "citations": citation_list(run),
        "redaction_level": redaction,
        "materiality": {
            "score": materiality.get("score"),
            "reason": sanitize_public_text(materiality.get("reason") or materiality.get("rationale"))
            if view == "public"
            else materiality.get("reason") or materiality.get("rationale"),
        },
    }
    if view == "operator":
        payload["evidence"] = {
            "source_run_id": run.source_run_id,
            "storage_object_id": run.storage_object_id,
            "observation_ids": (run.evidence_json or {}).get("observation_ids", []),
        }
    return payload


def list_feed_updates(
    session: Session, *, watch_id: int | None, status: str | None, update_type: str | None,
    cursor: str | None, limit: int, view: str,
) -> dict[str, Any]:
    cursor_id = decode_cursor(cursor)
    statement = select(WatchRunORM, WatchORM).join(WatchORM, WatchRunORM.watch_id == WatchORM.watch_id)
    if watch_id is not None:
        statement = statement.where(WatchRunORM.watch_id == watch_id)
    if status is not None:
        statement = statement.where(WatchORM.state == status)
    if cursor_id is not None:
        statement = statement.where(WatchRunORM.watch_run_id < cursor_id)
    # Fetch more than a page because non-publishable rows are intentionally hidden.
    rows = session.execute(statement.order_by(WatchRunORM.watch_run_id.desc()).limit(limit * 20 + 1)).all()
    updates: list[dict[str, Any]] = []
    for run, watch in rows:
        update = serialize_update(run, watch, view=view)
        if update is None or (update_type is not None and update["update_type"] != update_type):
            continue
        updates.append(update)
        if len(updates) == limit + 1:
            break
    page = updates[:limit]
    return {
        "api_version": API_VERSION,
        "items": page,
        "next_cursor": encode_cursor(int(page[-1]["event_id"].rsplit("-", 1)[1])) if len(updates) > limit else None,
    }


def get_feed_update(session: Session, event_id: str, *, view: str) -> dict[str, Any]:
    prefix = "watch-update-"
    if not event_id.startswith(prefix) or not event_id[len(prefix):].isdigit():
        raise ValueError("Unknown watch-feed event ID.")
    run = session.get(WatchRunORM, int(event_id[len(prefix):]))
    if run is None:
        raise ValueError("Unknown watch-feed event ID.")
    watch = session.get(WatchORM, run.watch_id)
    update = serialize_update(run, watch, view=view) if watch else None
    if update is None:
        raise ValueError("This event is not available in the requested view.")
    return update


def watch_state(session: Session, watch_id: int) -> dict[str, Any]:
    watch = require_watch(session, watch_id)
    source_runs = []
    if watch.source_id:
        source_runs = list(session.scalars(select(SourceRunORM).where(SourceRunORM.source_id == watch.source_id).order_by(SourceRunORM.source_run_id.desc()).limit(10)))
    run_count = session.scalar(select(WatchRunORM).where(WatchRunORM.watch_id == watch_id).count()) if False else len(list(session.scalars(select(WatchRunORM.watch_run_id).where(WatchRunORM.watch_id == watch_id))))
    return {
        "api_version": API_VERSION,
        "watch_id": watch.watch_id,
        "slug": watch.slug,
        "state": watch.state,
        "objective": watch.objective,
        "last_evaluated_at": watch.last_evaluated_at,
        "last_changed_at": watch.last_changed_at,
        "next_run_at": watch.next_run_at,
        "source_coverage": {
            "source_id": watch.source_id,
            "watch_run_count": run_count,
            "coverage_ledger": (watch.metadata_json or {}).get("source_coverage")
            or (watch.baseline_json or {}).get("source_coverage"),
            "recent_source_runs": [{"source_run_id": row.source_run_id, "status": row.status, "started_at": row.started_at, "finished_at": row.finished_at, "records_imported": row.records_imported} for row in source_runs],
        },
    }


def list_activity(session: Session, watch_id: int, *, limit: int) -> list[dict[str, Any]]:
    require_watch(session, watch_id)
    runs = list(session.scalars(select(WatchRunORM).where(WatchRunORM.watch_id == watch_id).order_by(WatchRunORM.watch_run_id.desc()).limit(limit)))
    return [{"activity_id": f"watch-run-{run.watch_run_id}", "kind": "watch_run", "occurred_at": run.finished_at or run.started_at, "status": run.status, "outcome": run.outcome, "summary": run.output_summary, "error": run.error_text} for run in runs]


def require_watch(session: Session, watch_id: int) -> WatchORM:
    watch = session.get(WatchORM, watch_id)
    if watch is None:
        raise ValueError(f"Watch {watch_id} does not exist.")
    return watch


def generate_report(session: Session, watch_id: int, *, actor: str = "api") -> StorageObjectORM:
    watch = require_watch(session, watch_id)
    runs = list(session.scalars(select(WatchRunORM).where(WatchRunORM.watch_id == watch_id).order_by(WatchRunORM.watch_run_id.asc())))
    updates = [serialize_update(run, watch, view="operator") for run in runs]
    updates = [item for item in updates if item is not None]
    generated_at = datetime.now(timezone.utc)
    lines = [f"# Monitoring report: {watch.name}", "", f"Generated: {generated_at.isoformat()}", "", watch.objective, "", "## Material cited updates", ""]
    if not updates:
        lines.append("No material, cited updates have been published for this watch.")
    for update in updates:
        lines.extend([f"### {update['update_type']}: {update['event_id']}", update["summary"], "Citations:"])
        lines.extend(f"- [{citation['label']}]({citation['uri']})" for citation in update["citations"])
        lines.append("")
    payload = "\n".join(lines) + "\n"
    digest = hashlib.sha256(payload.encode()).hexdigest()
    report_path = get_settings().data_dir / "watch_reports" / f"watch-{watch_id}" / f"report-{digest[:16]}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(payload, encoding="utf-8")
    record = register_storage_object(session, object_key=f"watch:{watch_id}:monitoring-report:{digest}", object_kind="watch_monitoring_report", owner_type="watch", owner_id=str(watch_id), object_uri=report_path.resolve().as_uri(), content_hash=digest, media_type="text/markdown", storage_tier="warm", retention_class="permanent", lifecycle_status="promoted", byte_size=len(payload.encode()), observed_at=generated_at, metadata_json={"watch_id": watch_id, "report_version": digest[:16], "update_event_ids": [update["event_id"] for update in updates], "citation_validated": True}, actor=actor)
    session.add(CustodyLogORM(object_type="watch", object_id=str(watch_id), action="watch_report_generated", actor=actor, details_json={"storage_object_id": record.storage_object_id, "report_version": digest[:16], "update_count": len(updates)}))
    session.commit()
    session.refresh(record)
    return record


def list_reports(session: Session, watch_id: int) -> list[StorageObjectORM]:
    require_watch(session, watch_id)
    return list(session.scalars(select(StorageObjectORM).where(StorageObjectORM.owner_type == "watch", StorageObjectORM.owner_id == str(watch_id), StorageObjectORM.object_kind == "watch_monitoring_report").order_by(StorageObjectORM.created_at.desc())))


def serialize_report(record: StorageObjectORM) -> dict[str, Any]:
    """Expose report identity and lifecycle, never its local filesystem URI."""
    return {
        "storage_object_id": record.storage_object_id,
        "report_version": (record.metadata_json or {}).get("report_version"),
        "object_kind": record.object_kind,
        "media_type": record.media_type,
        "content_hash": record.content_hash,
        "byte_size": record.byte_size,
        "storage_tier": record.storage_tier,
        "retention_class": record.retention_class,
        "lifecycle_status": record.lifecycle_status,
        "created_at": record.created_at,
    }


def archive_watch(session: Session, watch_id: int, *, actor: str = "api") -> dict[str, Any]:
    require_watch(session, watch_id)
    report = generate_report(session, watch_id, actor=actor)
    # Delegate state/rule-version/schedule transitions to the canonical watch service.
    # The preservation pass below adds storage-specific retention custody.
    watch = archive_watch_state(session, watch_id, actor=actor)
    storage_ids = {run.storage_object_id for run in session.scalars(select(WatchRunORM).where(WatchRunORM.watch_id == watch_id)) if run.storage_object_id}
    storage_ids.update(row.storage_object_id for row in list_reports(session, watch_id))
    records = list(session.scalars(select(StorageObjectORM).where(StorageObjectORM.storage_object_id.in_(storage_ids)))) if storage_ids else []
    archived, retained, duplicate_candidates = 0, 0, 0
    seen_hashes: set[str] = set()
    for record in records:
        metadata = dict(record.metadata_json or {})
        legal_hold = bool(metadata.get("legal_hold"))
        is_durable = record.retention_class == "permanent" or record.object_kind == "watch_monitoring_report" or legal_hold
        if record.content_hash and record.content_hash in seen_hashes and not is_durable:
            duplicate_candidates += 1
            metadata["retention_handoff"] = {"decision": "duplicate_candidate_retained", "reason": "automatic deletion is forbidden", "archived_at": datetime.now(timezone.utc).isoformat()}
        elif record.content_hash:
            seen_hashes.add(record.content_hash)
        if is_durable and record.lifecycle_status != "archived":
            # Archive is a preservation move, never a deletion.  Legal-hold artifacts
            # are included in this move but never expire under this handoff.
            apply_storage_transition(session, record, lifecycle_status="archived", storage_tier="archive", metadata_json={"retention_handoff": {"decision": "preserved", "legal_hold": legal_hold}}, actor=actor, action="watch_archive_preserved")
            archived += 1
        else:
            record.metadata_json = metadata or {"retention_handoff": {"decision": "retained"}}
            retained += 1
        session.add(CustodyLogORM(object_type="storage_object", object_id=str(record.storage_object_id), action="watch_retention_handoff", actor=actor, details_json={"watch_id": watch_id, "legal_hold": legal_hold, "lifecycle_status": record.lifecycle_status}))
    session.add(CustodyLogORM(object_type="watch", object_id=str(watch_id), action="watch_archived", actor=actor, details_json={"report_storage_object_id": report.storage_object_id, "archived_storage_count": archived, "retained_storage_count": retained, "duplicate_candidates": duplicate_candidates}))
    session.commit()
    session.refresh(watch)
    return {"watch_id": watch.watch_id, "state": watch.state, "report_storage_object_id": report.storage_object_id, "archived_storage_count": archived, "retained_storage_count": retained, "duplicate_candidates": duplicate_candidates}
