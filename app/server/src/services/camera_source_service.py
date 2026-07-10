from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    CameraInventoryORM,
    CameraSourceInventoryORM,
    CustodyLogORM,
    ObservationORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
)
from src.services.camera_service import query_camera_inventory, refresh_task_matches_scope, serialize_refresh_run
from src.services.trust_service import normalize_domain


@dataclass(frozen=True)
class CameraSourceCandidate:
    candidate_key: str
    camera_inventory_id: int
    observation_id: int | None
    external_id: str | None
    name: str
    source_domain: str | None
    layer_key: str
    provider: str
    endpoint_kind: str
    endpoint_url: str
    status: str
    verification_state: str
    active: bool
    last_observed_at: datetime | None
    last_checked_at: datetime
    confidence_score: float
    graduation_score: float
    metadata_json: dict[str, Any]


def source_inventory_now() -> datetime:
    return datetime.now(timezone.utc)


def list_camera_sources(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = None,
    limit: int | None = 200,
) -> list[CameraSourceInventoryORM]:
    statement = select(CameraSourceInventoryORM).order_by(
        CameraSourceInventoryORM.graduation_score.desc(),
        CameraSourceInventoryORM.updated_at.desc(),
    )
    if layer_key:
        statement = statement.where(CameraSourceInventoryORM.layer_key == layer_key)
    if source_domain:
        statement = statement.where(CameraSourceInventoryORM.source_domain == source_domain)
    if endpoint_kind:
        statement = statement.where(CameraSourceInventoryORM.endpoint_kind == endpoint_kind)
    if status:
        statement = statement.where(CameraSourceInventoryORM.status == status)
    if verification_state:
        statement = statement.where(CameraSourceInventoryORM.verification_state == verification_state)
    if active is not None:
        statement = statement.where(CameraSourceInventoryORM.active == active)
    if limit is not None:
        statement = statement.limit(limit)
    return list(session.scalars(statement))


def materialize_camera_source_inventory(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    active: bool | None = None,
    limit: int = 500,
    actor: str = "camera_source_registry",
) -> dict[str, object]:
    cameras = query_camera_inventory(
        session,
        layer_key=layer_key,
        active=active,
        limit=limit,
    )
    if source_domain:
        cameras = [
            camera
            for camera in cameras
            if camera_source_domain_matches_scope(camera.source_domain, source_domain)
        ]
    created_count = 0
    updated_count = 0
    touched_keys: list[str] = []
    touched_ids: list[int] = []
    scanned_endpoint_count = 0
    seen_candidate_keys: set[str] = set()

    for camera in cameras:
        for candidate in build_camera_source_candidates(camera):
            scanned_endpoint_count += 1
            if candidate.candidate_key in seen_candidate_keys:
                continue
            seen_candidate_keys.add(candidate.candidate_key)
            record = session.scalar(
                select(CameraSourceInventoryORM).where(
                    CameraSourceInventoryORM.candidate_key == candidate.candidate_key
                )
            )
            if record is None:
                record = CameraSourceInventoryORM(
                    candidate_key=candidate.candidate_key,
                    camera_inventory_id=candidate.camera_inventory_id,
                    observation_id=candidate.observation_id,
                    external_id=candidate.external_id,
                    name=candidate.name,
                    source_domain=candidate.source_domain,
                    layer_key=candidate.layer_key,
                    provider=candidate.provider,
                    endpoint_kind=candidate.endpoint_kind,
                    endpoint_url=candidate.endpoint_url,
                    status=candidate.status,
                    verification_state=candidate.verification_state,
                    active=candidate.active,
                    last_observed_at=candidate.last_observed_at,
                    last_checked_at=candidate.last_checked_at,
                    confidence_score=candidate.confidence_score,
                    graduation_score=candidate.graduation_score,
                    metadata_json=candidate.metadata_json,
                )
                session.add(record)
                session.flush()
                created_count += 1
                touched_ids.append(record.camera_source_inventory_id)
                touched_keys.append(record.candidate_key)
                session.add(
                    CustodyLogORM(
                        object_type="camera_source_inventory",
                        object_id=str(record.camera_source_inventory_id),
                        action="camera_source_registered",
                        actor=actor,
                        details_json={
                            "candidate_key": record.candidate_key,
                            "camera_inventory_id": record.camera_inventory_id,
                            "endpoint_kind": record.endpoint_kind,
                            "endpoint_url": record.endpoint_url,
                            "status": record.status,
                        },
                    )
                )
                continue

            changes = apply_camera_source_candidate(record, candidate)
            if not changes:
                continue
            updated_count += 1
            touched_ids.append(record.camera_source_inventory_id)
            touched_keys.append(record.candidate_key)
            session.add(
                CustodyLogORM(
                    object_type="camera_source_inventory",
                    object_id=str(record.camera_source_inventory_id),
                    action="camera_source_updated",
                    actor=actor,
                    details_json={
                        "candidate_key": record.candidate_key,
                        "changes": to_json_safe(changes),
                    },
                )
            )

    session.add(
        CustodyLogORM(
            object_type="camera_source_materialization",
            object_id=f"{layer_key or 'all'}:{source_domain or 'all'}:{active if active is not None else 'all'}",
            action="camera_source_materialization_completed",
            actor=actor,
            details_json={
                "layer_key": layer_key,
                "source_domain": source_domain,
                "active": active,
                "limit": limit,
                "scanned_camera_count": len(cameras),
                "scanned_endpoint_count": scanned_endpoint_count,
                "created_count": created_count,
                "updated_count": updated_count,
                "candidate_keys": touched_keys,
            },
        )
    )
    session.commit()

    sources = (
        list(
            session.scalars(
                select(CameraSourceInventoryORM)
                .where(CameraSourceInventoryORM.camera_source_inventory_id.in_(touched_ids))
                .order_by(CameraSourceInventoryORM.camera_source_inventory_id.asc())
            )
        )
        if touched_ids
        else []
    )
    return {
        "created_count": created_count,
        "updated_count": updated_count,
        "scanned_camera_count": len(cameras),
        "scanned_endpoint_count": scanned_endpoint_count,
        "sources": sources,
    }


def build_camera_source_inventory_summary(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = None,
) -> dict[str, object]:
    sources = list_camera_sources(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        endpoint_kind=endpoint_kind,
        status=status,
        verification_state=verification_state,
        active=active,
        limit=5000,
    )
    return {
        "generated_at": source_inventory_now(),
        "total_count": len(sources),
        "active_count": sum(1 for source in sources if source.active),
        "ready_count": sum(1 for source in sources if source.status == "ready"),
        "review_count": sum(1 for source in sources if source.status == "review"),
        "candidate_count": sum(1 for source in sources if source.status == "candidate"),
        "graduated_count": sum(1 for source in sources if source.status == "graduated"),
        "source_domain_counts": build_camera_source_summary_buckets(
            sources,
            lambda source: source.source_domain or "unknown",
        ),
        "endpoint_kind_counts": build_camera_source_summary_buckets(
            sources,
            lambda source: source.endpoint_kind,
        ),
        "status_counts": build_camera_source_summary_buckets(
            sources,
            lambda source: source.status,
        ),
    }


def build_camera_source_inventory_ops_detail(
    session: Session,
    camera_source_inventory_id: int,
) -> dict[str, object]:
    source = session.get(CameraSourceInventoryORM, camera_source_inventory_id)
    if source is None:
        raise ValueError(f"Camera source inventory record {camera_source_inventory_id} does not exist.")
    camera = (
        session.get(CameraInventoryORM, source.camera_inventory_id)
        if source.camera_inventory_id is not None
        else None
    )
    latest_observation = (
        session.get(ObservationORM, source.observation_id)
        if source.observation_id is not None
        else None
    )
    custody_logs = list(
        session.scalars(
            select(CustodyLogORM)
            .where(
                CustodyLogORM.object_type == "camera_source_inventory",
                CustodyLogORM.object_id == str(source.camera_source_inventory_id),
            )
            .order_by(CustodyLogORM.created_at.desc())
            .limit(50)
        )
    )
    return {
        "source": source,
        "camera": camera,
        "latest_observation": latest_observation,
        "custody_logs": custody_logs,
    }


def build_camera_source_ops_report_index(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = None,
    stale_after_hours: float = 24.0,
    limit: int = 25,
    stale_source_limit: int = 25,
) -> dict[str, object]:
    generated_at = source_inventory_now()
    stale_before = generated_at - timedelta(hours=max(0.0, stale_after_hours))
    summary = build_camera_source_inventory_summary(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        endpoint_kind=endpoint_kind,
        status=status,
        verification_state=verification_state,
        active=active,
    )
    scoped_sources = list_camera_sources(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        endpoint_kind=endpoint_kind,
        status=status,
        verification_state=verification_state,
        active=active,
        limit=None,
    )
    stale_sources = [
        source
        for source in scoped_sources
        if is_stale_camera_source(source, stale_before)
    ][:stale_source_limit]

    refresh_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.task_type == "camera_inventory_refresh")
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    matching_refresh_tasks = [
        task
        for task in refresh_tasks
        if refresh_task_matches_scope(task, layer_key=layer_key, source_domain=source_domain)
    ]
    task_lookup = {task.task_id: task for task in matching_refresh_tasks}
    task_ids = list(task_lookup)

    all_refresh_runs: list[ScheduledTaskRunORM] = []
    recent_refresh_runs: list[dict[str, object]] = []
    if task_ids:
        all_refresh_runs = list(
            session.scalars(
                select(ScheduledTaskRunORM)
                .where(ScheduledTaskRunORM.task_id.in_(task_ids))
                .order_by(ScheduledTaskRunORM.task_run_id.desc())
            )
        )
        recent_refresh_runs = [
            serialize_refresh_run(task_lookup[run.task_id], run)
            for run in all_refresh_runs[:limit]
            if run.task_id in task_lookup
        ]

    materialization_logs = list(
        session.scalars(
            select(CustodyLogORM)
            .where(CustodyLogORM.object_type == "camera_source_materialization")
            .order_by(CustodyLogORM.created_at.desc())
            .limit(max(limit * 5, 50))
        )
    )
    recent_materializations = [
        log
        for log in materialization_logs
        if camera_source_materialization_matches_scope(log, layer_key=layer_key, source_domain=source_domain)
    ][:limit]
    latest_materialization_at = recent_materializations[0].created_at if recent_materializations else None

    return {
        "generated_at": generated_at,
        "stale_after_hours": stale_after_hours,
        "latest_materialization_at": latest_materialization_at,
        "inventory_summary": summary,
        "refresh_task_count": len(matching_refresh_tasks),
        "refresh_run_count": len(all_refresh_runs),
        "refresh_failure_count": sum(1 for run in all_refresh_runs if run.status == "failed"),
        "refresh_tasks": matching_refresh_tasks,
        "recent_refresh_runs": recent_refresh_runs,
        "recent_materializations": recent_materializations,
        "stale_sources": stale_sources,
    }


def build_camera_source_ops_export_summary(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    endpoint_kind: str | None = None,
    status: str | None = None,
    verification_state: str | None = None,
    active: bool | None = None,
    stale_after_hours: float = 24.0,
    source_limit: int = 500,
    report_limit: int = 25,
    stale_source_limit: int = 25,
) -> dict[str, object]:
    generated_at = source_inventory_now()
    sources = list_camera_sources(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        endpoint_kind=endpoint_kind,
        status=status,
        verification_state=verification_state,
        active=active,
        limit=source_limit,
    )
    return {
        "generated_at": generated_at,
        "filters_json": {
            "layer_key": layer_key,
            "source_domain": source_domain,
            "endpoint_kind": endpoint_kind,
            "status": status,
            "verification_state": verification_state,
            "active": active,
            "stale_after_hours": stale_after_hours,
            "source_limit": source_limit,
            "report_limit": report_limit,
            "stale_source_limit": stale_source_limit,
        },
        "report_index": build_camera_source_ops_report_index(
            session,
            layer_key=layer_key,
            source_domain=source_domain,
            endpoint_kind=endpoint_kind,
            status=status,
            verification_state=verification_state,
            active=active,
            stale_after_hours=stale_after_hours,
            limit=report_limit,
            stale_source_limit=stale_source_limit,
        ),
        "sources": sources,
    }


def build_camera_source_candidates(camera: CameraInventoryORM) -> list[CameraSourceCandidate]:
    checked_at = source_inventory_now()
    candidates: list[CameraSourceCandidate] = []
    endpoints = [
        ("image", camera.image_url),
        ("stream", camera.stream_url),
        ("page", camera.page_url),
    ]
    for endpoint_kind, endpoint_url in endpoints:
        if not endpoint_url:
            continue
        score = compute_camera_source_score(camera, endpoint_kind, checked_at)
        candidates.append(
            CameraSourceCandidate(
                candidate_key=build_camera_source_key(camera, endpoint_kind, endpoint_url),
                camera_inventory_id=camera.camera_inventory_id,
                observation_id=camera.observation_id,
                external_id=camera.external_id,
                name=camera.name,
                source_domain=camera.source_domain,
                layer_key=camera.layer_key,
                provider=camera.provider,
                endpoint_kind=endpoint_kind,
                endpoint_url=endpoint_url,
                status=derive_camera_source_status(score),
                verification_state="observed",
                active=camera.active,
                last_observed_at=camera.last_observed_at,
                last_checked_at=checked_at,
                confidence_score=camera.confidence_score,
                graduation_score=score,
                metadata_json={
                    "camera_key": camera.camera_key,
                    "camera_status": camera.status,
                    "road_name": camera.road_name,
                    "page_url": camera.page_url,
                },
            )
        )
    return candidates


def build_camera_source_key(
    camera: CameraInventoryORM,
    endpoint_kind: str,
    endpoint_url: str,
) -> str:
    raw_key = "|".join(
        [
            camera.source_domain or "",
            camera.external_id or "",
            endpoint_kind,
            endpoint_url.strip(),
            camera.layer_key,
        ]
    )
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:16]
    prefix = camera.external_id or camera.camera_key or "camera-source"
    prefix = "".join(ch for ch in prefix.lower() if ch.isalnum() or ch in {"-", "_"})[:48] or "camera-source"
    return f"{prefix}-{endpoint_kind}-{digest}"


def compute_camera_source_score(
    camera: CameraInventoryORM,
    endpoint_kind: str,
    checked_at: datetime,
) -> float:
    score = 0.1
    if endpoint_kind == "stream":
        score += 0.25
    elif endpoint_kind == "image":
        score += 0.2
    else:
        score += 0.1
    if camera.active:
        score += 0.15
    if camera.status in {"active", "online", "open", "ok"}:
        score += 0.15
    if camera.external_id:
        score += 0.1
    if camera.source_domain:
        score += 0.1
    if camera.last_observed_at is not None:
        observed_at = normalize_timestamp(camera.last_observed_at)
        if observed_at >= checked_at - timedelta(days=2):
            score += 0.1
    score += min(max(float(camera.confidence_score), 0.0), 1.0) * 0.1
    if endpoint_kind == "page" and (camera.image_url or camera.stream_url):
        score -= 0.05
    return round(max(0.0, min(score, 1.0)), 3)


def derive_camera_source_status(score: float) -> str:
    if score >= 0.85:
        return "graduated"
    if score >= 0.7:
        return "ready"
    if score >= 0.45:
        return "review"
    return "candidate"


def apply_camera_source_candidate(
    record: CameraSourceInventoryORM,
    candidate: CameraSourceCandidate,
) -> dict[str, dict[str, object]]:
    changes: dict[str, dict[str, object]] = {}
    status = (
        record.status
        if record.status in {"graduated", "ignored", "retired"}
        else candidate.status
    )
    preserve_verified_state = (
        candidate.verification_state == "observed"
        and record.verification_state in {"reachable", "failed"}
    )
    verification_state = (
        record.verification_state if preserve_verified_state else candidate.verification_state
    )
    last_checked_at = (
        record.last_checked_at
        if preserve_verified_state and record.last_checked_at is not None
        else candidate.last_checked_at
    )
    field_mapping = {
        "camera_inventory_id": candidate.camera_inventory_id,
        "observation_id": candidate.observation_id,
        "external_id": candidate.external_id,
        "name": candidate.name,
        "source_domain": candidate.source_domain,
        "layer_key": candidate.layer_key,
        "provider": candidate.provider,
        "endpoint_kind": candidate.endpoint_kind,
        "endpoint_url": candidate.endpoint_url,
        "status": status,
        "verification_state": verification_state,
        "active": candidate.active,
        "last_observed_at": candidate.last_observed_at,
        "last_checked_at": last_checked_at,
        "confidence_score": candidate.confidence_score,
        "graduation_score": candidate.graduation_score,
        "metadata_json": merge_metadata(record.metadata_json, candidate.metadata_json),
    }
    for field_name, new_value in field_mapping.items():
        old_value = getattr(record, field_name)
        if values_equal(old_value, new_value):
            continue
        setattr(record, field_name, new_value)
        changes[field_name] = {"old": old_value, "new": new_value}
    return changes


def build_camera_source_summary_buckets(
    sources: list[CameraSourceInventoryORM],
    key_fn,
) -> list[dict[str, object]]:
    counts: dict[str, dict[str, object]] = {}
    for source in sources:
        key = str(key_fn(source) or "unknown")
        bucket = counts.setdefault(
            key,
            {
                "key": key,
                "total_count": 0,
                "active_count": 0,
                "ready_count": 0,
                "review_count": 0,
            },
        )
        bucket["total_count"] += 1
        if source.active:
            bucket["active_count"] += 1
        if source.status in {"ready", "graduated"}:
            bucket["ready_count"] += 1
        if source.status == "review":
            bucket["review_count"] += 1
    return sorted(
        counts.values(),
        key=lambda item: (-int(item["total_count"]), str(item["key"]).lower()),
    )


def camera_source_domain_matches_scope(
    candidate_domain: str | None,
    scope_domain: str,
) -> bool:
    normalized_scope = normalize_domain(scope_domain)
    normalized_candidate = normalize_domain(candidate_domain) if candidate_domain else None
    if not normalized_scope:
        return True
    if not normalized_candidate:
        return False
    return (
        normalized_candidate == normalized_scope
        or normalized_candidate.endswith(f".{normalized_scope}")
        or normalized_scope.endswith(f".{normalized_candidate}")
    )


def merge_metadata(
    current: dict[str, Any] | None,
    incoming: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(current or {})
    merged.update(incoming or {})
    return merged


def normalize_timestamp(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def is_stale_camera_source(source: CameraSourceInventoryORM, stale_before: datetime) -> bool:
    if source.last_observed_at is None:
        return True
    return normalize_timestamp(source.last_observed_at) < stale_before


def camera_source_materialization_matches_scope(
    log: CustodyLogORM,
    *,
    layer_key: str | None,
    source_domain: str | None,
) -> bool:
    details = log.details_json if isinstance(log.details_json, dict) else {}
    detail_layer = details.get("layer_key")
    detail_source_domain = details.get("source_domain")
    if layer_key is not None and detail_layer is not None and detail_layer != layer_key:
        return False
    if source_domain is None:
        return True
    if not isinstance(detail_source_domain, str) or not detail_source_domain.strip():
        return True
    normalized_scope_domain = normalize_domain(source_domain)
    normalized_detail_domain = normalize_domain(detail_source_domain)
    if not normalized_scope_domain or not normalized_detail_domain:
        return False
    return (
        normalized_scope_domain == normalized_detail_domain
        or normalized_scope_domain.endswith(f".{normalized_detail_domain}")
        or normalized_detail_domain.endswith(f".{normalized_scope_domain}")
    )


def values_equal(old_value: Any, new_value: Any) -> bool:
    if isinstance(old_value, datetime) and isinstance(new_value, datetime):
        return normalize_timestamp(old_value) == normalize_timestamp(new_value)
    return old_value == new_value


def to_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: to_json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_json_safe(item) for item in value]
    return value
