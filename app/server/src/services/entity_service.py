from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import EntityORM, ScheduledTaskORM, ScheduledTaskRunORM


def entity_ops_now() -> datetime:
    return datetime.now(timezone.utc)


def list_entity_records(
    session: Session,
    *,
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    limit: int | None = 200,
) -> list[EntityORM]:
    statement = select(EntityORM).order_by(EntityORM.confidence_score.desc(), EntityORM.created_at.desc())
    if entity_type is not None:
        statement = statement.where(EntityORM.entity_type == entity_type)
    if redaction_level is not None:
        statement = statement.where(EntityORM.redaction_level == redaction_level)
    if min_confidence_score is not None:
        statement = statement.where(EntityORM.confidence_score >= min_confidence_score)
    if limit is not None:
        statement = statement.limit(limit)
    return list(session.scalars(statement))


def build_entity_inventory_summary(
    session: Session,
    *,
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
) -> dict[str, object]:
    generated_at = entity_ops_now()
    entities = list_entity_records(
        session,
        entity_type=entity_type,
        redaction_level=redaction_level,
        min_confidence_score=min_confidence_score,
        limit=None,
    )
    return {
        "generated_at": generated_at,
        "total_count": len(entities),
        "high_confidence_count": sum(1 for entity in entities if entity.confidence_score >= 0.85),
        "medium_confidence_count": sum(1 for entity in entities if 0.6 <= entity.confidence_score < 0.85),
        "low_confidence_count": sum(1 for entity in entities if entity.confidence_score < 0.6),
        "signal_conflict_count": sum(entity_signal_conflict_count(entity) for entity in entities),
        "entity_type_counts": build_entity_summary_buckets(entities, key_fn=lambda entity: entity.entity_type or "unknown"),
        "redaction_level_counts": build_entity_summary_buckets(
            entities,
            key_fn=lambda entity: entity.redaction_level or "unknown",
        ),
        "confidence_band_counts": build_entity_summary_buckets(
            entities,
            key_fn=lambda entity: classify_entity_confidence_band(entity.confidence_score),
        ),
        "evidence_strength_counts": build_entity_summary_buckets(
            entities,
            key_fn=lambda entity: entity_evidence_strength(entity),
        ),
    }


def build_entity_ops_report_index(
    session: Session,
    *,
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    limit: int = 25,
    conflict_limit: int = 25,
) -> dict[str, object]:
    generated_at = entity_ops_now()
    entities = list_entity_records(
        session,
        entity_type=entity_type,
        redaction_level=redaction_level,
        min_confidence_score=min_confidence_score,
        limit=None,
    )
    resolution_tasks = list(
        session.scalars(
            select(ScheduledTaskORM)
            .where(ScheduledTaskORM.task_type == "entity_resolution_refresh")
            .order_by(ScheduledTaskORM.task_id.asc())
        )
    )
    task_ids = [task.task_id for task in resolution_tasks]
    resolution_runs = (
        list(
            session.scalars(
                select(ScheduledTaskRunORM)
                .where(ScheduledTaskRunORM.task_id.in_(task_ids))
                .order_by(ScheduledTaskRunORM.task_run_id.desc())
            )
        )
        if task_ids
        else []
    )
    conflicting_entities = [entity for entity in entities if entity_signal_conflict_count(entity) > 0][:conflict_limit]
    return {
        "generated_at": generated_at,
        "latest_entity_at": entities[0].created_at if entities else None,
        "inventory_summary": build_entity_inventory_summary(
            session,
            entity_type=entity_type,
            redaction_level=redaction_level,
            min_confidence_score=min_confidence_score,
        ),
        "entity_resolution_task_count": len(resolution_tasks),
        "entity_resolution_run_count": len(resolution_runs),
        "entity_resolution_failure_count": sum(1 for run in resolution_runs if run.status == "failed"),
        "entity_resolution_tasks": resolution_tasks,
        "recent_resolution_runs": resolution_runs[:limit],
        "recent_entities": entities[:limit],
        "conflicting_entities": conflicting_entities,
    }


def build_entity_ops_export_summary(
    session: Session,
    *,
    entity_type: str | None = None,
    redaction_level: str | None = None,
    min_confidence_score: float | None = None,
    entity_limit: int = 500,
    report_limit: int = 25,
    conflict_limit: int = 25,
) -> dict[str, object]:
    generated_at = entity_ops_now()
    entities = list_entity_records(
        session,
        entity_type=entity_type,
        redaction_level=redaction_level,
        min_confidence_score=min_confidence_score,
        limit=entity_limit,
    )
    return {
        "generated_at": generated_at,
        "filters_json": {
            "entity_type": entity_type,
            "redaction_level": redaction_level,
            "min_confidence_score": min_confidence_score,
            "entity_limit": entity_limit,
            "report_limit": report_limit,
            "conflict_limit": conflict_limit,
        },
        "report_index": build_entity_ops_report_index(
            session,
            entity_type=entity_type,
            redaction_level=redaction_level,
            min_confidence_score=min_confidence_score,
            limit=report_limit,
            conflict_limit=conflict_limit,
        ),
        "entities": entities,
    }


def build_entity_summary_buckets(entities: list[EntityORM], *, key_fn) -> list[dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for entity in entities:
        key = str(key_fn(entity) or "unknown")
        bucket = buckets.setdefault(
            key,
            {
                "key": key,
                "total_count": 0,
                "high_confidence_count": 0,
                "signal_conflict_count": 0,
            },
        )
        bucket["total_count"] += 1
        if entity.confidence_score >= 0.85:
            bucket["high_confidence_count"] += 1
        bucket["signal_conflict_count"] += entity_signal_conflict_count(entity)
    return sorted(buckets.values(), key=lambda item: (-int(item["total_count"]), str(item["key"]).lower()))


def entity_signal_conflict_count(entity: EntityORM) -> int:
    metadata = entity.metadata_json if isinstance(entity.metadata_json, dict) else {}
    value = metadata.get("signal_conflict_count", 0)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return max(0, int(value))
    return 0


def entity_evidence_strength(entity: EntityORM) -> str:
    metadata = entity.metadata_json if isinstance(entity.metadata_json, dict) else {}
    value = metadata.get("evidence_strength")
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return "unknown"


def classify_entity_confidence_band(score: float) -> str:
    if score >= 0.85:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"
