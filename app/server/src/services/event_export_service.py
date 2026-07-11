from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    AlertORM,
    CustodyLogORM,
    EntityORM,
    EntityObservationLinkORM,
    EventORM,
    EventObservationLinkORM,
    LocalImportRunORM,
    ObservationORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SituationProductORM,
    SourceDefinitionORM,
    SourceRunORM,
)
from src.services.redaction_service import (
    enforce_export_redaction,
    filter_records_by_redaction_level,
    normalize_redaction_level,
)


def export_now() -> datetime:
    return datetime.now(timezone.utc)


def build_event_export_bundle(
    session: Session,
    event_id: int,
    max_redaction_level: str | None = None,
) -> dict[str, object]:
    event = session.get(EventORM, event_id)
    if event is None:
        raise ValueError(f"Event {event_id} does not exist.")
    enforce_export_redaction(event, max_redaction_level)
    exported_at = export_now()

    observation_links = list(
        session.scalars(
            select(EventObservationLinkORM)
            .where(EventObservationLinkORM.event_id == event_id)
            .order_by(EventObservationLinkORM.event_observation_link_id.asc())
        )
    )
    observation_ids = [link.observation_id for link in observation_links]
    observations = (
        list(
            session.scalars(
                select(ObservationORM)
                .where(ObservationORM.observation_id.in_(observation_ids))
                .order_by(ObservationORM.observation_id.asc())
            )
        )
        if observation_ids
        else []
    )
    entity_observation_links = (
        list(
            session.scalars(
                select(EntityObservationLinkORM)
                .where(EntityObservationLinkORM.observation_id.in_(observation_ids))
                .order_by(EntityObservationLinkORM.entity_observation_link_id.asc())
            )
        )
        if observation_ids
        else []
    )
    entity_ids = sorted({link.entity_id for link in entity_observation_links})
    entities = (
        list(
            session.scalars(
                select(EntityORM)
                .where(EntityORM.entity_id.in_(entity_ids))
                .order_by(EntityORM.entity_id.asc())
            )
        )
        if entity_ids
        else []
    )
    entities = filter_records_by_redaction_level(entities, max_redaction_level)
    entity_ids = sorted({entity.entity_id for entity in entities})
    entity_observation_links = [
        link for link in entity_observation_links if link.entity_id in entity_ids
    ]

    import_run_ids = sorted(
        {
            observation.import_run_id
            for observation in observations
            if observation.import_run_id is not None
        }
    )
    import_runs = (
        list(
            session.scalars(
                select(LocalImportRunORM)
                .where(LocalImportRunORM.import_run_id.in_(import_run_ids))
                .order_by(LocalImportRunORM.import_run_id.asc())
            )
        )
        if import_run_ids
        else []
    )

    source_runs = (
        list(
            session.scalars(
                select(SourceRunORM)
                .where(SourceRunORM.import_run_id.in_(import_run_ids))
                .order_by(SourceRunORM.source_run_id.asc())
            )
        )
        if import_run_ids
        else []
    )
    source_ids = sorted({source_run.source_id for source_run in source_runs})
    source_definitions = (
        list(
            session.scalars(
                select(SourceDefinitionORM)
                .where(SourceDefinitionORM.source_id.in_(source_ids))
                .order_by(SourceDefinitionORM.source_id.asc())
            )
        )
        if source_ids
        else []
    )

    products = list(
        session.scalars(
            select(SituationProductORM)
            .where(SituationProductORM.event_id == event_id)
            .order_by(SituationProductORM.product_id.asc())
        )
    )
    products = filter_records_by_redaction_level(products, max_redaction_level)
    alerts = filter_relevant_alerts(session, event_id=event_id, observation_ids=observation_ids)
    geofence_ids = sorted({alert.geofence_id for alert in alerts if alert.geofence_id is not None})
    scheduled_tasks = (
        list(
            session.scalars(
                select(ScheduledTaskORM)
                .where(ScheduledTaskORM.source_id.in_(source_ids) if source_ids else False)
                .order_by(ScheduledTaskORM.task_id.asc())
            )
        )
        if source_ids
        else []
    )
    geofence_tasks = (
        list(
            session.scalars(
                select(ScheduledTaskORM)
                .where(ScheduledTaskORM.geofence_id.in_(geofence_ids) if geofence_ids else False)
                .order_by(ScheduledTaskORM.task_id.asc())
            )
        )
        if geofence_ids
        else []
    )
    scheduled_task_map = {task.task_id: task for task in [*scheduled_tasks, *geofence_tasks]}
    scheduled_tasks = list(sorted(scheduled_task_map.values(), key=lambda task: task.task_id))
    task_ids = [task.task_id for task in scheduled_tasks]
    scheduled_task_runs = (
        list(
            session.scalars(
                select(ScheduledTaskRunORM)
                .where(ScheduledTaskRunORM.task_id.in_(task_ids))
                .order_by(ScheduledTaskRunORM.task_run_id.asc())
            )
        )
        if task_ids
        else []
    )

    custody_logs = filter_relevant_custody_logs(
        session,
        event=event,
        observation_links=observation_links,
        observations=observations,
        entities=entities,
        entity_observation_links=entity_observation_links,
        alerts=alerts,
        import_runs=import_runs,
        source_definitions=source_definitions,
        source_runs=source_runs,
        scheduled_tasks=scheduled_tasks,
        scheduled_task_runs=scheduled_task_runs,
        geofence_ids=geofence_ids,
        products=products,
        export_log=log_bundle_export(
            session,
            event=event,
            exported_at=exported_at,
            observation_count=len(observations),
            product_count=len(products),
            entity_count=len(entities),
            max_redaction_level=max_redaction_level,
        ),
    )
    citations_json = flatten_citations(products)
    session.commit()

    return {
        "exported_at": exported_at,
        "event": event,
        "observation_links": observation_links,
        "observations": observations,
        "entities": entities,
        "entity_observation_links": entity_observation_links,
        "alerts": alerts,
        "import_runs": import_runs,
        "source_runs": source_runs,
        "source_definitions": source_definitions,
        "scheduled_tasks": scheduled_tasks,
        "scheduled_task_runs": scheduled_task_runs,
        "products": products,
        "custody_logs": custody_logs,
        "citations_json": citations_json,
        "requested_redaction_level": normalize_redaction_level(max_redaction_level)
        if max_redaction_level is not None
        else None,
    }


def filter_relevant_custody_logs(
    session: Session,
    *,
    event: EventORM,
    observation_links: list[EventObservationLinkORM],
    observations: list[ObservationORM],
    entities: list[EntityORM],
    entity_observation_links: list[EntityObservationLinkORM],
    alerts: list[AlertORM],
    import_runs: list[LocalImportRunORM],
    source_definitions: list[SourceDefinitionORM],
    source_runs: list[SourceRunORM],
    scheduled_tasks: list[ScheduledTaskORM],
    scheduled_task_runs: list[ScheduledTaskRunORM],
    geofence_ids: list[int],
    products: list[SituationProductORM],
    export_log: CustodyLogORM,
) -> list[CustodyLogORM]:
    relevant_pairs = {
        ("event", str(event.event_id)),
        ("event_fusion", str(event.event_id)),
        ("event_export", str(event.event_id)),
    }
    relevant_pairs.update(("entity", str(entity.entity_id)) for entity in entities)
    relevant_pairs.update(("entity_resolution", str(entity.entity_id)) for entity in entities)
    relevant_pairs.update(("alert", str(alert.alert_id)) for alert in alerts)
    relevant_pairs.update(
        ("observation", str(observation.observation_id)) for observation in observations
    )
    relevant_pairs.update(
        ("local_import_run", str(import_run.import_run_id)) for import_run in import_runs
    )
    relevant_pairs.update(
        ("source_definition", str(source_definition.source_id))
        for source_definition in source_definitions
    )
    relevant_pairs.update(
        ("source_run", str(source_run.source_run_id)) for source_run in source_runs
    )
    relevant_pairs.update(("scheduled_task", str(task.task_id)) for task in scheduled_tasks)
    relevant_pairs.update(
        ("scheduled_task_run", str(task_run.task_run_id)) for task_run in scheduled_task_runs
    )
    relevant_pairs.update(("geofence_scan", str(geofence_id)) for geofence_id in geofence_ids)
    relevant_pairs.update(("situation_product", str(product.product_id)) for product in products)
    relevant_pairs.update(
        ("event_observation_link", str(link.event_observation_link_id))
        for link in observation_links
    )
    relevant_pairs.update(
        ("entity_observation_link", str(link.entity_observation_link_id))
        for link in entity_observation_links
    )

    logs = list(session.scalars(select(CustodyLogORM).order_by(CustodyLogORM.created_at.asc())))
    filtered_logs = [log for log in logs if (log.object_type, log.object_id) in relevant_pairs]
    if export_log not in filtered_logs:
        filtered_logs.append(export_log)
    return filtered_logs


def filter_relevant_alerts(
    session: Session,
    *,
    event_id: int,
    observation_ids: list[int],
) -> list[AlertORM]:
    observation_id_set = set(observation_ids)
    rows = list(session.scalars(select(AlertORM).order_by(AlertORM.created_at.asc())))
    relevant: list[AlertORM] = []
    for alert in rows:
        trigger_observation_id = alert.trigger_basis_json.get("observation_id")
        if alert.event_id == event_id:
            relevant.append(alert)
            continue
        if trigger_observation_id in observation_id_set:
            relevant.append(alert)
    return relevant


def log_bundle_export(
    session: Session,
    *,
    event: EventORM,
    exported_at: datetime,
    observation_count: int,
    product_count: int,
    entity_count: int,
    max_redaction_level: str | None,
) -> CustodyLogORM:
    record = CustodyLogORM(
        object_type="event_export",
        object_id=str(event.event_id),
        action="bundle_exported",
        actor="exporter",
        details_json={
            "event_id": event.event_id,
            "exported_at": exported_at.isoformat(),
            "observation_count": observation_count,
            "product_count": product_count,
            "entity_count": entity_count,
            "requested_redaction_level": normalize_redaction_level(max_redaction_level)
            if max_redaction_level is not None
            else None,
        },
    )
    session.add(record)
    session.flush()
    return record


def flatten_citations(products: list[SituationProductORM]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    seen_keys: set[tuple[object, ...]] = set()
    for product in products:
        for citation in product.citations_json:
            key = (
                citation.get("observation_id"),
                citation.get("source_domain"),
                citation.get("layer_key"),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            rows.append(citation)
    return rows
