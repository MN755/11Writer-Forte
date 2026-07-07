from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    CustodyLogORM,
    EntityORM,
    EntityObservationLinkORM,
    EventORM,
    EventObservationLinkORM,
    LocalImportRunORM,
    ObservationORM,
    SituationProductORM,
    SourceDefinitionORM,
    SourceRunORM,
)


def export_now() -> datetime:
    return datetime.now(timezone.utc)


def build_event_export_bundle(session: Session, event_id: int) -> dict[str, object]:
    event = session.get(EventORM, event_id)
    if event is None:
        raise ValueError(f"Event {event_id} does not exist.")
    exported_at = export_now()

    observation_links = list(
        session.scalars(
            select(EventObservationLinkORM)
            .where(EventObservationLinkORM.event_id == event_id)
            .order_by(EventObservationLinkORM.event_observation_link_id.asc())
        )
    )
    observation_ids = [link.observation_id for link in observation_links]
    observations = list(
        session.scalars(
            select(ObservationORM)
            .where(ObservationORM.observation_id.in_(observation_ids))
            .order_by(ObservationORM.observation_id.asc())
        )
    ) if observation_ids else []
    entity_observation_links = list(
        session.scalars(
            select(EntityObservationLinkORM)
            .where(EntityObservationLinkORM.observation_id.in_(observation_ids))
            .order_by(EntityObservationLinkORM.entity_observation_link_id.asc())
        )
    ) if observation_ids else []
    entity_ids = sorted({link.entity_id for link in entity_observation_links})
    entities = list(
        session.scalars(
            select(EntityORM)
            .where(EntityORM.entity_id.in_(entity_ids))
            .order_by(EntityORM.entity_id.asc())
        )
    ) if entity_ids else []

    import_run_ids = sorted(
        {
            observation.import_run_id
            for observation in observations
            if observation.import_run_id is not None
        }
    )
    import_runs = list(
        session.scalars(
            select(LocalImportRunORM)
            .where(LocalImportRunORM.import_run_id.in_(import_run_ids))
            .order_by(LocalImportRunORM.import_run_id.asc())
        )
    ) if import_run_ids else []

    source_runs = list(
        session.scalars(
            select(SourceRunORM)
            .where(SourceRunORM.import_run_id.in_(import_run_ids))
            .order_by(SourceRunORM.source_run_id.asc())
        )
    ) if import_run_ids else []
    source_ids = sorted({source_run.source_id for source_run in source_runs})
    source_definitions = list(
        session.scalars(
            select(SourceDefinitionORM)
            .where(SourceDefinitionORM.source_id.in_(source_ids))
            .order_by(SourceDefinitionORM.source_id.asc())
        )
    ) if source_ids else []

    products = list(
        session.scalars(
            select(SituationProductORM)
            .where(SituationProductORM.event_id == event_id)
            .order_by(SituationProductORM.product_id.asc())
        )
    )

    custody_logs = filter_relevant_custody_logs(
        session,
        event=event,
        observation_links=observation_links,
        observations=observations,
        entities=entities,
        entity_observation_links=entity_observation_links,
        import_runs=import_runs,
        source_definitions=source_definitions,
        products=products,
        export_log=log_bundle_export(
            session,
            event=event,
            exported_at=exported_at,
            observation_count=len(observations),
            product_count=len(products),
            entity_count=len(entities),
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
        "import_runs": import_runs,
        "source_runs": source_runs,
        "source_definitions": source_definitions,
        "products": products,
        "custody_logs": custody_logs,
        "citations_json": citations_json,
    }


def filter_relevant_custody_logs(
    session: Session,
    *,
    event: EventORM,
    observation_links: list[EventObservationLinkORM],
    observations: list[ObservationORM],
    entities: list[EntityORM],
    entity_observation_links: list[EntityObservationLinkORM],
    import_runs: list[LocalImportRunORM],
    source_definitions: list[SourceDefinitionORM],
    products: list[SituationProductORM],
    export_log: CustodyLogORM,
) -> list[CustodyLogORM]:
    relevant_pairs = {
        ("event", str(event.event_id)),
        ("event_fusion", str(event.event_id)),
        ("event_export", str(event.event_id)),
    }
    relevant_pairs.update(
        ("entity", str(entity.entity_id))
        for entity in entities
    )
    relevant_pairs.update(
        ("entity_resolution", str(entity.entity_id))
        for entity in entities
    )
    relevant_pairs.update(
        ("observation", str(observation.observation_id))
        for observation in observations
    )
    relevant_pairs.update(
        ("local_import_run", str(import_run.import_run_id))
        for import_run in import_runs
    )
    relevant_pairs.update(
        ("source_definition", str(source_definition.source_id))
        for source_definition in source_definitions
    )
    relevant_pairs.update(
        ("situation_product", str(product.product_id))
        for product in products
    )
    relevant_pairs.update(
        ("event_observation_link", str(link.event_observation_link_id))
        for link in observation_links
    )
    relevant_pairs.update(
        ("entity_observation_link", str(link.entity_observation_link_id))
        for link in entity_observation_links
    )

    logs = list(session.scalars(select(CustodyLogORM).order_by(CustodyLogORM.created_at.asc())))
    filtered_logs = [
        log for log in logs
        if (log.object_type, log.object_id) in relevant_pairs
    ]
    if export_log not in filtered_logs:
        filtered_logs.append(export_log)
    return filtered_logs


def log_bundle_export(
    session: Session,
    *,
    event: EventORM,
    exported_at: datetime,
    observation_count: int,
    product_count: int,
    entity_count: int,
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
