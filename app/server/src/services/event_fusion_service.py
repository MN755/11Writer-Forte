from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    CustodyLogORM,
    EventORM,
    EventObservationLinkORM,
    ObservationORM,
    SituationProductORM,
)
from src.schemas import EventFusionRequest
from src.services.observation_service import build_cross_verification_summaries, query_observations


@dataclass
class MaterializedEvent:
    event: EventORM
    observation_count: int
    product_count: int
    verification_score: float
    created_new: bool


def materialize_fused_events(
    session: Session,
    request: EventFusionRequest,
    actor: str = "event_fusion",
) -> list[MaterializedEvent]:
    observations = query_observations(
        session,
        layer_key=request.layer_key,
        source_domain=request.source_domain,
        trust_level=request.trust_level,
        min_lon=request.min_lon,
        min_lat=request.min_lat,
        max_lon=request.max_lon,
        max_lat=request.max_lat,
        since=request.since,
        until=request.until,
        limit=request.limit,
    )
    observation_map = {observation.observation_id: observation for observation in observations}
    summaries = build_cross_verification_summaries(
        session,
        observations,
        time_window_minutes=request.time_window_minutes,
        distance_km=request.distance_km,
        min_independent_signals=request.min_independent_signals,
    )

    results: list[MaterializedEvent] = []
    for summary in summaries:
        linked_observations = [
            observation_map[observation_id]
            for observation_id in summary["observation_ids"]
            if observation_id in observation_map
        ]
        if not linked_observations:
            continue
        slug = build_event_slug(summary)
        event = session.scalar(select(EventORM).where(EventORM.slug == slug))
        created_new = event is None
        if event is None:
            event = EventORM(
                slug=slug,
                title=build_event_title(summary),
                summary=build_event_summary(summary, linked_observations),
                occurred_at=summary["started_at"],
                redaction_level=request.redaction_level,
                metadata_json={
                    "fusion_cluster_id": summary["cluster_id"],
                    "verification_score": summary["verification_score"],
                    "observation_count": summary["observation_count"],
                    "centroid_geojson": summary["centroid_geojson"],
                },
            )
            session.add(event)
            session.flush()
        else:
            event.redaction_level = request.redaction_level
            event.summary = build_event_summary(summary, linked_observations)
            event.metadata_json = {
                **event.metadata_json,
                "fusion_cluster_id": summary["cluster_id"],
                "verification_score": summary["verification_score"],
                "observation_count": summary["observation_count"],
                "centroid_geojson": summary["centroid_geojson"],
            }

        log_event_fusion(
            session,
            event,
            linked_observations,
            summary,
            request.redaction_level,
            created_new,
            actor=actor,
        )
        ensure_event_links(
            session,
            event,
            linked_observations,
            summary["verification_score"],
            actor=actor,
        )
        product_count = ensure_situation_products(
            session,
            event,
            linked_observations,
            summary,
            request.redaction_level,
            actor=actor,
        )
        results.append(
            MaterializedEvent(
                event=event,
                observation_count=len(linked_observations),
                product_count=product_count,
                verification_score=summary["verification_score"],
                created_new=created_new,
            )
        )
        if created_new:
            session.flush()

    session.commit()
    for result in results:
        session.refresh(result.event)
    return results


def build_event_slug(summary: dict[str, object]) -> str:
    centroid = summary["centroid_geojson"]["coordinates"]
    started_at = summary["started_at"]
    basis = (
        f"{round(float(centroid[0]), 3)}:"
        f"{round(float(centroid[1]), 3)}:"
        f"{started_at.isoformat()}:"
        f"{summary['observation_count']}"
    )
    return f"fusion-{sha1(basis.encode('utf-8')).hexdigest()[:12]}"


def build_event_title(summary: dict[str, object]) -> str:
    centroid = summary["centroid_geojson"]["coordinates"]
    return f"Fused Event near {round(float(centroid[1]), 3)}, {round(float(centroid[0]), 3)}"


def build_event_summary(summary: dict[str, object], observations: list[ObservationORM]) -> str:
    domains = sorted(
        {observation.source_domain for observation in observations if observation.source_domain}
    )
    layers = sorted({observation.layer_key for observation in observations})
    citations = ", ".join(domains) if domains else "uncited local imports"
    layer_phrase = ", ".join(layers)
    return (
        f"{summary['observation_count']} observations across {summary['layer_count']} layers "
        f"and {summary['source_domain_count']} source domains corroborate a shared occurrence. "
        f"Verification score {summary['verification_score']:.2f}. "
        f"Trusted observations: {summary['trusted_observation_count']}. "
        f"Integrity sources: {summary['integrity_source_count']}. "
        f"Ground truth hits: {summary['ground_truth_count']}. "
        f"Layers: {layer_phrase}. Sources: {citations}."
    )


def ensure_event_links(
    session: Session,
    event: EventORM,
    observations: list[ObservationORM],
    verification_score: float,
    actor: str = "event_fusion",
) -> None:
    existing_ids = {
        link.observation_id
        for link in session.scalars(
            select(EventObservationLinkORM).where(
                EventObservationLinkORM.event_id == event.event_id
            )
        )
    }
    for observation in observations:
        if observation.observation_id in existing_ids:
            continue
        link = EventObservationLinkORM(
            event_id=event.event_id,
            observation_id=observation.observation_id,
            relationship_type="supporting",
            confidence_contribution=verification_score,
        )
        session.add(link)
        session.flush()
        session.add(
            CustodyLogORM(
                object_type="event_observation_link",
                object_id=str(link.event_observation_link_id),
                action="link_created",
                actor=actor,
                details_json={
                    "event_id": event.event_id,
                    "observation_id": observation.observation_id,
                    "relationship_type": link.relationship_type,
                    "confidence_contribution": verification_score,
                },
            )
        )
        existing_ids.add(observation.observation_id)


def ensure_situation_products(
    session: Session,
    event: EventORM,
    observations: list[ObservationORM],
    summary: dict[str, object],
    redaction_level: str,
    actor: str = "event_fusion",
) -> int:
    cited_summary = build_cited_summary(event, observations, summary)
    detailed_report = build_detailed_report(event, observations, summary)
    citations = build_citations(observations)
    specs = [
        ("cited_summary", cited_summary),
        ("report", detailed_report),
    ]
    for product_type, body_text in specs:
        product = session.scalar(
            select(SituationProductORM).where(
                SituationProductORM.event_id == event.event_id,
                SituationProductORM.product_type == product_type,
            )
        )
        title = f"{event.title} {product_type.replace('_', ' ').title()}"
        if product is None:
            product = SituationProductORM(
                event_id=event.event_id,
                product_type=product_type,
                redaction_level=redaction_level,
                title=title,
                body_text=body_text,
                citations_json=citations,
                generated_by="rule_based",
            )
            session.add(product)
            session.flush()
            action = "product_generated"
        else:
            product.redaction_level = redaction_level
            product.title = title
            product.body_text = body_text
            product.citations_json = citations
            action = "product_regenerated"
        session.add(
            CustodyLogORM(
                object_type="situation_product",
                object_id=str(product.product_id),
                action=action,
                actor=actor,
                details_json={
                    "event_id": event.event_id,
                    "product_type": product_type,
                    "redaction_level": redaction_level,
                    "citation_count": len(citations),
                    "observation_count": len(observations),
                },
            )
        )
    return len(specs)


def log_event_fusion(
    session: Session,
    event: EventORM,
    observations: list[ObservationORM],
    summary: dict[str, object],
    redaction_level: str,
    created_new: bool,
    actor: str,
) -> None:
    observation_ids = [observation.observation_id for observation in observations]
    details = {
        "fusion_cluster_id": summary["cluster_id"],
        "verification_score": summary["verification_score"],
        "observation_ids": observation_ids,
        "observation_count": len(observations),
        "redaction_level": redaction_level,
    }
    session.add(
        CustodyLogORM(
            object_type="event",
            object_id=str(event.event_id),
            action="event_created_from_fusion" if created_new else "event_updated_from_fusion",
            actor=actor,
            details_json=details,
        )
    )
    session.add(
        CustodyLogORM(
            object_type="event_fusion",
            object_id=str(event.event_id),
            action="fusion_materialized",
            actor=actor,
            details_json=details,
        )
    )


def build_citations(observations: list[ObservationORM]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for observation in observations:
        rows.append(
            {
                "observation_id": observation.observation_id,
                "source_domain": observation.source_domain,
                "layer_key": observation.layer_key,
                "confidence_score": observation.confidence_score,
                "trust_level": observation.trust_level,
                "approval_policy": observation.approval_policy,
                "observed_at": observation.content_json.get("observed_at"),
            }
        )
    return rows


def build_cited_summary(
    event: EventORM,
    observations: list[ObservationORM],
    summary: dict[str, object],
) -> str:
    domains = [observation.source_domain or "local-import" for observation in observations]
    unique_domains = ", ".join(sorted(set(domains)))
    return (
        f"{event.title} is supported by {summary['observation_count']} observations collected "
        f"between {format_time(summary['started_at'])} and {format_time(summary['ended_at'])}. "
        f"Independent corroboration spans {summary['layer_count']} layers and "
        f"{summary['source_domain_count']} domains, with a rule-based verification score of "
        f"{summary['verification_score']:.2f}. Trusted observations: {summary['trusted_observation_count']}. "
        f"Integrity sources: {summary['integrity_source_count']}. Ground-truth hits: "
        f"{summary['ground_truth_count']}. Sources represented in this product: {unique_domains}."
    )


def build_detailed_report(
    event: EventORM,
    observations: list[ObservationORM],
    summary: dict[str, object],
) -> str:
    lines = [
        event.title,
        "",
        "Situation Overview",
        build_cited_summary(event, observations, summary),
        "",
        "Evidence Table",
    ]
    for observation in observations:
        lines.append(
            f"- Observation {observation.observation_id}: layer={observation.layer_key}, "
            f"source={observation.source_domain or 'local-import'}, "
            f"confidence={observation.confidence_score:.2f}, "
            f"text={observation.content_text}"
        )
    centroid = summary["centroid_geojson"]["coordinates"]
    lines.extend(
        [
            "",
            "Fusion Notes",
            (
                f"- Cluster centroid: lat {round(float(centroid[1]), 4)}, "
                f"lon {round(float(centroid[0]), 4)}"
            ),
            f"- Time span minutes: {summary['time_span_minutes']}",
            f"- Independent signals: {summary['independent_signal_count']}",
            f"- Trusted observations: {summary['trusted_observation_count']}",
            f"- Integrity sources: {summary['integrity_source_count']}",
            f"- Ground-truth hits: {summary['ground_truth_count']}",
            f"- Redaction level: {event.redaction_level}",
            f"- Generated at: {format_time(datetime.now(timezone.utc))}",
        ]
    )
    return "\n".join(lines)


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
