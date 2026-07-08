from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    CustodyLogORM,
    EntityORM,
    EntityObservationLinkORM,
    EventORM,
    EventObservationLinkORM,
    ObservationORM,
    SituationProductORM,
)
from src.schemas import EventFusionRequest
from src.services.entity_resolution_service import extract_entity_signals
from src.services.observation_service import (
    build_cross_verification_summaries,
    extract_observation_timestamp,
    is_ground_truth_observation,
    query_observations,
)


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
        related_entities = resolve_related_entities(session, linked_observations)
        provisional_mentions = build_provisional_entity_mentions(linked_observations)
        slug = build_event_slug(summary)
        event = session.scalar(select(EventORM).where(EventORM.slug == slug))
        created_new = event is None
        if event is None:
            event = EventORM(
                slug=slug,
                title=build_event_title(summary),
                summary=build_event_summary(summary, linked_observations, related_entities, provisional_mentions),
                occurred_at=summary["started_at"],
                redaction_level=request.redaction_level,
                metadata_json=build_event_metadata(summary, linked_observations, related_entities, provisional_mentions),
            )
            session.add(event)
            session.flush()
        else:
            event.redaction_level = request.redaction_level
            event.summary = build_event_summary(summary, linked_observations, related_entities, provisional_mentions)
            event.metadata_json = {
                **event.metadata_json,
                **build_event_metadata(summary, linked_observations, related_entities, provisional_mentions),
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
            related_entities,
            provisional_mentions,
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
    return (
        f"Fused Event near {round(float(centroid[1]), 3)}, "
        f"{round(float(centroid[0]), 3)}"
    )


def build_event_summary(
    summary: dict[str, object],
    observations: list[ObservationORM],
    related_entities: list[dict[str, object]],
    provisional_mentions: list[dict[str, object]],
) -> str:
    domains = sorted({observation.source_domain for observation in observations if observation.source_domain})
    layers = sorted({observation.layer_key for observation in observations})
    citations = ", ".join(domains) if domains else "uncited local imports"
    layer_phrase = ", ".join(layers)
    confidence_band = classify_verification_band(float(summary["verification_score"]))
    entity_phrase = summarize_entity_phrase(related_entities, provisional_mentions)
    return (
        f"{summary['observation_count']} observations across {summary['layer_count']} layers "
        f"and {summary['source_domain_count']} source domains corroborate a shared occurrence. "
        f"Verification score {summary['verification_score']:.2f} ({confidence_band} confidence). "
        f"Trusted observations: {summary['trusted_observation_count']}. "
        f"Integrity sources: {summary['integrity_source_count']}. "
        f"Ground truth hits: {summary['ground_truth_count']}. "
        f"Layers: {layer_phrase}. Sources: {citations}. {entity_phrase}"
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
            select(EventObservationLinkORM).where(EventObservationLinkORM.event_id == event.event_id)
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
    related_entities: list[dict[str, object]],
    provisional_mentions: list[dict[str, object]],
    redaction_level: str,
    actor: str = "event_fusion",
) -> int:
    cited_summary = build_cited_summary(event, observations, summary, related_entities, provisional_mentions)
    detailed_report = build_detailed_report(
        event,
        observations,
        summary,
        related_entities,
        provisional_mentions,
    )
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
        observed_at = extract_observation_timestamp(observation)
        rows.append(
            {
                "observation_id": observation.observation_id,
                "source_domain": observation.source_domain,
                "layer_key": observation.layer_key,
                "confidence_score": observation.confidence_score,
                "trust_level": observation.trust_level,
                "approval_policy": observation.approval_policy,
                "source_type": observation.source_type,
                "record_format": observation.record_format,
                "title": str(observation.content_json.get("title") or observation.content_text[:120]),
                "text_excerpt": observation.content_text[:240],
                "ground_truth": is_ground_truth_observation(observation),
                "integrity_source": observation.trust_level == "trusted",
                "observed_at": format_time(observed_at) if observed_at is not None else observation.content_json.get("observed_at"),
            }
        )
    return rows


def build_cited_summary(
    event: EventORM,
    observations: list[ObservationORM],
    summary: dict[str, object],
    related_entities: list[dict[str, object]],
    provisional_mentions: list[dict[str, object]],
) -> str:
    domains = [observation.source_domain or "local-import" for observation in observations]
    unique_domains = ", ".join(sorted(set(domains)))
    confidence_band = classify_verification_band(float(summary["verification_score"]))
    entity_phrase = summarize_entity_phrase(related_entities, provisional_mentions)
    time_phrase = (
        f"between {format_time(summary['started_at'])} and {format_time(summary['ended_at'])}"
        if summary["started_at"] != summary["ended_at"]
        else f"at approximately {format_time(summary['started_at'])}"
    )
    lines = [
        (
            f"{event.title} is supported by {summary['observation_count']} observations collected {time_phrase}. "
            f"Independent corroboration spans {summary['layer_count']} layers and {summary['source_domain_count']} domains, "
            f"with a rule-based verification score of {summary['verification_score']:.2f} and an overall {confidence_band} confidence assessment."
        ),
        (
            f"Trusted observations: {summary['trusted_observation_count']}. Integrity sources: {summary['integrity_source_count']}. "
            f"Ground-truth hits: {summary['ground_truth_count']}. Sources represented in this product: {unique_domains}."
        ),
    ]
    if entity_phrase:
        lines.append(entity_phrase)
    return "\n\n".join(lines)


def build_detailed_report(
    event: EventORM,
    observations: list[ObservationORM],
    summary: dict[str, object],
    related_entities: list[dict[str, object]],
    provisional_mentions: list[dict[str, object]],
) -> str:
    confidence_band = classify_verification_band(float(summary["verification_score"]))
    timeline_rows = build_timeline_rows(observations)
    source_rows = build_source_breakdown(observations)
    linked_entity_lines = build_linked_entity_lines(related_entities, provisional_mentions)
    collection_gaps = build_collection_gap_lines(summary, observations, related_entities)
    lines = [
        event.title,
        "",
        "Executive Assessment",
        build_cited_summary(event, observations, summary, related_entities, provisional_mentions),
        "",
        "Confidence Assessment",
        f"- Verification score: {summary['verification_score']:.2f}",
        f"- Confidence band: {confidence_band}",
        f"- Independent signals: {summary['independent_signal_count']}",
        f"- Trusted observations: {summary['trusted_observation_count']}",
        f"- Integrity sources: {summary['integrity_source_count']}",
        f"- Ground-truth hits: {summary['ground_truth_count']}",
        "",
        "Temporal And Geospatial Profile",
        f"- Started at: {format_time(summary['started_at'])}",
        f"- Ended at: {format_time(summary['ended_at'])}",
        (
            f"- Cluster centroid: lat {round(float(summary['centroid_geojson']['coordinates'][1]), 4)}, "
            f"lon {round(float(summary['centroid_geojson']['coordinates'][0]), 4)}"
        ),
        f"- Time span minutes: {summary['time_span_minutes']}",
        "",
        "Linked Entities",
    ]
    lines.extend(linked_entity_lines)
    lines.extend(["", "Source Reliability Breakdown"])
    lines.extend(source_rows)
    lines.extend(["", "Timeline"])
    lines.extend(timeline_rows)
    lines.extend(["", "Evidence Table"])
    for observation in sorted(observations, key=observation_timeline_key):
        observed_at = extract_observation_timestamp(observation) or observation.created_at
        lines.append(
            f"- Observation {observation.observation_id}: time={format_time(observed_at)}, "
            f"layer={observation.layer_key}, source={observation.source_domain or 'local-import'}, "
            f"trust={observation.trust_level}, confidence={observation.confidence_score:.2f}, "
            f"text={observation.content_text}"
        )
    lines.extend(["", "Collection Gaps And Follow-Up"])
    lines.extend(collection_gaps)
    lines.extend(
        [
            "",
            "Report Metadata",
            f"- Redaction level: {event.redaction_level}",
            f"- Generated at: {format_time(datetime.now(timezone.utc))}",
        ]
    )
    return "\n".join(lines)


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def resolve_related_entities(session: Session, observations: list[ObservationORM]) -> list[dict[str, object]]:
    observation_ids = [observation.observation_id for observation in observations]
    if not observation_ids:
        return []
    entity_links = list(
        session.scalars(
            select(EntityObservationLinkORM)
            .where(EntityObservationLinkORM.observation_id.in_(observation_ids))
            .order_by(EntityObservationLinkORM.entity_observation_link_id.asc())
        )
    )
    entity_ids = sorted({link.entity_id for link in entity_links})
    if not entity_ids:
        return []
    entities = list(
        session.scalars(
            select(EntityORM)
            .where(EntityORM.entity_id.in_(entity_ids))
            .order_by(EntityORM.confidence_score.desc(), EntityORM.entity_id.asc())
        )
    )
    links_by_entity: dict[int, list[EntityObservationLinkORM]] = defaultdict(list)
    for link in entity_links:
        links_by_entity[link.entity_id].append(link)
    rows: list[dict[str, object]] = []
    for entity in entities:
        signal_keys = entity.metadata_json.get("signal_keys") if isinstance(entity.metadata_json, dict) else []
        rows.append(
            {
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "canonical_name": entity.canonical_name,
                "confidence_score": entity.confidence_score,
                "observation_count": len(links_by_entity.get(entity.entity_id, [])),
                "signal_keys": signal_keys if isinstance(signal_keys, list) else [],
            }
        )
    return rows


def build_provisional_entity_mentions(observations: list[ObservationORM]) -> list[dict[str, object]]:
    mention_support: dict[tuple[str, str], dict[str, object]] = {}
    for observation in observations:
        grouped = defaultdict(set)
        for signal in extract_entity_signals(observation):
            grouped[(signal.entity_type, signal.normalized_value)].add(signal)
        for (entity_type, normalized_value), signals in grouped.items():
            display_signal = sorted(
                signals,
                key=lambda item: (
                    item.signal_key not in {"vessel_name", "full_name", "organization"},
                    item.raw_value.lower(),
                ),
            )[0]
            entry = mention_support.setdefault(
                (entity_type, normalized_value),
                {
                    "entity_type": entity_type,
                    "canonical_name": display_signal.raw_value,
                    "observation_ids": set(),
                    "signal_keys": set(),
                },
            )
            entry["observation_ids"].add(observation.observation_id)
            entry["signal_keys"].update(signal.signal_key for signal in signals)
    rows = [
        {
            "entity_type": row["entity_type"],
            "canonical_name": row["canonical_name"],
            "observation_count": len(row["observation_ids"]),
            "signal_keys": sorted(row["signal_keys"]),
        }
        for row in mention_support.values()
        if len(row["observation_ids"]) >= 2
    ]
    return sorted(rows, key=lambda row: (-int(row["observation_count"]), row["entity_type"], row["canonical_name"].lower()))[:5]


def build_event_metadata(
    summary: dict[str, object],
    observations: list[ObservationORM],
    related_entities: list[dict[str, object]],
    provisional_mentions: list[dict[str, object]],
) -> dict[str, object]:
    domains = [observation.source_domain or "local-import" for observation in observations]
    domain_counts = Counter(domains)
    layer_counts = Counter(observation.layer_key for observation in observations)
    trust_counts = Counter(observation.trust_level for observation in observations)
    return {
        "fusion_cluster_id": summary["cluster_id"],
        "verification_score": summary["verification_score"],
        "observation_count": summary["observation_count"],
        "centroid_geojson": summary["centroid_geojson"],
        "confidence_band": classify_verification_band(float(summary["verification_score"])),
        "source_domain_counts": [
            {"key": key, "count": count}
            for key, count in sorted(domain_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        "layer_counts": [
            {"key": key, "count": count}
            for key, count in sorted(layer_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        "trust_level_counts": [
            {"key": key, "count": count}
            for key, count in sorted(trust_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        "linked_entity_count": len(related_entities),
        "linked_entity_types": sorted({row["entity_type"] for row in related_entities}),
        "provisional_entity_mentions": provisional_mentions,
    }


def summarize_entity_phrase(
    related_entities: list[dict[str, object]],
    provisional_mentions: list[dict[str, object]],
) -> str:
    if related_entities:
        top = ", ".join(row["canonical_name"] for row in related_entities[:3])
        return f"Linked entities in the current evidence set include {top}."
    if provisional_mentions:
        top = ", ".join(row["canonical_name"] for row in provisional_mentions[:3])
        return f"Repeated named signals in the current evidence set include {top}."
    return ""


def build_linked_entity_lines(
    related_entities: list[dict[str, object]],
    provisional_mentions: list[dict[str, object]],
) -> list[str]:
    if related_entities:
        return [
            (
                f"- {row['canonical_name']} ({row['entity_type']}): confidence={row['confidence_score']:.2f}, "
                f"observations={row['observation_count']}, signals={', '.join(row['signal_keys']) or 'unknown'}"
            )
            for row in related_entities
        ]
    if provisional_mentions:
        return [
            (
                f"- {row['canonical_name']} ({row['entity_type']}): repeated across {row['observation_count']} observations, "
                f"signals={', '.join(row['signal_keys'])}"
            )
            for row in provisional_mentions
        ]
    return ["- No resolved entities are currently linked to this event."]


def build_source_breakdown(observations: list[ObservationORM]) -> list[str]:
    grouped: dict[str, list[ObservationORM]] = defaultdict(list)
    for observation in observations:
        grouped[observation.source_domain or "local-import"].append(observation)
    rows = []
    for domain, domain_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        trusted_count = sum(1 for row in domain_rows if row.trust_level == "trusted")
        ground_truth_count = sum(1 for row in domain_rows if is_ground_truth_observation(row))
        avg_confidence = sum(row.confidence_score for row in domain_rows) / len(domain_rows)
        rows.append(
            f"- {domain}: observations={len(domain_rows)}, trusted={trusted_count}, ground_truth={ground_truth_count}, avg_confidence={avg_confidence:.2f}"
        )
    return rows


def build_timeline_rows(observations: list[ObservationORM]) -> list[str]:
    rows = []
    for observation in sorted(observations, key=observation_timeline_key):
        observed_at = extract_observation_timestamp(observation) or observation.created_at
        title = str(observation.content_json.get("title") or observation.content_text[:120])
        rows.append(
            f"- {format_time(observed_at)} | observation {observation.observation_id} | {observation.layer_key} | {observation.source_domain or 'local-import'} | {title}"
        )
    return rows


def build_collection_gap_lines(
    summary: dict[str, object],
    observations: list[ObservationORM],
    related_entities: list[dict[str, object]],
) -> list[str]:
    rows: list[str] = []
    if int(summary["integrity_source_count"]) == 0:
        rows.append("- No integrity source is present in the corroborating set; acquisition from a trusted source would materially improve confidence.")
    if int(summary["ground_truth_count"]) == 0:
        rows.append("- No ground-truth observation is present; the event still lacks direct confirmation from on-scene or sensor-verifiable evidence.")
    if len({observation.source_domain for observation in observations if observation.source_domain}) < 2:
        rows.append("- Cross-domain diversity is thin; additional independent domains should be collected to reduce single-source bias.")
    if not related_entities:
        rows.append("- No persisted entity is linked yet; running entity resolution over the relevant area/time window would improve contextual depth.")
    if not rows:
        rows.append("- Immediate evidence gaps are limited; the next priority is persistence monitoring for follow-on observations and alert drift.")
    return rows


def classify_verification_band(score: float) -> str:
    if score >= 0.9:
        return "high"
    if score >= 0.75:
        return "moderate"
    return "low"


def observation_timeline_key(observation: ObservationORM) -> tuple[datetime, int]:
    observed_at = extract_observation_timestamp(observation) or observation.created_at
    return (observed_at, observation.observation_id)
