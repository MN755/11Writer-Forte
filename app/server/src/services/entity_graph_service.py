from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.models import (
    CustodyLogORM,
    EntityAliasCitationLinkORM,
    EntityAliasORM,
    EntityCandidateCitationLinkORM,
    EntityCandidateORM,
    EntityCitationORM,
    EntityIdentityAssertionCitationLinkORM,
    EntityIdentityAssertionORM,
    EntityORM,
    EntityRelationshipAssertionCitationLinkORM,
    EntityRelationshipAssertionORM,
    EntityRelationshipTypeORM,
    ObservationORM,
    StorageObjectORM,
)
from src.schemas import (
    EntityAliasCreate,
    EntityCandidateCreate,
    EntityCitationCreate,
    EntityIdentityAssertionCreate,
    EntityRelationshipAssertionCreate,
    EntityRelationshipTypeCreate,
)
from src.services.redaction_service import is_visible_at_level


def create_citation(session: Session, payload: EntityCitationCreate, *, actor: str = "api_entity_graph") -> EntityCitationORM:
    if payload.storage_object_id is not None and session.get(StorageObjectORM, payload.storage_object_id) is None:
        raise ValueError(f"Storage object {payload.storage_object_id} does not exist.")
    if payload.observation_id is not None and session.get(ObservationORM, payload.observation_id) is None:
        raise ValueError(f"Observation {payload.observation_id} does not exist.")
    record = EntityCitationORM(**payload.model_dump())
    session.add(record)
    session.flush()
    log_graph_change(session, "entity_citation", record.entity_citation_id, "citation_created", actor)
    session.commit()
    session.refresh(record)
    return record


def create_relationship_type(
    session: Session, payload: EntityRelationshipTypeCreate, *, actor: str = "api_entity_graph"
) -> EntityRelationshipTypeORM:
    existing = session.scalar(
        select(EntityRelationshipTypeORM).where(
            EntityRelationshipTypeORM.relation_type == payload.relation_type,
            EntityRelationshipTypeORM.relation_version == payload.relation_version,
        )
    )
    if existing is not None:
        raise ValueError("Relationship type and version already exist.")
    record = EntityRelationshipTypeORM(**payload.model_dump())
    session.add(record)
    session.flush()
    log_graph_change(session, "entity_relationship_type", record.relationship_type_id, "relationship_type_created", actor)
    session.commit()
    session.refresh(record)
    return record


def create_candidate(
    session: Session, payload: EntityCandidateCreate, *, actor: str = "api_entity_graph"
) -> EntityCandidateORM:
    citations = require_citations(session, payload.citation_ids)
    if payload.resolved_entity_id is not None and session.get(EntityORM, payload.resolved_entity_id) is None:
        raise ValueError(f"Entity {payload.resolved_entity_id} does not exist.")
    values = payload.model_dump(exclude={"citation_ids"})
    record = EntityCandidateORM(**values)
    session.add(record)
    session.flush()
    for citation in citations:
        session.add(EntityCandidateCitationLinkORM(entity_candidate_id=record.entity_candidate_id, entity_citation_id=citation.entity_citation_id))
    log_graph_change(session, "entity_candidate", record.entity_candidate_id, "candidate_created", actor)
    session.commit()
    session.refresh(record)
    return record


def create_alias(session: Session, payload: EntityAliasCreate, *, actor: str = "api_entity_graph") -> EntityAliasORM:
    entity = require_entity(session, payload.entity_id)
    if entity.entity_type != payload.entity_type:
        raise ValueError("Alias entity_type must match the canonical entity type.")
    citations = require_citations(session, payload.citation_ids)
    record = EntityAliasORM(**payload.model_dump(exclude={"citation_ids"}))
    session.add(record)
    session.flush()
    for citation in citations:
        session.add(EntityAliasCitationLinkORM(entity_alias_id=record.entity_alias_id, entity_citation_id=citation.entity_citation_id))
    log_graph_change(session, "entity_alias", record.entity_alias_id, "alias_created", actor)
    session.commit()
    session.refresh(record)
    return record


def create_identity_assertion(
    session: Session, payload: EntityIdentityAssertionCreate, *, actor: str = "api_entity_graph"
) -> EntityIdentityAssertionORM:
    candidate = session.get(EntityCandidateORM, payload.entity_candidate_id)
    if candidate is None:
        raise ValueError(f"Entity candidate {payload.entity_candidate_id} does not exist.")
    require_entity(session, payload.entity_id)
    citations = require_citations(session, payload.citation_ids)
    record = EntityIdentityAssertionORM(**payload.model_dump(exclude={"citation_ids"}))
    session.add(record)
    session.flush()
    for citation in citations:
        session.add(EntityIdentityAssertionCitationLinkORM(entity_identity_assertion_id=record.entity_identity_assertion_id, entity_citation_id=citation.entity_citation_id))
    log_graph_change(session, "entity_identity_assertion", record.entity_identity_assertion_id, "identity_assertion_created", actor)
    session.commit()
    session.refresh(record)
    return record


def create_relationship_assertion(
    session: Session, payload: EntityRelationshipAssertionCreate, *, actor: str = "api_entity_graph"
) -> EntityRelationshipAssertionORM:
    subject = require_entity(session, payload.subject_entity_id)
    object_ = require_entity(session, payload.object_entity_id)
    definition = session.scalar(
        select(EntityRelationshipTypeORM).where(
            EntityRelationshipTypeORM.relation_type == payload.relation_type,
            EntityRelationshipTypeORM.relation_version == payload.relation_version,
            EntityRelationshipTypeORM.active.is_(True),
        )
    )
    if definition is None:
        raise ValueError("Relationship type/version is not registered and active.")
    if subject.entity_type not in definition.allowed_subject_types_json:
        raise ValueError("Subject entity type is not allowed by the relationship definition.")
    if object_.entity_type not in definition.allowed_object_types_json:
        raise ValueError("Object entity type is not allowed by the relationship definition.")
    citations = require_citations(session, payload.citation_ids)
    record = EntityRelationshipAssertionORM(**payload.model_dump(exclude={"citation_ids"}))
    session.add(record)
    session.flush()
    for citation in citations:
        session.add(
            EntityRelationshipAssertionCitationLinkORM(
                entity_relationship_assertion_id=record.entity_relationship_assertion_id,
                entity_citation_id=citation.entity_citation_id,
            )
        )
    promote_cited_artifacts(session, citations, record.entity_relationship_assertion_id, actor)
    log_graph_change(session, "entity_relationship_assertion", record.entity_relationship_assertion_id, "relationship_assertion_created", actor)
    session.commit()
    session.refresh(record)
    return record


def get_entity_profile(session: Session, entity_id: int, *, max_redaction_level: str = "public") -> dict[str, Any]:
    entity = require_entity(session, entity_id)
    if not is_visible_at_level(entity.redaction_level, max_redaction_level):
        raise ValueError("Entity is not visible at the requested clearance.")
    aliases = list(session.scalars(select(EntityAliasORM).where(EntityAliasORM.entity_id == entity_id)))
    assertions = relationship_assertions_for_entities(session, {entity_id})
    return {
        "entity": entity,
        "aliases": [serialize_alias(session, row) for row in aliases],
        "relationships": [serialize_relationship(session, row) for row in assertions],
        "evidence_count": sum(len(row.citation_links) for row in assertions),
        "provenance_links": sorted({citation.source_uri for row in assertions for citation in relationship_citations(session, row) if citation.source_uri}),
    }


def query_network_slice(
    session: Session,
    *,
    entity_id: int,
    max_depth: int = 2,
    jurisdiction: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    min_confidence: float = 0.0,
    source_reliability: set[str] | None = None,
    max_redaction_level: str = "public",
) -> dict[str, Any]:
    root = require_entity(session, entity_id)
    if not is_visible_at_level(root.redaction_level, max_redaction_level):
        raise ValueError("Entity is not visible at the requested clearance.")
    all_assertions = list(session.scalars(select(EntityRelationshipAssertionORM)))
    eligible = [
        row for row in all_assertions
        if assertion_matches(row, jurisdiction, since, until, min_confidence)
        and relationship_citations_match(session, row, source_reliability)
        and is_visible_at_level(require_entity(session, row.subject_entity_id).redaction_level, max_redaction_level)
        and is_visible_at_level(require_entity(session, row.object_entity_id).redaction_level, max_redaction_level)
    ]
    adjacency: dict[int, list[EntityRelationshipAssertionORM]] = defaultdict(list)
    for row in eligible:
        adjacency[row.subject_entity_id].append(row)
        adjacency[row.object_entity_id].append(row)
    visited = {entity_id}
    frontier: deque[tuple[int, int]] = deque([(entity_id, 0)])
    assertion_ids: set[int] = set()
    while frontier:
        node, depth = frontier.popleft()
        if depth >= max(0, min(max_depth, 6)):
            continue
        for row in adjacency[node]:
            assertion_ids.add(row.entity_relationship_assertion_id)
            neighbor = row.object_entity_id if row.subject_entity_id == node else row.subject_entity_id
            if neighbor not in visited:
                visited.add(neighbor)
                frontier.append((neighbor, depth + 1))
    visible = [row for row in eligible if row.entity_relationship_assertion_id in assertion_ids]
    evidence_count = sum(len(relationship_citations(session, row)) for row in visible)
    if not visible or evidence_count == 0:
        return insufficient_evidence_result(entity_id, jurisdiction, since, until, max_depth)
    entities = [require_entity(session, node) for node in sorted(visited)]
    serialized_relationships = [serialize_relationship(session, row) for row in visible]
    analysis = build_explainable_analysis(entities, visible, serialized_relationships)
    return {
        "status": "ok",
        "root_entity_id": entity_id,
        "entities": entities,
        "relationships": serialized_relationships,
        "evidence_count": evidence_count,
        "provenance_links": sorted({citation.source_uri for row in visible for citation in relationship_citations(session, row) if citation.source_uri}),
        "coverage": {"max_depth": max_depth, "jurisdiction": jurisdiction, "since": since, "until": until, "assertion_count": len(visible)},
        "analysis": analysis,
    }


def relationship_assertions_for_entities(session: Session, entity_ids: set[int]) -> list[EntityRelationshipAssertionORM]:
    return [row for row in session.scalars(select(EntityRelationshipAssertionORM)) if row.subject_entity_id in entity_ids or row.object_entity_id in entity_ids]


def serialize_relationship(session: Session, row: EntityRelationshipAssertionORM) -> dict[str, Any]:
    citations = relationship_citations(session, row)
    return {"assertion": row, "citations": citations, "evidence_count": len(citations), "provenance_links": [citation.source_uri for citation in citations if citation.source_uri]}


def serialize_alias(session: Session, row: EntityAliasORM) -> dict[str, Any]:
    citations = list(session.scalars(select(EntityCitationORM).join(EntityAliasCitationLinkORM).where(EntityAliasCitationLinkORM.entity_alias_id == row.entity_alias_id)))
    return {"alias": row, "citations": citations, "evidence_count": len(citations)}


def relationship_citations(session: Session, row: EntityRelationshipAssertionORM) -> list[EntityCitationORM]:
    return list(session.scalars(select(EntityCitationORM).join(EntityRelationshipAssertionCitationLinkORM).where(EntityRelationshipAssertionCitationLinkORM.entity_relationship_assertion_id == row.entity_relationship_assertion_id)))


def relationship_citations_match(session: Session, row: EntityRelationshipAssertionORM, allowed: set[str] | None) -> bool:
    citations = relationship_citations(session, row)
    return bool(citations) and (allowed is None or any(citation.source_reliability in allowed for citation in citations))


def assertion_matches(row: EntityRelationshipAssertionORM, jurisdiction: str | None, since: datetime | None, until: datetime | None, min_confidence: float) -> bool:
    if row.confidence_score < min_confidence or (jurisdiction and row.jurisdiction != jurisdiction):
        return False
    if since and row.valid_to and row.valid_to < since:
        return False
    return not (until and row.valid_from and row.valid_from > until)


def insufficient_evidence_result(entity_id: int, jurisdiction: str | None, since: datetime | None, until: datetime | None, max_depth: int) -> dict[str, Any]:
    return {"status": "insufficient_evidence", "root_entity_id": entity_id, "entities": [], "relationships": [], "evidence_count": 0, "provenance_links": [], "coverage": {"max_depth": max_depth, "jurisdiction": jurisdiction, "since": since, "until": until}, "reason": "No cited, policy-eligible relationships met the requested coverage."}


def build_explainable_analysis(
    entities: list[EntityORM],
    assertions: list[EntityRelationshipAssertionORM],
    serialized: list[dict[str, Any]],
) -> dict[str, Any]:
    """Deterministic, inspectable ranking and views; no label is inferred from a score."""
    degrees: dict[int, int] = defaultdict(int)
    confidence: dict[int, float] = defaultdict(float)
    sources: dict[int, set[str]] = defaultdict(set)
    clusters: dict[str, set[int]] = defaultdict(set)
    timeline: list[dict[str, Any]] = []
    for assertion, display in zip(assertions, serialized):
        for entity_id in (assertion.subject_entity_id, assertion.object_entity_id):
            degrees[entity_id] += 1
            confidence[entity_id] += assertion.confidence_score
            sources[entity_id].update(
                citation.source_uri for citation in display["citations"] if citation.source_uri
            )
            clusters[assertion.jurisdiction or "unscoped"].add(entity_id)
        timeline.append({
            "relationship_assertion_id": assertion.entity_relationship_assertion_id,
            "valid_from": assertion.valid_from,
            "valid_to": assertion.valid_to,
            "observed_from": assertion.observed_from,
            "observed_to": assertion.observed_to,
            "status": assertion.status,
            "contradiction_state": assertion.contradiction_state,
        })
    ranking = []
    for entity in entities:
        source_count = len(sources[entity.entity_id])
        score = round(confidence[entity.entity_id] + 0.1 * source_count + 0.05 * degrees[entity.entity_id], 4)
        ranking.append({"entity_id": entity.entity_id, "score": score, "relationship_count": degrees[entity.entity_id], "distinct_source_count": source_count, "inputs": {"confidence_sum": round(confidence[entity.entity_id], 4), "source_diversity_weight": 0.1, "degree_weight": 0.05}, "label": "documented prominence signal, not a factual designation"})
    ranking.sort(key=lambda row: (-row["score"], row["entity_id"]))
    timeline.sort(key=lambda row: (row["observed_from"] or row["valid_from"] or datetime.min.replace(tzinfo=timezone.utc), row["relationship_assertion_id"]))
    return {"ranking_formula": "sum(assertion confidence) + 0.1 * distinct cited source URIs + 0.05 * relationship degree", "prominence_ranking": ranking, "source_diversity": {str(entity_id): len(values) for entity_id, values in sources.items()}, "regional_clusters": {key: sorted(values) for key, values in sorted(clusters.items())}, "evidence_timeline": timeline}


def require_entity(session: Session, entity_id: int) -> EntityORM:
    entity = session.get(EntityORM, entity_id)
    if entity is None:
        raise ValueError(f"Entity {entity_id} does not exist.")
    return entity


def require_citations(session: Session, citation_ids: list[int]) -> list[EntityCitationORM]:
    citations = [session.get(EntityCitationORM, citation_id) for citation_id in sorted(set(citation_ids))]
    missing = [citation_id for citation_id, citation in zip(sorted(set(citation_ids)), citations) if citation is None]
    if missing:
        raise ValueError(f"Citations do not exist: {missing}.")
    return [citation for citation in citations if citation is not None]


def promote_cited_artifacts(session: Session, citations: list[EntityCitationORM], assertion_id: int, actor: str) -> None:
    for citation in citations:
        if citation.storage_object_id is None:
            continue
        artifact = session.get(StorageObjectORM, citation.storage_object_id)
        if artifact is None:
            continue
        artifact.retention_class = "investigative"
        artifact.promoted_by_type = "entity_relationship_assertion"
        artifact.promoted_by_id = str(assertion_id)
        artifact.metadata_json = {**(artifact.metadata_json or {}), "legal_hold_eligible": True, "entity_graph_assertion_id": assertion_id}


def log_graph_change(session: Session, object_type: str, object_id: int, action: str, actor: str) -> None:
    session.add(CustodyLogORM(object_type=object_type, object_id=str(object_id), action=action, actor=actor, details_json={"recorded_at": datetime.now(timezone.utc).isoformat()}))
