from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select

from src.config.settings import Settings
from src.services.ops_audit_service import init_ops_audit_db, record_provenance_event
from src.source_discovery.db import session_scope
from src.source_discovery.models import (
    SourceContentSnapshotORM,
    SourceEventArtifactORM,
    SourceEventClusterORM,
    SourceEventMemberORM,
    SourceEventOpenQuestionORM,
    SourceMemoryORM,
)
from src.types.source_discovery import (
    SourceDiscoveryEventArtifactCitationSummary,
    SourceDiscoveryEventArtifactDetail,
    SourceDiscoveryEventArtifactDetailResponse,
    SourceDiscoveryEventArtifactGenerationRequest,
    SourceDiscoveryEventArtifactListResponse,
    SourceDiscoveryEventArtifactSummary,
)


SOURCE_EVENT_ARTIFACT_CAVEATS = [
    "Event artifacts are deterministic analyst aids built from current backend evidence and do not adjudicate ultimate truth.",
    "Redaction levels are export-control labels for downstream consumers and do not imply legal classification authority.",
]


@dataclass(frozen=True)
class _ArtifactBuildResult:
    summary_text: str
    body_text: str
    citations: list[SourceDiscoveryEventArtifactCitationSummary]
    chain_of_custody: list[str]
    confidence_score: float
    confidence_label: str
    metadata: dict[str, object]
    caveats: list[str]


class SourceEventArtifactService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def list_event_artifacts(self, event_id: str) -> SourceDiscoveryEventArtifactListResponse:
        with session_scope(self._settings.source_discovery_database_url) as session:
            rows = list(
                session.scalars(
                    select(SourceEventArtifactORM)
                    .where(SourceEventArtifactORM.event_id == event_id)
                    .order_by(SourceEventArtifactORM.generated_at.desc(), SourceEventArtifactORM.artifact_id.desc())
                )
            )
            return SourceDiscoveryEventArtifactListResponse(
                count=len(rows),
                artifacts=[_serialize_event_artifact_summary(row) for row in rows],
                caveats=SOURCE_EVENT_ARTIFACT_CAVEATS,
            )

    def get_event_artifact(self, artifact_id: str) -> SourceDiscoveryEventArtifactDetailResponse:
        with session_scope(self._settings.source_discovery_database_url) as session:
            row = session.get(SourceEventArtifactORM, artifact_id)
            if row is None:
                raise ValueError(f"Unknown artifact_id: {artifact_id}")
            return SourceDiscoveryEventArtifactDetailResponse(
                artifact=_serialize_event_artifact_detail(row),
                caveats=SOURCE_EVENT_ARTIFACT_CAVEATS,
            )

    def generate_event_artifact(
        self,
        event_id: str,
        request: SourceDiscoveryEventArtifactGenerationRequest,
    ) -> SourceDiscoveryEventArtifactDetailResponse:
        init_ops_audit_db(self._settings.source_discovery_database_url)
        with session_scope(self._settings.source_discovery_database_url) as session:
            event = session.get(SourceEventClusterORM, event_id)
            if event is None:
                raise ValueError(f"Unknown event_id: {event_id}")
            members = list(
                session.scalars(
                    select(SourceEventMemberORM)
                    .where(SourceEventMemberORM.event_id == event_id)
                    .order_by(SourceEventMemberORM.created_at.asc(), SourceEventMemberORM.member_id.asc())
                )
            )
            open_questions = list(
                session.scalars(
                    select(SourceEventOpenQuestionORM)
                    .where(SourceEventOpenQuestionORM.event_id == event_id)
                    .order_by(SourceEventOpenQuestionORM.created_at.asc(), SourceEventOpenQuestionORM.question_id.asc())
                )
            )
            now = _utc_now()
            built = _build_artifact_payload(
                session,
                event=event,
                members=members,
                open_questions=open_questions,
                request=request,
                generated_at=now,
            )
            row = SourceEventArtifactORM(
                artifact_id=f"event-artifact:{request.artifact_kind}:{_compact_timestamp(now)}:{event.event_id.replace(':', '-')[:48]}",
                event_id=event.event_id,
                provenance_event_id=None,
                artifact_kind=request.artifact_kind,
                redaction_level=request.redaction_level,
                title=request.title or _default_artifact_title(event, request.artifact_kind),
                generated_by=request.generated_by,
                confidence_score=built.confidence_score,
                confidence_label=built.confidence_label,
                supporting_source_count=event.supporting_source_count,
                contradiction_source_count=event.contradiction_source_count,
                corrective_source_count=event.corrective_source_count,
                open_question_count=event.open_question_count,
                citation_count=len(built.citations),
                summary_text=built.summary_text,
                body_text=built.body_text,
                citations_json=json.dumps([item.model_dump(mode="json", by_alias=True) for item in built.citations]),
                chain_of_custody_json=json.dumps(built.chain_of_custody),
                metadata_json=json.dumps(built.metadata),
                generated_at=now,
                caveats_json=json.dumps(built.caveats),
            )
            session.add(row)
            session.flush()
            provenance = record_provenance_event(
                self._settings,
                subsystem="event_reports",
                event_kind="event_artifact_generation",
                operation=request.artifact_kind,
                status="completed",
                actor=request.generated_by,
                subject_type="source_event_cluster",
                subject_id=event.event_id,
                summary=f"Generated {request.artifact_kind} artifact for {event.event_id}.",
                output_refs=[row.artifact_id],
                evidence_refs=sorted({citation.source_id for citation in built.citations}),
                chain_of_custody=built.chain_of_custody,
                metadata={
                    "artifact_kind": request.artifact_kind,
                    "redaction_level": request.redaction_level,
                    "confidence_score": built.confidence_score,
                    "confidence_label": built.confidence_label,
                    "citation_count": len(built.citations),
                },
                session=session,
            )
            row.provenance_event_id = provenance.provenance_event_id
            session.flush()
            return SourceDiscoveryEventArtifactDetailResponse(
                artifact=_serialize_event_artifact_detail(row),
                caveats=SOURCE_EVENT_ARTIFACT_CAVEATS,
            )


def _build_artifact_payload(
    session,
    *,
    event: SourceEventClusterORM,
    members: list[SourceEventMemberORM],
    open_questions: list[SourceEventOpenQuestionORM],
    request: SourceDiscoveryEventArtifactGenerationRequest,
    generated_at: str,
) -> _ArtifactBuildResult:
    citations = _build_citations(session, members)
    confidence_score, confidence_label, confidence_basis = _score_event_confidence(event, members, open_questions)
    chain_of_custody = [
        f"event_id={event.event_id}",
        f"artifact_kind={request.artifact_kind}",
        f"redaction_level={request.redaction_level}",
        f"generated_at={generated_at}",
        f"member_count={len(members)}",
        f"citation_count={len(citations)}",
        "deterministic event artifact built from persisted event-cluster, member, question, snapshot, and source-memory rows.",
    ]
    metadata = {
        "confidence_basis": confidence_basis,
        "wave_id": event.wave_id,
        "knowledge_node_ids": _loads_list(event.knowledge_node_ids_json),
        "member_source_ids": _loads_list(event.member_source_ids_json),
    }
    caveats = list(dict.fromkeys(_loads_list(event.caveats_json) + SOURCE_EVENT_ARTIFACT_CAVEATS))
    summary_text = _summary_text(event, citations, confidence_score, confidence_label)
    body_text = (
        _build_cited_summary_body(event, members, open_questions, citations, confidence_score, confidence_label)
        if request.artifact_kind == "cited_summary"
        else _build_report_body(event, members, open_questions, citations, confidence_score, confidence_label, chain_of_custody)
    )
    return _ArtifactBuildResult(
        summary_text=summary_text,
        body_text=body_text,
        citations=citations,
        chain_of_custody=chain_of_custody,
        confidence_score=confidence_score,
        confidence_label=confidence_label,
        metadata=metadata,
        caveats=caveats,
    )


def _build_citations(session, members: list[SourceEventMemberORM]) -> list[SourceDiscoveryEventArtifactCitationSummary]:
    citations: list[SourceDiscoveryEventArtifactCitationSummary] = []
    for index, member in enumerate(members, start=1):
        snapshot = session.get(SourceContentSnapshotORM, member.snapshot_id) if member.snapshot_id else None
        memory = session.get(SourceMemoryORM, member.source_id)
        citations.append(
            SourceDiscoveryEventArtifactCitationSummary(
                citation_id=f"C{index}",
                source_id=member.source_id,
                title=(snapshot.title if snapshot is not None and snapshot.title else (memory.title if memory is not None else member.source_id)),
                url=(snapshot.url if snapshot is not None else (memory.url if memory is not None else None)),
                snapshot_id=member.snapshot_id,
                observed_at=member.observed_at,
                evidence_basis=member.evidence_basis,
                role=member.role,
                claim_text=member.claim_text,
            )
        )
    return citations


def _score_event_confidence(
    event: SourceEventClusterORM,
    members: list[SourceEventMemberORM],
    open_questions: list[SourceEventOpenQuestionORM],
) -> tuple[float, str, list[str]]:
    score = 0.35
    basis = ["Base score starts neutral until multiple persisted claims converge."]
    if event.supporting_source_count > 0:
        bonus = min(0.24, event.supporting_source_count * 0.08)
        score += bonus
        basis.append(f"{event.supporting_source_count} supporting sources added +{bonus:.2f}.")
    unique_source_count = len({member.source_id for member in members})
    if unique_source_count >= 2:
        score += 0.10
        basis.append("Independent source count reached cross-verification threshold (+0.10).")
    if _loads_list(event.knowledge_node_ids_json):
        score += 0.04
        basis.append("Linked knowledge-node corroboration added +0.04.")
    if any(member.evidence_basis in {"observed", "primary"} for member in members):
        score += 0.05
        basis.append("Observed or primary evidence basis added +0.05.")
    if event.contradiction_source_count > 0:
        penalty = min(0.24, event.contradiction_source_count * 0.12)
        score -= penalty
        basis.append(f"{event.contradiction_source_count} contradicting sources applied -{penalty:.2f}.")
    if event.corrective_source_count > 0:
        penalty = min(0.16, event.corrective_source_count * 0.08)
        score -= penalty
        basis.append(f"{event.corrective_source_count} corrective sources applied -{penalty:.2f}.")
    open_question_count = max(event.open_question_count, len(open_questions))
    if open_question_count > 0:
        penalty = min(0.12, open_question_count * 0.06)
        score -= penalty
        basis.append(f"{open_question_count} open questions applied -{penalty:.2f}.")
    score = round(max(0.05, min(0.95, score)), 4)
    if score >= 0.80:
        label = "high"
    elif score >= 0.60:
        label = "medium"
    elif score >= 0.40:
        label = "guarded"
    else:
        label = "low"
    basis.append(f"Final rule-based confidence score was clamped to {score:.4f} ({label}).")
    return score, label, basis


def _summary_text(
    event: SourceEventClusterORM,
    citations: list[SourceDiscoveryEventArtifactCitationSummary],
    confidence_score: float,
    confidence_label: str,
) -> str:
    return (
        f"{event.canonical_claim_text} "
        f"Evidence currently spans {len(citations)} cited member claims with event status {event.status}. "
        f"Rule-based confidence is {confidence_score:.2f} ({confidence_label})."
    )


def _build_cited_summary_body(
    event: SourceEventClusterORM,
    members: list[SourceEventMemberORM],
    open_questions: list[SourceEventOpenQuestionORM],
    citations: list[SourceDiscoveryEventArtifactCitationSummary],
    confidence_score: float,
    confidence_label: str,
) -> str:
    supporting = [citation.citation_id for citation, member in zip(citations, members) if member.role == "supporting"]
    contradicting = [citation.citation_id for citation, member in zip(citations, members) if member.role == "contradicting"]
    corrective = [citation.citation_id for citation, member in zip(citations, members) if member.role == "corrective"]
    paragraph_one = (
        f"The current event cluster records the claim \"{event.canonical_claim_text}\" as a {event.status} event posture. "
        f"It aggregates {len(members)} persisted member claims across {len({member.source_id for member in members})} sources"
        f"{_inline_citation_block(supporting or [citation.citation_id for citation in citations[:2]])}. "
        f"The event was last refreshed at {event.last_graph_refresh_at or event.last_seen_at}."
    )
    paragraph_two = (
        f"Rule-based confidence is {confidence_score:.2f} ({confidence_label}) based on supporting-source count, independent corroboration, "
        f"and any contradiction, correction, or open-question posture currently attached to the cluster. "
        f"Contradicting citations {', '.join(contradicting) if contradicting else 'none'} and corrective citations {', '.join(corrective) if corrective else 'none'} remain visible rather than merged away. "
        f"Open questions recorded for this event: {len(open_questions)}."
    )
    paragraph_three = (
        "This cited summary is a deterministic export over backend evidence rows only. "
        "It preserves source disagreement, review caveats, and chain-of-custody instead of claiming automated truth adjudication."
    )
    return "\n\n".join([paragraph_one, paragraph_two, paragraph_three])


def _build_report_body(
    event: SourceEventClusterORM,
    members: list[SourceEventMemberORM],
    open_questions: list[SourceEventOpenQuestionORM],
    citations: list[SourceDiscoveryEventArtifactCitationSummary],
    confidence_score: float,
    confidence_label: str,
    chain_of_custody: list[str],
) -> str:
    lines = [
        f"# Event Report: {event.canonical_claim_text}",
        "",
        "## Executive Summary",
        _summary_text(event, citations, confidence_score, confidence_label),
        "",
        "## Event Posture",
        f"- Status: {event.status}",
        f"- Claim type: {event.claim_type}",
        f"- Observed day: {event.observed_day or 'unknown'}",
        f"- Wave id: {event.wave_id or 'unassigned'}",
        f"- Rule-based confidence: {confidence_score:.2f} ({confidence_label})",
        "",
        "## Source Evidence",
    ]
    for citation in citations:
        lines.append(
            f"- [{citation.citation_id}] {citation.source_id} | role={citation.role} | evidence_basis={citation.evidence_basis} | observed_at={citation.observed_at or 'unknown'} | {citation.claim_text}"
        )
        if citation.url:
            lines.append(f"  url: {citation.url}")
    lines.extend(["", "## Open Questions"])
    if open_questions:
        for question in open_questions:
            lines.append(f"- {question.question_text}")
    else:
        lines.append("- None recorded.")
    lines.extend(["", "## Chain of Custody"])
    for item in chain_of_custody:
        lines.append(f"- {item}")
    lines.extend(["", "## Caveats"])
    lines.extend(
        [
            "- Event reports remain review-oriented backend artifacts; they do not create legal, attribution, or action authority on their own.",
            "- Contradictions, corrections, and unresolved questions are preserved explicitly rather than hidden behind a single narrative.",
        ]
    )
    lines.extend(["", "## Citation Appendix"])
    for citation in citations:
        lines.append(
            f"- [{citation.citation_id}] title={citation.title or citation.source_id} | source_id={citation.source_id} | snapshot_id={citation.snapshot_id or 'none'}"
        )
    return "\n".join(lines)


def _default_artifact_title(event: SourceEventClusterORM, artifact_kind: str) -> str:
    if artifact_kind == "cited_summary":
        return f"Cited Summary :: {event.canonical_claim_text[:120]}"
    return f"Event Report :: {event.canonical_claim_text[:120]}"


def _serialize_event_artifact_summary(row: SourceEventArtifactORM) -> SourceDiscoveryEventArtifactSummary:
    return SourceDiscoveryEventArtifactSummary(
        artifact_id=row.artifact_id,
        event_id=row.event_id,
        provenance_event_id=row.provenance_event_id,
        artifact_kind=row.artifact_kind,  # type: ignore[arg-type]
        redaction_level=row.redaction_level,  # type: ignore[arg-type]
        title=row.title,
        generated_by=row.generated_by,
        generated_at=row.generated_at,
        confidence_score=row.confidence_score,
        confidence_label=row.confidence_label,  # type: ignore[arg-type]
        supporting_source_count=row.supporting_source_count,
        contradiction_source_count=row.contradiction_source_count,
        corrective_source_count=row.corrective_source_count,
        open_question_count=row.open_question_count,
        citation_count=row.citation_count,
        summary_text=row.summary_text,
    )


def _serialize_event_artifact_detail(row: SourceEventArtifactORM) -> SourceDiscoveryEventArtifactDetail:
    return SourceDiscoveryEventArtifactDetail(
        **_serialize_event_artifact_summary(row).model_dump(),
        body_text=row.body_text,
        citations=[
            SourceDiscoveryEventArtifactCitationSummary.model_validate(item)
            for item in _loads_json_list_of_dicts(row.citations_json)
        ],
        chain_of_custody=_loads_list(row.chain_of_custody_json),
        metadata=_loads_json_dict(row.metadata_json),
        caveats=_loads_list(row.caveats_json),
    )


def _loads_json_dict(raw: str | None) -> dict[str, object]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _loads_json_list_of_dicts(raw: str | None) -> list[dict[str, object]]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _loads_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return [str(item) for item in value] if isinstance(value, list) else []


def _inline_citation_block(citation_ids: list[str]) -> str:
    rendered = ", ".join(citation_ids[:4])
    return f" [{rendered}]" if rendered else ""


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _compact_timestamp(value: str) -> str:
    return "".join(character for character in value if character.isdigit())[:20]
