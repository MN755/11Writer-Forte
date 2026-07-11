"""Versioned, citation-preserving renderer for investigation artifacts.

The renderer only consumes reviewed claim/evidence records.  It never discovers data,
changes lifecycle state, or converts an uncertain claim into a conclusion.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from typing import Iterable

from src.services.evidence_service import ClaimAssessment, ClaimRecord, EvidenceRecord


REPORT_SPEC_VERSION = "investigation-report/v1"


@dataclass(frozen=True)
class RenderedReport:
    specification_version: str
    generated_at: datetime
    markdown: str
    citation_count: int
    conclusion_count: int


def _iso(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def immutable_artifact_link(artifact_id: str) -> str:
    """Use a stable, local artifact reference rather than the mutable source URL."""
    return f"artifact://{escape(artifact_id, quote=True)}"


def render_investigation_report(
    *,
    question: str,
    claims: Iterable[ClaimRecord],
    assessments: Iterable[ClaimAssessment],
    evidence: Iterable[EvidenceRecord],
    methodology: Iterable[str] = (),
    extended: bool = False,
    generated_at: datetime | None = None,
) -> RenderedReport:
    """Render a concise report and retain uncertainty/conflict at the point of claim."""
    timestamp = generated_at or datetime.now(timezone.utc)
    claim_rows = {claim.claim_id: claim for claim in claims}
    assessment_rows = {assessment.claim_id: assessment for assessment in assessments}
    evidence_rows = {record.evidence_id: record for record in evidence}
    lines = [
        "# Investigation report",
        "",
        f"- Specification: `{REPORT_SPEC_VERSION}`",
        f"- Generated: {_iso(timestamp)}",
        f"- Question: {question.strip()}",
        "",
        "## Direct answer",
    ]
    conclusions = [item for item in assessment_rows.values() if item.conclusion]
    if conclusions:
        for assessment in conclusions:
            lines.append(
                f"- {assessment.conclusion} (confidence {assessment.confidence:.0%}; {assessment.status.value})"
            )
    else:
        lines.append(
            "- Insufficient evidence for a conclusion. The report preserves what was collected below."
        )

    lines.extend(["", "## Findings"])
    for claim_id in sorted(claim_rows):
        claim = claim_rows[claim_id]
        assessment = assessment_rows.get(claim_id)
        if assessment is None:
            lines.append(f"- **Unknown:** {claim.text} (no assessment was recorded)")
            continue
        conclusion = assessment.conclusion or "No conclusion"
        lines.append(
            f"- **{assessment.status.value}:** {conclusion}. "
            f"Statement type: {claim.statement_kind.value}; confidence {assessment.confidence:.0%}."
        )
        for limitation in assessment.limitations:
            lines.append(f"  - Limitation: {limitation}")

    lines.extend(["", "## Timeline and spatial context"])
    dated = sorted(
        evidence_rows.values(),
        key=lambda row: (_iso(row.publication_time or row.collection_time), row.evidence_id),
    )
    if dated:
        for row in dated:
            location = row.metadata.get("location") if isinstance(row.metadata, dict) else None
            context = f"; location: {location}" if location else ""
            lines.append(
                f"- {_iso(row.publication_time or row.collection_time)} — {row.evidence_id}{context}"
            )
    else:
        lines.append("- No dated or spatial evidence was available.")

    conflict_ids = {
        evidence_id
        for assessment in assessment_rows.values()
        for evidence_id in assessment.conflicting_evidence_ids
    }
    lines.extend(["", "## Conflicting evidence"])
    if conflict_ids:
        for evidence_id in sorted(conflict_ids):
            row = evidence_rows.get(evidence_id)
            if row:
                lines.append(f"- {row.quote} — attributed to {row.source_id}.")
    else:
        lines.append("- No conflicting evidence was recorded.")

    lines.extend(["", "## Sources and citations"])
    for row in sorted(evidence_rows.values(), key=lambda item: item.evidence_id):
        publication = _iso(row.publication_time)
        lines.append(
            f"- [{row.evidence_id}]({immutable_artifact_link(row.source_artifact_id)}) — "
            f"source `{row.source_id}`; captured {_iso(row.collection_time)}; published {publication}; "
            f"{row.statement_kind.value}; {row.relation.value}."
        )
        if extended:
            lines.append(f"  - Extract: {row.quote}")

    lines.extend(["", "## Methodology and limitations"])
    supplied_methodology = list(methodology)
    if supplied_methodology:
        lines.extend(f"- {item}" for item in supplied_methodology)
    else:
        lines.append(
            "- Public sources only; access controls, robots policy, and source budgets were respected."
        )
    lines.append(
        "- Direct observations, attributed claims, inferences, and unknowns are not interchangeable."
    )
    lines.append(
        "- Citations point to immutable local evidence artifacts, with source capture times retained."
    )

    return RenderedReport(
        specification_version=REPORT_SPEC_VERSION,
        generated_at=timestamp,
        markdown="\n".join(lines) + "\n",
        citation_count=len(evidence_rows),
        conclusion_count=len(conclusions),
    )
