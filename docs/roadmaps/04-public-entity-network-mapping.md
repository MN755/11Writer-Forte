# Roadmap 04: Public Entity and Network Mapping

## Outcome

Forte can turn public, attributed records into an inspectable entity/network map. An
operator can ask a question such as which major publicly documented criminal figures are
connected to a region, and receive sourced facts, aliases, relationships, time bounds,
confidence, disagreement, and clear gaps—not an unsourced accusation graph.

## Shared merge gate and ownership

Start from the repaired, green baseline. This worktree owns entity canonicalization,
relationship/claim provenance, graph queries, public-record adapters, confidence,
privacy safeguards, and mapping APIs. It publishes an entity-evidence interface for
investigations and watches; it does not own visual processing, report narration, or
feed delivery.

## Investigation and privacy standard

- Public sources only: permitted government, court, financial, news, public datasets,
  websites, RSS/APIs, public social/video pages, and configured public-data feeds.
- Do not infer or expose sensitive personal information, home locations, routine
  patterns, credentials, or operationally actionable details. Do not bypass login,
  payment, access controls, or platform restrictions.
- Preserve factual distinctions: an allegation, indictment, sanction/designation,
  conviction, reported association, and independently corroborated fact are different
  claims with different weights.
- State evidence fairly. A report may contain facts with proper attribution while
  retaining credible disputes, dates, jurisdiction, and unresolved ambiguity.

## Milestones

### E1. Provenance-first entity graph model

Extend the current entity-resolution design with durable records for entity candidates,
aliases, identity assertions, relationship assertions, source citations/spans,
jurisdiction, observed/valid time ranges, confidence, review state, and contradiction.
Every edge must link to one or more source artifacts/normalized records. Do not create a
bare `person A -> crime group B` edge because a classifier felt vibes.

Support nodes such as person, organization, vessel/aircraft, location, account, legal
case, document, event, and public institution, while making relationship types
explicitly versioned and constrained.

Acceptance tests:

- Every visible relationship resolves to at least one cited source artifact.
- Conflicting aliases/relationships coexist with their evidence and status.
- Snapshot/restore and event-export retain graph provenance.

### E2. Rule-first resolution and public-record adapters

Build deterministic normalizers and entity-resolution signals: canonical text,
transliteration, aliases, document/case IDs, organization registration IDs, vessel/aircraft
identifiers, dates, jurisdiction, and source-domain reliability. Implement adapters only
for legally accessible, no-login public sources and honor regional language/format
differences.

Require strong identifiers or independently corroborated context before automatic merge;
otherwise retain candidates and surface ambiguity. Add per-source schema/version tests
so source changes fail visibly instead of silently contaminating the graph.

Acceptance tests:

- Same-name people with weak context do not merge.
- Strong public identifiers merge with cited evidence.
- Source schema changes quarantine or fail the adapter safely.

### E3. Network analysis without opaque conclusions

Implement explainable graph views: relationship paths, evidence timelines, regional
clusters, source diversity, confidence bands, and change history. Rank prominence only
with documented, configurable criteria such as public legal/official designations,
corroborated reporting, role evidence, network centrality, and recency. Never label a
person "biggest" without exposing the formula, time range, data coverage, and
uncertainty.

Use deterministic graph algorithms first. Codex may only assist with difficult
multilingual public-source disambiguation or proposed research gaps, and its output must
be converted to cited candidate assertions and validated by Forte.

Acceptance tests:

- Rankings expose inputs and do not treat reprints as independent evidence.
- A disputed edge remains visibly disputed in path and cluster results.
- The graph yields an `insufficient_evidence` result when coverage is too weak.

### E4. Geographic/time-aware mapping API

Expose API and CLI queries for entity profiles, cited relationships, network slices,
jurisdiction, date ranges, and spatial/event context. Return structured data suitable
for the report worktree and a minimal map client later, but do not add a frontend here.
Protect sensitive outputs through default redaction and explicit clearance/retention
policies even when source material is public.

Acceptance tests:

- Queries honor date, geography, source policy, and redaction settings.
- Every response includes evidence count and provenance links.
- Entity results join to investigations/events without leaking unlinked records.

### E5. Evidence promotion and lifecycle

Integrate with the universal artifact policy: raw documents/pages expire by policy,
normalized facts and derived entity indexes remain useful longer, and source artifacts
backing an event/investigation are promoted with hashes, custody, transform chain, and
legal-hold support. All first-release bytes are local disk artifacts.

## Validation and handoff

- Unit tests: aliases, transliteration, positive/negative identity matches, claim types,
  edge provenance, contradictory evidence, ranking, redaction, and retention.
- Integration fixture: multilingual public records and reporting with a same-name
collision, a corroborated relationship, and a credible contradiction.
- Document supported source types/jurisdictions, source policy, confidence semantics,
  privacy boundaries, and known coverage gaps.
- Provide the stable entity/relationship evidence schema required by Roadmaps 01 and 02.
