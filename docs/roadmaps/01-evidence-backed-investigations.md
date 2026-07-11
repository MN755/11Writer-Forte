# Roadmap 01: Evidence-Backed Investigations

## Outcome

An operator can ask a current or historical question in ordinary language. Forte creates
an investigation, exhausts a bounded public-source research plan, assembles a cited,
New-York-Times-style report, and either archives the investigation or hands it to the
watch system. The product is an investigative system, not a targeting system.

Examples in scope include explaining publicly reported patterns in military logistics,
mapping a publicly documented criminal network, and investigating an unfolding event.
The system must separate directly observed facts, attributed claims, analysis, and
unknowns. It must not turn weak sources into a confident conclusion.

## Shared merge gate

Do not begin integration until the baseline branch has removed all committed conflict
markers and is green. The current main branch contains literal `<<<<<<<`, `=======`, and
`>>>>>>>` text in production code and tests, so it cannot import or collect tests.

Required baseline evidence:

```powershell
cd app/server
rg -n '^(<<<<<<<|=======|>>>>>>>)' ..
python -m compileall src
python -m pytest -q
python -m ruff check src tests
```

This worktree owns investigation/report/agent orchestration code. It must not modify
the visual-media pipeline (Roadmap 03), watch-delivery surface (Roadmap 02), or entity
graph/resolution core (Roadmap 04) except through agreed interfaces.

## Product contract

- Use public information only. Never bypass a paywall, login, access control, bot
  protection, or robots policy.
- Current events are in scope, but the system must not provide operational targeting or
  present stale, uncertain, or inferred details as live facts.
- A dead end is a research-state transition, not success. The deterministic planner
  must try alternate approved discovery paths until its bounded search budget is spent
  or its evidence threshold is met.
- A report is an artifact with reproducible citations, source capture times, source
  artifacts, and a versioned report specification.
- Only the Forte runtime communicates with the operator or controls schedules,
  promotions, retention, and alerts. The Codex investigator is a sandboxed tool.

## Milestones

### I1. Investigation domain model and lifecycle

Add durable investigation records and APIs/CLI commands with states:

`draft -> planning -> collecting -> normalizing -> corroborating -> reporting ->
ready -> monitoring | archived | insufficient_evidence | failed`.

Persist the question, operator scope, time window, geography, source-policy snapshot,
research budget, evidence threshold, discovery attempts, report versions, and why a
run stopped. Create custody entries for state changes and evidence promotions.

Acceptance tests:

- An investigation survives a runtime snapshot and restore.
- A failed source does not fail the investigation; it records a structured attempt and
  queues eligible alternate discovery work.
- Archiving retains reports and evidence but applies the storage policy to ordinary raw
  artifacts.

### I2. Deterministic research planner

Build a rule-first planner that turns the query into typed research leads: entities,
locations, time ranges, languages, claimed relationships, required evidence types, and
known aliases. It should use the existing source catalog, discovery campaigns, source
trust profiles, observation/event data, and public inbound feeds first.

Define a source-budget policy by domain class, not a flat per-domain limit:

- official/public-data, established news, and known high-value public sources receive
  larger caps;
- an unknown personal domain begins with a smaller cap that may be raised only after
  reliability and relevance checks;
- every request still obeys robots, rate limits, byte limits, concurrency limits,
  deduplication, and a global investigation budget.

Each source attempt produces a machine-readable reason: `confirmed`, `corroborating`,
`conflicting`, `low_quality`, `unavailable`, `blocked_by_policy`, or `dead_end`.

Acceptance tests:

- Equivalent queries generate a deterministic initial research plan.
- A plan uses alternate sources when the first source is unavailable.
- Domain-class budgets are enforced and are auditable.
- Search/planning never silently crosses an access-control boundary.

### I3. Evidence and confidence engine

Implement claim/evidence records rather than a single opaque confidence score. A claim
must point to quoted/extracted spans or normalized observations, source identifiers,
collection time, publication time when known, and a statement of whether it is direct,
attributed, or inferential.

Confidence should combine source-trust policy, independence of corroboration, recency,
geo/time fit, contradiction, and completeness. Set no conclusion when evidence is
insufficient. Keep conflicting evidence visible in the report rather than laundering it
away because it is inconvenient.

Acceptance tests:

- Two reposts of the same original do not count as independent corroboration.
- A high-trust source and a weak contradictory source are both reported with attribution.
- The system emits `insufficient_evidence` instead of fabricating an answer.

### I4. Conservative Codex investigator

Create an internal `forte-investigator` plugin/skill contract for the headless Codex
CLI. It receives only a minimal, preprocessed evidence packet: normalized records,
selected source excerpts, artifact IDs, research gaps, and policy constraints. It must
not receive raw private data, filesystem access beyond its report workspace, or direct
network/scheduling/delivery authority.

It may classify ambiguity, propose aliases/search gaps, resolve difficult public entity
mentions, assess whether an image candidate depicts the requested subject, and draft a
cited narrative. Forte validates every structured recommendation before acting.

Routing policy:

1. `gpt-5.3-codex-spark` first.
2. Escalate to `gpt-5.4-mini` with `medium` reasoning only for a recorded uncertainty,
   Spark failure, or an approved budget rule.
3. Escalate to `gpt-5.6-luna` with `medium` reasoning only when Mini cannot resolve the
   same documented uncertainty.
4. If the configured model is unavailable, fail closed and surface the reason; never
   quietly substitute a different model.

Implement hard per-investigation call/token/time budgets and a global governor. LLM
work must stay under 10% of investigation work by both eligible-work-item count and
tracked cost/token usage. Add an explicit operator-approved calibration mode that may
consume one or two five-hour windows, records observed capacity/reset information, and
then protects a 60% remaining-quota reserve. Until telemetry is reliable, be
pessimistic and pause rather than exceed the reserve.

Acceptance tests:

- Rule-engine-complete tasks never invoke Codex.
- Escalation requires a reason and respects budgets.
- Codex output cannot create a schedule, source, alert, or lifecycle change without
  Forte validation.
- A quota-floor breach pauses LLM work and leaves the deterministic investigation alive.

### I5. Report renderer and archive handoff

Generate a concise default report with: direct answer, confidence, findings, timeline
or spatial context where available, cited source table, conflicting evidence,
methodology, and limitations. Preserve citations as links to immutable source/evidence
artifacts and include source/capture dates. Support a requested extended report without
changing the factual standard.

On `archive`, retain the report and promoted evidence, compact/dedupe other artifacts,
and apply the local-disk retention policies. On `monitor`, publish the structured
investigation contract consumed by Roadmap 02.

## Storage and privacy contract

Use the attached policy's roles throughout: `raw`, `normalized`, `derived`, `evidence`,
and `cache`. For this phase, all bytes stay on local disk. Store object references,
hashes, provenance, transform chain, and lifecycle state in the operational database;
do not treat a database blob column as an evidence archive. Use BLAKE3 plus SHA-256 for
new content-addressed evidence artifacts after verifying dependency and implementation
security.

## Validation and handoff

- Unit tests for lifecycle, source-policy budget, confidence, report citations, model
  routing, quota governor, archive, and snapshot round-trip.
- Integration fixture: a multi-source public question with a contradiction and a dead
  source; verify alternate research, no invented claim, and stable citations.
- Document exact model availability checks, model IDs, calibration observations, and any
  unvalidated provider behavior.
- Deliver a migration plan, API/CLI examples, threat/privacy notes, and a list of
  intentionally unsupported actions.
