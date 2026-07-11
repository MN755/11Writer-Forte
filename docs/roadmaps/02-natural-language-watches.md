# Roadmap 02: Natural-Language Watches and API Feed

## Outcome

An operator can promote an investigation into a continuing watch: Forte keeps gathering
and processing public information, evaluates a deterministic relevance policy, and
publishes material updates to an authenticated local API feed. Email and webhooks are
explicitly deferred.

## Shared merge gate and ownership

Start from the repaired, green baseline described in Roadmap 01. This worktree owns
watch definitions, scheduler integration, source-coverage/retry policy, update
evaluation, API-feed delivery, and retention handoff. It consumes investigation report
and evidence contracts but does not implement Codex orchestration, local media models,
or entity graph algorithms.

## Design principles

- A watch is an instruction to continue systematic investigation, not a keyword alert.
- Rule engines perform discovery, fetch, dedupe, normalization, changes, source health,
  confidence thresholds, and scheduling first. Codex is exceptional review only.
- Public web access remains bounded and polite: robots, domain policy, concurrency,
  bytes, schedules, retries, and global budgets apply even to active watches.
- A source dead end triggers a recorded diversification/research-gap action where
  policy permits; it must not silently terminate the watch.
- The operator receives only material, cited updates—not a daily pile of duplicated
  headlines.

## Milestones

### W1. Watch contract and natural-language compilation

Extend the existing watch engine with an `investigation_watch` specification:

- parent investigation and query version;
- target concepts/entities/locations/time context;
- allowed source types and domain-policy snapshot;
- deterministic relevance and materiality thresholds;
- fetch/schedule budget, retry/backoff, review cadence, and expiry;
- report-on-demand, pause, resume, and archive controls.

Compile natural-language instructions to a structured candidate rule. Validate it
against policy and show the operator the resulting scope before enabling it. The
investigation planner may use Codex only for unresolved ambiguity, and the final
enforced rule must remain deterministic and versioned.

Acceptance tests:

- A watch's compiled rule is inspectable, versioned, and repeatable.
- Pausing prevents new source work without deleting history.
- A changed query creates a new version rather than rewriting past scope.

### W2. Continuous collection and source-fleet coverage

Connect watch execution to source discovery, managed sources, source-run history,
geofence/event observations, and user-configured inbound feeds. Implement a coverage
ledger that records what was searched, which sources were attempted, what changed, and
what gaps remain.

Create source expansion rules that use alternate public APIs, RSS feeds, websites,
news, permitted public social/video pages, datasets, government/court/financial
records, and free imagery sources only when they fit policy. Never require a paid
account or login. User-provided local feeds are permitted after rule-engine reduction;
the LLM sees only minimized evidence packets when needed.

Acceptance tests:

- Failed or stale sources lead to bounded alternate-source discovery.
- A domain's budget varies only by declared, auditable domain class.
- Duplicate source payloads do not create duplicate watch updates.
- A watch can continue after an LLM quota pause.

### W3. Material-change engine

Define materiality as a rule-based score over novelty, source independence, claim
impact, temporal/geographic relevance, confidence change, and event linkage. Maintain
an update ledger keyed by evidence and claim versions to suppress repeats.

The engine should distinguish `new_source`, `new_fact`, `corroboration`,
`contradiction`, `status_change`, `visual_change_candidate`, and `no_material_change`.
Use the visual-change contract from Roadmap 03 for image-related updates and the entity
contract from Roadmap 04 for network updates.

Acceptance tests:

- Reposted or byte-identical content produces no new update.
- Independent corroboration can produce an update even if the core claim is unchanged.
- A contradictory credible source produces an attributed update.
- Low-confidence candidates remain reviewable without alerting.

### W4. Local API feed

Expose a versioned API feed for watch updates, current state, evidence links, report
versions, source coverage, and activity history. Prefer cursor pagination, stable event
IDs, filtering by watch/status/type, and an optional RSS-compatible public-summary
representation only if its data policy is explicit.

The existing backend has no multi-user authentication boundary. Keep the feed bound to
localhost/trusted-network deployment until an authentication layer is designed; never
accidentally publish investigative records through a wide-open port.

Acceptance tests:

- Feed pagination is deterministic and does not omit updates.
- A redacted/public view cannot leak higher-class artifacts or paths.
- The API emits an update only after materiality and citation validation.

### W5. Monitoring reports, stop, and retention

Support on-demand report generation from a watch's accumulated, cited evidence. On
stop/archive, preserve reports and promoted evidence, compact and deduplicate ordinary
raw captures, retain useful normalized/derived facts according to policy, and write
custody/lifecycle entries. Never auto-delete evidence or legal-hold items.

## Local storage policy

All storage is local for this release. Apply the supplied retention tiers: short-lived
spool/cache, short raw rolling buffers, longer normalized/derived records, and durable
evidence. Track per-source and per-layer ingestion, dedupe savings, compression savings,
expirations, and storage pressure. Make local budget limits configurable, with a safe
default that pauses noncritical work before exhausting the disk.

## Validation and handoff

- Unit tests: rule compilation, source coverage, retry/diversification, materiality,
  dedupe, pause/resume/archive, retention, and feed pagination.
- Integration fixture: a watch encounters an unavailable primary source, discovers an
  eligible alternate, receives a duplicate, then a genuine corroborating update.
- API/CLI documentation includes how to create, inspect, pause, resume, report on, and
  archive a watch.
- Explicitly document deferred email/webhook delivery and all public-web constraints.
