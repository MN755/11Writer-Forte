# Roadmap 05: Live Research Fleet, Local Intelligence, and End-to-End Investigations

## Goal

Make 11Writer Forte reliably execute this product loop on public information:

```text
operator question
  -> deterministic research plan
  -> bounded source discovery and collection
  -> normalization, deduplication, evidence/claim assessment
  -> cited report or explicit insufficient-evidence result
  -> optional durable watch and material-update feed
```

The product remains a public-information investigative system. It does not bypass
paywalls, logins, robots rules, access controls, or platform protections; it does not
perform targeting; and it must distinguish direct evidence, attributed reporting,
analysis, and unknowns.

## Definition of done

Forte is ready for this release only when all of the following hold:

1. A configured, public-source research fleet can pursue an approved question across
   multiple independent source types, recover from bounded dead ends, and retain a
   coverage ledger explaining what was and was not searched.
2. Local image/audio/data inference uses approved, benchmarked models on the target
   RTX 4070 Laptop GPU or a bounded CPU fallback. The default fixture processor is no
   longer presented as production inference.
3. A live end-to-end investigation can produce a cited report, declare insufficient
   evidence when appropriate, and become a continuing watch without an LLM controlling
   source creation, scheduling, retention, or delivery.
4. The LLM share stays under 10% of eligible investigation work and tracked token/cost
   use. Spark -> Mini -> Luna escalation remains fail-closed and quota-reserve-aware.
5. Live acceptance runs, security checks, recovery tests, and operator runbooks provide
   evidence that the workflow works outside synthetic fixtures.

## Non-negotiable control plane

- Rule/code workers own discovery, fetch policy, deduplication, parsing, source scoring,
  scheduler actions, retention, notification eligibility, and all external delivery.
- Codex gets a minimized evidence packet only. It may identify ambiguity, propose
  aliases/research gaps, assess a visual candidate, or draft a citation-bound narrative.
  Forte validates every response as data before acting.
- All network collection uses a deployment policy: public targets only by default,
  DNS/IP revalidation, robots policy, domain pacing, byte caps, timeouts, concurrency
  caps, artifact hashes, and immutable custody events.
- Local model inference has no runtime outbound network. Model acquisition is a separate
  operator-approved setup operation with license, checksum, SBOM, CVE review, and
  benchmark evidence.

## Worktree plan

| Worktree | Owns | Must not own |
|---|---|---|
| `codex/live-research-fleet` | Discovery providers, browser workers, source adapters, frontier/job execution | Investigation report wording, model implementations |
| `codex/local-intelligence-models` | Model registry, local GPU/CPU runners, feature extraction, benchmarks | Source discovery and report/orchestration state |
| `codex/investigation-e2e` | Investigation worker, evidence thresholds, report assembly, watch handoff, live fixtures | Provider internals and model internals |
| `codex/release-integration` | Migrations, deployment, live acceptance, observability, load/recovery/security checks | New product features without an approved change request |

Freeze shared contracts before parallel work begins: `StorageObject`, artifact roles,
source-run records, `Investigation`, claim/evidence records, `VisualObservation`, and
watch-update schema. Changes to these contracts require a migration, snapshot/restore
test, and agreement by the affected worktrees.

## Phase 0: Baseline and shared contracts

### 0.1 Make upgrades safe

- Replace ad-hoc additive schema reconciliation with a versioned migration strategy.
  Maintain a migration test from the current production-shaped SQLite/Postgres schema.
- Add CI jobs for compile, Ruff, full pytest, CodeQL, and a source-only CodeQL database
  scan. Keep test fixtures excluded from production CodeQL reporting, but retain their
  ordinary pytest coverage.
- Record dependency locks, Python version, local-media optional dependency versions, and
  GPU-driver/runtime compatibility in an operator runbook.

### 0.2 Complete the artifact contract

Every input and output must be one or more of `raw`, `normalized`, `derived`,
`evidence`, or `cache`, with BLAKE3 and SHA-256, source/capture time, provenance,
transform chain, lifecycle state, owner reference, and custody history.

For the current local-first release:

- raw captures, media, WARC/WACZ, reports, and model outputs live under configured
  local data roots;
- Postgres/PostGIS stores operational metadata and relationships, not large blobs;
- ClickHouse stays optional for high-volume time series; no feature may require it to
  work locally;
- evidence is immutable and never auto-deleted; ordinary raw bytes are deduplicated and
  compacted according to the retention policy.

### Exit criteria

- Snapshot/bundle restore succeeds from a populated investigation, watch, artifact, and
  entity graph fixture.
- A downgrade/upgrade test proves no schema drift or orphaned artifact reference.
- Full existing test suite, CodeQL source scan, and backup/restore smoke test pass.

## Phase 1: Genuine public-source research fleet

### 1.1 Provider and source capability registry

Create a first-class provider contract with:

- source kind, license/terms notes, jurisdiction, languages, freshness, cost (`free`,
  `operator-supplied`, or disabled), access requirement, robots support, and evidence
  capture method;
- explicit capabilities: search, structured API, RSS/Atom, sitemap, static HTML,
  browser-rendered page, public video/channel, public social/ActivityPub, document,
  image, dataset, court/legal record, financial/public registry;
- per-provider/domain default request, byte, concurrency, retry, and retention budgets;
- health, schema/version, and coverage-gap signals that feed the scheduler.

Start with sources that are publicly accessible, automatable, legally clear, and useful
for the initial acceptance cases. Do not create a generic “scrape everything” adapter.

### 1.2 Search and discovery

Implement a pluggable search-provider interface. The initial providers must be
operator-configured public APIs or licensed/free endpoints; no default scraping of a
commercial search engine.

- Normalize query, entity aliases, language, place, date, source class, and evidence
  requirement into search tasks.
- Persist query, provider, response hash, candidate URLs, ranking reason, and source
  lineage.
- Deduplicate canonical URLs/domains globally and promote candidates only through
  existing trust, robots, and runtime-support policies.
- When a planned source fails, generate bounded alternate work based on the recorded
  gap rather than declaring success or endlessly retrying the same host.

### 1.3 JavaScript/browser collection

Add an isolated Playwright/Chromium worker only for candidates already admitted by the
source policy. It must:

- run with no credentials, no extensions, a fresh profile, egress safeguards, strict
  navigation/redirect validation, request and response byte caps, wall-clock caps, and
  per-domain concurrency;
- capture final URL, normalized DOM/text, selected response headers, screenshot, and
  an evidence-grade WARC/WACZ or equivalent response manifest when policy promotes it;
- expose rendered links and structured-data/API hints to the deterministic discovery
  engine; never allow page JavaScript to schedule a task or invoke Codex;
- retain only changed/promoted captures according to the artifact policy.

Add adversarial tests for private-network redirects, giant resources, infinite page
activity, login walls, consent walls, and content that changes after navigation.

### 1.4 Public social/video/document adapters

Implement adapters by public, no-login protocol—not by impersonating a browser:

- RSS/Atom and official newsroom feeds;
- ActivityPub public objects where instance policy permits;
- public YouTube channel/video metadata and captions only through permitted public
  mechanisms;
- public document repositories, government portals, court/official datasets, and
  structured public financial/registry records by jurisdiction-specific adapter.

Each adapter needs parser fixtures from permitted public records, license/terms notes,
schema-change quarantine, canonical source identity, language handling, and citation
spans. A social profile or inaccessible page remains reference-only; Forte must not
pretend that it ingested it.

### 1.5 Horizontal worker fleet

Promote the current bounded scheduler to durable worker coordination:

- durable job table with idempotency key, lease/heartbeat, retry class, priority,
  cancellation, per-investigation budget, and ownership;
- separate worker classes for search, fetch/browser, parser, artifact processing,
  source health, entity resolution, report readiness, and retention;
- Postgres-backed leases first; add a queue broker only after load testing demonstrates
  that it is required;
- global and per-domain fairness, emergency stop, source pause, and storage-pressure
  backoff;
- metrics for queue age, success/error reason, policy blocks, bytes, dedupe savings,
  sources tried, independent corroboration, and evidence coverage.

### Exit criteria

- A test fleet survives worker restart and executes each job at most once logically.
- A live canary query reaches at least three independent, permitted source categories;
  unavailable sources create recorded alternatives, not silent termination.
- Browser collection cannot reach loopback/private targets or write outside managed
  storage roots.
- Provider health and coverage gaps are visible from the API/CLI.

## Phase 2: Production-grade local image, audio, and data intelligence

### 2.1 Model selection and security gate

Select models through a written scorecard, not demo vibes. For each candidate record:

- task and expected input/output contract;
- license and redistribution suitability;
- model/source checksum, SBOM, package lock, CVE review, upstream origin, local model
  path, required RAM/VRAM, CPU fallback, and benchmark version;
- offline proof: inference worker has no network namespace/permissions and fails closed
  if it attempts acquisition at runtime.

Required initial capabilities:

1. object/person/vehicle detection for basic image plausibility and scene evidence;
2. image embeddings for semantic retrieval and near-duplicate comparison;
3. OCR for signs/documents and timestamps;
4. speech-to-text for permitted public audio/video;
5. time-series/data anomaly features for flight, vessel, camera-health, and source-run
   data.

Treat satellite imagery as a distinct model/evaluation lane. Do not use a general
photograph classifier to label an image “satellite” with false confidence.

### 2.2 Hardware-aware worker runtime

- Implement a versioned worker interface for GPU (`RTX 4070 Laptop`) and deterministic
  CPU fallback, with VRAM, batch size, timeout, and queue-depth limits.
- Keep inference inputs and outputs as artifact IDs/features, not arbitrary file paths.
- Persist input/output hashes, model/config version, device, elapsed time, confidence,
  reason codes, and transform lineage.
- Separate model setup/update from runtime. An operator approves a downloaded model;
  production workers only load the recorded local immutable path.

### 2.3 Evaluation corpus and benchmarks

Build a licensed/permissioned benchmark corpus with a manifest, not loose files:

- public construction progression images across seasons, viewpoints, reposts, and
  unrelated keyword collisions;
- camera stills, map/satellite-like imagery, ordinary ground photographs, people, text,
  and low-quality images;
- permitted public audio/video clips with transcripts;
- flight/vessel/source-run time-series anomalies.

Publish baseline metrics by model/task. Initial release thresholds should be set against
the labeled corpus and include precision, recall, false-alert rate, top-k retrieval
recall, latency, VRAM, CPU fallback, and no-network verification. A recommended
construction-watch gate is at least 0.85 precision for material-change candidates and
at least 0.80 recall, subject to a human-reviewed holdout set.

### 2.4 Visual-change production pipeline

- Ingest candidate image/page/source context; exact-dedupe first, then perceptual hash,
  site association, metadata/EXIF, OCR, object/person plausibility, embedding similarity,
  and temporal baseline.
- Only candidates that pass deterministic evidence gates become `material_change_candidate`.
- Codex can review an already minimized candidate packet only when rules cannot resolve
  relevance. Its answer cannot promote evidence or publish an update by itself.
- Retain original bytes for promoted evidence; retain derived features longer than rolling
  non-evidence media; test storage pressure, corruption, and replay.

### Exit criteria

- The default fixture processor is no longer the active production configuration.
- Every active model has an approval manifest and benchmark report.
- End-to-end local inference demonstrates GPU use when available, a bounded CPU fallback,
  and no runtime outbound network.
- Reposts, irrelevant images, and keyword-only matches do not create alerts; a verified
  same-site progress change creates a cited reviewable candidate.

## Phase 3: One real investigation loop

### 3.1 Orchestrator state machine

Connect the existing investigation lifecycle to durable worker jobs. The orchestrator
must advance only on evidence/coverage events:

- build typed leads and deterministic source plan;
- run discovery/collection/normalization jobs;
- materialize claims, citations, corroboration groups, conflicts, and coverage gaps;
- evaluate a configurable confidence/completeness threshold;
- either request narrowly scoped Codex help, continue bounded research, emit an
  `insufficient_evidence` report, or produce a ready cited report;
- expose state, current evidence, blocked sources, remaining budget, and exact stop
  reason through API/CLI.

Define explicit stop conditions: evidence threshold met, coverage exhausted, policy
block, storage budget, operator cancellation, deadline, or failed infrastructure. “It
gave up because the first URL was broken” is not a state.

### 3.2 Codex production integration

- Use the existing fixed route: `gpt-5.3-codex-spark` first, then
  `gpt-5.4-mini`/`gpt-5.6-luna` at medium reasoning only with a recorded escalation
  reason.
- Wire real quota telemetry/calibration storage to the Codex CLI/operator-approved
  calibration window. If telemetry is unavailable or stale, pause LLM work while rule
  workers continue.
- Enforce per-investigation and global budgets plus the 60% reserve; emit usage and
  pause reason in operations/report views.
- Validate model output against evidence IDs and citation spans. Reject uncited claims,
  raw/private data, tool instructions, and control-plane actions.

### 3.3 Report and monitoring handoff

Render the default detailed-news report with:

- direct answer and confidence;
- sourced findings, chronology, geographic context when applicable, and methodology;
- citations with publication/capture time and archived artifact identity;
- source diversity, contradictory evidence, coverage gaps, and limitations;
- clear attribution vocabulary for allegation, reporting, official designation, and
  conviction.

On operator approval, compile the report/investigation into a deterministic watch.
Continue source coverage and publish only citation-gated, material changes to the local
API feed. Archive preserves reports/evidence and compacts eligible raw/derived artifacts.

### Exit criteria

- The same question produces stable initial rule plans and reproducible report citations.
- A live question can succeed, continue researching after a dead end, or emit a truthful
  insufficient-evidence report without a human manually driving each stage.
- LLM metrics prove it stayed within the 10% limit and no control-plane action came from
  an LLM response.
- A watch created from the completed investigation survives restart, report generation,
  pause/resume, and archive.

## Phase 4: Live acceptance and release gate

Run against live, lawful, public sources in a dedicated canary environment. Keep source
URLs, credentials, and optional operator-provided feeds out of the repository.

### Required acceptance scenarios

1. **Aviation research:** a publicly reported, non-targeting question about historical
   or current aviation/logistics patterns. The report must cite independent public
   sources, preserve time bounds, and explicitly distinguish observation from inference.
2. **Public-record entity map:** a documented public legal/official-record case with
   same-name ambiguity and at least one credible contradiction. The result must retain
   provenance and avoid unsupported identity/relationship assertions.
3. **Construction imagery watch:** monitor permitted public sources for a known project.
   The system must reject reposts/irrelevant images, retain evidence for a valid change,
   and emit a cited API-feed update. Use the Piston Peak target only after legitimate
   public sources are identified; do not invent a source merely to satisfy the demo.
4. **Failure drill:** primary source unavailable, browser blocked by policy, stale quota
   telemetry, GPU unavailable, storage pressure, and worker restart. Forte must record
   each reason, degrade safely, and keep/recover deterministic work.

### Release evidence

- Full pytest, Ruff, migration, backup/restore, and local source-only CodeQL scan pass.
- GitHub CodeQL and CI are green on the release commit.
- Benchmark reports and approval manifests for active local models are versioned.
- A retention/storage report proves no evidence loss and bounded growth.
- Operator runbooks cover source onboarding, model approval, quota calibration, canary
  execution, incident handling, and archive/restore.

## Sequencing

1. Complete Phase 0 before parallel implementation changes land.
2. Execute Phase 1 and Phase 2 in parallel after the shared-contract freeze.
3. Start Phase 3 orchestration against deterministic fixtures immediately, but enable
   live source/model adapters only after their Phase 1/2 exit criteria are met.
4. Phase 4 is a release gate, not a documentation exercise. A failed live scenario
   reopens the owning workstream.

## Explicit deferrals

- Login-required, paid, or access-control-bypassing collection.
- Automated publication or external communications beyond the local API/RSS feed.
- A general frontend or multi-user SaaS interface.
- Treating LLM narration as evidence or allowing it to manage Forte.
