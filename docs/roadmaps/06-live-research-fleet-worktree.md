# Worktree 06: Live Public-Source Research Fleet

## Worktree contract

| Field | Decision |
|---|---|
| Branch | codex/live-research-fleet |
| Owns | Provider registry, search intake, durable research jobs, static fetch orchestration, browser-worker contract, permitted public adapters, coverage-gap planning, fleet observability |
| Does not own | Report prose, investigation state-machine decisions, Codex routing/quota logic, local model inference, external delivery, login/paywall/access-control bypass, generic whole-internet crawling |
| Existing foundation | Discovery campaigns/runs/candidates/frontier/artifacts/domain policies, bounded fetch policy, scheduler, managed artifact storage |
| Product outcome | Given a deterministic plan, collect lawful public evidence from multiple permitted source categories, record gaps/dead ends, and pursue bounded alternatives rather than silently stopping. |

This is a public-information research fleet, not a targeting system, surveillance product,
or unrestricted scraper. It collects only sources available without payment, login, or
access-control bypass. It respects provider policy, terms/licensing records, robots rules
where applicable, and the existing public-network fetch protections.

## Non-negotiable invariants

1. Rules own collection. No LLM creates network jobs, chooses arbitrary URLs, changes
   budgets, bypasses policy, or promotes evidence.
2. Public network only by default. Validate the initial URL, every redirect, browser
   navigation, subresource, DNS result, and connection against private/loopback/link-local/
   reserved-address policy.
3. No credentials. Browser profiles are fresh and ephemeral. No cookies, extensions,
   saved sessions, password-manager access, OAuth, CAPTCHAs, proxy rotation, or evasion.
4. No default commercial-search scraping. Search is a provider interface; initial search
   providers are configured APIs or documented public endpoints with terms/licensing
   records. Missing capability is a gap, never permission to scrape a search-engine UI.
5. Every byte has provenance: requested/final URL, provider, time, content type, byte
   count, SHA-256/BLAKE3, policy decision, transform chain, and job/candidate/run links.
6. A failure creates a classified coverage gap and may create a finite alternate task. It
   must not hammer one host, retry forever, or emit complete because its first lead died.
7. Blocked/login-required/unsupported/robots-denied sources remain reference-only or
   policy-blocked. Do not call them ingested evidence.
8. Raw captures and derived artifacts use managed local storage roots only. Database rows
   hold references and metadata, never arbitrary filesystem paths supplied by callers.
9. Idempotency wins. A restart may repeat an attempt, but must not logically duplicate a
   job result, capture, candidate, or promotion.
10. This worktree makes zero Codex calls. Later investigation code receives structured,
    minimized output only.

## Pre-flight before editing

1. Read these files completely:

   - app/server/src/services/discovery_service.py
   - app/server/src/services/discovery_fetch.py
   - app/server/src/services/scheduler_service.py
   - app/server/src/models.py
   - app/server/src/schemas.py
   - app/server/src/routes/discovery.py
   - app/server/src/services/storage_service.py
   - docs/roadmaps/05-live-investigation-completion.md

2. Establish the baseline:

       cd app/server
       python -m pytest -q tests/test_discovery_fetch.py tests/test_discovery_unit.py tests/test_discovery_integration.py
       python -m ruff check src tests

3. Record baseline commit, test count, Python version, database initialization behavior, and
   current snapshot/bundle behavior in the first worktree handoff.

4. Coordinate persistent-model changes with codex/release-integration. This repository
   still needs versioned migrations. Define contracts/tests now, but do not call an
   additive create_all upgrade a production migration. Merge schema changes only with a
   migration and SQLite/Postgres upgrade test.

5. Freeze these shared contracts unless coordinated: StorageObject lifecycle, discovery
   campaign/run/candidate/frontier/artifact lineages, source/domain policy rules, runtime
   snapshot/bundle, and operator auth scopes.

## Delivery sequence

    provider contract
      -> provider registry and policy gates
      -> persisted search tasks/results
      -> durable research jobs and leases
      -> static fetch worker and artifact capture
      -> parsers/initial lawful adapters
      -> coverage ledger and alternate planner
      -> isolated browser fallback
      -> canary, observability, release handoff

Each phase gets its own reviewable commit range. Do not start browser rendering or live
adapters before relevant policy and durable-job tests pass.

## Phase 1: provider and policy registry

### Goal

Make source capability explicit, auditable, disabled by default where required, and
consumable by deterministic planners. A provider is not merely a URL template.

### Persistent contract

Add a migrated research_providers table and ORM/schema. It must contain:

| Field | Requirement |
|---|---|
| provider_id | Stable UUID or integer; never inferred from a display name. |
| provider_key | Unique lowercase slug; immutable after activation. |
| display_name | Operator-facing label. |
| provider_kind | Closed enum: search_api, rss_atom, sitemap, static_html, document_repository, structured_dataset, activitypub, video_metadata, browser_rendered. |
| capabilities_json | Enumerated capabilities; no free-form claims. |
| base_urls_json | Explicit approved origins/prefixes; no entire-internet wildcard. |
| access_mode | public_no_login, operator_supplied_public_feed, or disabled only. |
| terms_url and license_note | Required before the provider can be enabled. |
| robots_mode | required, not_applicable, or provider_terms_override_documented. |
| jurisdictions_json/languages_json | Empty allowed, but shown as a coverage limitation. |
| request_budget_json | Requests/run/day, concurrency, bytes, timeout, retry ceiling; clamp to global safe maxima. |
| artifact_capture_mode | metadata_only, normalized_text, raw_and_normalized, evidence_candidate. |
| health_status/reason/schema_version/last_checked | Explicit machine-readable operational state. |
| enabled/paused_at/disabled_reason | Unavailable provider cannot be scheduled; it creates a coverage gap. |
| created_by/approved_by/timestamps | Operator identity only, never credentials. |

Create a structured ProviderPolicyDecision service result: allowed, reason_code, provider,
URL, capability, and applied budget. Suggested reason codes: provider_disabled,
unsupported_capability, origin_not_allowed, login_required, terms_not_recorded,
robots_denied, domain_paused, budget_exhausted, unsafe_target, unsupported_content_type,
and schema_quarantined.

### Service/API work

Create provider_registry_service.py with create_provider, update_provider, get_provider,
list_providers, validate_provider_configuration, evaluate_provider_request,
mark_provider_health, and provider_coverage_summary.

Add routes under /api/discovery/providers:

| Route | Required scope | Behavior |
|---|---|---|
| GET /providers | read | Filter kind, enabled, health, capability, jurisdiction. |
| GET /providers/{id} | read | Safe configuration/health view, never secrets. |
| POST /providers | operate | Create disabled unless all approval fields validate. |
| PATCH /providers/{id} | operate | Record configuration change in custody. |
| POST /providers/{id}/enable | admin | Explicit activation after validation. |
| POST /providers/{id}/pause | operate | Immediate stop with a reason. |
| GET /providers/coverage | read | Provider availability/gaps by capability. |

Extend central scope classification for provider enable if necessary. No route-local auth
bypass.

### Tests and phase exit

- Reject unknown capability, missing terms/license, missing origin, userinfo/file/private-IP
  origin, and wildcard origins.
- Disabled, paused, unhealthy, or over-budget provider cannot create a job.
- Provider configuration round-trips through runtime snapshot/bundle.
- Provider health failure appears as a coverage gap.
- Read cannot mutate; operate cannot perform an admin-only enable.
- Exit: configure one lawful RSS provider and one configured search API provider, and
  expose exactly why either may not be used.

## Phase 2: normalized search tasks and results

### Goal

Turn a typed research lead into reproducible provider-specific searches. Persist what was
asked, returned, ranked, and declined.

### Data contracts

Add migrated research_queries and research_query_results tables.

research_queries must record parent campaign/investigation (nullable only for explicit
operator exploration), provider/schema version, normalized query text, aliases, language,
place, date range, source classes, evidence requirements, plan version, idempotency key,
state, attempts, retry class, stop reason, request/byte cost, timestamps, response artifact,
and response hash.

Allowed query states: queued, leased, running, succeeded, partial, failed, blocked,
cancelled.

research_query_results must record query, rank, result URL, canonical URL, title/snippet
only where provided by source, publication time, provider-native ID, source type/language,
deterministic ranking components, duplicate relation, and disposition.

Allowed dispositions: new_candidate, duplicate, policy_blocked, not_relevant_by_rules,
deferred_budget, reference_only, parse_error.

### Provider interface

Create research_provider_contracts.py. The planner must depend on a typed interface, never
a provider HTTP implementation:

    SearchProvider.search(NormalizedSearchRequest, ProviderRequestContext) -> SearchResponse

Request/response objects have strict size limits. They contain no arbitrary headers, shell
commands, bearer tokens, arbitrary paths, or model instructions.

Implement a deterministic compiler:

1. Normalize Unicode, whitespace, aliases, domains, language tags, dates.
2. Generate a finite alias x source-class x locale/date query matrix.
3. Deduplicate equivalent provider requests.
4. Cap total planned queries per campaign, provider, and date bucket.
5. Persist the plan before dispatch.
6. Emit coverage entries for unavailable provider/source class.

No LLM search planner. Later work may submit typed aliases/gaps but this worktree validates
and bounds all input.

### First real search provider

Implement one OperatorConfiguredSearchApiProvider only after endpoint format, response
schema, terms/license, request limit, and secret-reference convention are documented.
Resolve its secret from a named runtime reference; never store it in DB/API/log/custody.
If no legal configured provider exists, ship interface + fixture provider disabled. Never
scrape consumer search-result pages.

### Tests and exit

- Same normalized request yields same plan/idempotency key.
- Invalid date/language/alias fails before persistence.
- Oversize/malformed/paginated/duplicate/empty provider result fixtures are bounded.
- URLs canonicalize and deduplicate across query/provider.
- Timeout retries only configured transient count then emits a gap.
- Secrets are absent from repr/API/custody/errors.
- Schema-version mismatch quarantines provider.
- Exit: fixture search produces canonical candidate leads and visible gaps, without Codex.

## Phase 3: durable research-job engine

### Goal

Replace inline campaign execution with database-backed jobs that survive restart and are fair
across domains/investigations.

### Job tables

Add migrated research_jobs and research_job_attempts.

research_jobs requires job_id, closed job_type enum (search, fetch_static, fetch_feed,
parse_document, browser_render, source_health, coverage_replan, retention), closed state,
unique idempotency_key, bounded priority, provider/domain/campaign/investigation links,
typed/versioned input_json, budget reservation, lease owner/expiry/heartbeat, retry class/
next_attempt/max_attempts, bounded redacted outcome/error, and timestamps.

Allowed job state: queued, leased, running, succeeded, partial, retry_wait, blocked,
failed, cancelled, dead_letter.

Attempts record worker version, input hash, policy decision, timings/counters, result
reference, and error classification. Never store raw body bytes in an attempt row.

### Lease semantics

Implement research_job_service.py:

1. Enqueue validates provider/capability/payload/budget and persists one job atomically.
2. Duplicate enqueue returns existing active/successful logical job.
3. Claim selects only due, unpaused, non-cancelled jobs, enforcing global/provider/domain/
   investigation limits.
4. Claim is atomic. Postgres uses locking/skip-locked behavior; SQLite gets a tested
   single-writer-compatible strategy behind the same interface.
5. Heartbeats cannot revive or steal an expired/reclaimed lease.
6. Completion verifies lease owner and input hash; stale workers cannot overwrite current
   work.
7. Expiration records lease_expired then performs one bounded retry as policy permits.
8. Cancellation is checked before network, after headers, before storage, before promotion.
9. Successful logical work only repeats after an explicit freshness/version bucket changes.

### Worker commands

Add explicit CLI processes, never hidden FastAPI background threads:

    elevenwriter research-worker --worker-id <stable-id> --types search,fetch_static
    elevenwriter research-worker --once
    elevenwriter research-jobs --state queued --provider <key>
    elevenwriter cancel-research-job <job-id> --reason <reason>
    elevenwriter show-research-fleet-health

Existing scheduler may enqueue due work, but must not execute new network collection inline
once durable jobs exist. Release integration owns Docker worker services after stable CLI.

### Fairness/backpressure

- Conservative configurable global concurrency.
- Default provider/domain concurrency one unless policy explicitly permits more.
- Persist/reconstruct request-window and byte budgets from attempts.
- Enforce per-domain and per-investigation totals.
- Storage pressure blocks new non-evidence fetches before disk exhaustion.
- Emergency stop and provider pause stop claims immediately.
- Aging prevents high-priority campaigns permanently starving older work.

### Tests and exit

- Duplicate enqueue gives one logical job; concurrent claims give one lease.
- Crash/lease expiry produces one bounded retry, not duplicate artifact/candidate promotion.
- Stale/wrong-worker completion is rejected.
- Cancellation works before and after claim.
- Slow/noisy domain cannot consume all worker slots.
- Over-budget becomes blocked with coverage entry.
- SQLite/Postgres public semantics match.
- Snapshot/bundle behavior for active leases is safe and explicit.
- Exit: kill worker mid-fetch in a test; replacement yields one logical result and complete
  attempt/custody/stop history.

## Phase 4: static fetch and artifact capture

### Goal

Use existing hardened discovery_fetch primitives through the provider/job contract, then
turn permitted responses into managed raw and normalized artifacts.

### Implementation rules

1. static_fetch_worker accepts only a claimed fetch_static job.
2. Re-evaluate provider origin/capability, domain pause, robots, budget at execution time.
   Persisted approval is not permanent authorization.
3. Use existing FetchPolicy bounds. Do not introduce a weaker second HTTP client.
4. Validate every redirect and re-resolve destination at connection time with the validated
   address pool.
5. Write a response manifest even if body retention is disallowed: URLs, redirects, status,
   selected safe headers, content type, byte count, timing, policy version/decision, hashes.
6. Store allowed raw bytes through managed storage with content hashes/retention only; no
   API input can select an output path.
7. Make normalized text/metadata a separate derived artifact with transform/parser version,
   source artifact ID, and warnings.
8. Parser only sees allowlisted content type/size. Unsupported binaries/archives yield a
   classified result, never unbounded decode.
9. Login/403/consent/CAPTCHA behavior becomes access_restricted/reference_only. Never add
   alternate credentials or browser evasion.

### Parser boundary

CapturedDocument contains artifact IDs/metadata, never arbitrary paths. Initial parsers:
RSS/Atom XML with item/depth limits; static HTML metadata/canonical/visible text/JSON-LD/
links; JSON/CSV with row/depth limits; document/PDF metadata only after a bounded parser is
selected. Parser outputs NormalizedDocument with spans, field provenance, links, warnings.
It never makes a network request.

### Adversarial tests

Test DNS rebinding, redirect-to-private, IPv4/IPv6 literal/mapped addresses, userinfo URL,
bad IDNA, non-HTTP URL, redirect loop, giant/chunked body, compressed/archive bomb,
content-type mismatch, deep XML/JSON, huge-link HTML, robots denial, cancellation at every
checkpoint, path traversal, hash mismatch, dedupe, storage pressure, and prompt-injection
text in source content. Prompt injection remains untrusted evidence text, never control data.

### Phase exit

Fixture fetches result in immutable raw/normalized lineage and canonical candidates. Unsafe
targets and bounds violations are blocked before connection or during bounded read; nothing
writes outside managed storage.

## Phase 5: initial lawful adapters

Ship a small credible set, not a magic scrape-everything adapter.

1. RSS/Atom official/newsroom feeds: provider-approved URL, stable entry ID then canonical
   URL/content-hash dedupe, author/title/summary/publication/update time only if supplied.
2. Sitemaps/static official pages: strict sitemap limits and same-origin policy; no arbitrary
   third-party recursive crawling.
3. Structured public government/dataset adapter: one provider/jurisdiction at a time, with
   endpoint docs, license, schema version, pagination/date semantics, fixture corpus, field
   lineage.
4. Official document repository: metadata/index first; preserve issuing body, repository ID,
   filing/publication date, and page/section citation spans when extraction supports it.
5. ActivityPub public object adapter: approved public no-login instances only; capture object/
   actor URI, timestamps, reply/repost relation and provenance; profile is not proof.
6. Public video metadata/caption adapter only through permitted documented public mechanism.
   Do not download media or transcribe it in this worktree.

Each adapter implements discover, fetch, normalize, health_check, coverage_descriptor and
declares source type, output version, page/result limits, date behavior, limitations, and
policy needs. Changed source schema causes schema_quarantined until fixtures/parser update.

Explicit deferrals: session-simulated social scraping, paid/login court portals, generic
financial crawler, YouTube media download, and browser fallback prior to Phase 7.

Tests per adapter: permitted/synthetic fixtures, ID/URL canonicalization, pagination/empty
sets, changed schema quarantine, Unicode/date/language parsing, repost/duplicate handling,
citation span or explicit span-unavailable, and zero outbound unit-test network.

Phase exit: three independent categories, for example RSS + public structured government
data + official documents, populate one coverage ledger with distinguishable provenance.

## Phase 6: coverage ledger and dead-end recovery

### Coverage contract

Add migrated research_coverage_entries linked to campaign/investigation, provider, query/
job, source category, geography/language/date scope, result/artifact. Allowed status:
planned, searched, produced_candidates, no_results, blocked_policy, blocked_budget,
unavailable, access_restricted, schema_quarantined, deferred, exhausted, satisfied.

Each entry includes deterministic reason_code, safe summary, timestamp, remaining budget,
and alternate-task references. Correcting history appends an event/version; never overwrite
why a source was unavailable.

### Alternate planner

coverage_replan_service receives typed gaps and returns finite typed research jobs, never
raw URLs lifted from page text. Allowed strategies:

- another enabled provider of same category;
- an independent required category;
- known alias/language/date variant within precomputed limits;
- stale-source revisit after configured freshness window;
- operator decision when all alternatives require a provider/budget change.

Hard stops: never repeat same provider/query/canonical target after terminal failure absent
a freshness/policy version change; bounded alternate depth/count; no domain hopping around a
policy block; no retry for login_required/robots_denied/unsafe_target/terms_not_recorded;
no LLM replan input.

### Visibility and exit

Extend discovery ops/API/CLI with queue counts/oldest age, provider health, throttle/budget,
gaps by campaign/category, sources tried, independent categories, policy blocks/dead ends,
remaining budget, and terminal stop reason. Export excludes secrets, local paths, hidden raw
artifacts.

Use a fixture: primary RSS no results, dataset malformed, official repo succeeds. Assert all
gaps, one allowed alternate each, eventual independent source, and no false claim that
blocked/malformed source was successfully searched.

## Phase 7: isolated browser fallback

Do not start until Phases 1-6 pass and telemetry proves approved sources need rendering.
Browser is a constrained fallback, never default fetcher.

- Separate process/container, locked Playwright/Chromium version, no extensions/
  credentials/persistent profiles/downloads/clipboard/arbitrary host mounts/commands.
- New context/page per job with navigation/request/response/wall-clock limits.
- Intercept every navigation/subresource and validate origin/address against provider/public
  network policy. Default-block unknown third-party resources and record the decision.
- Block downloads. Capture final URL, safe headers, normalized DOM/text, screenshot, DOM
  hash, structured data, response manifest/WARC-equivalent only if retention permits.
- Page JavaScript cannot call Forte APIs, enqueue work, access local network, or invoke
  Codex. All DOM is untrusted evidence.
- browser_render accepts provider ID, approved candidate/artifact, approved initial URL,
  capture profile version, bounds; no script/cookie/browser arg/proxy/output-dir input.

Adversarial tests: private redirect/subresource, infinite activity, login/consent/CAPTCHA,
download/popup/service-worker/websocket, giant DOM/screenshot, hostile instructions, renderer
crash/lease recovery, and no network except allowed egress. Exit only after a JS page gives a
bounded attributable capture and every hostile scenario blocks safely.

## API, CLI, and observability

Minimum routes:

    GET  /api/discovery/providers
    POST /api/discovery/providers
    GET  /api/discovery/providers/{id}
    PATCH /api/discovery/providers/{id}
    POST /api/discovery/providers/{id}/enable
    POST /api/discovery/providers/{id}/pause
    GET  /api/discovery/research-jobs
    GET  /api/discovery/research-jobs/{id}
    POST /api/discovery/research-jobs/{id}/cancel
    GET  /api/discovery/coverage
    GET  /api/discovery/fleet/health
    GET  /api/discovery/fleet/export

Never return raw artifact paths, secret references/values, outbound headers, or stack traces.
Use existing operator scopes; activation, egress-policy, and retention changes are admin.

Required CLI parity:

    elevenwriter add-research-provider <config-file>
    elevenwriter validate-research-provider <provider-key>
    elevenwriter enable-research-provider <provider-key>
    elevenwriter pause-research-provider <provider-key> --reason <reason>
    elevenwriter enqueue-research-query <campaign-id> --provider <key> --query <text>
    elevenwriter research-worker --once
    elevenwriter list-research-jobs --state retry_wait
    elevenwriter show-research-coverage <campaign-id>
    elevenwriter show-research-fleet-health

Provider config validates secret-reference names but never copies a secret into database or
prints it. Provide dry-run wherever policy/budget decisions can be shown without networking.

Metrics: queue state/age, success/partial/blocked/failure reason by provider/domain/job,
bytes requested/read/stored/deduped, policy/robots/unsafe/cancellation counts, provider
health/schema quarantine/gaps, lease recovery/attempt duration, storage pressure. Never log
credentials, raw auth headers, deployment-private addresses, raw corpus text, local paths.

## Mandatory quality/release gates

    cd app/server
    python -m ruff check src tests
    python -m compileall -q src
    python -m pytest -q

Run the local source-only CodeQL process after worker/browser/storage changes. Do not
blanket-ignore production paths; fix SSRF, path traversal, uncontrolled command, unsafe
archive extraction, and secret exposure at source.

Test layers: unit policy/parser/idempotency; database migrations + SQLite/Postgres leases;
worker crash/timeout/cancel/fairness; security SSRF/rebinding/path/secrets/containment;
contract API/CLI/fixtures/scopes; operator-approved lawful live canary. Commit only
synthetic or legally redistributable fixtures. Do not use private feeds, credentials, paid
accounts, or a local flight receiver without separate approval.

## Definition of done

1. Enabled documented public providers create persisted search/feed/fetch work from typed
   campaign plans.
2. Restart-safe workers execute each logical task once, with tested idempotency, leases,
   cancellation, budgets, and fairness.
3. At least three permitted independent categories create provenance-preserving normalized
   candidates/artifacts.
4. Every no-result, failure, policy block, unavailable provider, schema change, and
   exhausted budget is a visible coverage entry with exact reason.
5. Alternate planning is finite, compliant, and never silent/looping.
6. Static fetch rejects unsafe targets and honors all bounds.
7. Browser fallback is isolated/tested or explicitly deferred.
8. Raw/normalized/derived/evidence artifacts have hashes, lineage, lifecycle, managed
   references; no arbitrary path write.
9. Ruff, compilation, full tests, source-only CodeQL, migrations/restore, and documented
   canary are green.
10. Investigation worktree can consume typed candidates, artifacts, citation spans/gaps,
    blocks, and remaining budgets without provider internals or LLM collection.

## Handoff package

Before merge provide commit list/owned files; migration names and SQLite/Postgres upgrade
results; exact test commands/results; secret-free provider inventory and limitations; one
sanitized canary showing plan/jobs/sources/gaps/stop reason; shared contract changes; explicit
deferrals.

## Recommended first PR

Keep it deliberately narrow:

1. Provider registry models/schemas/services/routes, disabled by default.
2. Typed search query/result persistence and fixture-only provider.
3. Durable research jobs with enqueue/claim/complete/cancel for search only.
4. Coverage entries for provider unavailable, no result, budget exhaustion.
5. Migration, snapshot/restore, scope, crash/idempotency tests.

This proves the fleet control plane. The second PR can connect one legitimate configured
public provider. Shipping Chromium or the whole web before the control plane is how an
investigative platform becomes an untestable chaos machine.

