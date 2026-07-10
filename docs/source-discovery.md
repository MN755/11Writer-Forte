# Source discovery operations

11Writer Forte source discovery is a bounded source-intelligence pipeline. It finds and
revisits public endpoints, preserves why they entered inventory, scores them with
inspectable rules, and can promote useful candidates into the existing managed-source
and scheduler system.

It is not a general web index, an internet scanner, or a browser farm. Query discovery
only visits URLs produced from operator-supplied templates or direct URL queries. If no
search template or direct seed exists, no magical Google-shaped rabbit appears from a
hat. That limitation is intentional.

## Pipeline and ownership

One discovery pass follows this path:

```text
campaign request
  -> persisted run and bounded frontier
  -> policy, suppression, and robots checks
  -> paced and size-limited fetch
  -> canonicalize, classify, extract, and fingerprint
  -> globally deduplicated candidate
  -> revision + graph edge + health check + optional artifact
  -> rule-based score and disposition
  -> operator promotion, suppression, revisit, or scheduled monitoring
```

The analysis layer is dependency-free and does not fetch. The fetch layer owns network
safety, redirects, timeouts, retries, response bounds, and header sanitization. The
discovery service owns campaigns, frontier checkpoints, persistence, alerts, promotion,
health, scheduler integration, artifacts, and custody.

The primary tables are:

| Table | Durable responsibility |
| --- | --- |
| `discovery_campaigns` | Operator intent, modes, seeds, geography, limits, and policy |
| `discovery_runs` | Immutable request/policy snapshots, counters, status, and checkpoint |
| `discovery_frontier_entries` | Per-run queue, depth, attempts, parent, state, and dead letter |
| `source_candidates` | Current globally deduplicated view keyed by canonical URL hash |
| `source_candidate_revisions` | Append-only observations and disposition history |
| `discovery_graph_edges` | Parent-child discovery lineage and method evidence |
| `candidate_health_checks` | Reachability, hashes, schema, latency, and failure history |
| `candidate_suppressions` | Candidate/domain ignore policy with operator reasoning |
| `candidate_promotion_decisions` | Promotion evidence, risks, mapping, and schedule decision |
| `discovery_domain_policies` | Domain pacing, robots mode, path rules, and fetch bounds |
| `robots_observations` | Fetched rules, result, expiry, sitemap URLs, and errors |
| `discovery_artifacts` | Metadata and storage-ledger link for fetched documents |

A candidate row is the latest state, not the whole story. Revisions, edges, health
checks, promotion decisions, suppressions, custody logs, and run snapshots are the
audit trail. Repeated discovery updates that trail instead of overwriting it.

## Discovery modes

Campaigns may combine modes in `modes_json`.

### `query_seeded`

Expands operator queries through `search_templates_json`. Templates may use:

- `{query}`: URL-encoded query
- `{raw_query}`: unescaped query; use only with a template that safely handles it
- `{language}` and `{locale}`: URL-encoded configured variants
- `{recency_days}`: configured recency window or an empty value

A query that is already an absolute HTTP(S) URL is also queued directly. Forte ships
without a search-engine provider and does not scrape a default search engine. An
operator must supply a lawful public search endpoint/template, a local metasearch
instance, or direct URLs.

### `seed_url`

Queues the URLs in `seed_urls_json`, then allows bounded same-policy expansion when the
campaign also permits neighborhood or targeted-format links. Seeds are canonicalized,
tracking parameters are discarded for identity, and fragments do not produce duplicate
candidates.

### `sitemap`

Probes `/sitemap.xml` for each seed origin, reads sitemap and sitemap-index documents,
and queues `<loc>` children. Sitemap declarations observed in `robots.txt` are also
queued. Depth, domain policy, suppression, and run bounds still apply.

### `neighborhood`

Follows extracted outbound links within campaign and domain policy. Extraction covers
HTML anchors, link/meta/embed endpoints, JSON URL fields, XML locations, feeds,
manifests, and plain-text URLs. This is a bounded graph walk, not “crawl forever.”

### `format_targeted`

Prioritizes machine-usable or operational formats such as RSS/Atom, JSON/JSONL, XML,
CSV, GeoJSON, KML, PDF, TXT, images, stream manifests, and API/OpenAPI documents.
Extension signals help queue a URL; content and structure determine the persisted type.

### `geospatial`

Uses campaign bbox, polygon, place, jurisdiction, route, and corridor targets during
scoring. Analysis extracts coordinates from structured records, GeoJSON/KML, HTML
metadata, schema.org JSON-LD, and practical inline latitude/longitude patterns. Named
geo hints are normalized for overlap but are not sent to an external geocoder.

### `entity_led`

Turns configured agencies, operators, vessels, ports, routes, candidates, or other
organizations into query terms, including supplied aliases. It still needs a search
template or URL seed to reach the network.

### `historical_backfill`

Marks archive-oriented campaigns and gives discovered archive datasets a slower revisit
schedule. It does not invent an archive index. Supply catalog, sitemap, portal, or
dataset seeds that make the historical scope concrete.

## Campaign configuration

The durable campaign object separates intent from runtime state:

- `query_strings_json`, locale/language variants, and search templates describe query
  expansion.
- `seed_urls_json` and `format_targets_json` establish starting inventory and preferred
  formats.
- `domain_allowlist_json` and `domain_denylist_json` bound domains. The same domain
  cannot appear in both.
- `target_geography_json` accepts `bbox`, GeoJSON-style polygon geometry, place names,
  jurisdictions, routes, and corridors.
- `entity_seeds_json` accepts named entities plus optional type and aliases.
- `max_depth`, `max_pages`, and `max_candidates` bound the campaign.
- `crawl_policy_json` controls network behavior.
- `scoring_weights_json` may override positive score weights; overrides are normalized
  before use.

Example API request:

```bash
curl -X POST http://127.0.0.1:8000/api/discovery/campaigns \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Minnesota transport sources",
    "status": "active",
    "mode": "multi",
    "modes_json": ["query_seeded", "seed_url", "sitemap", "neighborhood", "format_targeted", "geospatial"],
    "query_strings_json": ["Minnesota road closures", "Metro Transit service alerts"],
    "search_templates_json": ["https://search.example.net/search?q={query}&lang={language}"],
    "seed_urls_json": ["https://511mn.org/", "https://www.dot.state.mn.us/"],
    "format_targets_json": ["rss", "json", "geojson", "csv"],
    "language_variants_json": ["en"],
    "target_geography_json": {
      "bbox": [-97.3, 43.4, -89.4, 49.4],
      "jurisdictions": ["Minnesota"],
      "routes": ["I-94", "I-35W"]
    },
    "max_depth": 2,
    "max_pages": 100,
    "max_candidates": 1000,
    "crawl_policy_json": {
      "robots_aware": true,
      "crawl_delay_seconds": 1,
      "max_response_bytes": 5000000
    }
  }'
```

`mode` is a compact primary label. The authoritative multi-mode list is
`modes_json`; run snapshots preserve that full list.

## Network and unattended-run safety

Defaults are conservative:

| Setting | Default |
| --- | ---: |
| Robots-aware | `true` |
| Private/local network fetches | blocked |
| Request timeout | 15 seconds |
| Attempts | 2 |
| Retry backoff | 1 second |
| Maximum response | 5,000,000 bytes |
| Per-domain delay | 1 second |
| Attempts per domain per persisted run | 25 |
| Cross-domain link expansion | blocked |
| Wall-clock run bound | 300 seconds |
| Robots observation TTL | 86,400 seconds |
| Store fetched artifacts | `true` |

Private, loopback, link-local, reserved, multicast, and unspecified addresses are
blocked before a request. Every redirect target is checked again. Hostnames are
resolved once per fetch and connections are pinned to that validated address set while
HTTPS still verifies the original hostname. Ambient proxy environment variables are
disabled for safe discovery fetches. Cross-origin redirects discard authorization,
cookies, API keys, tokens, secrets, referer, and stale host headers. Credentials in target
URLs are rejected. Operators can request `allow_private_networks=true` for a controlled
local fixture or approved internal deployment, but that request remains inert unless the
server owner also sets `ELEVENWRITER_DISCOVERY_ALLOW_PRIVATE_NETWORKS=true`. Literal
private targets remain visibly risky in candidate analysis. That two-part override is
not a stealth tunnel.

Responses are streamed under a hard byte limit. Declared or actual overflow becomes a
dead-letter entry. Retries use bounded backoff and a maximum 60-second `Retry-After`
courtesy delay; fatal safety and size errors do not spin. Frontier state is checkpointed
so a run can resume without rebuilding or corrupting its inventory. Page, candidate, and
per-domain attempt budgets remain attached to that persisted run instead of resetting on
resume. Stale worker claims are recovered, and PostgreSQL frontier claims use row locks
with skip-locked selection. Per-domain policies may further deny domains or paths,
constrain allowed paths/content types, lower size/time limits, or ignore/respect robots
explicitly.

Robots rules are respected by default and observations are persisted. A missing robots
file (`404` or another ordinary non-authentication `4xx`) permits collection. Access
denials, throttling, server errors, DNS failures, and timeouts fail closed until the
persisted robots observation expires. Positive robots rules always block denied targets,
including health/revisit fetches.

Only a response-header allowlist is persisted. Request secrets and custom authorization
headers are redacted from run metadata even though they may be used for an authorized
managed-source request.

## Classification and fingerprints

Analysis recognizes article/news pages, government notices, open-data portals and
datasets, RSS/Atom, APIs and OpenAPI descriptions, sitemaps, JSON/JSONL/XML/CSV/GeoJSON
endpoints, KML, PDF/TXT, repeating status pages, camera pages and image/stream endpoints,
social/profile references, and historical archives.

Each result includes:

- canonical and discovered URL, normalized domain, path pattern, parent, method, run,
  and campaign;
- format, geo, temporal, structural, trust, and operational hints;
- title/text excerpt and outbound link evidence;
- content hash plus a schema fingerprint based on field/tag structure rather than
  volatile values;
- last-seen, last-changed, last-checked, next-revisit, failure, and disposition state.

The parser reads at most 4 MiB and caps link, JSON-node, coordinate, and text extraction.
The network fetch limit is separately configurable and is enforced before analysis.

## Geospatial ranking

Candidate footprints are estimated as `point`, `route`, `local_area`, `jurisdiction`,
`nationwide`, `global`, or `unknown`. Exact bbox/polygon point intersection scores
highest. Route, jurisdiction, and place overlap follows. Generic nationwide/global
coverage receives less credit than a local intersecting source. A source with explicit
but non-intersecting geography is penalized.

This is deliberately not a GIS thesis or a hidden geocoding service. Name matching is
deterministic. Operators get better ranking by supplying normalized names, routes,
coordinates, or geometry in campaign targets and entity seeds.

## Score model and dispositions

Raw components use a 0–100 scale:

| Positive component | Weight |
| --- | ---: |
| Relevance | 0.18 |
| Geospatial relevance | 0.18 |
| Temporal usefulness | 0.10 |
| Structural usefulness | 0.12 |
| Freshness | 0.10 |
| Stability | 0.10 |
| Trust/integrity hints | 0.12 |
| Novelty | 0.10 |

Operational cost deducts `0.10 * raw_cost`; redundancy deducts
`0.15 * raw_redundancy`. The final score is clamped to 0–100. Stored explanations
include all raw components, weights, deductions, evidence, and plain-language reasons.

Default thresholds are:

- `promote_now`: score at least 72 and a runnable/recommended mapping exists
- `keep_candidate`: score at least 52
- `revisit_later`: score at least 32
- `ignore`: below 32, reference-only social profiles, or no useful mapping
- `quarantine`: private/unsafe target, blocked trust, or a hard health/safety signal

Safety disposition wins over arithmetic. A score is evidence for an operator decision,
not an oracle with a blazer on.

Canonical candidates are global, but campaign observations remain separate revisions.
The current candidate retains the best campaign-owned score and its campaign/run/time
provenance; a later unrelated geography cannot silently downgrade it. Every later score,
including a lower one, remains inspectable in candidate revision history.

## Promotion behavior

Promotion reuses the managed source for a canonical candidate. Repeating the operation
updates its discovery evidence and reuses the existing source-sync schedule instead of
creating duplicates. Every decision remains a separate audit record.

| Candidate evidence | Recommended kind | Runtime behavior |
| --- | --- | --- |
| JSON, GeoJSON, OpenAPI | `http_json` | Runnable |
| JSON Lines / NDJSON | `http_jsonl` | Runnable |
| XML, KML, sitemap | `http_xml` | Runnable |
| RSS or Atom | `rss` | Runnable |
| CSV | `http_csv` | Runnable |
| ArcGIS FeatureServer query JSON | `arcgis_feature_json` | Runnable |
| CKAN `package_search` catalog JSON | `ckan_package_search` | Runnable |
| Plain text | `http_text` | Runnable |
| Search results | `web_search` | Runnable bounded materialization |
| Article/general HTML | `web_crawl` | Runnable bounded materialization |
| API docs, camera page, other discovery document | `web_discovery` | Runnable bounded materialization |
| WebSocket | `websocket_stream` | Persisted reference only |
| Server-sent events | `sse_stream` | Persisted reference only |
| Webhook description | `webhook_ingest` | Persisted reference only |
| Camera image endpoint | `camera_image` | Reference only; camera-source inventory updated |
| Camera stream endpoint | `camera_stream` | Reference only; camera-source inventory updated |
| Social/profile page | none | Reference evidence only; never auto-promoted |

Only enabled runnable kinds receive a `source_sync` schedule. Reference-only definitions
are persisted disabled with `runtime_support=reference_only`. This avoids pretending a
batch HTTP importer can consume a WebSocket, webhook, or camera stream.

Promotion records the candidate score and breakdown, trust and integrity reasoning,
health risks, geo evidence, revision count, source ID, schedule choice, and custody
events. Suppressed or quarantined candidates cannot be promoted until their disposition
is deliberately resolved. Operator-supplied promotion metadata is stored beneath
`operator_metadata`; it cannot replace enforced private-network blocking, response
limits, retries, or inject runtime credential headers.

## CLI workflow

Create a campaign:

```bash
elevenwriter create-discovery-campaign "mn-transport" \
  --mode multi \
  --discovery-mode seed_url \
  --discovery-mode sitemap \
  --discovery-mode neighborhood \
  --discovery-mode format_targeted \
  --discovery-mode geospatial \
  --seed https://511mn.org/ \
  --format-target json \
  --format-target rss \
  --geo-json '{"bbox":[-97.3,43.4,-89.4,49.4],"routes":["I-94"]}' \
  --policy-json '{"robots_aware":true,"crawl_delay_seconds":1}' \
  --max-depth 2 --max-pages 100
```

Run and inspect it:

```bash
elevenwriter add-source-web-search search-seed "https://search.example.net/search?q=mn+transport" alert-feed
elevenwriter add-source-web-crawl alert-page https://example.com/alerts alert-feed
elevenwriter add-source-web-discovery api-docs https://example.com/developer/api alert-feed
elevenwriter run-discovery 1 --max-pages 25
elevenwriter list-discovery-campaigns
elevenwriter show-discovery-campaign 1
elevenwriter list-discovery-runs --campaign-id 1
elevenwriter list-discovery-candidates --campaign-id 1 --min-score 50
elevenwriter show-discovery-candidate 12
elevenwriter explain-discovery-candidate 12
elevenwriter show-discovery-lineage 12
elevenwriter update-discovery-campaign 1 --status paused --enabled false --crawl-policy-json '{"max_concurrency":4}'
elevenwriter list-discovery-domain-policies --domain 511mn.org
elevenwriter upsert-discovery-domain-policy 511mn.org --policy allow --robots-mode respect --max-concurrency 2
elevenwriter update-discovery-domain-policy 511mn.org --policy deny --robots-mode ignore --enabled false --notes "Temporarily blocked"
```

Disposition and operations:

```bash
elevenwriter promote-discovery-candidate 12 "Verified official GeoJSON endpoint" \
  --source-kind http_json --layer road-events --schedule-interval-seconds 1800
elevenwriter suppress-discovery-candidate 13 "Duplicate mirror" \
  --reason-code redundant_mirror
elevenwriter revisit-discovery --candidate-id 12 --force
elevenwriter list-failing-discovery-candidates --stale-after-hours 24
elevenwriter show-discovery-ops --stale-after-hours 24
elevenwriter export-discovery-summary ./exports/discovery.json
elevenwriter diff-discovery-inventories 4 7 --campaign-id 1
elevenwriter add-discovery-schedule mn-discovery 1 3600 --max-pages 50
elevenwriter add-discovery-health-scan-schedule mn-discovery-health 3600 --campaign-id 1 --limit 100
elevenwriter add-discovery-revisit-schedule mn-discovery-revisit 3600 --campaign-id 1 --force
```

Use `run-discovery --dry-run` to persist and inspect the run/frontier request without
fetching pages. Use small `--max-pages` values to checkpoint an unfamiliar campaign,
review the resulting inventory, and then resume it.

## API workflow

Core routes are mounted under `/api/discovery`:

| Method and route | Purpose |
| --- | --- |
| `POST /campaigns` | Create durable campaign intent |
| `GET /campaigns`, `GET /campaigns/{id}` | List and inspect campaigns |
| `PATCH /campaigns/{id}` | Update policy, seeds, scope, or lifecycle state |
| `POST /campaigns/{id}/run` | Start or resume a bounded run |
| `GET /runs`, `GET /runs/{id}` | Inspect runs, frontier, revisions, and candidates |
| `GET /candidates`, `GET /candidates/{id}` | Filter and inspect candidate inventory |
| `GET /candidates/{id}/score` | Explain score components and reasons |
| `GET /candidates/{id}/lineage` | Reconstruct campaigns, runs, parents, and children |
| `POST /candidates/{id}/promote` | Create/update a managed source and optional schedule |
| `POST /candidates/{id}/suppress` | Persist candidate/domain suppression reasoning |
| `POST /candidates/{id}/health` | Fetch and persist one health/revision observation |
| `POST /candidates/{id}/revisit` | Queue a candidate for frontier revisit |
| `POST /health-scan` | Scan candidate/domain/campaign health |
| `POST /revisit` | Queue candidate/domain/campaign inventory revisits |
| `GET /health`, `GET /ops` | Read readiness and operational summaries |
| `GET /export/summary` | Export campaign/run/candidate/promotion inventory |
| `GET /inventory-diff` | Compare candidate state between two runs |
| `GET`, `PUT /domain-policies` | Inspect or upsert crawl policy |
| `PATCH /domain-policies/{normalized_domain}` | Update an existing persisted domain policy |

Example run and promotion:

```bash
curl -X POST http://127.0.0.1:8000/api/discovery/campaigns/1/run \
  -H "Content-Type: application/json" \
  -d '{"resume":true,"max_pages":25,"max_candidates":250,"max_seconds":120}'

curl http://127.0.0.1:8000/api/discovery/candidates/12/score
curl http://127.0.0.1:8000/api/discovery/candidates/12/lineage

curl -X POST http://127.0.0.1:8000/api/discovery/candidates/12/promote \
  -H "Content-Type: application/json" \
  -d '{
    "source_kind":"http_json",
    "layer_key":"road-events",
    "create_schedule":true,
    "schedule_interval_seconds":1800,
    "reason":"Verified official structured endpoint"
  }'
```

## Scheduler, alerts, and health

`discovery_campaign` scheduled tasks call the same resume-aware run service as CLI/API.
Task output persists run ID, page and candidate counters, errors, frontier counts, and
candidate IDs. The scheduler also accepts `discovery_health_scan` and
`discovery_revisit` tasks with the same filter payloads used by their API request
schemas, and the CLI exposes `add-discovery-health-scan-schedule` plus
`add-discovery-revisit-schedule` so operators do not have to handcraft generic
scheduler payload JSON every time. Candidate promotion can create `source_sync` tasks
for runnable sources.

Discovery emits deduplicated alerts for:

- high-value new candidates;
- excessive junk ratios or a single-domain candidate spike;
- individual or spike-level robots blocks;
- candidate schema or endpoint content changes;
- candidates becoming unreachable or reachable again;
- stale or failing promoted sources;
- an official source disappearing while an available fallback candidate exists.

Health checks append history and update current reachability, content/schema hashes,
failure count, next revisit time, and camera endpoint state. Failures use increasing
revisit backoff. `/api/discovery/health` reports running campaigns, reachable/failing/
stale candidates, queued and dead-letter frontier state, robots blocks, and warnings.
`/api/discovery/ops` adds inventory buckets plus failing, stale, and due candidates.

## Artifacts, custody, export, and restore

When artifact storage is enabled, each fetched document is written beneath the local
data directory and registered in both `discovery_artifacts` and the storage-object
ledger. Metadata preserves source URL, hashes, media type, size, request attempt count,
response headers, run, frontier entry, and candidate. The default expiry is 30 days;
the normal storage lifecycle can promote, archive, or expire it. For managed files that
live beneath Forte's configured local `data_dir`, archive transitions move the bytes
into a storage-archive subtree and expiry deletes the managed file while preserving the
action in custody and storage metadata.

Campaign creation/update, run start/resume/checkpoint/completion, frontier failures,
alerts, promotion, suppression, revisit, schedules, and health changes all add custody
records.

Runtime snapshots include every discovery table listed above, including frontier
checkpoints, revisions, graph edges, robots observations, health, promotion/suppression,
and artifact links. Restore validates the snapshot section contracts and inserts tables
in dependency order. Use the JSON snapshot commands for metadata-only database export,
or the runtime bundle commands when you need the managed local `data_dir` bytes as
well:

```bash
elevenwriter export-runtime-snapshot ./exports/runtime.json
elevenwriter restore-runtime-snapshot ./exports/runtime.json --replace-existing
elevenwriter export-runtime-bundle ./exports/runtime-bundle.zip
elevenwriter restore-runtime-bundle ./exports/runtime-bundle.zip --replace-existing
```

## Known limitations and intentionally skipped adapters

- There is no bundled whole-web search provider. Search templates are operator-owned.
- No JavaScript renderer or browser farm is used. JS-only pages are marked costly and
  may need a structured endpoint found in page metadata or an operator-supplied seed.
- PDF analysis extracts metadata and URL references but does not perform OCR or full PDF
  layout extraction.
- Named geography matching is deterministic and gazetteer-free. No external geocoder is
  called.
- Robots access denial, throttling, server failure, DNS failure, and timeout are persisted
  and fail closed; an ordinary missing-file `4xx` is treated as no robots policy.
- WebSocket, SSE, webhook, and camera stream/image definitions are reference-only until
  a kind-specific runtime is deliberately implemented.
- Social/profile pages are references, not a promise of full social ingestion.
- CKAN `package_search` and ArcGIS FeatureServer query endpoints are wired into the
  managed source runtime with bounded pagination. Broader registry adapters such as
  Socrata, ArcGIS Hub catalogs, APIs.guru OpenAPI Directory, Transitland Atlas, and
  other licensing-aware registries are still intentionally not wired in. Their catalogs
  can be used today as bounded campaign seeds without vendoring giant crawlers that
  would undermine this subsystem's local-first and bounded operating model.
- The engine does not bypass authentication, paywalls, access controls, or robots rules.
  It has no dark-web mode, credential guessing, or “scan everything forever” switch.
- JSON runtime snapshots preserve artifact and storage-ledger metadata, not fetched
  artifact bytes themselves. Use the runtime bundle workflow when byte-faithful recovery
  of the managed local `data_dir` is required.
- Crawling remains intentionally conservative, but it is no longer strictly single-file.
  PostgreSQL still protects frontier row claims and Forte records a per-campaign
  active-run lease with heartbeat and stale-lease recovery so overlapping
  scheduler/manual resumes fail closed instead of racing the same run. Within one run,
  `max_concurrency` now enables bounded parallel HTTP fetches while preserving
  per-domain pacing and the persisted frontier as the source of truth. There is still no
  horizontal crawler fleet.
- A temporary suppression expiry restores candidate eligibility and history, but it does
  not automatically re-enable a managed source that the suppression disabled. Re-promote
  with `enabled=true` after review to resume that schedule deliberately.
- The API has no built-in multi-user authentication boundary. Docker publishes it on
  loopback by default; external exposure requires an authenticated reverse proxy or an
  otherwise trusted network.
