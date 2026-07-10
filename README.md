# 11Writer Forte

11Writer Forte is the backend-only continuation of 11Writer: a geospatial-first OSINT data platform for ingesting intersecting public-source data, preserving provenance, and supporting headless collection, fusion, alerting, and export workflows.

The upstream `MN755/11Writer` repository does contain a much larger camera/webcam stack. This local Forte workspace does not currently carry that upstream subsystem wholesale. See [UPSTREAM_CAMERA_INVENTORY.md](UPSTREAM_CAMERA_INVENTORY.md) for the exact upstream camera paths and the current local gap.

This repo intentionally removes the frontend runtime. The only operator-facing interface is a custom CLI plus the API surface.

## Workspace location note

Keep the writable git checkout for this repo in a normal local path such as `C:\Repos\11Writer Forte`.

Avoid cloud-synced folders for the primary checkout. Git worktree metadata and lock files can fail there on Windows, which breaks normal commits and makes worktree automation unreliable.

## What exists here

- FastAPI runtime for event, layer, geofence, alert, import, and trust-management workflows
- Persisted scheduler runtime for unattended imports, geofence scans, source syncs, camera inventory refresh, entity resolution, event fusion, and integrity-source seed tasks
- Observation query and rule-based cross-verification surfaces for spatial filtering and corroboration
- Rule-based entity resolution that links observations into reusable entity records
- Event fusion materialization plus exportable cited summaries and rule-based reports
- Managed source definitions with persisted source-run history and scheduler-driven sync hooks
- Managed source adapters now cover local files plus HTTP JSON, JSONL, text, XML, CSV, RSS/Atom, ArcGIS feature payloads, and CKAN package search catalogs
- Bounded, geospatial-first source discovery with persisted campaigns, checkpointed frontier runs, globally deduplicated candidates, revisions, lineage, scoring, health, suppression, and managed-source promotion
- Camera inventory materialization that turns imported/public traffic camera observations into persisted geospatial camera records with provenance
- Camera source inventory lifecycle that graduates observed camera endpoints into a backend-native candidate registry with rule-based readiness scoring
- Storage-object ledger that tracks retained artifacts, retention class, lifecycle state, and provenance for imports and camera-derived references
- SQLAlchemy storage foundation that runs on SQLite for local development and Postgres/PostGIS-oriented URLs for deployment
- Optional ClickHouse analytics/archive backend that can mirror runtime facts and archive observation data to Cloudflare R2 over the S3-compatible API
- PostGIS-aware spatial query path that persists WKT alongside GeoJSON and automatically provisions spatial indexes on PostgreSQL
- Local import pipeline for JSON, JSONL, TXT, and SQLite inputs with row-level dedupe inside each layer
- Rule-based domain trust and integrity source seeding
- Chain-of-custody logging for imports and system actions
- Dockerized backend deployment path for Windows, macOS, and Linux hosts

## Layout

```text
app/
  server/
    src/
    tests/
docker-compose.yml
```

## Quick start

```bash
cd app/server
python -m venv .venv
# PowerShell
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .[dev]
elevenwriter init-db
elevenwriter seed-integrity
elevenwriter add-trust-profile example.gov --trust-level trusted --approval-policy auto_approve_stable --integrity-source
elevenwriter update-trust-profile 1 --trust-level blocked --approval-policy always_review --notes "Escalated for analyst review"
uvicorn src.main:app --reload --port 8000
```

MnDOT live-feed notes and source-ingestion examples live in [MNDOT_FEEDS.md](MNDOT_FEEDS.md).

Upstream camera/webcam inventory and local parity notes live in [UPSTREAM_CAMERA_INVENTORY.md](UPSTREAM_CAMERA_INVENTORY.md).

Source discovery architecture, safety defaults, scoring, promotion, and operator workflows live in [docs/source-discovery.md](docs/source-discovery.md).

## Codex Research Agent

Forte now has a local, headless Codex research adapter. Its MCP server exposes only read-only local evidence tools: runtime inventory, sources, alerts, events, recent observations, and custody records. The scheduler, source ingestion, alerting, storage lifecycle, and outbound delivery stay rule/code-driven.

```bash
cd app/server
.\.venv\Scripts\Activate.ps1
$env:PYTHONPATH = (Get-Location).Path
elevenwriter configure-codex-agent
elevenwriter research "Summarize the current local evidence for open alerts" --model gpt-5.4-mini --reasoning-effort medium
```

`research` runs `codex exec` with a read-only sandbox and no approval prompts. It retains a completed briefing under `var/agent_reports`, registers it in the local storage ledger, and writes a custody event. `ELEVENWRITER_CODEX_MODEL` and `ELEVENWRITER_CODEX_REASONING_EFFORT` set defaults; pass the desired Codex model string explicitly to use a different approved model.

## CLI

```bash
elevenwriter status
elevenwriter doctor
elevenwriter show-codex-mcp-command
elevenwriter configure-codex-agent
elevenwriter research "Summarize the current local evidence for open alerts" --model gpt-5.4-mini --reasoning-effort medium
elevenwriter init-db
elevenwriter seed-integrity
elevenwriter add-trust-profile example.gov --trust-level trusted --approval-policy auto_approve_stable --integrity-source
elevenwriter update-trust-profile 1 --trust-level blocked --approval-policy always_review --notes "Escalated for analyst review"
elevenwriter trust-profiles
elevenwriter add-layer marine-track "Marine Track" --temporal-resolution live --data-latency low
elevenwriter list-layers
elevenwriter add-geofence "Port Watch" "{\"type\":\"Polygon\",\"coordinates\":[[[-96.0,29.0],[-94.0,29.0],[-94.0,31.0],[-96.0,31.0],[-96.0,29.0]]]}" --rule-expression "observation enters polygon"
elevenwriter list-geofences
elevenwriter import-local path/to/file.json --layer incident-feed
elevenwriter list-imports
elevenwriter add-source-file harbor-source ./feeds/harbor.json marine-track
elevenwriter add-source-http-json remote-feed https://example.com/feed.json remote-track --retry-attempts 3 --no-skip-unchanged --header "Authorization: Bearer token"
elevenwriter add-source-http-jsonl remote-jsonl https://example.com/feed.jsonl remote-track
elevenwriter add-source-http-csv remote-csv https://example.com/feed.csv traffic-camera-feed
elevenwriter add-source-rss port-alerts https://example.com/alerts.xml alert-feed
elevenwriter add-source-arcgis-feature-json city-arcgis "https://example.com/arcgis/rest/services/Cameras/FeatureServer/0/query?where=1%3D1&outFields=*&f=json" traffic-camera-feed
elevenwriter add-source-ckan-package-search state-catalog "https://data.example.gov/api/3/action/package_search?q=traffic" catalog-feed
elevenwriter add-source-web-search search-seed "https://search.example.net/search?q=port+alerts" alert-feed
elevenwriter add-source-web-crawl alert-page https://example.com/alerts alert-feed
elevenwriter add-source-web-discovery api-docs https://example.com/developer/api alert-feed
elevenwriter list-sources
elevenwriter update-source 1 --enabled false --notes "Disabled for review"
elevenwriter run-source 1
elevenwriter list-source-runs
elevenwriter create-discovery-campaign mn-transport --mode multi --discovery-mode seed_url --discovery-mode sitemap --discovery-mode neighborhood --seed https://511mn.org/ --max-depth 2 --max-pages 100
elevenwriter run-discovery 1 --max-pages 25
elevenwriter list-discovery-candidates --campaign-id 1 --min-score 50
elevenwriter explain-discovery-candidate 1
elevenwriter show-discovery-lineage 1
elevenwriter update-discovery-campaign 1 --status paused --enabled false --crawl-policy-json "{\"max_concurrency\":4}"
elevenwriter list-discovery-domain-policies --domain 511mn.org
elevenwriter upsert-discovery-domain-policy 511mn.org --policy allow --robots-mode respect --max-concurrency 2
elevenwriter update-discovery-domain-policy 511mn.org --policy deny --robots-mode ignore --enabled false --notes "Temporarily blocked"
elevenwriter promote-discovery-candidate 1 "Verified official endpoint" --source-kind http_json --layer road-events --schedule-interval-seconds 1800
elevenwriter revisit-discovery --candidate-id 1 --force
elevenwriter show-discovery-ops --stale-after-hours 24
elevenwriter export-discovery-summary ./exports/discovery-summary.json
elevenwriter add-discovery-schedule mn-discovery 1 3600 --max-pages 50
elevenwriter add-discovery-health-scan-schedule mn-discovery-health 3600 --campaign-id 1 --limit 100
elevenwriter add-discovery-revisit-schedule mn-discovery-revisit 3600 --campaign-id 1 --force
elevenwriter show-source-ops 1
elevenwriter show-source-summary --stale-after-hours 24
elevenwriter show-source-report-index --stale-after-hours 24
elevenwriter export-source-summary ./exports/source-summary.json --source-limit 500 --report-limit 25
elevenwriter materialize-cameras --layer traffic-camera-feed
elevenwriter list-cameras --layer traffic-camera-feed --active true
elevenwriter materialize-camera-sources --layer traffic-camera-feed
elevenwriter list-camera-sources --layer traffic-camera-feed --status ready
elevenwriter show-camera-source-summary --layer traffic-camera-feed
elevenwriter show-camera-source-ops 1
elevenwriter show-camera-source-report-index --layer traffic-camera-feed --source-domain cams.example.com
elevenwriter export-camera-source-summary ./exports/camera-source-summary.json --layer traffic-camera-feed
elevenwriter show-camera-summary --layer traffic-camera-feed --stale-after-hours 24
elevenwriter show-camera-ops 1
elevenwriter show-camera-report-index --layer traffic-camera-feed --source-domain cams.example.com
elevenwriter export-camera-summary ./exports/camera-summary.json --layer traffic-camera-feed
elevenwriter add-camera-refresh-schedule mndot-camera-refresh 300 --layer traffic-camera-feed --source-domain 511mn.org --limit 1000
elevenwriter list-storage-objects --owner-type camera_inventory --retention-class operational
elevenwriter add-storage-object manual:casefile:1 report_export event 42 file:///tmp/casefile-42.json --storage-tier hot --retention-class investigative
elevenwriter promote-storage-object 1 event 42 --storage-tier archive --retention-class permanent
elevenwriter transition-storage-object 1 archived --storage-tier archive
elevenwriter show-clickhouse-status
elevenwriter provision-clickhouse
elevenwriter sync-clickhouse --layer marine-track --limit 5000
elevenwriter archive-clickhouse-observations --layer marine-track --limit 5000
elevenwriter rehydrate-clickhouse-observations "https://<ACCOUNT_ID>.r2.cloudflarestorage.com/11writer-archive/11writer-archive/observations/layer=*/date=*/*.parquet"
elevenwriter show-clickhouse-r2-config
elevenwriter write-clickhouse-r2-config
elevenwriter query-observations --backend clickhouse --layer marine-track --limit 500
elevenwriter cross-verify --backend r2_archive --bbox "-96,29,-94,31" --time-window-minutes 120 --distance-km 10
elevenwriter add-clickhouse-sync-schedule clickhouse-sync 900 --layer marine-track --limit 5000
elevenwriter add-clickhouse-archive-schedule clickhouse-archive 3600 --layer marine-track --limit 50000
elevenwriter add-entity-resolution-schedule nightly-entities 600 --bbox "-96,29,-94,31" --min-observations 2
elevenwriter add-event-fusion-schedule nightly-fusion 600 --bbox "-96,29,-94,31" --distance-km 10 --time-window-minutes 120
elevenwriter query-observations --bbox "-96,29,-94,31"
elevenwriter cross-verify --bbox "-96,29,-94,31" --distance-km 10
elevenwriter resolve-entities --bbox "-96,29,-94,31" --min-observations 2
elevenwriter list-entities
elevenwriter show-entity-observations 1
elevenwriter create-event analyst-note-1 "Analyst Note Event" --summary "Operator-created event for tracking" --redaction-level restricted --metadata-json "{\"case_id\":\"AN-1\"}"
elevenwriter fuse-events --bbox "-96,29,-94,31" --distance-km 10
elevenwriter list-events
elevenwriter show-event-products 1
elevenwriter export-event-product 1 cited_summary ./exports/event-1-summary.txt --max-redaction-level public
elevenwriter export-event-bundle 1 ./exports/event-1-bundle.json --max-redaction-level public
elevenwriter export-runtime-snapshot ./exports/runtime-snapshot.json
elevenwriter export-runtime-bundle ./exports/runtime-bundle.zip
elevenwriter restore-runtime-snapshot ./exports/runtime-snapshot.json --replace-existing
elevenwriter restore-runtime-bundle ./exports/runtime-bundle.zip --replace-existing
elevenwriter add-source-sync-schedule nightly-sync 1 300 --retry-attempts 3 --retry-backoff-seconds 5
elevenwriter add-integrity-seed-schedule nightly-integrity 86400
elevenwriter add-geofence-scan-schedule nightly-watch 300 --geofence-id 1
elevenwriter update-schedule 1 --enabled false --notes "Paused for maintenance"
elevenwriter scheduler-worker --poll-seconds 5
elevenwriter list-schedules
elevenwriter list-schedule-runs
elevenwriter create-alert "Manual analyst alert" --event-id 1 --severity warning --trigger-basis-json "{\"origin\":\"cli\"}"
elevenwriter list-alerts --status open --event-id 1
elevenwriter update-alert 1 acknowledged --disposition-note "Reviewed by operator"
elevenwriter run-due-schedules
elevenwriter list-custody
elevenwriter list-custody --object-type source_trust_profile --action source_trust_profile_updated --limit 20
elevenwriter show-scheduler-summary
elevenwriter show-scheduler-report-index --limit 25
elevenwriter export-scheduler-summary ./exports/scheduler-summary.json --task-limit 500 --report-limit 25
```

## Docker

```bash
docker compose up --build
docker compose --profile clickhouse up --build
```

By default the compose stack starts the API on `127.0.0.1:8000`, a scheduler worker, and PostGIS-ready Postgres. The backend does not provide a multi-user authentication boundary; only override `ELEVENWRITER_API_BIND` behind an authenticated reverse proxy or on an otherwise trusted network. The optional `clickhouse` profile starts a self-hosted ClickHouse server on `8123`/`9000`; Forte will only use it if you also set the ClickHouse env vars below. The compose file also mounts [`app/server/11writer-r2-storage.xml`](app/server/11writer-r2-storage.xml), and `elevenwriter write-clickhouse-r2-config` now targets that mounted file automatically when you run it from the repo root, so the generated R2 disk policy lands where Docker actually reads it.

### Optional ClickHouse + R2 env

```bash
ELEVENWRITER_CLICKHOUSE_ENABLED=true
ELEVENWRITER_CLICKHOUSE_URL=http://127.0.0.1:8123
ELEVENWRITER_CLICKHOUSE_DATABASE=elevenwriter
ELEVENWRITER_CLICKHOUSE_USER=default
ELEVENWRITER_CLICKHOUSE_PASSWORD=
ELEVENWRITER_CLICKHOUSE_R2_ENDPOINT=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
ELEVENWRITER_CLICKHOUSE_R2_BUCKET=11writer-archive
ELEVENWRITER_CLICKHOUSE_R2_ACCESS_KEY_ID=<R2_ACCESS_KEY_ID>
ELEVENWRITER_CLICKHOUSE_R2_SECRET_ACCESS_KEY=<R2_SECRET_ACCESS_KEY>
ELEVENWRITER_CLICKHOUSE_R2_REGION=auto
ELEVENWRITER_CLICKHOUSE_R2_ARCHIVE_PREFIX=11writer-archive
ELEVENWRITER_CLICKHOUSE_R2_STORAGE_MODE=archive_only
ELEVENWRITER_CLICKHOUSE_R2_STORAGE_BUCKET=11writer-archive
ELEVENWRITER_CLICKHOUSE_R2_STORAGE_PREFIX=11writer-clickhouse
ELEVENWRITER_CLICKHOUSE_R2_STORAGE_POLICY=r2_main
ELEVENWRITER_CLICKHOUSE_R2_CACHE_SIZE=10Gi
```

### ClickHouse + R2 modes

- `archive_only`: keep ClickHouse local and use R2 for Parquet archive export only.
- `hybrid`: keep hot tables local, archive to R2, and rehydrate/query R2 Parquet when needed.
- `r2_disk`: provision ClickHouse tables with the configured R2 storage policy. Use this only after you generate the mounted config with `elevenwriter write-clickhouse-r2-config` and restart the ClickHouse container.

## Design notes

- Events are the primary operational object.
- Data layers stay distinct from events so multiple layers can intersect around the same event.
- Data layers are auto-registered during ingest and source onboarding, so the layer catalog tracks the runtime instead of drifting out of date.
- Observations preserve raw text or raw structured content alongside extracted location and trust metadata.
- Trust scoring is rule-based first, seeded with starter integrity sources such as the New York Times, NPR, BBC, and Smithsonian.
- SQLite remains supported for local ingestion inputs and lightweight runtime mode, but primary backend storage targets Postgres/PostGIS deployment.
- Postgres runtime now auto-enables `postgis` plus GiST expression indexes for observation points and geofence geometries, while SQLite keeps the Python fallback path for local runs and tests.
- The headless CLI now includes a `doctor` command and the API exposes `/api/operations/database`, so operators can audit connectivity, additive schema drift, table counts, and PostGIS readiness without freestyling SQL in production.
- Runtime backup and recovery now have a first-class path too: `/api/operations/runtime/export`, `/api/operations/runtime/restore`, `/api/operations/runtime/bundle/export`, `/api/operations/runtime/bundle/restore`, and matching CLI commands serialize the core backend state in dependency-safe order, preserve byte-faithful managed `data_dir` bundles when needed, and log custody records for both export and restore.
- Runtime snapshot coverage now extends across newer operational subsystems too, including scheduler task/run state, source-run history, camera/source registries, storage objects, and maintenance-task lineage, so recovery testing is following the real backend instead of freezing at an older shape.
- Source discovery is durable infrastructure now: campaigns, bounded/checkpointed frontier entries, canonical candidates, revisions, graph lineage, robots observations, health, suppressions, promotion decisions, and fetched-artifact metadata all persist and round-trip through runtime snapshots. See [docs/source-discovery.md](docs/source-discovery.md) for the operator contract and its honest limitations.
- Discovery maintenance is scheduler-native now too: campaign runs, candidate health scans, and scoped revisit queues can all be scheduled headlessly, so source inventory does not rot the second an operator stops typing.
- The storage-core slice is real now, not a manifesto: `/api/storage/objects` plus the `list-storage-objects`, `add-storage-object`, `promote-storage-object`, and `transition-storage-object` CLI commands expose a first-class artifact ledger with retention classes, tiering, and lifecycle controls.
- Lifecycle changes now act on owned local bytes too: when a managed storage object under the configured `data_dir` is archived, Forte moves it into a durable archive subtree; when it expires, Forte deletes the managed file and preserves that action in custody metadata instead of only flipping a row state.
- Imports and camera materialization now auto-register storage manifests, so raw local files plus camera image/stream/page references get tracked as storage objects with expiration windows and custody events instead of disappearing into the void.
- Managed sources now participate in that same storage/provenance model too: each source run materialization is tracked as a storage object, and `/api/sources/{id}/ops` plus `show-source-ops` expose recent runs, stored payload artifacts, and related custody logs in one place.
- Managed sources now have a fleet-level ops surface too: `/api/sources/summary` and `/api/sources/report-index` expose stale sources, failing runs, schedule coverage, and recent sync activity so operators can see source health across the whole catalog instead of auditing one source at a time.
- Source fleet reporting is exportable now too: `/api/sources/export/summary` and `export-source-summary` package the source catalog plus fleet health report into a JSON artifact that registers in the storage/provenance ledger like the camera and scheduler exports.
- Operator exports now participate in that same storage/provenance model too: event bundles, exported products, camera summary exports, operations reports, and runtime snapshots all register as storage objects when written through the headless export path instead of falling off the ledger.
- ClickHouse is now an optional secondary backend instead of a hand-wavy future idea: `/api/operations/clickhouse` exposes diagnostics, provisioning, runtime sync, R2 archive export, R2 rehydration, and R2 storage-config preview, while the CLI mirrors those same flows for headless ops.
- Forte still keeps PostgreSQL/SQLite as the primary operational store. ClickHouse is wired for analytics, cold archive, and large-scale query workloads; pretending it fully replaces the relational runtime here would be unserious.
- Cloudflare R2 support now covers three operator paths: `archive_only` export into partitioned Parquet, `hybrid` rehydrate/query workflows over R2 data, and optional `r2_disk` provisioning for self-hosted ClickHouse storage policies backed by R2.
- Observation reads can now hit those optional backends too: `/api/observations` and `/api/observations/cross-verify` accept `backend=clickhouse` for synced hot tables or `backend=r2_archive` for direct archived Parquet reads over Cloudflare R2 when the operator needs cold-data investigation without bulk rehydration first.
- ClickHouse maintenance is scheduler-native now too: `clickhouse_sync` and `clickhouse_archive` tasks can mirror runtime facts and archive observation partitions toward R2 without waiting for an operator to remember the command at 2 AM.
- Camera/webcam work is no longer just notes: `/api/cameras` and `/api/cameras/materialize` now persist camera inventory from imported observations, including MnDOT-style feeds that expose image or stream endpoints plus geospatial coordinates.
- Camera endpoint lifecycle is backend-native now too: `/api/camera-sources`, `/api/camera-sources/materialize`, `/api/camera-sources/summary`, and `/api/camera-sources/{id}/ops` maintain a candidate source registry for observed camera image/stream/page endpoints, with rule-based graduation scores and custody history.
- Camera source candidate ops now have a fleet-level backend surface too: `/api/camera-sources/report-index` summarizes refresh-task coverage, recent source materializations, and stale candidate endpoints, while `/api/camera-sources/export/summary` emits a JSON-ready artifact for downstream systems and archival.
- Camera ops now have a proper backend reporting surface too: `/api/cameras/summary` rolls up fleet health by layer, domain, provider, and status, while `/api/cameras/{id}/ops` exposes per-camera custody, latest observation/import context, and matching refresh schedules.
- Camera reporting is exportable now too: `/api/cameras/report-index` summarizes refresh task coverage, recent materializations, stale inventory, and recent refresh runs, while `/api/cameras/export/summary` emits a JSON-ready artifact for downstream systems and archival.
- Camera inventory upkeep is scheduler-native now too, so the registry can be refreshed headlessly with `camera_inventory_refresh` tasks instead of waiting for an operator to remember the manual materialization command. Those refresh runs also keep the camera source candidate registry in sync automatically.
- The platform-wide operations report now includes source inventory and source sync health sections too, so source fleet issues show up alongside camera, alert, import, and scheduler activity in one backend report.
- The platform-wide operations report now carries camera inventory, camera source candidate inventory, and camera refresh sections too, so one headless report can show both event/alert activity and the current health of the camera subsystem.
- The platform-wide operations report now also carries storage lifecycle inventory plus ClickHouse backend diagnostics, so one headless report can expose artifact retention state and optional analytics-backend health instead of making operators hop across multiple commands.
- The scheduler finally has a fleet-level ops surface too: `/api/scheduler/summary`, `/api/scheduler/report-index`, `show-scheduler-summary`, and `show-scheduler-report-index` expose overdue tasks, failing latest runs, maintenance coverage, and task-type run/failure buckets instead of forcing operators to reverse-engineer health from raw run rows.
- Scheduler reporting is exportable now too: `/api/scheduler/export/summary` and `export-scheduler-summary` package the scheduler fleet report plus task inventory into a JSON artifact that registers in the storage/provenance ledger like the rest of the backend exports.
- Entity resolution and event fusion are scheduler-native too, so the backend can keep promoting raw observations into reusable entities, linked events, and generated products without a human sitting there pressing the button like it's 2009.
- Geofence scans create persisted alerts with dedupe keys so the same observation-hit pair does not spam duplicates.
- Scheduler runs and import operations both write custody records so unattended execution still leaves an audit trail.
- Cross-verification is rule-based today: it clusters nearby observations inside a time window and raises confidence when independent domains or layers corroborate the same occurrence.
- Verification outputs now include trusted-observation counts, integrity-source counts, ground-truth hits, and observed-time spans so operators can judge corroboration quality without spelunking raw rows.
- Entity resolution is rule-based today: it extracts stable identifiers such as vessel MMSI values, handles, emails, and registrations, then merges co-occurring identifiers into reusable entity records with linked evidence rows.
- Event fusion is rule-based today: corroborated clusters materialize into events, linked evidence rows, a cited summary, and a longer report so the headless runtime can emit usable products before LLM narrative generation exists.
- Event export bundles package the event, linked observations, resolved entities, import/source context, generated products, citations, and relevant custody records into one JSON artifact for downstream systems or archival.
- Event export bundles now also carry relevant alerts plus scheduled task and scheduled run history, so downstream review can see not just the evidence but the operational path that produced and monitored it.
- Event and product exports now honor requested redaction ceilings, so lower-clearance bundles can exclude higher-sensitivity entities and products instead of leaking them by accident like amateurs.
- Managed sources provide a named catalog for repeatable ingestion; scheduler tasks can now sync those source definitions instead of only replaying raw file paths.
- Managed sources and scheduled tasks now support update/enable-disable lifecycle controls with custody logs, so operators can pause, retarget, and resume headless workflows without deleting history.
- HTTP-managed sources support retry attempts, timeout controls, custom headers, cached payload materialization, and persisted fetch metadata so headless sync jobs have enough operational context to debug failures.
- Managed source runs are idempotent by default: when the payload hash matches the most recent completed or skipped run for that source, the new run is marked `skipped` and does not create duplicate imports.
- Local imports are dedupe-aware too: exact duplicate records in the same layer are skipped, and each import run reports `records_seen`, `records_imported`, and `records_skipped`.
- Scheduled tasks now support task-level retry attempts and linear retry backoff, with custody records for failed attempts, retry scheduling, and final completion/failure outcomes.
- The scheduler worker is a first-class CLI runtime now, and due-task execution is resilient: one failed task run does not stall the rest of the due queue.

