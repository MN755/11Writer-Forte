# 11Writer Forte

11Writer Forte is the backend-only continuation of 11Writer: a geospatial-first OSINT data platform for ingesting intersecting public-source data, preserving provenance, and supporting headless collection, fusion, alerting, and export workflows.

The upstream `MN755/11Writer` repository does contain a much larger camera/webcam stack. This local Forte workspace does not currently carry that upstream subsystem wholesale. See [UPSTREAM_CAMERA_INVENTORY.md](UPSTREAM_CAMERA_INVENTORY.md) for the exact upstream camera paths and the current local gap.

This repo intentionally removes the frontend runtime. The only operator-facing interface is a custom CLI plus the API surface.

## What exists here

- FastAPI runtime for event, layer, geofence, alert, import, and trust-management workflows
- Persisted scheduler runtime for unattended imports, geofence scans, source syncs, camera inventory refresh, entity resolution, event fusion, and integrity-source seed tasks
- Observation query and rule-based cross-verification surfaces for spatial filtering and corroboration
- Rule-based entity resolution that links observations into reusable entity records
- Event fusion materialization plus exportable cited summaries and rule-based reports
- Managed source definitions with persisted source-run history and scheduler-driven sync hooks
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
uvicorn src.main:app --reload --port 8000
```

MnDOT live-feed notes and source-ingestion examples live in [MNDOT_FEEDS.md](MNDOT_FEEDS.md).

Upstream camera/webcam inventory and local parity notes live in [UPSTREAM_CAMERA_INVENTORY.md](UPSTREAM_CAMERA_INVENTORY.md).

## CLI

```bash
elevenwriter status
elevenwriter doctor
elevenwriter init-db
elevenwriter seed-integrity
elevenwriter add-layer marine-track "Marine Track" --temporal-resolution live --data-latency low
elevenwriter list-layers
elevenwriter import-local path/to/file.json --layer incident-feed
elevenwriter list-imports
elevenwriter add-source-file harbor-source ./feeds/harbor.json marine-track --skip-unchanged true
elevenwriter add-source-http-json remote-feed https://example.com/feed.json remote-track --retry-attempts 3 --skip-unchanged true --header "Authorization: Bearer token"
elevenwriter list-sources
elevenwriter update-source 1 --enabled false --notes "Disabled for review"
elevenwriter run-source 1
elevenwriter list-source-runs
elevenwriter show-source-ops 1
elevenwriter materialize-cameras --layer traffic-camera-feed
elevenwriter list-cameras --layer traffic-camera-feed --active true
elevenwriter materialize-camera-sources --layer traffic-camera-feed
elevenwriter list-camera-sources --layer traffic-camera-feed --status ready
elevenwriter show-camera-source-summary --layer traffic-camera-feed
elevenwriter show-camera-source-ops 1
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
elevenwriter show-clickhouse-r2-config
elevenwriter add-entity-resolution-schedule nightly-entities 600 --bbox "-96,29,-94,31" --min-observations 2
elevenwriter add-event-fusion-schedule nightly-fusion 600 --bbox "-96,29,-94,31" --distance-km 10 --time-window-minutes 120
elevenwriter query-observations --bbox "-96,29,-94,31"
elevenwriter cross-verify --bbox "-96,29,-94,31" --distance-km 10
elevenwriter resolve-entities --bbox "-96,29,-94,31" --min-observations 2
elevenwriter list-entities
elevenwriter show-entity-observations 1
elevenwriter fuse-events --bbox "-96,29,-94,31" --distance-km 10
elevenwriter show-event-products 1
elevenwriter export-event-product 1 cited_summary ./exports/event-1-summary.txt --max-redaction-level public
elevenwriter export-event-bundle 1 ./exports/event-1-bundle.json --max-redaction-level public
elevenwriter export-runtime-snapshot ./exports/runtime-snapshot.json
elevenwriter restore-runtime-snapshot ./exports/runtime-snapshot.json --replace-existing
elevenwriter add-source-sync-schedule nightly-sync 1 300 --retry-attempts 3 --retry-backoff-seconds 5
elevenwriter add-geofence-scan-schedule nightly-watch 300
elevenwriter update-schedule 1 --enabled false --notes "Paused for maintenance"
elevenwriter scheduler-worker --poll-seconds 5
elevenwriter list-schedules
elevenwriter list-schedule-runs
elevenwriter update-alert 1 acknowledged --disposition-note "Reviewed by operator"
elevenwriter run-due-schedules
elevenwriter list-custody
```

## Docker

```bash
docker compose up --build
docker compose --profile clickhouse up --build
```

By default the compose stack starts the API, a scheduler worker, and PostGIS-ready Postgres. The optional `clickhouse` profile starts a self-hosted ClickHouse server on `8123`/`9000`; Forte will only use it if you also set the ClickHouse env vars below.

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
```

## Design notes

- Events are the primary operational object.
- Data layers stay distinct from events so multiple layers can intersect around the same event.
- Data layers are auto-registered during ingest and source onboarding, so the layer catalog tracks the runtime instead of drifting out of date.
- Observations preserve raw text or raw structured content alongside extracted location and trust metadata.
- Trust scoring is rule-based first, seeded with starter integrity sources such as the New York Times, NPR, BBC, and Smithsonian.
- SQLite remains supported for local ingestion inputs and lightweight runtime mode, but primary backend storage targets Postgres/PostGIS deployment.
- Postgres runtime now auto-enables `postgis` plus GiST expression indexes for observation points and geofence geometries, while SQLite keeps the Python fallback path for local runs and tests.
- The headless CLI now includes a `doctor` command and the API exposes `/api/operations/database`, so operators can audit connectivity, additive schema drift, table counts, and PostGIS readiness without freestyling SQL in production.
- Runtime backup and recovery now have a first-class path too: `/api/operations/runtime/export`, `/api/operations/runtime/restore`, and matching CLI commands serialize the core backend state in dependency-safe order and log custody records for both export and restore.
- The storage-core slice is real now, not a manifesto: `/api/storage/objects` plus the `list-storage-objects`, `add-storage-object`, `promote-storage-object`, and `transition-storage-object` CLI commands expose a first-class artifact ledger with retention classes, tiering, and lifecycle controls.
- Imports and camera materialization now auto-register storage manifests, so raw local files plus camera image/stream/page references get tracked as storage objects with expiration windows and custody events instead of disappearing into the void.
- Managed sources now participate in that same storage/provenance model too: each source run materialization is tracked as a storage object, and `/api/sources/{id}/ops` plus `show-source-ops` expose recent runs, stored payload artifacts, and related custody logs in one place.
- ClickHouse is now an optional secondary backend instead of a hand-wavy future idea: `/api/operations/clickhouse` exposes diagnostics, provisioning, runtime sync, R2 archive export, and R2 storage-config preview, while the CLI mirrors those same flows for headless ops.
- Forte still keeps PostgreSQL/SQLite as the primary operational store. ClickHouse is wired for analytics, cold archive, and large-scale query workloads; pretending it fully replaces the relational runtime here would be unserious.
- Cloudflare R2 support follows the S3-compatible path: set the R2 endpoint, bucket, and HMAC creds, then use `sync-clickhouse` to mirror runtime facts into ClickHouse and `archive-clickhouse-observations` to write Parquet archives toward R2.
- Camera/webcam work is no longer just notes: `/api/cameras` and `/api/cameras/materialize` now persist camera inventory from imported observations, including MnDOT-style feeds that expose image or stream endpoints plus geospatial coordinates.
- Camera endpoint lifecycle is backend-native now too: `/api/camera-sources`, `/api/camera-sources/materialize`, `/api/camera-sources/summary`, and `/api/camera-sources/{id}/ops` maintain a candidate source registry for observed camera image/stream/page endpoints, with rule-based graduation scores and custody history.
- Camera ops now have a proper backend reporting surface too: `/api/cameras/summary` rolls up fleet health by layer, domain, provider, and status, while `/api/cameras/{id}/ops` exposes per-camera custody, latest observation/import context, and matching refresh schedules.
- Camera reporting is exportable now too: `/api/cameras/report-index` summarizes refresh task coverage, recent materializations, stale inventory, and recent refresh runs, while `/api/cameras/export/summary` emits a JSON-ready artifact for downstream systems and archival.
- Camera inventory upkeep is scheduler-native now too, so the registry can be refreshed headlessly with `camera_inventory_refresh` tasks instead of waiting for an operator to remember the manual materialization command. Those refresh runs also keep the camera source candidate registry in sync automatically.
- The platform-wide operations report now carries camera inventory and camera refresh sections too, so one headless report can show both event/alert activity and the current health of the camera subsystem.
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
