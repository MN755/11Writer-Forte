# 11Writer Forte

11Writer Forte is the backend-only continuation of 11Writer: a geospatial-first OSINT data platform for ingesting intersecting public-source data, preserving provenance, and supporting headless collection, fusion, alerting, and export workflows.

The upstream `MN755/11Writer` repository does contain a much larger camera/webcam stack. This local Forte workspace does not currently carry that upstream subsystem wholesale. See [UPSTREAM_CAMERA_INVENTORY.md](UPSTREAM_CAMERA_INVENTORY.md) for the exact upstream camera paths and the current local gap.

This repo intentionally removes the frontend runtime. The only operator-facing interface is a custom CLI plus the API surface.

## What exists here

- FastAPI runtime for event, layer, geofence, alert, import, and trust-management workflows
- Persisted scheduler runtime for unattended imports, geofence scans, and integrity-source seed tasks
- Observation query and rule-based cross-verification surfaces for spatial filtering and corroboration
- Rule-based entity resolution that links observations into reusable entity records
- Event fusion materialization plus exportable cited summaries and rule-based reports
- Managed source definitions with persisted source-run history and scheduler-driven sync hooks
- Camera inventory materialization that turns imported/public traffic camera observations into persisted geospatial camera records with provenance
- SQLAlchemy storage foundation that runs on SQLite for local development and Postgres/PostGIS-oriented URLs for deployment
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
elevenwriter materialize-cameras --layer traffic-camera-feed
elevenwriter list-cameras --layer traffic-camera-feed --active true
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
```

By default the compose stack starts the API, a scheduler worker, and PostGIS-ready Postgres.

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
- Camera/webcam work is no longer just notes: `/api/cameras` and `/api/cameras/materialize` now persist camera inventory from imported observations, including MnDOT-style feeds that expose image or stream endpoints plus geospatial coordinates.
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
