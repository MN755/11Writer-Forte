# 11Writer Forte

11Writer Forte is the backend-only continuation of 11Writer: a headless, geospatial-first OSINT platform focused on unattended collection, bounded source discovery, provenance-preserving storage, and CLI-driven operations.

This repo snapshot no longer treats a browser or desktop frontend as a required runtime surface. The supported operator interface is the `11writer` CLI plus the backend API.

## Runtime Shape

- `app/server/`: primary FastAPI backend, worker loops, reference ingestion, and operational CLI
- `7Po8/apps/backend/`: imported backend reference used while Wave Monitor and source policy capabilities are folded into the main runtime
- `scripts/`: release and validation helpers

Removed from the runtime surface:

- `app/client`
- `7Po8/apps/frontend`
- `third_party/code-oss-reference`

## Quick Start

```bash
cd app/server
python -m venv .venv
# Windows PowerShell: .\.venv\Scripts\Activate.ps1
# POSIX shells: source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
copy .env.example .env
11writer doctor
11writer serve --host 127.0.0.1 --port 8000
```

Useful CLI commands:

```bash
11writer doctor
11writer db-status
11writer db-bootstrap
11writer ready
11writer geofences
11writer geofence-create geofence:austin-box "Austin Box" --shape-kind bbox --min-lat 30.0 --min-lon -98.0 --max-lat 30.5 --max-lon -97.5
11writer geofence-check geofence:austin-box --lat 30.2672 --lon -97.7431 --subject-type event --subject-id event:austin-sighting
11writer import-local ./data/austin-events.json --source-kind historical_source
11writer import-runs
11writer event-report source-event:example --kind report --redaction-level public
11writer serve --host 127.0.0.1 --port 8000
11writer worker --worker all --loop
11writer webcam-worker --once
11writer reference-ingest fixes ./path/to/reference/files
```

Docker/PostGIS quick start:

```bash
cd app/server
docker compose up --build
```

The compose stack runs:

- `postgres`: primary Postgres/PostGIS store
- `api`: bootstraps storage, serves FastAPI on `http://127.0.0.1:8000`
- `workers`: long-lived runtime worker loop for Source Discovery and Wave Monitor

Readiness surfaces:

- `11writer ready`
- `GET /health/live`
- `GET /health/ready`

## Operating Principles

- Preserve provenance, caveats, and source-health state.
- Keep observed, inferred, and derived facts separate.
- Treat discovered sources as candidates, not truth.
- Persist geofences and evaluation history so spatial triggers remain queryable, auditable, and alertable.
- Persist local JSON/TXT/SQLite import runs so historical packets and file-based source drops enter the backend with chain-of-custody metadata.
- Persist event-level cited summaries and reports with explicit redaction labels and deterministic citations.
- Prefer Postgres/PostGIS for future primary storage, but keep SQLite and file-based ingest paths usable for local and migration workflows.
- Keep the runtime cross-platform: Windows, macOS, and Linux.

## Current Status

- FastAPI backend foundation is real.
- Runtime workers and reference ingestion are CLI-operable.
- Primary-database fanout, storage bootstrap, and storage-status reporting now exist for headless operations.
- Runtime readiness probes and a Docker/PostGIS deployment stack now exist for headless operations.
- Geofence APIs and CLI commands now persist geospatial perimeters, bounded reference context, evaluation history, alerts, and provenance.
- Local dataset import now persists JSON, TXT, and SQLite inputs as reviewable backend evidence with import-run records and provenance.
- Event artifacts now persist cited summaries and reports with rule-based confidence and provenance logging.
- Wave Monitor and source-discovery concepts are being folded out of 7Po8 into the main runtime.
- Frontend code is not part of the supported runtime anymore.

## Validation

```bash
cd app/server
python -m compileall src
pytest tests/test_cli.py -q
pytest tests/test_geofences.py -q
pytest tests/test_local_imports.py -q
pytest tests/test_wave_monitor.py -q
pytest tests/test_source_discovery_memory.py -q
```

## License

This repository preserves the upstream `AGPL-3.0` license.
