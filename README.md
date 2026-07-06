# 11Writer Forte

11Writer Forte is a backend-only, geospatial-first OSINT runtime. It is designed to ingest public-source data, preserve provenance, run headless collection and monitoring tasks, and expose the resulting intelligence surfaces over an API plus an operator-focused CLI.

The browser client, desktop-shell direction, and legacy UI scaffolding have been removed from this repo. The canonical operator surface is now the `11writer` CLI.

## Current Shape

- `app/server`: canonical FastAPI runtime, scheduler logic, source-discovery stack, marine/reference/webcam subsystems, and the folded `forte` source-ops backend
- `app/docs`: architecture and domain notes that still need progressive cleanup after the backend-only cutover
- `scripts`: repository support utilities

## Folded Source-Ops Surface

The former `7Po8` backend has been folded into the main server under `app/server/src/forte` and is exposed at `/api/forte/*`.

That surface currently includes:

- waves
- connectors
- records
- scheduler tick control
- deterministic signals
- discovered sources
- source checks
- domain trust profiles
- wave-level trust overrides
- policy-action history

## Unified Intel Core

The backend now also exposes a unified intelligence substrate at `/api/intel/*` for:

- sources and trust metadata
- entities and entity-resolution records
- events and observations
- geofences and alert records
- chain-of-custody records
- rule-based confidence assessments
- cited summaries and report artifacts
- local file intake for JSON, text-like files, and SQLite datasets

## Runtime Principles

- Backend-first and CLI-first
- Postgres/PostGIS-first configuration
- Loopback by default
- Optional bearer-token protection for non-health endpoints via `APP_API_TOKEN`
- Rule-based source scoring and trust policy first
- LLM reporting deferred until the evidence/provenance substrate is hardened

## Quick Start

### 1. Start PostGIS

Use the bundled compose file:

```bash
docker compose -f deploy/docker-compose.postgis.yml up -d
```

### 2. Configure the backend

```bash
cd app/server
cp .env.example .env
python -m pip install -e .[dev]
```

### 3. Inspect the runtime

```bash
cd app/server
python -m src.cli doctor
python -m src.cli routes
```

### 4. Run the API

```bash
cd app/server
python -m src.cli serve
```

### 5. Run workers

```bash
cd app/server
python -m src.cli worker --worker all --once
python -m src.cli forte-worker --interval-seconds 30
```

## CLI

`11writer` is installed from `app/server/pyproject.toml` and currently supports:

- `11writer serve`
- `11writer worker`
- `11writer forte-worker`
- `11writer config`
- `11writer routes`
- `11writer doctor`
- `11writer intel-overview`
- `11writer ingest-file`
- `11writer evaluate-alerts`

## Key Environment Variables

- `APP_RUNTIME_MODE=backend-only`
- `APP_BIND_HOST=127.0.0.1`
- `APP_BIND_PORT=8000`
- `APP_API_TOKEN=` optional bearer token
- `DATABASE_URL=postgresql+psycopg://11writer:11writer@127.0.0.1:5432/11writer?connect_timeout=3`
- `REFERENCE_DATABASE_URL=` optional subsystem override
- `SOURCE_DISCOVERY_DATABASE_URL=` optional subsystem override
- `WAVE_MONITOR_DATABASE_URL=` optional subsystem override

## Known Gaps

- Many docs still describe the pre-Forte browser/desktop era and need cleanup.
- The folded `forte` subsystem currently uses SQLModel `create_all()` bootstrap rather than a unified migration history.
- Cross-platform service install UX is still in progress even though the runtime worker/service primitives already exist.
- Remote API access should be paired with explicit host binding and `APP_API_TOKEN`.

## License

This repository remains under `AGPL-3.0`.
