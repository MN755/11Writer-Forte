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
11writer serve --host 127.0.0.1 --port 8000
11writer worker --worker all --loop
11writer webcam-worker --once
11writer reference-ingest fixes ./path/to/reference/files
```

## Operating Principles

- Preserve provenance, caveats, and source-health state.
- Keep observed, inferred, and derived facts separate.
- Treat discovered sources as candidates, not truth.
- Prefer Postgres/PostGIS for future primary storage, but keep SQLite and file-based ingest paths usable for local and migration workflows.
- Keep the runtime cross-platform: Windows, macOS, and Linux.

## Current Status

- FastAPI backend foundation is real.
- Runtime workers and reference ingestion are CLI-operable.
- Wave Monitor and source-discovery concepts are being folded out of 7Po8 into the main runtime.
- Frontend code is not part of the supported runtime anymore.

## Validation

```bash
cd app/server
python -m compileall src
pytest tests/test_cli.py -q
pytest tests/test_wave_monitor.py -q
pytest tests/test_source_discovery_memory.py -q
```

## License

This repository preserves the upstream `AGPL-3.0` license.
