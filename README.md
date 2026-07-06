# 11Writer Forte

11Writer Forte is the backend-only continuation of 11Writer: a geospatial-first OSINT data platform for ingesting intersecting public-source data, preserving provenance, and supporting headless collection, fusion, alerting, and export workflows.

This repo intentionally removes the frontend runtime. The only operator-facing interface is a custom CLI plus the API surface.

## What exists here

- FastAPI runtime for event, layer, geofence, alert, import, and trust-management workflows
- SQLAlchemy storage foundation that runs on SQLite for local development and Postgres/PostGIS-oriented URLs for deployment
- Local import pipeline for JSON, JSONL, TXT, and SQLite inputs
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

## CLI

```bash
elevenwriter status
elevenwriter init-db
elevenwriter seed-integrity
elevenwriter import-local path/to/file.json --layer incident-feed
elevenwriter list-imports
```

## Docker

```bash
docker compose up --build
```

By default the compose stack starts the API plus PostGIS-ready Postgres.

## Design notes

- Events are the primary operational object.
- Data layers stay distinct from events so multiple layers can intersect around the same event.
- Observations preserve raw text or raw structured content alongside extracted location and trust metadata.
- Trust scoring is rule-based first, seeded with starter integrity sources such as the New York Times, NPR, BBC, and Smithsonian.
- SQLite remains supported for local ingestion inputs and lightweight runtime mode, but primary backend storage targets Postgres/PostGIS deployment.

