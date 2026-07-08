# 11Writer Forte Operations Runbook

This runbook assumes the current repo state is authoritative. The runtime now uses Alembic-managed migrations; startup verification is strict by default, and operators are expected to migrate on purpose instead of hoping `create_all()` covers production drift.

## Operator Commands

```bash
elevenwriter init-db
elevenwriter migrate-db
elevenwriter verify-db
elevenwriter doctor
elevenwriter backup-runtime ./backups/runtime-snapshot.json
elevenwriter restore-runtime ./backups/runtime-snapshot.json --replace-existing
elevenwriter show-storage-report
elevenwriter show-storage-manifest 42
elevenwriter archive-storage-object 42
elevenwriter verify-storage-object 42
elevenwriter request-storage-rehydration 42
elevenwriter rehydrate-storage-object 42 ./var/restored/artifact.bin --replace-existing
elevenwriter prune-storage-object 42
elevenwriter run-storage-lifecycle --operation archive --operation verify --operation prune --operation expire
```

`init-db` and `migrate-db` both drive Alembic upgrades. `verify-db` is the non-destructive gate you should run before starting API or worker processes during deployments and recovery drills.

## Storage Lifecycle Configuration

Local archive/rehydrate directories default under `ELEVENWRITER_DATA_DIR`:

```dotenv
ELEVENWRITER_STORAGE_ARCHIVE_BACKEND=local
ELEVENWRITER_STORAGE_ARCHIVE_DIR=artifacts/archive
ELEVENWRITER_STORAGE_REHYDRATE_DIR=artifacts/rehydrated
```

To archive managed artifacts into Cloudflare R2 or another S3-compatible backend instead:

```dotenv
ELEVENWRITER_STORAGE_ARCHIVE_BACKEND=r2
ELEVENWRITER_STORAGE_S3_ENDPOINT=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
ELEVENWRITER_STORAGE_S3_BUCKET=11writer-artifacts
ELEVENWRITER_STORAGE_S3_ACCESS_KEY_ID=<R2_ACCESS_KEY_ID>
ELEVENWRITER_STORAGE_S3_SECRET_ACCESS_KEY=<R2_SECRET_ACCESS_KEY>
ELEVENWRITER_STORAGE_S3_REGION=auto
ELEVENWRITER_STORAGE_S3_PREFIX=11writer-artifacts
```

The storage archive backend is optional and separate from the ClickHouse R2 configuration. If you omit the storage-specific S3 settings, Forte falls back to the existing ClickHouse R2 credentials when possible.

## Windows Direct Runtime

```powershell
cd app\server
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .[dev]
Copy-Item .env.example .env
```

Set `ELEVENWRITER_DATABASE_URL` in `.env` for either SQLite or PostgreSQL/PostGIS, then:

```powershell
elevenwriter init-db
elevenwriter verify-db
uvicorn src.main:app --host 127.0.0.1 --port 8000
```

Start the scheduler worker in a second shell:

```powershell
.\.venv\Scripts\Activate.ps1
cd app\server
elevenwriter verify-db
elevenwriter scheduler-worker
```

## macOS / Linux Direct Runtime

```bash
cd app/server
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
cp .env.example .env
```

Set `ELEVENWRITER_DATABASE_URL` in `.env`, then:

```bash
elevenwriter init-db
elevenwriter verify-db
uvicorn src.main:app --host 127.0.0.1 --port 8000
```

Worker shell:

```bash
source .venv/bin/activate
cd app/server
elevenwriter verify-db
elevenwriter scheduler-worker
```

## Artifact Operations

The storage subsystem now executes artifact transfers instead of just recording state changes.

1. Inspect backlog and replica state.
2. Archive eligible managed artifacts.
3. Verify archived replicas.
4. Request or run rehydration when an operator needs the bytes back locally.
5. Prune only after verified archival and retention expiry.

Direct commands:

```bash
elevenwriter show-storage-report
elevenwriter show-storage-manifest 42
elevenwriter archive-storage-object 42
elevenwriter verify-storage-object 42
elevenwriter request-storage-rehydration 42
elevenwriter rehydrate-storage-object 42 ./var/restored/artifact.bin --replace-existing
elevenwriter quarantine-storage-object 42 "checksum mismatch"
elevenwriter unquarantine-storage-object 42 --note "manual review cleared"
elevenwriter prune-storage-object 42
```

Scheduler-managed sweep:

```bash
elevenwriter add-storage-lifecycle-schedule artifact-maintenance 900 --operation archive --operation verify --operation rehydrate --operation prune --operation expire
elevenwriter run-storage-lifecycle --operation archive --operation verify --operation rehydrate --operation prune --operation expire
```

Operational rules:

- `archive-storage-object` does not mark the archive complete until checksum and byte-size verification succeed.
- `rehydrate-storage-object` writes a fresh local replica and verifies it before closing the loop.
- `prune-storage-object` only touches managed local files under the configured Forte data/archive/rehydrate roots.
- `quarantine-storage-object` and automatic transfer-failure handling both emit custody records and keep failed artifacts visible in `show-storage-report` and `/api/operations/report`.

## PostgreSQL / PostGIS Notes

- Use a database URL like `postgresql+psycopg://elevenwriter:change-me@127.0.0.1:5432/elevenwriter`.
- The Alembic baseline migration creates `postgis` and the required GiST expression indexes when the backend is PostgreSQL.
- Keep `ELEVENWRITER_DATABASE_AUTO_MIGRATE=false` for long-running API and worker processes unless you are doing disposable local development.
- `elevenwriter verify-db` should return zero warnings before you start the API or worker in a production-like environment.

## Docker Compose Workflow

```bash
docker compose up --build db
docker compose run --rm api elevenwriter verify-db
docker compose up --build api worker
```

The compose stack now waits for PostgreSQL health and runs `elevenwriter migrate-db` before `uvicorn` or `scheduler-worker` starts. That means fresh bootstraps and normal upgrades both converge on the Alembic head revision instead of runtime `create_all()` side effects.

If you are bringing the full stack up from zero, `docker compose up --build` is enough; the API and worker containers both run the migration gate before starting.

## Developer Migration Workflow

For operator use, prefer the CLI commands above. For revision authoring in the repo:

```bash
cd app/server
alembic -c alembic.ini current
alembic -c alembic.ini history
```

When you add a new revision:

1. Update `src/models.py` first.
2. Add a new script under `src/db_migrations/versions/`.
3. Run `elevenwriter migrate-db`.
4. Run `elevenwriter verify-db`.
5. Run the relevant pytest coverage before shipping.

## Backup and Restore

Runtime snapshots are logical application-state backups. They are not a substitute for database-native backups.

Storage artifacts are separate from database/runtime snapshots. If you care about recovery instead of wishful thinking, back up both the metadata and the actual bytes.

### Artifact Backup / Recovery

Local archive backend:

1. Stop API and worker processes if you need a point-in-time capture.
2. Copy `ELEVENWRITER_DATA_DIR/artifacts/archive/` and `ELEVENWRITER_DATA_DIR/artifacts/rehydrated/` if those directories matter to your workflow.
3. Export a runtime snapshot:

```bash
elevenwriter backup-runtime ./backups/runtime-snapshot.json
```

4. Restore database state first.
5. Restore archive directories second.
6. Run `elevenwriter verify-db`.
7. Use `elevenwriter show-storage-report` and `elevenwriter show-storage-manifest <id>` to confirm canonical URIs, replica statuses, and verification timestamps.

R2 archive backend:

1. Capture the runtime snapshot and database-native backup as usual.
2. Preserve the R2 bucket plus prefix configured by `ELEVENWRITER_STORAGE_S3_BUCKET` and `ELEVENWRITER_STORAGE_S3_PREFIX`.
3. Restore database state first, then restore or reattach the bucket credentials.
4. Run `elevenwriter verify-storage-object <id>` or `elevenwriter run-storage-lifecycle --operation verify` against a sample of archived artifacts before declaring the recovery complete.

### SQLite Recovery Workflow

1. Stop API and worker processes.
2. Copy the SQLite file referenced by `ELEVENWRITER_DATABASE_URL`.
3. Export a logical runtime snapshot for fast validation:

```bash
elevenwriter backup-runtime ./backups/runtime-snapshot.json
```

Restore into a clean SQLite database:

```bash
elevenwriter init-db
elevenwriter restore-runtime ./backups/runtime-snapshot.json --replace-existing
elevenwriter verify-db
```

### PostgreSQL / PostGIS Recovery Workflow

Create a logical runtime snapshot:

```bash
elevenwriter backup-runtime ./backups/runtime-snapshot.json
```

Create a PostgreSQL-native dump from the compose database container:

```bash
docker compose exec db pg_dump -U elevenwriter -d elevenwriter -Fc -f /tmp/elevenwriter.dump
docker compose cp db:/tmp/elevenwriter.dump ./backups/elevenwriter.dump
```

Restore the PostgreSQL dump into a clean target database, then verify and optionally rehydrate the application-level snapshot:

```bash
pg_restore --clean --if-exists --no-owner -U elevenwriter -d elevenwriter ./backups/elevenwriter.dump
elevenwriter verify-db
elevenwriter restore-runtime ./backups/runtime-snapshot.json --replace-existing
elevenwriter verify-db
```

Use the runtime snapshot restore when you want a deterministic re-seed of application tables after the database itself is back in place. Use the PostgreSQL dump when you need full-fidelity database recovery.

## Recovery Drill Checklist

1. Run `elevenwriter verify-db`.
2. Run `elevenwriter backup-runtime ./backups/runtime-snapshot.json`.
3. Capture the database-native backup for your backend.
4. Restore into a clean target.
5. Run `elevenwriter restore-runtime ./backups/runtime-snapshot.json --replace-existing`.
6. Run `elevenwriter verify-db`.
7. Run the specific operational checks you care about, such as `elevenwriter doctor`, `elevenwriter show-scheduler-summary`, `elevenwriter show-storage-report`, and a sample API health probe.
