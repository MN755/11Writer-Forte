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
```

`init-db` and `migrate-db` both drive Alembic upgrades. `verify-db` is the non-destructive gate you should run before starting API or worker processes during deployments and recovery drills.

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
7. Run the specific operational checks you care about, such as `elevenwriter doctor`, `elevenwriter show-scheduler-summary`, and a sample API health probe.
