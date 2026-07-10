# 11Writer Forte

11Writer Forte is the backend-only continuation of 11Writer: a geospatial-first OSINT data platform for ingesting intersecting public-source data, preserving provenance, and supporting headless collection, fusion, alerting, and export workflows.

The upstream `MN755/11Writer` repository does contain a larger camera/webcam stack. Forte now carries the backend-operational parts needed for headless ingest, materialization, verification, reporting, and scheduling, but it still does not carry the old frontend webcam UI or the upstream webcam module in its original form. See [UPSTREAM_CAMERA_INVENTORY.md](UPSTREAM_CAMERA_INVENTORY.md) for the exact upstream camera paths and the remaining parity gap.

This repo intentionally removes the frontend runtime. The only operator-facing interface is a custom CLI plus the API surface.

## What exists here

- FastAPI runtime for event, layer, geofence, alert, import, and trust-management workflows
- Persisted scheduler runtime for unattended imports, geofence scans, source syncs, camera inventory refresh, entity resolution, event fusion, and integrity-source seed tasks
- Observation query and rule-based cross-verification surfaces for spatial filtering and corroboration
- Rule-based entity resolution that links observations into reusable entity records while preserving conflicting identifier/name signals as explicit cautions
- Event fusion materialization plus exportable cited summaries and rule-based reports with explicit confidence drivers, weakening factors, linked-entity rationale, and related alert context
- Managed source definitions with persisted source-run history and scheduler-driven sync hooks
- First-class deterministic watches for source deltas, image hashes, observation rules, and source health, with independent run history, alerts, evidence, scheduling, and RSS
- Camera inventory materialization that turns imported/public traffic camera observations into persisted geospatial camera records with provenance
- Camera source inventory lifecycle that graduates observed camera endpoints into a backend-native candidate registry with rule-based readiness scoring
- Storage-object ledger that tracks retained artifacts, retention class, lifecycle state, and provenance for imports and camera-derived references
- SQLAlchemy storage foundation that runs on SQLite for local development and Postgres/PostGIS-oriented URLs for deployment
- Optional ClickHouse analytics/archive backend that can mirror runtime facts and archive observation data to Cloudflare R2 over the S3-compatible API
- PostGIS-aware spatial query path that persists WKT alongside GeoJSON and automatically provisions spatial indexes on PostgreSQL
- Local import pipeline for JSON, JSONL, TXT, SQLite, and source-code/text artifact inputs (`.c`, `.cpp`, `.go`, `.java`, `.php`, `.r`, and similar) with row-level dedupe inside each layer
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
elevenwriter bootstrap-local-runtime --base-url http://127.0.0.1:8010
elevenwriter bootstrap-local-runtime --base-url http://127.0.0.1:8010 --run-initial-cycle
elevenwriter add-runtime-snapshot-schedule local-backup 21600
uvicorn src.main:app --reload --port 8000
```

If you want web-facing ingestion with local-only persistence, start from [`app/server/.env.local-first.example`](app/server/.env.local-first.example). That mode keeps internet ingestion turned on while storing source caches, artifact archives, rehydrated files, and optional ClickHouse state on your own device instead of R2 or any other cloud backend.

MnDOT live-feed notes and source-ingestion examples live in [MNDOT_FEEDS.md](MNDOT_FEEDS.md).

Upstream camera/webcam inventory and local parity notes live in [UPSTREAM_CAMERA_INVENTORY.md](UPSTREAM_CAMERA_INVENTORY.md).

The complete local Watch Engine acceptance flow lives in [WATCH_ENGINE_DEMO.md](WATCH_ENGINE_DEMO.md). It uses a controlled replaceable image fixture and no cloud service.

## CLI

```bash
elevenwriter status
elevenwriter doctor
elevenwriter show-runtime-readiness
elevenwriter show-worker-summary
elevenwriter init-db
elevenwriter seed-integrity
elevenwriter add-layer marine-track "Marine Track" --temporal-resolution live --data-latency low
elevenwriter list-layers
elevenwriter import-local path/to/file.json --layer incident-feed
elevenwriter import-local path/to/local-bundle --layer incident-bundle
elevenwriter import-local path/to/collector.go --layer code-artifacts
elevenwriter list-imports
elevenwriter add-source-file harbor-source ./feeds/harbor.json marine-track --skip-unchanged true
elevenwriter add-source-http-json remote-feed https://example.com/feed.json remote-track --retry-attempts 3 --skip-unchanged true --header "Authorization: Bearer token"
elevenwriter add-source-http-text newsroom-scrape https://example.com/feed.txt news-track
elevenwriter add-source-http-xml road-events https://example.com/feed.xml roads-track
elevenwriter add-source-web-search global-search https://search.example.com/html web-discovery --query "port departure" --query "rail delay" --fetch-result-pages true --search-result-limit 25 --page-fetch-limit 10 --result-redirect-query-param target --include-url-pattern "/article/" --exclude-url-pattern "/login"
elevenwriter add-source-web-crawl site-crawl https://example.com/briefings/root web-crawl --crawl-depth 2 --crawl-page-limit 50 --crawl-link-limit 100 --same-domain-only true --include-url-pattern "/briefings/"
elevenwriter add-source-web-discovery broad-web world-events --query "harbor departure" --provider duckduckgo_html --seed-url https://example.com/briefings/root --discover-sitemaps-from-seeds true --crawl-depth 2 --crawl-page-limit 100
elevenwriter run-web-search-now global-search-now world-events --query "port departure" --provider duckduckgo_html --search-page-limit 2 --page-fetch-limit 10 --crawl-from-results
elevenwriter run-web-crawl-now site-crawl-now https://example.com/briefings/root web-crawl --crawl-depth 2 --crawl-page-limit 50 --same-domain-only
elevenwriter run-web-discovery-now broad-web-now world-events --query "harbor departure" --provider duckduckgo_html --seed-url https://example.com/briefings/root --discover-sitemaps-from-seeds --crawl-depth 2 --crawl-page-limit 100
elevenwriter add-source-sse live-port-feed https://example.com/events marine-track --stream-max-records 250 --stream-idle-timeout-seconds 2
elevenwriter add-source-websocket live-camera-feed ws://example.com/socket camera-track --stream-max-records 250 --metadata-json "{\"send_messages\":[{\"op\":\"subscribe\",\"topic\":\"cameras\"}]}"
elevenwriter add-source-webhook analyst-dropbox inbound-track
elevenwriter seed-local-live-sources --base-url http://127.0.0.1:8010
elevenwriter bootstrap-local-runtime --base-url http://127.0.0.1:8010 --layer-prefix local-live --schedule-prefix local-runtime
elevenwriter bootstrap-local-runtime --base-url http://127.0.0.1:8010 --layer-prefix local-live --schedule-prefix local-runtime --run-initial-cycle
elevenwriter smoke-test-local-runtime --base-url http://127.0.0.1:8010
elevenwriter list-sources
elevenwriter update-source 1 --enabled false --notes "Disabled for review"
elevenwriter run-source 1
elevenwriter run-source-runtime 2
elevenwriter run-source-maintenance --stale-after-hours 24 --source-limit 25 --dead-letter-limit 100
elevenwriter run-source-health-scan --stale-after-hours 24 --source-limit 100
elevenwriter run-enabled-schedules --task-type source_sync
elevenwriter run-platform-cycle --task-type source_sync --no-include-stream-runtime
elevenwriter list-source-runs
elevenwriter list-source-checkpoints
elevenwriter list-source-dead-letters --status pending --limit 50
elevenwriter push-source-webhook 3 --payload-json "{\"title\":\"Analyst note\",\"url\":\"https://example.com/note/1\",\"text\":\"Inbound webhook payload\"}"
elevenwriter replay-source-dead-letter 7
elevenwriter show-source-ops 1
elevenwriter show-source-summary --stale-after-hours 24
elevenwriter show-source-report-index --stale-after-hours 24
elevenwriter export-source-summary ./exports/source-summary.json --source-limit 500 --report-limit 25
elevenwriter materialize-cameras --layer traffic-camera-feed
elevenwriter list-cameras --layer traffic-camera-feed --active true
elevenwriter materialize-camera-sources --layer traffic-camera-feed
elevenwriter verify-camera-sources --layer traffic-camera-feed --limit 200 --timeout-seconds 5
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
elevenwriter add-source-maintenance-schedule source-fleet-maintenance 600 --stale-after-hours 24 --source-limit 25 --dead-letter-limit 100
elevenwriter add-source-health-scan-schedule source-fleet-alerts 600 --stale-after-hours 24 --source-limit 100
elevenwriter add-camera-source-verification-schedule mndot-camera-verify 600 --layer traffic-camera-feed --endpoint-kind stream --limit 200 --timeout-seconds 5
elevenwriter list-storage-objects --owner-type camera_inventory --retention-class operational
elevenwriter add-storage-object manual:casefile:1 report_export event 42 file:///tmp/casefile-42.json --storage-tier hot --retention-class investigative
elevenwriter show-storage-manifest 1
elevenwriter promote-storage-object 1 event 42 --storage-tier archive --retention-class permanent
elevenwriter transition-storage-object 1 archived --storage-tier archive
elevenwriter archive-storage-object 1
elevenwriter verify-storage-object 1
elevenwriter request-storage-rehydrate 1
elevenwriter rehydrate-storage-object 1 --target-path ./rehydrated/casefile-42.json
elevenwriter prune-storage-object 1
elevenwriter quarantine-storage-object 1 "integrity review"
elevenwriter unquarantine-storage-object 1 --note "cleared"
elevenwriter run-storage-lifecycle --operation expire --dry-run
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
elevenwriter show-entity-summary --entity-type vessel
elevenwriter show-entity-report-index --entity-type vessel
elevenwriter export-entity-summary ./exports/entity-summary.json --entity-type vessel
elevenwriter show-entity-observations 1
elevenwriter show-event-summary --status open
elevenwriter show-event-report-index --status open
elevenwriter export-event-summary ./exports/event-summary.json --status open
elevenwriter fuse-events --bbox "-96,29,-94,31" --distance-km 10
elevenwriter show-event-products 1
elevenwriter export-event-product 1 cited_summary ./exports/event-1-summary.txt --max-redaction-level public
elevenwriter export-event-bundle 1 ./exports/event-1-bundle.json --max-redaction-level public
elevenwriter export-runtime-snapshot ./exports/runtime-snapshot.json
elevenwriter verify-runtime-snapshot ./exports/runtime-snapshot.json
elevenwriter restore-runtime-snapshot ./exports/runtime-snapshot.json --replace-existing
elevenwriter export-runtime-bundle ./exports/runtime-bundle.zip
elevenwriter restore-runtime-bundle ./exports/runtime-bundle.zip --replace-existing
elevenwriter add-watch "Piston Peak construction image" image_change "Notify when the retained image hash changes." --slug piston-peak-construction-image --camera-inventory-id 1 --severity warning --rule-json '{"mode":"image_change","comparison":"sha256","alert_on_initial":false,"retention_class":"permanent"}'
elevenwriter list-watches --watch-type image_change
elevenwriter show-watch 1
elevenwriter update-watch 1 --severity critical --description "Escalated operator watch"
elevenwriter pause-watch 1
elevenwriter resume-watch 1
elevenwriter run-watch 1
elevenwriter add-watch-schedule 1 piston-peak-watch-poll 300 --retry-attempts 3 --retry-backoff-seconds 5
elevenwriter list-watch-runs --watch-id 1
elevenwriter list-watch-alerts --watch-id 1 --status open
elevenwriter show-watch-evidence 1
elevenwriter show-watch-feed --watch-id 1
elevenwriter show-watch-feed --watch-id 1 --preview
elevenwriter add-source-sync-schedule nightly-sync 1 300 --retry-attempts 3 --retry-backoff-seconds 5
elevenwriter add-geofence-scan-schedule nightly-watch 300
elevenwriter update-schedule 1 --enabled false --notes "Paused for maintenance"
elevenwriter scheduler-worker --poll-seconds 5
elevenwriter source-runtime-worker --poll-seconds 5
elevenwriter list-schedules
elevenwriter list-schedule-runs
elevenwriter list-alerts --status open --limit 50
elevenwriter show-alert-summary --stale-after-hours 24
elevenwriter show-alert-report-index --stale-after-hours 24 --limit 25
elevenwriter export-alert-summary ./exports/alert-summary.json --alert-limit 500 --report-limit 25
elevenwriter update-alert 1 acknowledged --disposition-note "Reviewed by operator"
elevenwriter run-due-schedules
elevenwriter list-custody
elevenwriter show-scheduler-summary
elevenwriter show-scheduler-report-index --limit 25
elevenwriter show-worker-summary
elevenwriter export-scheduler-summary ./exports/scheduler-summary.json --task-limit 500 --report-limit 25
```

Watch API catalog:

```text
GET/POST  /api/watches
GET       /api/watches/runs
GET       /api/watches/alerts
GET       /api/watches/feed.rss
GET/PATCH /api/watches/{watch_id}
POST      /api/watches/{watch_id}/pause
POST      /api/watches/{watch_id}/resume
POST      /api/watches/{watch_id}/run
POST      /api/watches/{watch_id}/schedule
GET       /api/watches/{watch_id}/runs
GET       /api/watches/{watch_id}/alerts
GET       /api/watches/{watch_id}/evidence
```

## Docker

```bash
docker compose up --build
docker compose --profile clickhouse up --build
docker compose --profile probe run --rm runtime-probe
```

By default the compose stack starts the API, local-upstreams, a one-shot `bootstrap` job, a unified `platform-runtime-worker`, and PostGIS-ready Postgres. All backend services share the persisted `forte-data` volume for cached source payloads, runtime state, and artifact lifecycle work, and they auto-run additive schema migration on boot. The bootstrap job seeds integrity sources, upserts the local demo source/schedule catalog against `http://local-upstreams:8010`, and runs one initial platform cycle before the long-running worker takes over, so `docker compose up` becomes a coherent local runtime instead of a scavenger hunt. The optional `clickhouse` profile starts a self-hosted ClickHouse server on `8123`/`9000`; Forte will only use it if you also set the ClickHouse env vars below. The compose file also mounts [`app/server/11writer-r2-storage.xml`](app/server/11writer-r2-storage.xml), and `elevenwriter write-clickhouse-r2-config` now targets that mounted file automatically when you run it from the repo root, so the generated R2 disk policy lands where Docker actually reads it.

The compose stack also ships health checks now: Postgres must pass `pg_isready`, and the API must answer `/health` before the unified runtime worker starts. That avoids the usual container-startup lottery.

If you want a one-shot proof that the whole local stack is actually operational after boot, run `docker compose --profile probe run --rm runtime-probe` or call `elevenwriter verify-local-runtime-stack --api-base-url http://127.0.0.1:8000 --upstreams-base-url http://127.0.0.1:8010`. That verifies API liveness, readiness, worker presence, and local-upstream availability in one place instead of making you manually click four endpoints like it’s amateur hour. The compose probe now also fails closed unless the backend reports the intended `postgresql` + `postgis` runtime with clean database diagnostics and current schema state, and it waits up to 60 seconds for the runtime to settle before failing, so startup validation is a real gate instead of a race condition.

`/health` is intentionally a liveness probe, not a full operational-readiness claim. Use `/ready` or `elevenwriter show-runtime-readiness` when you need the stricter answer about whether the database, trust registry, source catalog, scheduler coverage, executor coverage, source-fleet health, scheduler-fleet health, storage lifecycle coverage, recovery snapshot coverage, and analytic pipeline coverage are actually in place for unattended operation.

The local-first path is built in now too: the stack also starts `local-upstreams` on `127.0.0.1:8010`, which exposes simulated snapshot, SSE, WebSocket, HTML search, sitemap, crawlable website, and camera-verification feeds for end-to-end ingestion testing without touching third-party providers. Those endpoints now generate host-aware URLs instead of hard-coded loopback lies, so the same harness works both from the host and from Docker service-to-service networking. Use `elevenwriter seed-local-live-sources` to register those demo feeds into the source catalog, including `web_search`, `web_crawl`, `web_discovery`, and a camera-oriented WebSocket source that can actually materialize local camera inventory.

There is finally a one-shot local bootstrap path too: `elevenwriter bootstrap-local-runtime` initializes the database, seeds the default integrity/trust registry, upserts the local demo sources, and provisions the baseline maintenance, backup, health, camera, entity-resolution, and event-fusion schedules needed for a coherent headless runtime. Add `--run-initial-cycle` when you want bootstrap to immediately execute one platform pass and ingest the first batch of local demo data before the supervised worker loop starts. Because doing that by hand every time is fake productivity.

There is also a one-command acceptance path now: `elevenwriter smoke-test-local-runtime` can bootstrap the local runtime, optionally run an initial collection cycle plus one supervised worker iteration, and then fail closed unless database connectivity and runtime readiness both come back healthy. When you run it against the built-in demo sources, it now also reports `camera_runtime` counts and fails if the local camera stream never materializes into inventory and reachable candidate endpoints. That gives you an actual backend smoke test instead of a pile of vibes and crossed fingers.

For generic web-wide discovery, Forte now also supports provider-aware search-engine sources. The headless CLI can register a symbolic search source like `search://web/duckduckgo_html`, page through search engine result pages, fetch result documents locally, and optionally crawl outward from those discovered results while still persisting everything under the local runtime directories.

Forte now also has a first-class `web_discovery` campaign source for broader whole-web collection. That source type can combine search-engine seeding, explicit seed URLs, sitemap expansion, bounded crawl traversal, and checkpointed frontier resume across runs, while still storing the entire campaign state locally inside the runtime checkpoint/artifact ledger.

If you want local ClickHouse only, keep the `clickhouse` profile self-hosted and skip all R2 env vars entirely. Forte now treats that as an intentional local-only analytics mode instead of whining that cloud storage is missing.

## Remote Ingest, Local Persist

- `http_json`, `http_jsonl`, `http_text`, `http_xml`, and `rss` sources can pull from the public web and cache their payloads locally under `ELEVENWRITER_DATA_DIR/source_cache`.
- `web_search` sources can query HTML search-provider pages, including symbolic built-in providers such as `search://web/duckduckgo_html`, page through search results, filter and unwrap result links, fetch result pages, and persist normalized discovery records locally as JSON-backed source runs. When pages expose usable metadata, Forte now also lifts `observed_at` plus latitude/longitude from HTML meta tags and time elements so discovered records can participate in temporal and geospatial analysis instead of staying text-only.
- `web_search`, `web_crawl`, and `web_discovery` sources now honor `robots.txt` crawl policy by default, track blocked URLs in run metadata instead of quietly plowing through them, and expose policy coverage/blocked-count telemetry in the resulting source-run output so operators can see what the crawler refused to fetch.
- Remote source fetches are bounded now too: the source runtime enforces `max_payload_bytes` caps for HTTP, SSE, and WebSocket collection, supports optional denial of private/non-public network targets through `allow_private_networks=false`, and can pace whole-web discovery requests with per-source `min_request_interval_seconds` plus publisher-provided `crawl-delay` instead of machine-gunning a site because nobody bothered to add brakes.
- `web_search` sources can optionally expand discovered result pages into bounded crawls, so one search run can discover a relevant site, traverse a small local graph around it, and persist both the seed result and the follow-on crawl pages into the same local analysis layer.
- `web_crawl` sources can walk a bounded HTML site graph from one or more seeds, keep traversal local to allowed domains, and persist discovered page summaries locally without any cloud dependency. Crawl records use the same metadata lift path for page timestamps and coordinates when publishers expose them.
- `web_discovery` sources can run a broader discovery campaign: query one or more search providers, merge those results with operator-provided seeds, discover sitemap URLs from `robots.txt` or explicit configuration, expand sitemap indexes into page candidates, and resume unfinished crawl frontiers on later runs using persisted source checkpoints instead of restarting from zero every time.
- Whole-web collection has a direct execution path now too: `/api/sources/web/run` and the `run-web-search-now`, `run-web-crawl-now`, and `run-web-discovery-now` CLI commands will upsert the managed source and execute it in one call, so operators can run immediate web collection without doing the create-then-run dance by hand.
- That direct whole-web execution path is normalized now too: when `/api/sources/web/run` manages to persist a failing source run before collection dies, it returns the structured source failure payload with `source_run_id`, `source_kind`, and error class instead of dumping a raw server exception on the caller.
- Once data is collected, operators can search the local observation corpus directly through `elevenwriter search-observations "<query>"` or `/api/observations/search?q=...`, which ranks title, summary, body, and URL matches without punting the query flow out to a cloud search tier.
- Operators can also promote those local search queries into unattended watch tasks through `elevenwriter add-observation-watch-schedule ...` or the generic scheduler API, which emits durable unscoped alert records when newly collected observations match a saved rule-based query.
- First-class watches live under `/api/watches` and support deterministic `source_delta`, `image_change`, `observation_rule`, and `source_health` evaluation. Watch changes create local alerts and retained evidence before any optional downstream analysis; `/api/watches/feed.rss` exposes open/recent watch alerts without an outbound notification service.
- `sse_stream` and `websocket_stream` sources can consume live remote feeds, batch them locally, and persist observations into the local relational runtime without any cloud storage requirement.
- `webhook_ingest` sources let outside systems push data into the platform, but the payload cache, source-run artifacts, and downstream storage manifests still live under your local runtime directories.
- `webhook_ingest` sources now have full CLI parity too: operators can register a webhook source, submit payloads directly through `push-source-webhook`, inspect checkpoints and dead letters locally, and replay failed records with `replay-source-dead-letter` without leaving the headless runtime.
- Managed sources now have a scheduler-native self-heal pass too: `run-source-maintenance` and `add-source-maintenance-schedule` can replay pending source dead letters, re-run stale pull/stream sources, and keep the source fleet from silently rotting between human check-ins.
- Managed sources now have scheduler-native health alerting too: `run-source-health-scan` and `add-source-health-scan-schedule` can promote stale, failed, and dead-letter-backed source conditions into persistent unscoped alert records, then automatically close those alerts when the source recovers.
- Storage lifecycle actions default to the local filesystem when `ELEVENWRITER_STORAGE_ARCHIVE_BACKEND=local`, which is the repo default. That means archive, verify, prune, and rehydrate flows all stay on-device unless you explicitly opt into R2 later.

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
- The backend now has a first-class runtime readiness audit too: `elevenwriter show-runtime-readiness` and `/api/operations/readiness` evaluate whether the current database, trust registry, source catalog, scheduler coverage, local-import target health, camera pipeline coverage, source-fleet health, scheduler-fleet health, storage lifecycle coverage, and analytic pipeline coverage are actually in place for unattended operation instead of leaving operators to infer readiness from a pile of unrelated commands.
- There is a stack-level bring-up proof now too: `elevenwriter verify-local-runtime-stack` checks API liveness, readiness, worker coverage, local-upstream reachability, and optionally the actual database-runtime identity (`postgresql`, `postgis`, schema-current diagnostics) over HTTP so the Docker/local operator path has an actual pass/fail gate instead of vibes.
- The service now exposes a deployment-grade readiness probe too: `/ready` returns `200` only when the runtime readiness audit passes and `503` when action-required checks remain, while `/health` stays a lighter liveness probe so container orchestration does not confuse “process is alive” with “platform is fully ready.”
- Background worker state is observable now too: scheduler workers, source runtime workers, and the unified `platform-runtime-worker` publish persisted heartbeat/status records, `/api/operations/workers` plus `/api/operations/workers/summary` expose them over the API, `show-worker-summary` exposes them in the CLI, and the platform-wide operations report includes the same worker summary so operators can see whether the headless executors are actually alive.
- There is a unified executor path now too: `elevenwriter platform-runtime-worker` runs stream collection and enabled scheduled work in one supervised loop, publishes its own heartbeat, and can satisfy both scheduler and stream-runtime coverage when you want one local service instead of babysitting separate worker commands.
- Worker health is machine-checkable now too: `elevenwriter check-worker-health` returns exit code `0` only when a matching executor heartbeat is recent enough, and the default compose stack uses that to healthcheck the unified runtime worker instead of blindly assuming the process is still useful.
- The runtime can now also be force-executed coherently on demand: `/api/scheduler/run-enabled` and `run-enabled-schedules` execute enabled schedules immediately without waiting for `next_run_at`, while `/api/operations/runtime/cycle` and `run-platform-cycle` run one whole local platform cycle by combining stream-source collection with the enabled scheduled pipeline in dependency-aware order.
- The API-side runtime-cycle trigger is normalized now too: if `/api/operations/runtime/cycle` hits a persisted stream-source collection failure, it returns the structured source execution error payload instead of collapsing into a generic 500 and making operators guess which runtime source blew up.
- The search-provider catalog is no longer tribal knowledge now either: `/api/sources/web-search/providers` and `list-web-search-providers` expose the built-in symbolic search profiles so operators can see the default target URI, query param, redirect param, and paging behavior before wiring a whole campaign around them.
- Runtime backup and recovery now have a first-class path too: `/api/operations/runtime/export`, `/api/operations/runtime/restore`, and matching CLI commands serialize the core backend state in dependency-safe order and log custody records for both export and restore.
- Runtime snapshot exports are hardened now too: `export-runtime-snapshot` writes a sidecar manifest with the snapshot checksum, byte size, section counts, and row-count ledger; `verify-runtime-snapshot` checks that artifact locally; and `restore-runtime-snapshot` refuses to touch the database unless the snapshot still matches its manifest. Because blind restore from an unverified blob is clown behavior.
- Runtime bundles complement snapshots by packaging the actual locally retained watch-image bytes alongside database state and a checksum manifest. Use `export-runtime-bundle` and `restore-runtime-bundle` when evidence must remain recoverable; a snapshot preserves ledger state, not the pointed-to files.
- Runtime backup coverage is scheduler-native now too: `runtime_snapshot_export` can run as an enabled task, bootstrap provisions it by default, and readiness now checks for a recent paired snapshot+manifest artifact instead of declaring the runtime “ready” with zero recovery posture.
- Runtime snapshot coverage now extends across newer operational subsystems too, including scheduler task/run state, source-run history, persisted worker heartbeat/status state, camera/source registries, storage objects, and maintenance-task lineage, so recovery testing is following the real backend instead of freezing at an older shape.
- The storage-core slice is real now, not a manifesto: `/api/storage/*` plus the `list-storage-objects`, `show-storage-manifest`, `add-storage-object`, `promote-storage-object`, `transition-storage-object`, `archive-storage-object`, `verify-storage-object`, `request-storage-rehydrate`, `rehydrate-storage-object`, `prune-storage-object`, `quarantine-storage-object`, `unquarantine-storage-object`, and `run-storage-lifecycle` CLI commands expose a first-class artifact ledger with retention classes, tiering, and lifecycle controls.
- Storage mutation routes are operator-grade now too: manifest/archive/verify/rehydrate/prune/quarantine flows return structured failure payloads with `storage_object_id`, action name, and error class instead of a random plain string when a backing artifact or lifecycle precondition is missing.
- Imports and camera materialization now auto-register storage manifests, so raw local files plus camera image/stream/page references get tracked as storage objects with expiration windows and custody events instead of disappearing into the void.
- Managed sources now participate in that same storage/provenance model too: each source run materialization is tracked as a storage object, and `/api/sources/{id}/ops` plus `show-source-ops` expose recent runs, stored payload artifacts, and related custody logs in one place.
- Direct source execution is operator-grade now too: `/api/sources/{id}/run`, `/api/sources/{id}/runtime`, and `/api/sources/{id}/webhook` return structured failure payloads with `source_run_id`, `source_kind`, and error class when collection fails after the run has already been persisted, so operators can pivot straight from the API error into the recorded failed run and dead-letter/custody trail instead of guessing which ingestion attempt exploded.
- Managed sources now have a fleet-level ops surface too: `/api/sources/summary` and `/api/sources/report-index` expose stale sources, failing runs, schedule coverage, and recent sync activity so operators can see source health across the whole catalog instead of auditing one source at a time.
- Whole-web collection finally shows up in that fleet view too: source summaries and report indexes now aggregate `web_search`, `web_crawl`, and `web_discovery` campaign telemetry such as search-page counts, crawl-page counts, robots-policy blocks, private-network blocks, and fetch-error counts, and they call out the specific web collection sources carrying those issues instead of burying everything in per-run JSON.
- Source fleet reporting is exportable now too: `/api/sources/export/summary` and `export-source-summary` package the source catalog plus fleet health report into a JSON artifact that registers in the storage/provenance ledger like the camera and scheduler exports.
- API-side persisted export parity now covers the major summary domains too: the backend can write and register local source, scheduler, alert, event, entity, camera, and camera-source summary artifacts directly through POST export routes instead of forcing every downstream system to shell out through the CLI.
- Events and entities have proper fleet-grade ops surfaces now too: `/api/events/summary`, `/api/events/report-index`, `/api/events/export/summary`, `/api/entities/summary`, `/api/entities/report-index`, and `/api/entities/export/summary` expose analytic-object counts, stale/open posture, confidence bands, conflict load, recent fusion/resolution runs, and exportable JSON snapshots instead of leaving operators with raw list endpoints and vibes.
- Operator exports now participate in that same storage/provenance model too: event bundles, exported products, camera summary exports, operations reports, and runtime snapshots all register as storage objects when written through the headless export path instead of falling off the ledger.
- ClickHouse is now an optional secondary backend instead of a hand-wavy future idea: `/api/operations/clickhouse` exposes diagnostics, provisioning, runtime sync, R2 archive export, R2 rehydration, and R2 storage-config preview, while the CLI mirrors those same flows for headless ops.
- Those ClickHouse operator routes are normalized now too: provision/sync/archive/rehydrate/config failures return structured API payloads with action names and key context like `archive_glob_url` instead of forcing clients to scrape plain exception strings.
- Forte still keeps PostgreSQL/SQLite as the primary operational store. ClickHouse is wired for analytics, cold archive, and large-scale query workloads; pretending it fully replaces the relational runtime here would be unserious.
- Cloudflare R2 support now covers three operator paths: `archive_only` export into partitioned Parquet, `hybrid` rehydrate/query workflows over R2 data, and optional `r2_disk` provisioning for self-hosted ClickHouse storage policies backed by R2.
- Observation reads can now hit those optional backends too: `/api/observations` and `/api/observations/cross-verify` accept `backend=clickhouse` for synced hot tables or `backend=r2_archive` for direct archived Parquet reads over Cloudflare R2 when the operator needs cold-data investigation without bulk rehydration first.
- Those observation/query routes are operator-grade now too: backend/query failures come back as structured error payloads with action names and backend context, which matters when the API is the UI and a headless client needs something better than “409, trust me bro.”
- ClickHouse maintenance is scheduler-native now too: `clickhouse_sync` and `clickhouse_archive` tasks can mirror runtime facts and archive observation partitions toward R2 without waiting for an operator to remember the command at 2 AM.
- Camera/webcam work is no longer just notes: `/api/cameras` and `/api/cameras/materialize` now persist camera inventory from imported observations, including MnDOT-style feeds that expose image or stream endpoints plus geospatial coordinates.
- Camera endpoint lifecycle is backend-native now too: `/api/camera-sources`, `/api/camera-sources/materialize`, `/api/camera-sources/summary`, and `/api/camera-sources/{id}/ops` maintain a candidate source registry for observed camera image/stream/page endpoints, with rule-based graduation scores and custody history.
- Camera endpoint verification is backend-native now too: `/api/camera-sources/verify` plus `verify-camera-sources` can actively probe image, stream, and page endpoints, persist reachability state, and preserve that verification through later source rematerialization instead of resetting everything back to mere observation.
- Missing event/entity/alert/camera detail routes are normalized now too, so ops/export/detail endpoints return machine-readable 404/403 payloads with ids and actions instead of inconsistent plain strings across half the backend.
- Camera source candidate ops now have a fleet-level backend surface too: `/api/camera-sources/report-index` summarizes refresh-task coverage, recent source materializations, and stale candidate endpoints, while `/api/camera-sources/export/summary` emits a JSON-ready artifact for downstream systems and archival.
- Camera ops now have a proper backend reporting surface too: `/api/cameras/summary` rolls up fleet health by layer, domain, provider, and status, while `/api/cameras/{id}/ops` exposes per-camera custody, latest observation/import context, and matching refresh schedules.
- Camera reporting is exportable now too: `/api/cameras/report-index` summarizes refresh task coverage, recent materializations, stale inventory, and recent refresh runs, while `/api/cameras/export/summary` emits a JSON-ready artifact for downstream systems and archival.
- Camera inventory upkeep is scheduler-native now too, so the registry can be refreshed headlessly with `camera_inventory_refresh` tasks instead of waiting for an operator to remember the manual materialization command. Those refresh runs also keep the camera source candidate registry in sync automatically.
- Entity resolution and event fusion are still rule-based, but they are no longer just opaque score emitters: entity records now persist analytic drivers/cautions plus explicit signal-conflict records, and fused event products now explicitly state confidence drivers, weakening factors, linked-entity evidence, and linked-entity disagreement context so downstream operators and future agents can inspect why the system believes what it believes.

### Web Search CLI

```bash
elevenwriter add-source-web-search-engine broad-web-discovery \
  world-events \
  --query "harbor departure" \
  --query "rail delay" \
  --provider duckduckgo_html \
  --search-page-limit 2 \
  --page-fetch-limit 10 \
  --crawl-from-results \
  --result-crawl-depth 1 \
  --result-crawl-page-limit 25
```

If you already have a custom HTML search endpoint, keep using `add-source-web-search` and set `--search-page-param`, `--search-page-start`, and `--search-page-step` when the provider exposes paged result sets.

Use `elevenwriter list-web-search-providers` or `/api/sources/web-search/providers` to inspect the built-in symbolic provider profiles before you pick one. If you need crawl-policy compliance with a specific operator identity, pass `--robots-user-agent <name>`; `web_search`, `web_crawl`, and `web_discovery` sources now respect `robots.txt` by default and report blocked URLs in source-run metadata instead of ingesting them anyway like feral scrapers.

If you need stricter fetch guardrails, the web-source CLI commands now also accept `--max-payload-bytes` and `--min-request-interval-seconds`. Use API metadata or source updates to set `allow_private_networks=false` when you want to block loopback/private-target collection entirely.

### Web Discovery CLI

```bash
elevenwriter add-source-web-discovery broad-web-campaign \
  world-events \
  --query "harbor departure" \
  --query "rail delay" \
  --provider duckduckgo_html \
  --seed-url https://example.com/briefings/root \
  --discover-sitemaps-from-seeds true \
  --crawl-depth 2 \
  --crawl-page-limit 100 \
  --resume-frontier true
```

For local-only validation, point `--provider-target` at the repo's `local-upstreams` search harness and let the campaign discover `/robots.txt` plus `/sitemap.xml` on its own. The crawl frontier state is persisted into the source checkpoint so repeated runs continue the campaign instead of redoing the same first few pages forever.
- Camera source verification is scheduler-native now too: `camera_source_verification` tasks can continuously re-check endpoint reachability and surface failed or recovered camera URLs without a human babysitting the registry.
- Alerts finally have fleet-level ops parity too: `/api/alerts/summary`, `/api/alerts/report-index`, `show-alert-summary`, and `show-alert-report-index` expose stale open alerts, severity/status buckets, geofence-scan coverage, and recent alert activity without forcing operators to eyeball raw rows like cavemen.
- Alert reporting is exportable now too: `/api/alerts/export/summary` and `export-alert-summary` package alert inventory plus geofence-scan health into a JSON artifact that registers in the storage/provenance ledger like the rest of the backend exports.
- The platform-wide operations report now includes source inventory and source sync health sections too, so source fleet issues show up alongside camera, alert, import, and scheduler activity in one backend report.
- The platform-wide operations report now carries camera inventory, camera source candidate inventory, and camera refresh sections too, so one headless report can show both event/alert activity and the current health of the camera subsystem.
- The platform-wide operations report now also carries storage lifecycle inventory plus ClickHouse backend diagnostics, so one headless report can expose artifact retention state and optional analytics-backend health instead of making operators hop across multiple commands.
- The platform-wide operations report now carries alert inventory and alert geofence-scan health sections too, so stale open alerts and scan coverage show up in the same backend report as scheduler, source, camera, and storage state.
- API-side artifact export parity exists for core ops outputs now too: `/api/operations/report/export` can persist and register a scoped operations report artifact locally, and `/api/operations/runtime/export-artifacts` can write the runtime snapshot plus manifest pair through the backend without shelling out to the CLI.
- The scheduler finally has a fleet-level ops surface too: `/api/scheduler/summary`, `/api/scheduler/report-index`, `show-scheduler-summary`, and `show-scheduler-report-index` expose overdue tasks, failing latest runs, maintenance coverage, and task-type run/failure buckets instead of forcing operators to reverse-engineer health from raw run rows.
- Scheduler reporting is exportable now too: `/api/scheduler/export/summary` and `export-scheduler-summary` package the scheduler fleet report plus task inventory into a JSON artifact that registers in the storage/provenance ledger like the rest of the backend exports.
- Entity resolution and event fusion are scheduler-native too, so the backend can keep promoting raw observations into reusable entities, linked events, and generated products without a human sitting there pressing the button like it's 2009.
- Geofence scans create persisted alerts with dedupe keys so the same observation-hit pair does not spam duplicates.
- Scheduler runs and import operations both write custody records so unattended execution still leaves an audit trail.
- Cross-verification is rule-based today: it clusters nearby observations inside a time window and raises confidence when independent domains or layers corroborate the same occurrence.
- Verification outputs now include trusted-observation counts, integrity-source counts, ground-truth hits, and observed-time spans so operators can judge corroboration quality without spelunking raw rows.
- Entity resolution is rule-based today, but it is no longer just same-row signal stitching: it now builds evidence-graph components across hard identifiers, repeated supporting signals, and context-consistent soft matches so reusable entity records carry temporal/geospatial support, evidence strength, conflict tracking, and stronger confidence scoring.
- Event fusion is rule-based today, but the generated products are now materially richer: corroborated clusters materialize into events with linked entity context, related alert lineage, source-reliability breakdowns, timelines, confidence assessments, and explicit collection gaps so the headless runtime emits something closer to analyst-grade fusion before LLM narrative generation exists.
- Event export bundles package the event, linked observations, resolved entities, import/source context, generated products, citations, and relevant custody records into one JSON artifact for downstream systems or archival.
- Event export bundles now also carry relevant alerts plus scheduled task and scheduled run history, so downstream review can see not just the evidence but the operational path that produced and monitored it.
- Event and product exports now honor requested redaction ceilings, so lower-clearance bundles can exclude higher-sensitivity entities and products instead of leaking them by accident like amateurs.
- Managed sources provide a named catalog for repeatable ingestion; scheduler tasks can now sync those source definitions instead of only replaying raw file paths.
- Managed sources and scheduled tasks now support update/enable-disable lifecycle controls with custody logs, so operators can pause, retarget, and resume headless workflows without deleting history.
- HTTP-managed sources support retry attempts, timeout controls, custom headers, cached payload materialization, and persisted fetch metadata so headless sync jobs have enough operational context to debug failures.
- Managed source runs are idempotent by default: when the payload hash matches the most recent completed or skipped run for that source, the new run is marked `skipped` and does not create duplicate imports.
- Local imports are dedupe-aware too: exact duplicate records in the same layer are skipped, and each import run reports `records_seen`, `records_imported`, and `records_skipped`.
- Local file ingest is less brittle now too: `.json`, `.jsonl`, `.txt`, `.sqlite`, and source-code/text artifact imports (`.c`, `.cpp`, `.go`, `.java`, `.php`, `.r`, plus common adjacent code formats) normalize parser failures into structured API errors, and plain-text line feeds now materialize into proper observation records instead of falling over on a missing hash like an unserious MVP.
- `import-local` also accepts directory trees now, recursively ingesting supported JSON/text/SQLite/source-code files as one local bundle while preserving per-observation source-file provenance (`local_source_path`, `local_source_name`, and line/row metadata where applicable).
- Scheduled tasks now support task-level retry attempts and linear retry backoff, with custody records for failed attempts, retry scheduling, and final completion/failure outcomes.
- The scheduler worker is a first-class CLI runtime now, and due-task execution is resilient: one failed task run does not stall the rest of the due queue.
- Direct scheduler task execution is operator-grade now too: `/api/scheduler/tasks/{id}/run` returns structured failure payloads with `task_run_id`, `task_type`, and error class when a task run fails after being persisted, so operators can immediately pivot from the API error into the recorded failed run and custody trail instead of reverse-engineering which execution blew up.
