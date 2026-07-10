# Watch Engine: Local Deterministic Demo

This walkthrough proves the Watch Engine entirely on one machine. It uses a replaceable local image fixture, the existing camera materialization path, deterministic SHA-256 comparison, local artifact retention, scheduler execution, custody logs, the API, and RSS. It needs no frontend, cloud account, paid data source, or model call.

The boundary is deliberate: rules fetch and evaluate evidence, hashes decide whether content changed, dedupe state decides whether an alert is new, and the storage ledger records retained bytes. An LLM is not involved in any of those decisions.

## 1. Start Forte

From `app/server`:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
elevenwriter init-db
uvicorn src.main:app --reload --port 8000
```

Keep the API running and use another PowerShell terminal for the remaining commands.

## 2. Create a controlled changing image

From the repository root, create two tiny local GIF fixtures. They are intentionally different byte sequences, so SHA-256 comparison has an unambiguous answer.

```powershell
New-Item -ItemType Directory -Force .\watch-fixture | Out-Null

$imageV1 = [Convert]::FromBase64String(
  "R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw=="
)
$imageV2 = [Convert]::FromBase64String(
  "R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs="
)

[IO.File]::WriteAllBytes("$PWD\watch-fixture\piston-peak-v1.gif", $imageV1)
[IO.File]::WriteAllBytes("$PWD\watch-fixture\piston-peak-v2.gif", $imageV2)
Copy-Item .\watch-fixture\piston-peak-v1.gif .\watch-fixture\piston-peak-current.gif -Force
```

Serve that directory locally in a third terminal:

```powershell
python -m http.server 8765 --directory .\watch-fixture
```

The watched image is now available at `http://127.0.0.1:8765/piston-peak-current.gif`.

## 3. Register and materialize the camera

Create an observation fixture that points at the local image endpoint:

```powershell
@'
[
  {
    "camera_id": "piston-peak-local-fixture",
    "camera_name": "Piston Peak construction fixture",
    "title": "Piston Peak construction fixture",
    "status": "online",
    "image_url": "http://127.0.0.1:8765/piston-peak-current.gif",
    "page_url": "http://127.0.0.1:8765/",
    "provider": "local-controlled-fixture",
    "lat": 28.3772,
    "lon": -81.5707
  }
]
'@ | Set-Content .\watch-fixture\camera-observation.json -Encoding utf8

elevenwriter import-local .\watch-fixture\camera-observation.json --layer piston-peak-demo
elevenwriter materialize-cameras --layer piston-peak-demo
elevenwriter list-cameras --layer piston-peak-demo
```

Note the camera ID printed by `list-cameras`. The examples below use `1`; substitute the actual ID if yours differs.

## 4. Create and inspect the image watch

```powershell
elevenwriter add-watch `
  "Piston Peak construction image" `
  image_change `
  "Notify when the controlled Piston Peak construction image changes." `
  --slug piston-peak-construction-image `
  --camera-inventory-id 1 `
  --severity warning `
  --rule-json '{"mode":"image_change","comparison":"sha256","alert_on_initial":false,"retention_class":"permanent"}'

elevenwriter list-watches --watch-type image_change
elevenwriter show-watch 1
```

The default notification policy enables the local API and RSS and leaves `analysis_on_change` disabled.

Equivalent API calls are available at:

```text
GET    /api/watches
POST   /api/watches
GET    /api/watches/runs
GET    /api/watches/alerts
GET    /api/watches/feed.rss
GET    /api/watches/{watch_id}
PATCH  /api/watches/{watch_id}
POST   /api/watches/{watch_id}/pause
POST   /api/watches/{watch_id}/resume
POST   /api/watches/{watch_id}/run
POST   /api/watches/{watch_id}/schedule
GET    /api/watches/{watch_id}/runs
GET    /api/watches/{watch_id}/alerts
GET    /api/watches/{watch_id}/evidence
```

For example:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/watches/1
Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/watches/1/run
```

## 5. Establish the baseline

```powershell
elevenwriter run-watch 1
elevenwriter list-watch-runs --watch-id 1
elevenwriter list-watch-alerts --watch-id 1
```

The first evaluation should complete with `outcome=baseline`, `changed=False`, and no alert because `alert_on_initial` is false. The accepted image is still retained locally as evidence; baseline retention is not an alert.

## 6. Replace the image and detect the change

Replace the served file without changing its URL:

```powershell
Copy-Item .\watch-fixture\piston-peak-v2.gif .\watch-fixture\piston-peak-current.gif -Force
elevenwriter run-watch 1
```

The second evaluation should complete with `outcome=change` and `changed=True`. The decision is the SHA-256 comparison between the retained baseline and the newly fetched bytes.

Inspect the result:

```powershell
elevenwriter list-watch-runs --watch-id 1
elevenwriter list-watch-alerts --watch-id 1 --status open
elevenwriter show-watch-evidence 1
elevenwriter list-custody --limit 100
```

The evidence output gives the storage-object ID, content hash, media type, byte size, and local URI. The custody log includes watch evaluation, trigger, alert, and artifact-retention actions. Running the watch again without replacing the file should produce `outcome=no_change` and no duplicate alert.

The same records are available through the API:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/watches/1/runs
Invoke-RestMethod http://127.0.0.1:8000/api/watches/1/alerts
Invoke-RestMethod http://127.0.0.1:8000/api/watches/1/evidence
```

## 7. Read the local RSS feed

Print the feed URL or render the exact XML locally:

```powershell
elevenwriter show-watch-feed --watch-id 1
elevenwriter show-watch-feed --watch-id 1 --preview
```

The HTTP feed is:

```text
http://127.0.0.1:8000/api/watches/feed.rss?status=open&watch_id=1
```

It contains the alert message, severity, watch identity, timestamps, local API links, and evidence references. XML is generated through an XML library so operator-provided text is escaped safely.

## 8. Attach a scheduler-native watch task

```powershell
elevenwriter add-watch-schedule 1 piston-peak-watch-poll 60 --retry-attempts 3 --retry-backoff-seconds 2
elevenwriter list-schedules
elevenwriter run-schedule 1
elevenwriter run-due-schedules
elevenwriter scheduler-worker --poll-seconds 5
```

Use the scheduled-task ID reported by `add-watch-schedule` with `run-schedule`; it may not be `1` in an existing database. Scheduled evaluation writes both scheduled-task-run history and watch-run history.

Pause and resume remain explicit lifecycle operations:

```powershell
elevenwriter pause-watch 1
elevenwriter resume-watch 1
```

## 9. Back up database state and retained evidence

A runtime snapshot stores database state and storage-ledger pointers. A runtime bundle additionally packages the retained watch image bytes and verifies their hashes during restore.

```powershell
elevenwriter export-runtime-snapshot .\exports\runtime-snapshot.json
elevenwriter export-runtime-bundle .\exports\runtime-bundle.zip
elevenwriter restore-runtime-bundle .\exports\runtime-bundle.zip --replace-existing
```

Use a bundle when the evidence bytes themselves must survive a move or restore; a snapshot alone is not an evidence archive.

## Expected deterministic outcomes

| Evaluation | Served bytes | Outcome | New alert |
| --- | --- | --- | --- |
| First | Version 1 | `baseline` | No |
| Second | Version 2 | `change` | Yes |
| Third | Version 2 again | `no_change` | No |

That table is the whole point: the same evidence and checkpoint state produce the same result, with no model judgment hiding in the middle.
