# Evidence-backed investigations

Forte investigations use only public information and keep directly observed facts,
attributed claims, inference, conflict, and unknowns distinct. The runtime never
bypasses paywalls, sign-in walls, robots policy, or rate/byte/concurrency budgets.

The lifecycle is `draft → planning → collecting → normalizing → corroborating →
reporting → ready → monitoring | archived | insufficient_evidence | failed`.
Failures and dead ends remain structured attempt records; they do not become evidence
or a successful investigation.

Reports use `investigation-report/v1`, preserve capture/publication times and immutable
local artifact citations, and can be produced in default or extended form without
changing the evidence threshold. Archiving retains report and promoted evidence while
ordinary raw artifacts follow the local storage retention policy. Monitoring emits only
the structured investigation contract; schedules, promotions, retention, and alerts
remain Forte-runtime responsibilities.

After transition to `monitoring`, Roadmap 02 can read the data-only
`GET /api/investigations/{id}/monitor-contract` payload. This endpoint cannot create
or modify a schedule, source, alert, delivery, or lifecycle state.

Artifact bytes are stored locally under a BLAKE3 content address and retain a SHA-256
cross-check, provenance, transform-chain metadata, and one of the five data roles:
`raw`, `normalized`, `derived`, `evidence`, or `cache`. Operational database rows hold
references and hashes, never the archive bytes themselves.

Intentionally unsupported: private-data research, access-control bypasses, live
operational targeting, autonomous scheduling/delivery, or agent-created sources and
alerts. Model availability must be explicit; unavailable configured models fail closed.

## API and CLI

Create a record with `POST /api/investigations`, then advance it with
`POST /api/investigations/{id}/transition`. Discovery attempts are append-only at
`POST /api/investigations/{id}/attempts`; reports and promoted evidence use their
matching `/reports` and `/evidence` endpoints. `POST /api/investigations/{id}/archive`
applies raw-artifact retention while preserving report versions and promoted evidence.

```powershell
elevenwriter create-investigation public-logistics-pattern `
  "What do public sources document?" `
  --operator-scope-json '{"public_information_only": true}'
elevenwriter transition-investigation 1 planning
elevenwriter archive-investigation 1 --stop-reason "review complete"
```

## Migration and provider notes

This is an additive schema migration: deploy code, run the normal `init_db` path,
and verify the runtime snapshot is version 3 before restoring into another runtime.
Existing records are untouched. The configured investigator IDs are
`gpt-5.3-codex-spark`, `gpt-5.4-mini`, and `gpt-5.6-luna`; availability is checked by
the caller before routing. Provider quota telemetry has no assumed fallback: missing or
unreliable telemetry pauses LLM work unless an explicitly approved one- or two-window
calibration is active.
