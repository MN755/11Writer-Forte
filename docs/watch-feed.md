# Watch feed and retention API

The versioned watch feed is local-only by default. It has no user authentication layer,
because pretending an unauthenticated investigative API is fine on the public internet
would be spectacularly irresponsible. Start the server on `127.0.0.1`, or explicitly
set `ELEVENWRITER_LOCAL_API_TRUSTED_NETWORKS` to a JSON list of trusted CIDRs.

All endpoints below are under `/api/v1/watch-feed`.

- `GET /updates?cursor=&watch_id=&status=&update_type=&view=operator|public` returns
  stable `watch-update-<watch_run_id>` event IDs. The opaque cursor continues in
  descending ID order and filters cannot cause an eligible update to be skipped.
- `GET /updates/{event_id}` returns one eligible update.
- `GET /watches/{watch_id}` exposes current state plus available source coverage and
  recent source-run history; `GET /watches/{watch_id}/activity` exposes watch-run
  history.
- `GET /rss` is an explicitly public-summary representation. It excludes every
  restricted artifact, local path, and non-public citation.
- `POST /watches/{watch_id}/reports` writes a cited Markdown monitoring report to the
  local storage ledger. `GET /watches/{watch_id}/reports` lists its versions.
- `POST /watches/{watch_id}/archive` generates a final report, disables its schedule,
  archives durable report/evidence artifacts, and records custody/retention handoff.

The feed fails closed: a watch run is published only when it is completed, marked as a
material deterministic change (`material` or `is_material` is true), has an accepted
update type, and includes an HTTP(S) citation. Raw evidence, `file:` URIs, source-local
paths, and higher-redaction records never enter the public view. Email and webhook
delivery remain deliberately deferred. Collection remains subject to the watch's domain
policy, robots, concurrency, byte, schedule, retry, and budget controls.

Archiving never auto-deletes evidence or legal-hold records. It preserves durable
artifacts in the archive tier, records duplicate raw-capture candidates for later
operator compaction, and leaves deletion to an explicit storage lifecycle operation.
