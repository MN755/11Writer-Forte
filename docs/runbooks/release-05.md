# Roadmap 05 operator runbook

## Environment and dependency record

The supported operator runtime is Python 3.11. Install the application from
`app/server` with `python -m pip install -e '.[dev]'`; install
`.[dev,media-local]` only on an approved local-media worker. Record the resulting
`python --version`, `python -m pip freeze`, NVIDIA driver, CUDA runtime, and GPU name
with each deployment ticket. The optional local-media packages are deliberately not
installed by the API-only default.

## Upgrade, backup, and rollback

1. Stop workers after they finish their current leased work.
2. Run `elevenwriter export-runtime-bundle ./exports/pre-upgrade.zip` and retain its
   SHA-256 with the change record.
3. Deploy the new code and run `elevenwriter init-db`. The database records each
   versioned migration in `forte_schema_migrations`; a missing migration record is a
   failed upgrade, not something to hand-wave past.
4. Run `python -m pytest -q`, `ruff check src tests`, and the canary checks below.
5. To recover, restore the previous application build and use
   `elevenwriter restore-runtime-bundle ./exports/pre-upgrade.zip --replace-existing`.
   Never treat a downgrade as an untracked schema edit.

## Source onboarding and canary

- Register only a public, permitted provider with an explicit source type, terms note,
  access requirement, robots policy, byte/time/concurrency caps, and retention class.
- Verify the provider returns public content and that the coverage ledger records both
  successful work and bounded alternatives for a deliberate unavailable source.
- Confirm private, loopback, link-local, and redirect-to-private targets are rejected.
- Capture the canary question, source categories, artifact hashes, citations, report,
  and any insufficient-evidence stop reason outside the repository.

### Official provider bootstrap

- `POST /api/research-fleet/configured-providers/bootstrap-official` is idempotent and
  installs only the Federal Register API metadata feed. It stores its fixed public
  endpoint and operational budgets, but does not perform a probe or collect content.
- FederalRegister.gov documents that its API has public endpoints and does not require
  an API key. Its API presentation is informational rather than the official legal
  Federal Register edition; preserve source/capture metadata and verify legal claims
  against the corresponding GPO GovInfo edition.
- GovInfo is intentionally skipped by the bootstrap. Its API requires an
  `api.data.gov` key, so an operator must complete source onboarding and place any key
  only in the approved secret configuration—not in provider metadata, run payloads,
  snapshots, URLs, custody logs, or the repository.

## Local model approval and operations

- Acquire models only in the operator-approved setup process. Record the upstream
  origin, license, SHA-256, SBOM, CVE review, benchmark version, CPU and GPU results,
  and approval signer in an immutable approval manifest.
- Runtime workers load only approved immutable local paths. They must have no outbound
  network route; an acquisition attempt is a failure, not a fallback.
- Confirm GPU selection on the RTX 4070 Laptop where available; otherwise record the
  bounded CPU fallback reason, queue depth, and timeout.
- Reject a deployment when the default fixture processor is configured as production.

## Investigation, quota, and incident controls

- Rule workers own task creation, scheduling, retention, and delivery. Validate that
  a model response only references supplied evidence IDs and citation spans.
- Pause LLM work when quota telemetry is stale, reserve is exhausted, or the model work
  share would exceed 10%; deterministic source work may continue.
- Exercise worker restart, source policy block, storage pressure, stale telemetry, and
  GPU-unavailable drills before release. Preserve custody logs and exact stop reasons.
- A watch is activated only from an approved completed investigation and may publish
  only citation-gated material updates through the local feed.
