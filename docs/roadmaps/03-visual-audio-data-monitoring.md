# Roadmap 03: Visual, Audio, and Local Data Intelligence

## Outcome

Forte can discover, ingest, deduplicate, and locally analyze public images, video,
audio, and structured data. It can identify a likely meaningful construction-progress
image (or analogous visual change) before a rare LLM review, and provides a clean
candidate/evidence contract to investigations and watches.

The first end-to-end target is: monitor publicly accessible sources for an updated image
of a named construction site, reject obvious mismatches/duplicates, identify a material
visual update, retain cited evidence, and publish a candidate to the API-feed workflow.

## Shared merge gate and ownership

Start from the repaired, green baseline. This worktree owns artifact media ingestion,
content-addressed local object storage, media transforms, local inference, visual/audio
feature records, and visual-change candidates. It does not own report prose, watch API
delivery, or entity graph semantics.

## Security and local-model policy

- Hardware target: Intel i9 13th-generation CPU and NVIDIA RTX 4070 Laptop GPU, with
  a CPU fallback and no dependency on integrated graphics.
- Local models must perform inference without contacting the wider internet. Package
  installation/model download happens only as an explicit setup operation, never inside
  the runtime worker.
- Before adopting a model, record upstream origin, license, version, cryptographic
  checksum, package lock/SBOM, CVE review, hardware requirement, test fixtures, and
  network-disabled inference proof. Reject models that cannot meet this contract.
- Never allow a model's generated labels to replace source provenance or evidence.

## Milestones

### M1. Artifact taxonomy and local content-addressed store

Implement the shared artifact roles exactly: `raw`, `normalized`, `derived`, `evidence`,
and `cache`. Store large bytes on local disk in deterministic hash-sharded paths; track
object UID, BLAKE3 and SHA-256 hashes, media metadata, original URI, source/capture
times, retention class, transform chain, provenance, custody, and lifecycle state in
the storage ledger.

Use local directories for spool, transforms, cache, quarantined input, hot artifacts,
and archive. Do not store giant media blobs in Postgres. Preserve original bytes for
evidence; derivatives are children, never replacements.

Acceptance tests:

- Exact-byte duplicates share one local blob but retain multiple owner references.
- A promoted evidence original cannot be auto-deleted or overwritten.
- Snapshot/restore preserves manifests and detects a missing/corrupt object.

### M2. Deterministic image/video/audio intake

Build safe workers for image, video, audio, and public webpage image extraction:

- MIME/signature validation, size/dimension/duration limits, decompression-bomb
  defenses, quarantine state, metadata extraction, exact hash, and perceptual hash;
- thumbnails/proxies/keyframes/waveform/text extraction where appropriate;
- duplicate/near-duplicate suppression and source-page capture references;
- video ring buffers and trigger windows without retaining every stream forever.

For a claimed satellite image, deterministic checks should flag implausible image
properties and people/ground-photo cues before any LLM review. A flag is a review cue,
not a declaration that the image is fake.

Acceptance tests:

- Malformed, oversized, and spoofed-content files quarantine safely.
- Unchanged camera stills do not create duplicate stored bytes.
- A changed image produces a feature record and an auditable transform chain.

### M3. High-quality local inference adapters

Implement versioned adapters rather than hardwire one model: object/person detection,
scene/image embedding, OCR, speech-to-text, acoustic/event features, and structured
data anomaly detection. Run local GPU inference where supported, with bounded CPU
fallback. Record model/config/input/output hashes and confidence/reason codes.

Choose the actual models only after the security gate and benchmark them on a curated
public fixture set relevant to cameras, construction, satellite/map imagery, documents,
and audio. Quality gates must measure false positives/negatives, throughput, VRAM,
CPU fallback, and reproducibility; do not ship a model merely because its demo looks
cool on a Friday night.

Acceptance tests:

- The inference process has no outbound network dependency during a test run.
- Each output records model/version/config and input artifact IDs.
- GPU unavailability degrades predictably without dropping artifact provenance.

### M4. Material visual-change pipeline

Implement a rule-first score using source credibility, named-location association,
capture time, exact/near dedupe, scene similarity, semantic/object features, OCR/text,
geospatial hints, and observed change against a baseline. Classify candidates as
`duplicate`, `irrelevant`, `insufficient_context`, `possible_change`,
`material_change_candidate`, or `confirmed_change`.

Only `material_change_candidate` items may request exceptional Codex review. Supply the
agent cropped/derived features, captions, source excerpts, and relevant historical
baseline—not an unbounded media dump. Forte validates any conclusion and requires
citable source context before a watch update is emitted.

Acceptance tests:

- A reposted photo produces no alert.
- A same-site, visibly advanced construction photo becomes a reviewable candidate.
- An unrelated image with a matching keyword is rejected by deterministic evidence.
- LLM quota exhaustion leaves candidates queued; the pipeline continues collecting.

### M5. Lifecycle, compaction, and operations

Implement supplied policy defaults for local disk: rolling camera stills and streams
expire quickly; original event evidence is retained; thumbnails/features/OCR/transcripts
often outlive non-evidence media; every deletion leaves a tombstone. Add disk pressure
controls: lower sampling, retain features over bytes, pause noncritical sources, and
surface actionable health metrics before storage is exhausted.

## Validation and handoff

- Fixture-driven tests for image, video, audio, web image, duplicate, near-duplicate,
  construction change, unrelated candidate, quarantine, retention, and model isolation.
- Benchmark report against the target CPU/GPU with reproducible model versions and no
  secret or external-runtime dependency.
- Document local-disk layout, model installation approval/checksum procedure, resource
  budgets, known accuracy limitations, and the API contract consumed by Roadmap 02.
