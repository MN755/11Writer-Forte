# Media intelligence

Forte's media path is local-only at runtime. It accepts already-acquired bytes, validates
them before persistence, stores originals by content hash, and produces deterministic
evidence records for downstream investigations and watches. It never downloads a model,
opens a remote media URL, or permits generated labels to overwrite source provenance.

## Local-disk layout

All paths are under `ELEVENWRITER_DATA_DIR` (default `./var`):

- `media_intake/hot/sha256/<aa>/<bb>/<hash>` contains validated intake bytes.
- `media_intake/quarantine/` retains rejected input plus a receipt explaining why it was
  quarantined.
- `artifact_store/hot/blobs/<aa>/<bb>/<hash>` is the authoritative artifact object store.
- `artifact_store/ledger.json` records roles (`raw`, `normalized`, `derived`, `evidence`,
  `cache`), SHA-256, BLAKE3 (or explicitly named BLAKE2b fallback), source/capture times,
  transform chain, provenance, custody, owners, lifecycle state, and lineage.
- `artifact_store/{spool,transforms,cache,quarantine,archive,tombstones}` reserve the
  operational locations required by media workers and retention sweeps.

Exact-byte artifacts share a hash-sharded blob while retaining independent owner records.
Evidence promotion makes an original immutable and permanent. Snapshot/restore checks both
the snapshot manifest and every retained blob before it writes the destination.

## API contract

- `POST /api/media-intelligence/intake` accepts base64 of bytes already collected by an
  approved worker. It validates signatures and claimed MIME, applies byte/dimension/duration
  limits, quarantines unsafe input, adds a content-addressed artifact, and records a storage
  ledger reference.
- `POST /api/media-intelligence/web-image-reference` only validates and canonicalizes public
  HTTP(S) image/page references; it does not fetch them.
- `POST /api/media-intelligence/embeddings` runs a checksum-approved local ONNX image
  encoder and returns an L2-normalized scene embedding plus model/config/input/output hashes,
  device, and CPU-fallback evidence. `POST /api/media-intelligence/inference` is retired;
  the former deterministic pass-through must not be presented as production inference.
- `GET /api/media-intelligence/models` inventories valid local ONNX approvals without
  exposing model paths, model bytes, secrets, or local filesystem layout.
- `POST /api/media-intelligence/visual-change` performs rule-first visual triage. It emits
  `duplicate`, `irrelevant`, `insufficient_context`, `possible_change`,
  `material_change_candidate`, or `confirmed_change`. Only material candidates receive a
  bounded review packet. If review quota is exhausted, they remain queued and collection
  continues.

The visual-change response is the candidate/evidence contract for the watch/API delivery
layer: `observation_artifact_id`, `baseline_artifact_id`, `classification`, score components,
reason codes, review status, and a bounded source/derived-feature packet. The delivery layer
must require citable source context before emitting an update.

## Model approval gate

An operator must explicitly install a model before registering it. Record all of the
following in the model manifest and retain the associated files in the artifact ledger:

1. Upstream origin, license, pinned model version, artifact SHA-256, package-lock hash, SBOM
   hash, and CVE-review reference.
2. Hardware requirement and benchmark fixture reference for the i9/RTX 4070 Laptop target,
   including CPU fallback throughput and VRAM use.
3. A network-disabled inference test. Runtime adapters have no downloader or HTTP client; a
   failure here rejects the model.
4. Fixture measurements for false positives/negatives on construction cameras, satellite/map
   imagery, documents, and audio. A demo with good vibes is not a quality gate.

The approved local STT reference runtime is `faster-whisper==1.2.1` with the MIT-licensed
`Systran/faster-whisper-tiny.en` CTranslate2 conversion of OpenAI Whisper tiny.en. It is
installed only through the `media-local` extra and provisioned once into `var/models`; its
approval JSON, dependency lock, SBOM, and CVE audit remain there rather than being committed.
Use the project venv when operating this path:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e '.[media-local]'
# Explicit setup only; never invoke this from a Forte worker.
.\.venv\Scripts\python.exe -c "from huggingface_hub import snapshot_download; snapshot_download('Systran/faster-whisper-tiny.en', local_dir='var/models/Systran__faster-whisper-tiny.en')"
```

`POST /api/media-intelligence/transcribe` accepts only local files beneath `data_dir`, checks
the approved model tree hash, sets the Hugging Face and Transformers offline switches before
loading, and falls back predictably from CUDA to CPU int8 if GPU libraries are unavailable.
`POST /api/media-intelligence/derivatives` creates registered child artifacts for image
thumbnails/proxies or FFmpeg video proxies/keyframes/waveforms; originals are never replaced.
`POST /api/media-intelligence/ocr` runs the explicitly installed Tesseract 5 LSTM engine using
its checksum-approved, Apache-2.0 English language data. Install it with
`winget install --id UB-Mannheim.TesseractOCR --exact`, then create an approval record that
hashes the engine, `tessdata`, installer lock, SBOM, and CVE-review evidence. The OCR endpoint
accepts only data-dir image and approval paths and invokes Tesseract with fixed `--oem 1`
arguments—no shell interpolation, no runtime network access.

## Resource defaults and limits

The intake defaults are an 80 MiB byte limit, 12,000-pixel width/height, 48 megapixels,
256 MiB bounded PNG decode, and four-hour media duration. Inputs violating any limit are
quarantined. Current standard-library transforms provide signature validation, metadata,
hashes, PNG perceptual fingerprints, and audio/video duration checks. Approved local packages
add FFmpeg thumbnails/proxies/keyframes/waveforms, Whisper STT, and Tesseract OCR. Any further
codec or model remains unavailable until it has an equivalent approval record; Forte does not
pretend a fallback is a completed transform.

Disk pressure policy is conservative: retain evidence originals, preserve small feature/
transcript records before bytes, lower camera sampling before pausing noncritical sources, and
always write a tombstone before bytes are collected. Operators should monitor the `data_dir`
filesystem and run artifact verification/snapshots as part of routine storage maintenance.

## Accuracy limits

Perceptual hashing currently supports safely decoded, non-interlaced PNGs; unsupported formats
remain exact-hash dedupe only. Satellite plausibility and people/ground-photo cues are review
signals, never authenticity declarations. Local model outputs are supplemental feature records
and never replace source URI, page citation, capture time, or chain of custody.

## ONNX image-embedding lane

The optional `media-vision` extra provides the local ONNX Runtime, NumPy, and Pillow stack:

```powershell
python -m pip install -e '.[media-local,media-vision]'
```

Forte ships **no image model weights**. An operator must acquire a candidate model in a
separate approved setup process; record its license, checksum, SBOM, package lock, CVE review,
hardware benchmark, and offline proof before placing it under `data_dir`. An approval file
under `data_dir/model_approvals/` has this shape:

```json
{
  "manifest": {
    "model_id": "operator-approved-image-encoder",
    "version": "pinned-version",
    "kind": "image_embedding",
    "upstream_origin": "https://upstream.example/model",
    "license_id": "recorded-license",
    "artifact_sha256": "sha256-of-local-onnx-file",
    "package_lock_sha256": "sha256-of-lockfile",
    "sbom_sha256": "sha256-of-sbom",
    "cve_review_ref": "security-review-reference",
    "hardware_requirement": "RTX 4070 Laptop GPU; bounded CPU fallback",
    "test_fixture_ref": "benchmark-manifest-reference",
    "config": {"preprocess": "rgb-nchw"}
  },
  "model_path": "<absolute path beneath ELEVENWRITER_DATA_DIR>",
  "input_name": "image",
  "output_name": "embedding",
  "input_size": 224,
  "mean": [0.485, 0.456, 0.406],
  "std": [0.229, 0.224, 0.225]
}
```

At inference, Forte re-hashes the ONNX file, validates approval/image paths under `data_dir`,
bounds decoded pixels, selects TensorRT/CUDA only when ONNX Runtime actually reports it, and
otherwise uses CPU. It sets offline environment flags before loading. A failed approval, hash
mismatch, invalid output, unavailable runtime, or decode limit fails closed. The embedding is
a supplemental feature for rule-first visual-change comparison; it does not establish site
identity, construction progress, or source authenticity.
