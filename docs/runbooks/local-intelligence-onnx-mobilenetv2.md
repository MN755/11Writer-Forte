# Offline ONNX MobileNetV2 provisioning

This is a setup operation, not an inference operation. The production command runner
never downloads models, labels, Python packages, or anything else. Network access is
needed only for an operator to stage the model in a review environment.

## Approved candidate and fixed digest

Use `mobilenetv2-7.onnx` from the ONNX Model Zoo mirror:

`https://huggingface.co/onnxmodelzoo/mobilenetv2-7/resolve/main/mobilenetv2-7.onnx`

The expected SHA-256 is:

`c1c513582d56afceff8516c73804e484c81c6a830712ab6d682253f4a3cd042f`

That digest is the Git LFS object ID recorded by the official `onnx/models` source for
this 14,246,826-byte model. The official repository is Apache-2.0 and directs users to
the `onnxmodelzoo` mirror because its own LFS downloads ended in July 2025. Treat the
model as a classifier/embedding feature extractor only; it does not establish person,
vehicle, location, satellite, or construction-change evidence.

Before any download, the operator must approve the source, Apache-2.0 terms, model
provenance, local `onnxruntime-gpu` package version/SBOM, CUDA compatibility, and CVE
review. After staging, calculate SHA-256 locally; a mismatch is a hard stop.

## Provision without inference-time network access

1. In a reviewed setup environment, stage the file at a local path. Verify its digest
   against the value above; do not substitute a floating URL or model revision.
2. Provision `numpy`, `onnxruntime-gpu`, and `Pillow` into an operator-approved Python
   environment. Record exact package hashes in the package lock and SBOM. Do not add
   them to the Forte server process or allow pip in the inference worker.
3. Create a benchmark report JSON containing the registry-required fields:
   `benchmark_version`, `corpus_manifest_sha256`, `metrics.precision`,
   `metrics.recall`, `metrics.false_alert_rate`, and `metrics.latency_ms`. Use a
   licensed holdout corpus and record GPU/CPU latency. Do not fabricate metrics.
4. Run the provisioning script (it makes no network calls):

```powershell
.\app\server\local_intelligence\onnx_image_runner\provision_mobilenetv2.ps1 `
  -DataDir C:\ForteData `
  -StagedModelPath C:\staging\mobilenetv2-7.onnx `
  -RunnerPython C:\ForteRuntimes\onnx\python.exe `
  -BenchmarkReportPath C:\staging\mobilenetv2-7.benchmark.json
```

5. Review the resulting `mobilenetv2-7.approval.json`, replace the CVE-review
   placeholder, then submit it to `POST /api/local-intelligence/models/approve`.
   The service must validate the emitted `runner_asset_path` and
   `runner_asset_sha256` alongside the Python executable before approval or execution.

The runner receives an artifact UID and custody hash over JSON stdin. It resolves the
blob from the local artifact ledger, validates the hash, loads the approved model from
its environment-only path, and returns top-k class indices/probabilities. It accepts no
image paths, URLs, download directives, or labels from a caller.

## Operational limits

Use an OS-level no-egress sandbox/network namespace for the runner process. Environment
flags and the parent process socket guard are defense in depth, not a substitute for an
actual network boundary. Review the model and runner hashes after every update, retain
benchmark evidence, and reject the manifest if either digest changes.
