#!/usr/bin/env python3
"""Narrow stdin/stdout wrapper for a no-egress approved model command.

The image is intentionally model-agnostic.  It validates the artifact ledger,
hash-pins the immutable model runner, then forwards only the approved inference
request on stdin.  The actual model executable lives on the operator-mounted
``/models`` volume; downloads, package installation, and network transports are
not part of this image or its interface.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


DATA_ROOT = Path("/data")
MODEL_ROOT = Path("/models")
RUNNER_ROOT = Path("/opt/runner")
ARTIFACT_ROOT = DATA_ROOT / "artifact_store"
MAX_OUTPUT_BYTES = 100_000
REQUEST_FIELDS = {"schema_version", "artifact_id", "input_sha256", "byte_size", "task", "model_id", "model_version"}
OUTPUT_FIELDS = {"confidence", "features", "reason_codes", "output_artifact_id"}


class WorkerError(RuntimeError):
    pass


def main() -> int:
    try:
        _require_offline_environment()
        request = _read_request()
        artifact_path = _verify_artifact(request)
        runner = _approved_runner()
        model_path = _approved_model()
        output = _run_model(runner, model_path, request, artifact_path)
        sys.stdout.write(json.dumps(output, separators=(",", ":")))
        return 0
    except WorkerError as exc:
        print(f"local-intelligence-worker: {exc}", file=sys.stderr)
        return 64


def _require_offline_environment() -> None:
    if os.environ.get("ELEVENWRITER_LOCAL_INTELLIGENCE_OFFLINE") != "1":
        raise WorkerError("offline execution must be explicitly enabled")
    if os.environ.get("HF_HUB_OFFLINE") != "1" or os.environ.get("TRANSFORMERS_OFFLINE") != "1":
        raise WorkerError("model download controls are not enabled")
    if not DATA_ROOT.is_dir() or not MODEL_ROOT.is_dir() or not ARTIFACT_ROOT.is_dir():
        raise WorkerError("required read-only /data and /models mounts are unavailable")


def _read_request() -> dict[str, Any]:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        raise WorkerError("stdin must contain exactly one JSON request") from exc
    if not isinstance(payload, dict) or set(payload) != REQUEST_FIELDS:
        raise WorkerError("request does not match the approved inference contract")
    if payload.get("schema_version") != 1:
        raise WorkerError("unsupported request schema")
    if not isinstance(payload["artifact_id"], str) or not payload["artifact_id"].strip():
        raise WorkerError("artifact_id is required")
    if not _is_sha256(payload.get("input_sha256")):
        raise WorkerError("input_sha256 must be a SHA-256 digest")
    if not isinstance(payload.get("byte_size"), int) or payload["byte_size"] < 0:
        raise WorkerError("byte_size must be a non-negative integer")
    if not all(isinstance(payload[field], str) and payload[field].strip() for field in ("task", "model_id", "model_version")):
        raise WorkerError("task and model identity fields are required")
    return payload


def _verify_artifact(request: dict[str, Any]) -> Path:
    ledger_path = ARTIFACT_ROOT / "ledger.json"
    try:
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
        record = ledger["artifacts"][request["artifact_id"]]
        relative_blob = Path(record["blob_path"])
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise WorkerError("artifact is absent from the immutable ledger") from exc
    blob = (ARTIFACT_ROOT / relative_blob).resolve()
    _require_under(blob, ARTIFACT_ROOT, "artifact blob")
    if not blob.is_file():
        raise WorkerError("artifact blob is missing")
    if record.get("sha256") != request["input_sha256"] or record.get("byte_size") != request["byte_size"]:
        raise WorkerError("artifact ledger identity does not match the approved request")
    if _sha256(blob) != request["input_sha256"]:
        raise WorkerError("artifact bytes fail their custody hash")
    return blob


def _approved_runner() -> Path:
    raw_path = os.environ.get("ELEVENWRITER_LOCAL_INTELLIGENCE_MODEL_RUNNER", "")
    expected_digest = os.environ.get("ELEVENWRITER_LOCAL_INTELLIGENCE_MODEL_RUNNER_SHA256", "")
    if not raw_path or not _is_sha256(expected_digest):
        raise WorkerError("approved runner path and SHA-256 are required")
    runner = Path(raw_path).resolve()
    if not (
        _is_under(runner, MODEL_ROOT) or _is_under(runner, RUNNER_ROOT)
    ):
        raise WorkerError("approved model runner escapes its immutable image/model roots")
    if not runner.is_file():
        raise WorkerError("approved model runner is not executable")
    if _sha256(runner) != expected_digest:
        raise WorkerError("approved model runner checksum does not match")
    return runner


def _approved_model() -> Path:
    raw_path = os.environ.get("ELEVENWRITER_APPROVED_MODEL_PATH", "")
    expected_digest = os.environ.get("ELEVENWRITER_APPROVED_MODEL_SHA256", "")
    if not raw_path or not _is_sha256(expected_digest):
        raise WorkerError("approved model path and SHA-256 are required")
    model = Path(raw_path).resolve()
    _require_under(model, MODEL_ROOT, "approved model")
    if not model.is_file() or _sha256(model) != expected_digest:
        raise WorkerError("approved model checksum does not match")
    return model


def _run_model(
    runner: Path, model_path: Path, request: dict[str, Any], artifact_path: Path
) -> dict[str, Any]:
    try:
        timeout = max(1.0, min(float(os.environ.get("ELEVENWRITER_LOCAL_INTELLIGENCE_TIMEOUT_SECONDS", "60")), 120.0))
    except ValueError as exc:
        raise WorkerError("configured timeout is invalid") from exc
    environment = {
        "ELEVENWRITER_LOCAL_INTELLIGENCE_OFFLINE": "1",
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
        "NO_PROXY": "*",
        "ELEVENWRITER_APPROVED_INPUT_PATH": str(artifact_path),
        "ELEVENWRITER_ARTIFACT_STORE_ROOT": str(ARTIFACT_ROOT),
        "ELEVENWRITER_APPROVED_MODEL_PATH": str(model_path),
        "PATH": "/usr/local/bin:/usr/bin:/bin",
    }
    try:
        command = [sys.executable, str(runner)] if runner.suffix == ".py" else [str(runner)]
        completed = subprocess.run(
            command, input=json.dumps(request), text=True, capture_output=True,
            check=False, shell=False, cwd="/output", env=environment, timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise WorkerError(f"approved model runner failed: {exc}") from exc
    if completed.returncode:
        raise WorkerError(f"approved model runner failed: {(completed.stderr or 'non-zero exit')[:500]}")
    if len(completed.stdout.encode("utf-8")) > MAX_OUTPUT_BYTES:
        raise WorkerError("approved model runner output exceeds the contract limit")
    try:
        output = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise WorkerError("approved model runner did not emit JSON") from exc
    if not isinstance(output, dict) or set(output) - OUTPUT_FIELDS or not isinstance(output.get("features"), dict):
        raise WorkerError("approved model runner output violates the feature contract")
    try:
        confidence = float(output.get("confidence"))
    except (TypeError, ValueError) as exc:
        raise WorkerError("approved model runner confidence is invalid") from exc
    if not 0 <= confidence <= 1:
        raise WorkerError("approved model runner confidence is out of range")
    reasons = output.get("reason_codes", [])
    if not isinstance(reasons, list) or not all(isinstance(item, str) and len(item) <= 120 for item in reasons):
        raise WorkerError("approved model runner reason_codes are invalid")
    return output


def _require_under(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise WorkerError(f"{label} escapes its immutable mount") from exc


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


if __name__ == "__main__":
    raise SystemExit(main())
