#!/usr/bin/env python
"""Offline MobileNetV2 ONNX image inference command runner.

This command accepts exactly one JSON object on stdin from Forte's approved-command
runtime.  It resolves the artifact through the configured local artifact ledger rather
than accepting a caller-controlled path.  It never downloads a model, labels, or
dependencies; provision those separately and run the command in a no-egress sandbox.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any


_DLL_DIRECTORY_HANDLES: list[Any] = []


class RunnerError(RuntimeError):
    """A local command-contract or artifact-custody violation."""


def main() -> int:
    try:
        request = _read_request()
        artifact = _resolve_artifact(request)
        model_path = _approved_model_path()
        features, confidence = _infer(artifact, model_path)
        print(
            json.dumps(
                {
                    "confidence": confidence,
                    "features": features,
                    "reason_codes": ["onnxruntime_local_session", "artifact_ledger_resolved"],
                },
                separators=(",", ":"),
            )
        )
        return 0
    except RunnerError as exc:
        print(str(exc), file=sys.stderr)
        return 2


def _read_request() -> dict[str, Any]:
    if os.environ.get("ELEVENWRITER_LOCAL_INTELLIGENCE_OFFLINE") != "1":
        raise RunnerError("Offline runtime assertion is absent.")
    try:
        request = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        raise RunnerError("Expected a JSON command request.") from exc
    allowed = {"schema_version", "artifact_id", "input_sha256", "byte_size", "task", "model_id", "model_version"}
    if not isinstance(request, dict) or set(request) - allowed:
        raise RunnerError("Command request violates the artifact-ID-only contract.")
    if request.get("schema_version") != 1 or request.get("task") != "image_embedding":
        raise RunnerError("Unsupported command schema or task.")
    artifact_id, digest = request.get("artifact_id"), request.get("input_sha256")
    if not isinstance(artifact_id, str) or not isinstance(digest, str) or len(digest) != 64:
        raise RunnerError("Command request is missing an artifact ID or SHA-256.")
    return request


def _resolve_artifact(request: dict[str, Any]) -> Path:
    root = Path(os.environ.get("ELEVENWRITER_ARTIFACT_STORE_ROOT", "")).resolve()
    if not root.is_dir():
        raise RunnerError("Approved artifact store root is unavailable.")
    try:
        ledger = json.loads((root / "ledger.json").read_text(encoding="utf-8"))
        artifact = ledger["artifacts"][request["artifact_id"]]
        relative_blob = Path(artifact["blob_path"])
        blob = (root / relative_blob).resolve()
        blob.relative_to(root)
    except (KeyError, OSError, ValueError, json.JSONDecodeError) as exc:
        raise RunnerError("Artifact ID does not resolve to a valid local custody record.") from exc
    if artifact.get("sha256") != request["input_sha256"] or not blob.is_file():
        raise RunnerError("Artifact custody hash or local bytes do not match the command request.")
    if _sha256(blob) != request["input_sha256"]:
        raise RunnerError("Artifact bytes fail the SHA-256 custody check.")
    return blob


def _approved_model_path() -> Path:
    path = Path(os.environ.get("ELEVENWRITER_APPROVED_MODEL_PATH", "")).resolve()
    if not path.is_file() or path.suffix.lower() != ".onnx":
        raise RunnerError("Approved ONNX model path is unavailable.")
    return path


def _infer(image_path: Path, model_path: Path) -> tuple[dict[str, Any], float]:
    try:
        import numpy as np
        import onnxruntime as ort
        from PIL import Image
    except ImportError as exc:
        raise RunnerError("onnxruntime, numpy, and Pillow must be operator-provisioned locally.") from exc
    try:
        # ONNX Runtime discovers the operator-provisioned CUDA/cuDNN DLL wheels
        # before session construction.  Windows also needs their bin folders in
        # the current process DLL search path; this is setup state, never a network call.
        _register_windows_cuda_dlls()
        if "CUDAExecutionProvider" in ort.get_available_providers():
            ort.preload_dlls(directory="")
        with Image.open(image_path) as image:
            rgb = image.convert("RGB").resize((224, 224))
            pixels = np.asarray(rgb, dtype=np.float32) / 255.0
    except (OSError, ValueError) as exc:
        raise RunnerError("Artifact is not a decodable image.") from exc
    # Standard ImageNet MobileNetV2 normalization, NCHW batch input.
    pixels = (pixels - np.array([0.485, 0.456, 0.406], dtype=np.float32)) / np.array(
        [0.229, 0.224, 0.225], dtype=np.float32
    )
    tensor = np.transpose(pixels, (2, 0, 1))[None, ...]
    providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]
    try:
        session = ort.InferenceSession(str(model_path), providers=providers)
        output = session.run(None, {session.get_inputs()[0].name: tensor})[0]
    except Exception as exc:
        raise RunnerError(f"ONNX Runtime inference failed: {exc}") from exc
    logits = np.asarray(output, dtype=np.float64).reshape(-1)
    probabilities = _softmax(logits)
    top_indexes = np.argsort(probabilities)[-5:][::-1]
    top_k = [
        {"class_index": int(index), "probability": round(float(probabilities[index]), 6)}
        for index in top_indexes
    ]
    return {
        "model_format": "onnx",
        "top_k": top_k,
        "output_dimension": int(logits.size),
        "execution_provider": session.get_providers()[0],
    }, top_k[0]["probability"]


def _softmax(values: Any) -> Any:
    import numpy as np

    shifted = values - np.max(values)
    exponentials = np.exp(shifted)
    return exponentials / np.sum(exponentials)


def _register_windows_cuda_dlls() -> None:
    if os.name != "nt" or not hasattr(os, "add_dll_directory"):
        return
    site_packages = Path(sys.executable).resolve().parent.parent / "Lib" / "site-packages" / "nvidia"
    if not site_packages.is_dir():
        return
    for directory in sorted(site_packages.glob("*/bin")):
        _DLL_DIRECTORY_HANDLES.append(os.add_dll_directory(str(directory)))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
