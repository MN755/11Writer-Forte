"""Verified local ONNX image-embedding runtime with no model acquisition path.

This module deliberately loads only an operator-approved model file beneath Forte's
configured data directory.  It has no HTTP client, downloader, subprocess call, or
provider-discovery code.  Model acquisition and security review happen outside runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Mapping

from src.services.local_media_runtime_service import LocalMediaRuntimeError
from src.services.local_media_runtime_service import hash_file
from src.services.local_media_runtime_service import resolve_data_dir_path
from src.services.visual_change_service import InferenceRecord
from src.services.visual_change_service import ModelManifest


class LocalVisionRuntimeError(LocalMediaRuntimeError):
    """A verified local vision runtime cannot safely execute the requested model."""


@dataclass(frozen=True)
class OnnxEmbeddingApproval:
    """A model approval that is complete enough to run an ONNX image encoder offline."""

    manifest: ModelManifest
    model_path: str
    input_name: str
    output_name: str
    input_size: int
    mean: tuple[float, float, float]
    std: tuple[float, float, float]
    layout: str = "NCHW"
    max_image_pixels: int = 48_000_000

    def __post_init__(self) -> None:
        if self.manifest.kind != "image_embedding":
            raise ValueError("ONNX embedding approvals require model kind image_embedding.")
        if not self.manifest.network_disabled:
            raise ValueError("Network-enabled image models are forbidden.")
        if not self.input_name or not self.output_name:
            raise ValueError("ONNX input and output names are required.")
        if not 32 <= int(self.input_size) <= 1024:
            raise ValueError("ONNX embedding input_size must be between 32 and 1024 pixels.")
        if self.layout != "NCHW":
            raise ValueError("Only NCHW ONNX image encoders are supported.")
        if len(self.mean) != 3 or len(self.std) != 3 or any(value <= 0 for value in self.std):
            raise ValueError("ONNX image normalization requires three means and positive std values.")
        if not 1 <= int(self.max_image_pixels) <= 48_000_000:
            raise ValueError("max_image_pixels must be between 1 and 48,000,000.")

    def safe_summary(self) -> dict[str, Any]:
        return {
            "model_id": self.manifest.model_id,
            "version": self.manifest.version,
            "kind": self.manifest.kind,
            "license_id": self.manifest.license_id,
            "artifact_sha256": self.manifest.artifact_sha256,
            "hardware_requirement": self.manifest.hardware_requirement,
            "test_fixture_ref": self.manifest.test_fixture_ref,
            "network_disabled": self.manifest.network_disabled,
            "input_size": self.input_size,
            "layout": self.layout,
        }


def load_onnx_embedding_approval(path: str | Path) -> OnnxEmbeddingApproval:
    """Load a data-dir approval file and verify the model bytes before execution."""
    try:
        approval_path = resolve_data_dir_path(path, label="ONNX model approval record")
        raw = json.loads(approval_path.read_text(encoding="utf-8"))
        manifest = ModelManifest(**raw["manifest"])
        approval = OnnxEmbeddingApproval(
            manifest=manifest,
            model_path=str(raw["model_path"]),
            input_name=str(raw["input_name"]),
            output_name=str(raw["output_name"]),
            input_size=int(raw["input_size"]),
            mean=tuple(float(value) for value in raw["mean"]),  # type: ignore[arg-type]
            std=tuple(float(value) for value in raw["std"]),  # type: ignore[arg-type]
            layout=str(raw.get("layout", "NCHW")),
            max_image_pixels=int(raw.get("max_image_pixels", 48_000_000)),
        )
        model_path = resolve_data_dir_path(approval.model_path, label="Approved ONNX model")
    except (KeyError, LocalMediaRuntimeError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LocalVisionRuntimeError(f"Invalid ONNX image-embedding approval: {exc}") from exc

    if not model_path.is_file():
        raise LocalVisionRuntimeError("Approved ONNX model file does not exist.")
    if hash_file(model_path) != approval.manifest.artifact_sha256:
        raise LocalVisionRuntimeError("Approved ONNX model checksum differs from its approval record.")
    return approval


def list_onnx_embedding_approvals() -> list[dict[str, Any]]:
    """List valid/invalid approval records without disclosing model filesystem paths."""
    from src.config import get_settings

    root = get_settings().data_dir / "model_approvals"
    if not root.is_dir():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.onnx-embedding.approval.json")):
        try:
            record = load_onnx_embedding_approval(path).safe_summary()
            record["approval_file"] = path.name
            record["status"] = "approved"
        except LocalVisionRuntimeError:
            # Approval parsing/checksum errors can include local runtime paths. The API
            # inventory exposes a stable reason code; detailed diagnosis stays local.
            record = {
                "approval_file": path.name,
                "status": "invalid",
                "reason_code": "invalid_or_unverified_approval",
            }
        records.append(record)
    return records


def extract_onnx_image_embedding(
    *,
    artifact_id: str,
    image_path: str | Path,
    approval_path: str | Path,
    prefer_gpu: bool = True,
) -> InferenceRecord:
    """Extract one normalized embedding using only approved local model/image files."""
    if not artifact_id:
        raise LocalVisionRuntimeError("artifact_id is required for image-embedding provenance.")
    approval = load_onnx_embedding_approval(approval_path)
    try:
        image = resolve_data_dir_path(image_path, label="Image embedding input")
    except LocalMediaRuntimeError as exc:
        raise LocalVisionRuntimeError(str(exc)) from exc
    if not image.is_file():
        raise LocalVisionRuntimeError("Image embedding input does not exist.")
    try:
        model_path = resolve_data_dir_path(approval.model_path, label="Approved ONNX model")
    except LocalMediaRuntimeError as exc:
        raise LocalVisionRuntimeError(str(exc)) from exc
    if hash_file(model_path) != approval.manifest.artifact_sha256:
        raise LocalVisionRuntimeError("Approved ONNX model changed after approval validation.")
    input_hash = hash_file(image)
    tensor, dimensions = _prepare_image_tensor(image, approval)
    if hash_file(image) != input_hash:
        raise LocalVisionRuntimeError("Image embedding input changed while it was being prepared.")
    session, providers, gpu_selected = _create_offline_session(model_path, prefer_gpu=prefer_gpu)
    started = time.perf_counter()
    try:
        output = session.run([approval.output_name], {approval.input_name: tensor})[0]
    except Exception as exc:
        raise LocalVisionRuntimeError(f"Approved ONNX model inference failed: {exc}") from exc
    elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
    embedding = _normalize_embedding(output)
    reason_codes = ["approved_onnx_embedding", "network_disabled_runtime"]
    if gpu_selected:
        reason_codes.append("gpu_execution_provider")
    elif prefer_gpu:
        reason_codes.append("gpu_unavailable_cpu_fallback")
    payload = {
        "scene_embedding": embedding,
        "embedding_dimensions": len(embedding),
        "source_dimensions": list(dimensions),
        "input_size": approval.input_size,
        "execution_ms": elapsed_ms,
        "onnx_providers": providers,
    }
    return InferenceRecord(
        artifact_id=artifact_id,
        model_id=approval.manifest.model_id,
        model_version=approval.manifest.version,
        model_kind=approval.manifest.kind,
        execution_device="gpu" if gpu_selected else "cpu",
        cpu_fallback=bool(prefer_gpu and not gpu_selected),
        config_hash=approval.manifest.config_hash,
        input_hash=input_hash,
        output_hash=_canonical_output_hash(payload),
        confidence=1.0,
        reason_codes=tuple(sorted(reason_codes)),
        output=payload,
        network_disabled=True,
    )


def _create_offline_session(model_path: Path, *, prefer_gpu: bool) -> tuple[Any, list[str], bool]:
    # ONNX Runtime has no model hub/downloader. These flags also make any accidental use
    # of common model-hub libraries fail rather than quietly reaching the network.
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    try:
        import onnxruntime as ort
    except ImportError as exc:
        raise LocalVisionRuntimeError(
            "ONNX Runtime is not installed; install the approved media-vision extra first."
        ) from exc
    available = set(ort.get_available_providers())
    gpu_providers = [
        name
        for name in ("TensorrtExecutionProvider", "CUDAExecutionProvider")
        if name in available
    ]
    requested = [*gpu_providers, "CPUExecutionProvider"] if prefer_gpu and gpu_providers else ["CPUExecutionProvider"]
    try:
        session = ort.InferenceSession(str(model_path), providers=requested)
    except Exception as exc:
        raise LocalVisionRuntimeError(f"Approved ONNX model could not be loaded: {exc}") from exc
    actual = list(session.get_providers())
    gpu_selected = any(name in {"TensorrtExecutionProvider", "CUDAExecutionProvider"} for name in actual)
    return session, actual, gpu_selected


def _prepare_image_tensor(image_path: Path, approval: OnnxEmbeddingApproval) -> tuple[Any, tuple[int, int]]:
    try:
        import numpy as np
        from PIL import Image, ImageOps, UnidentifiedImageError
    except ImportError as exc:
        raise LocalVisionRuntimeError("Pillow and NumPy are required for image embeddings.") from exc
    try:
        with Image.open(image_path) as source:
            source.load()
            normalized = ImageOps.exif_transpose(source).convert("RGB")
            width, height = normalized.size
            if width < 1 or height < 1 or width * height > approval.max_image_pixels:
                raise LocalVisionRuntimeError("Image dimensions exceed the approved embedding decode limit.")
            resized = normalized.resize((approval.input_size, approval.input_size), Image.Resampling.BICUBIC)
            values = np.asarray(resized, dtype=np.float32) / 255.0
    except (OSError, UnidentifiedImageError) as exc:
        raise LocalVisionRuntimeError(f"Image embedding input cannot be decoded: {exc}") from exc
    mean = np.asarray(approval.mean, dtype=np.float32).reshape((1, 1, 3))
    std = np.asarray(approval.std, dtype=np.float32).reshape((1, 1, 3))
    tensor = np.transpose((values - mean) / std, (2, 0, 1))[None, :, :, :]
    return tensor, (width, height)


def _normalize_embedding(value: Any) -> list[float]:
    try:
        import numpy as np
    except ImportError as exc:
        raise LocalVisionRuntimeError("NumPy is required for image embeddings.") from exc
    vector = np.asarray(value, dtype=np.float32).reshape(-1)
    if not 16 <= vector.size <= 4096 or not bool(np.isfinite(vector).all()):
        raise LocalVisionRuntimeError("Approved ONNX model returned an invalid image embedding vector.")
    magnitude = float(np.linalg.norm(vector))
    if not math.isfinite(magnitude) or magnitude <= 0:
        raise LocalVisionRuntimeError("Approved ONNX model returned a zero-norm image embedding.")
    return [round(float(item / magnitude), 8) for item in vector]


def _canonical_output_hash(value: Mapping[str, Any]) -> str:
    import hashlib

    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
