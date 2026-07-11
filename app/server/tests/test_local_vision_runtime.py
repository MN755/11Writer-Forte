from __future__ import annotations

import json
import socket
from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient
from PIL import Image

from src.config import get_settings
from src.services import local_vision_runtime_service as vision


class FakeOnnxSession:
    def run(self, output_names: list[str], inputs: dict[str, object]) -> list[np.ndarray]:
        assert output_names == ["embedding"]
        assert set(inputs) == {"image"}
        return [np.arange(1, 17, dtype=np.float32).reshape(1, 16)]

    def get_providers(self) -> list[str]:
        return ["CPUExecutionProvider"]


def _approved_embedding_files() -> tuple[Path, Path, Path]:
    root = get_settings().data_dir
    model_dir = root / "models"
    approval_dir = root / "model_approvals"
    model_dir.mkdir(parents=True, exist_ok=True)
    approval_dir.mkdir(parents=True, exist_ok=True)
    model_path = model_dir / "fixture-encoder.onnx"
    model_path.write_bytes(b"local-test-model-bytes")
    image_path = model_dir / "fixture-image.png"
    Image.new("RGB", (32, 24), "navy").save(image_path)
    approval_path = approval_dir / "fixture.onnx-embedding.approval.json"
    approval_path.write_text(
        json.dumps(
            {
                "manifest": {
                    "model_id": "fixture-encoder",
                    "version": "1",
                    "kind": "image_embedding",
                    "upstream_origin": "https://example.invalid/fixture",
                    "license_id": "Apache-2.0",
                    "artifact_sha256": vision.hash_file(model_path),
                    "package_lock_sha256": "a" * 64,
                    "sbom_sha256": "b" * 64,
                    "cve_review_ref": "fixture-security-review",
                    "hardware_requirement": "CPU with optional CUDA",
                    "test_fixture_ref": "fixture-image.png",
                    "config": {"preprocess": "rgb-224-nchw"},
                },
                "model_path": str(model_path),
                "input_name": "image",
                "output_name": "embedding",
                "input_size": 32,
                "mean": [0.485, 0.456, 0.406],
                "std": [0.229, 0.224, 0.225],
            }
        ),
        encoding="utf-8",
    )
    return model_path, image_path, approval_path


def test_approved_onnx_embedding_is_offline_and_uses_bounded_cpu_fallback(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, image_path, approval_path = _approved_embedding_files()

    def outbound_network_is_forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("local vision runtime attempted outbound network access")

    monkeypatch.setattr(socket, "create_connection", outbound_network_is_forbidden)
    monkeypatch.setattr(
        vision,
        "_create_offline_session",
        lambda model_path, prefer_gpu: (FakeOnnxSession(), ["CPUExecutionProvider"], False),
    )
    record = vision.extract_onnx_image_embedding(
        artifact_id="artifact-image-1",
        image_path=image_path,
        approval_path=approval_path,
    )

    assert record.execution_device == "cpu"
    assert record.cpu_fallback is True
    assert record.network_disabled is True
    assert "gpu_unavailable_cpu_fallback" in record.reason_codes
    embedding = record.output["scene_embedding"]
    assert len(embedding) == 16
    assert sum(float(value) ** 2 for value in embedding) == pytest.approx(1.0, abs=1e-6)
    assert record.output["source_dimensions"] == [32, 24]


def test_onnx_approval_checksum_and_data_root_are_enforced(client: TestClient) -> None:
    model_path, image_path, approval_path = _approved_embedding_files()
    model_path.write_bytes(b"changed-after-approval")
    with pytest.raises(vision.LocalVisionRuntimeError, match="checksum"):
        vision.extract_onnx_image_embedding(
            artifact_id="artifact-image-2",
            image_path=image_path,
            approval_path=approval_path,
            prefer_gpu=False,
        )

    outside = get_settings().data_dir.parent / "outside.approval.json"
    outside.write_text("{}", encoding="utf-8")
    with pytest.raises(vision.LocalVisionRuntimeError, match="approval"):
        vision.load_onnx_embedding_approval(outside)


def test_onnx_approval_inventory_exposes_no_model_paths(client: TestClient) -> None:
    _, _, approval_path = _approved_embedding_files()
    records = vision.list_onnx_embedding_approvals()
    selected = next(record for record in records if record["approval_file"] == approval_path.name)
    assert selected["status"] == "approved"
    assert "model_path" not in selected
    assert "fixture-encoder.onnx" not in json.dumps(selected)

    invalid = get_settings().data_dir / "model_approvals" / "broken.onnx-embedding.approval.json"
    invalid.write_text("{not-json", encoding="utf-8")
    broken = next(record for record in vision.list_onnx_embedding_approvals() if record["approval_file"] == invalid.name)
    assert broken == {
        "approval_file": invalid.name,
        "status": "invalid",
        "reason_code": "invalid_or_unverified_approval",
    }
