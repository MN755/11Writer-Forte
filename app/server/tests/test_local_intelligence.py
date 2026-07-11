from __future__ import annotations

import hashlib
from pathlib import Path
import socket
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.routes.local_intelligence import router
from src.services.artifact_store_service import ArtifactOwner, ArtifactStore
from src.services.local_intelligence_service import (
    LocalIntelligenceError,
    LocalIntelligenceRuntime,
    ModelRegistry,
    no_network_runtime_guard,
)


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _approved_payload(
    data_dir: Path, *, model_id: str = "local-image-features", runtime: str = "approved_command_v1"
) -> dict[str, object]:
    model = data_dir / "models" / "image-features.bin"
    model.parent.mkdir(parents=True, exist_ok=True)
    model.write_bytes(b"approved-local-model-v1")
    payload: dict[str, object] = {
        "model_id": model_id,
        "model_version": "1.0.0",
        "task": "image_embedding",
        "runtime": runtime,
        "model_path": str(model),
        "model_sha256": _sha(model.read_bytes()),
        "license_id": "Apache-2.0",
        "upstream_origin": "operator-managed-local-package",
        "sbom_sha256": _sha(b"sbom"),
        "package_lock_sha256": _sha(b"lock"),
        "cve_review_ref": "SEC-42",
        "network_disabled": True,
        "gpu_vram_mb": 4096,
        "cpu_fallback": True,
        "timeout_seconds": 10,
        "queue_depth_limit": 2,
        "approved_by": "operator",
        "benchmark": {
            "benchmark_version": "corpus-2026.07",
            "corpus_manifest_sha256": _sha(b"licensed benchmark corpus"),
            "metrics": {"precision": 0.9, "recall": 0.85, "false_alert_rate": 0.04, "latency_ms": 12.5, "top_k_recall": 0.9},
            "environment": {"gpu": "RTX 4070 Laptop", "driver": "provisioned"},
        },
    }
    if runtime == "approved_command_v1":
        asset = data_dir / "models" / "approved_runner.py"
        asset.write_text(
            "import json, sys\n"
            "request = json.load(sys.stdin)\n"
            "print(json.dumps({'confidence': 0.88, 'features': {'seen_artifact_id': request['artifact_id']}, "
            "'reason_codes': ['approved_command']}))\n",
            encoding="utf-8",
        )
        payload["runner_executable_path"] = sys.executable
        payload["runner_executable_sha256"] = _sha(Path(sys.executable).read_bytes())
        payload["runner_asset_path"] = str(asset)
        payload["runner_asset_sha256"] = _sha(asset.read_bytes())
        payload["runner_arguments"] = [str(asset)]
    return payload


def _artifact(data_dir: Path) -> str:
    store = ArtifactStore(data_dir / "artifact_store")
    return store.ingest_bytes(b"local input bytes", role="raw", owner=ArtifactOwner("test", "input")).artifact_uid


def test_registry_rejects_fixture_and_checksum_mismatch(tmp_path: Path) -> None:
    payload = _approved_payload(tmp_path, model_id="fixture-detector")
    with pytest.raises(LocalIntelligenceError, match="Fixture models"):
        ModelRegistry(tmp_path).approve(payload)

    payload = _approved_payload(tmp_path)
    payload["model_sha256"] = "0" * 64
    with pytest.raises(LocalIntelligenceError, match="checksum"):
        ModelRegistry(tmp_path).approve(payload)

    command_checksum = _approved_payload(tmp_path)
    command_checksum["runner_executable_sha256"] = "0" * 64
    with pytest.raises(LocalIntelligenceError, match="executable checksum"):
        ModelRegistry(tmp_path).approve(command_checksum)

    development = _approved_payload(tmp_path, runtime="artifact_hash_features_v1")
    with pytest.raises(LocalIntelligenceError, match="development-only"):
        ModelRegistry(tmp_path).approve(development)
    assert ModelRegistry(tmp_path, allow_development_runner=True).approve(development).runtime == "artifact_hash_features_v1"


def test_artifact_only_command_runtime_selects_gpu_or_bounded_cpu_fallback(tmp_path: Path) -> None:
    registry = ModelRegistry(tmp_path)
    manifest = registry.approve(_approved_payload(tmp_path))
    artifact_id = _artifact(tmp_path)

    gpu = LocalIntelligenceRuntime(tmp_path, gpu_available=True).run(artifact_id=artifact_id, model_id=manifest.model_id)
    cpu = LocalIntelligenceRuntime(tmp_path, gpu_available=False).run(artifact_id=artifact_id, model_id=manifest.model_id)
    assert gpu.device == "cuda:0" and not gpu.cpu_fallback
    assert cpu.device == "cpu" and cpu.cpu_fallback
    assert gpu.features["seen_artifact_id"] == artifact_id
    assert "approved_command" in gpu.reason_codes


def test_no_network_guard_fails_closed() -> None:
    with no_network_runtime_guard():
        with pytest.raises(LocalIntelligenceError, match="Outbound network"):
            socket.create_connection(("example.com", 443))
    # The process-level patch is restored after the bounded inference scope.
    assert socket.getaddrinfo is not None


def test_routes_approve_and_infer_artifact_ids_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(tmp_path))
    from src.config import reset_settings_cache

    reset_settings_cache()
    app = FastAPI()
    app.include_router(router, prefix="/api")
    artifact_id = _artifact(tmp_path)
    with TestClient(app) as client:
        assert client.get("/api/local-intelligence/health").json()["fixture_inference"] == "rejected"
        approved = client.post("/api/local-intelligence/models/approve", json=_approved_payload(tmp_path))
        assert approved.status_code == 201
        rejected = client.post("/api/local-intelligence/infer", json={"artifact_id": artifact_id, "model_id": "local-image-features", "input_path": "C:/nope"})
        assert rejected.status_code == 422
        response = client.post("/api/local-intelligence/infer", json={"artifact_id": artifact_id, "model_id": "local-image-features", "prefer_gpu": False})
        assert response.status_code == 200
        assert response.json()["device"] == "cpu"
    reset_settings_cache()
