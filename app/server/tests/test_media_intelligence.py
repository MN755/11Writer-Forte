from __future__ import annotations

import base64
import struct
import zlib
from pathlib import Path

from fastapi.testclient import TestClient
import pytest


def _png() -> bytes:
    pixels = bytes([0, 32, 128, 255])
    raw = b"\x00" + pixels[:2] + b"\x00" + pixels[2:]

    def chunk(kind: bytes, value: bytes) -> bytes:
        return struct.pack(">I", len(value)) + kind + value + struct.pack(">I", zlib.crc32(kind + value) & 0xFFFFFFFF)

    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 2, 2, 8, 0, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")


def test_intake_publishes_local_artifact_and_storage_evidence(client: TestClient) -> None:
    health = client.get("/api/media-intelligence/health")
    assert health.status_code == 200
    assert health.json()["recommended_action"] in {
        "normal_collection",
        "lower_sampling_retain_features_over_bytes",
        "pause_noncritical_sources",
    }

    response = client.post(
        "/api/media-intelligence/intake",
        json={
            "payload_base64": base64.b64encode(_png()).decode("ascii"),
            "owner_type": "construction_watch",
            "owner_id": "river-bridge",
            "claimed_mime": "image/png",
            "source_uri": "https://public.example/river-bridge.jpg",
            "source_page_uri": "https://public.example/updates",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "accepted"
    assert payload["artifact"]["role"] == "raw"
    assert payload["artifact"]["provenance"]["source_page_uri"] == "https://public.example/updates"
    assert payload["storage_object_id"]

    duplicate = client.post(
        "/api/media-intelligence/intake",
        json={
            "payload_base64": base64.b64encode(_png()).decode("ascii"),
            "owner_type": "construction_watch",
            "owner_id": "river-bridge-repeat",
        },
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate_of"] == payload["sha256"]
    assert duplicate.json()["artifact"]["blob_path"] == payload["artifact"]["blob_path"]

    derivatives = client.post(
        "/api/media-intelligence/derivatives",
        json={"artifact_uid": payload["artifact"]["artifact_uid"], "derivative_kind": "image"},
    )
    assert derivatives.status_code == 200
    derivative_payload = derivatives.json()["derivatives"]
    assert set(derivative_payload) == {"thumbnail", "proxy"}
    assert all(item["parent_artifact_uid"] == payload["artifact"]["artifact_uid"] for item in derivative_payload.values())


def test_visual_change_endpoint_rejects_keyword_only_and_queues_material_change(client: TestClient) -> None:
    base = {
        "source_uri": "https://public.example/camera.jpg",
        "source_excerpt": "Municipal construction camera archive",
        "source_credibility": 0.95,
        "site_id": "river-bridge",
        "capture_time": "2026-07-10T12:00:00Z",
        "target_location_names": ["River Bridge"],
        "geospatial_match": 0.95,
        "object_labels": ["foundation"],
    }
    irrelevant = client.post(
        "/api/media-intelligence/visual-change",
        json={
            "observation": {
                **base,
                "artifact_id": "unrelated",
                "site_id": None,
                "target_location_names": ["River Bridge"],
                "geospatial_match": 0.0,
                "scene_embedding": [0.0, 1.0],
                "keyword_hits": ["construction"],
            }
        },
    )
    assert irrelevant.status_code == 200
    assert irrelevant.json()["classification"] == "irrelevant"

    response = client.post(
        "/api/media-intelligence/visual-change",
        json={
            "baseline": {**base, "artifact_id": "before", "scene_embedding": [1.0, 0.0]},
            "observation": {
                **base,
                "artifact_id": "after",
                "scene_embedding": [0.0, 1.0],
                "object_labels": ["foundation", "crane", "steel"],
                "derived_asset_ids": ["after:crop"],
            },
            "llm_quota_available": False,
        },
    )
    assert response.status_code == 200
    candidate = response.json()
    assert candidate["classification"] == "material_change_candidate"
    assert candidate["review_status"] == "queued_quota_exhausted"
    assert candidate["review_packet"]["candidate_artifact_id"] == "after"


def test_tesseract_endpoint_uses_approved_local_engine(client: TestClient) -> None:
    tesseract = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    tessdata = tesseract.parent / "tessdata"
    if not tesseract.is_file():
        pytest.skip("Tesseract is an explicitly provisioned local runtime.")
    from PIL import Image, ImageDraw, ImageFont

    from src.config import get_settings
    from src.services.local_media_runtime_service import build_tesseract_approval, write_tesseract_approval

    model_dir = get_settings().data_dir / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    lock, sbom = model_dir / "tesseract-lock.json", model_dir / "tesseract-sbom.json"
    lock.write_text("{}", encoding="utf-8")
    sbom.write_text("[]", encoding="utf-8")
    approval_path = write_tesseract_approval(
        build_tesseract_approval(
            tesseract,
            tessdata,
            package_lock_path=lock,
            sbom_path=sbom,
            cve_review_ref="test-fixture-reviewed",
        ),
        model_dir / "tesseract.approval.json",
    )
    image_path = model_dir / "ocr-fixture.png"
    image = Image.new("RGB", (720, 160), "white")
    ImageDraw.Draw(image).text(
        (20, 50),
        "RIVER BRIDGE UPDATE",
        fill="black",
        font=ImageFont.truetype(r"C:\Windows\Fonts\arial.ttf", 40),
    )
    image.save(image_path)

    response = client.post(
        "/api/media-intelligence/ocr",
        json={"image_path": str(image_path), "approval_path": str(approval_path)},
    )
    assert response.status_code == 200
    payload = response.json()
    assert "RIVER BRIDGE UPDATE" in payload["text"]
    assert payload["engine_version"].startswith("tesseract v5.4.0")


def test_local_media_endpoints_reject_paths_outside_data_dir(
    client: TestClient,
    tmp_path: Path,
) -> None:
    response = client.post(
        "/api/media-intelligence/transcribe",
        json={
            "audio_path": str(tmp_path.parent / "outside-data-dir.wav"),
            "approval_path": str(tmp_path / "models" / "approval.json"),
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "Audio input must be inside the configured data_dir."


def test_generic_inference_endpoint_is_retired(client: TestClient) -> None:
    response = client.post(
        "/api/media-intelligence/inference",
        json={"artifact_id": "fixture", "model_manifest": {}, "input_features": {}},
    )
    assert response.status_code == 410
    assert "approved local model endpoint" in response.json()["detail"]


def test_embedding_endpoint_does_not_expose_local_runtime_errors(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.routes import media_intelligence
    from src.services.local_vision_runtime_service import LocalVisionRuntimeError

    monkeypatch.setattr(
        media_intelligence,
        "extract_onnx_image_embedding",
        lambda **kwargs: (_ for _ in ()).throw(
            LocalVisionRuntimeError("C:/private/model-path/provider implementation stack")
        ),
    )
    response = client.post(
        "/api/media-intelligence/embeddings",
        json={
            "artifact_id": "fixture",
            "image_path": "C:/ignored/image.png",
            "approval_path": "C:/ignored/approval.json",
        },
    )
    assert response.status_code == 422
    assert "private" not in response.text
    assert response.json()["detail"] == "Local image embedding request was rejected by the approved runtime."
