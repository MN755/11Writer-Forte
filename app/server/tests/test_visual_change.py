from __future__ import annotations

from datetime import datetime, timezone
import socket

import pytest

from src.services.visual_change_service import (
    LocalInferenceAdapter,
    LocalInferenceRegistry,
    MaterialVisualChangePipeline,
    ModelManifest,
    VisualObservation,
)


def _manifest(kind: str = "object_detection") -> ModelManifest:
    return ModelManifest(
        model_id="fixture-construction-detector",
        version="2026.07.10",
        kind=kind,
        upstream_origin="https://example.invalid/local-fixture",
        license_id="Apache-2.0",
        artifact_sha256="a" * 64,
        package_lock_sha256="b" * 64,
        sbom_sha256="c" * 64,
        cve_review_ref="security-review-2026-07-10",
        hardware_requirement="CPU; optional CUDA GPU",
        test_fixture_ref="fixtures/construction-v1",
        config={"threshold": 0.42},
    )


def _observation(
    artifact_id: str,
    *,
    exact_hash: str | None = None,
    phash: str | None = None,
    site_id: str | None = "river-bridge",
    embedding: tuple[float, ...] = (1.0, 0.0),
    labels: tuple[str, ...] = ("foundation",),
    text: str = "River bridge construction update",
) -> VisualObservation:
    return VisualObservation(
        artifact_id=artifact_id,
        source_uri=f"https://camera.example/{artifact_id}.jpg",
        source_excerpt="City camera archive, captured on site.",
        source_credibility=0.95,
        site_id=site_id,
        exact_hash=exact_hash,
        perceptual_hash=phash,
        capture_time=datetime(2026, 7, 10, tzinfo=timezone.utc),
        target_location_names=("River Bridge",),
        geospatial_match=0.95,
        scene_embedding=embedding,
        object_labels=labels,
        ocr_text=text,
        derived_asset_ids=(f"{artifact_id}:crop",),
    )


def test_local_adapter_records_versioned_provenance_and_cpu_fallback_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def outbound_network_is_forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("local inference attempted outbound network access")

    monkeypatch.setattr(socket, "create_connection", outbound_network_is_forbidden)
    adapter = LocalInferenceAdapter(_manifest(), gpu_available=lambda: False)
    record = adapter.infer(
        artifact_id="artifact-1",
        input_features={"confidence": 0.81, "reason_codes": ["fixture_detected"]},
    )

    assert record.network_disabled is True
    assert record.execution_device == "cpu"
    assert record.cpu_fallback is True
    assert record.model_version == "2026.07.10"
    assert record.config_hash == _manifest().config_hash
    assert record.input_hash != record.output_hash
    assert "gpu_unavailable_cpu_fallback" in record.reason_codes


def test_registry_rejects_unregistered_or_duplicate_adapter_kind() -> None:
    registry = LocalInferenceRegistry()
    adapter = LocalInferenceAdapter(_manifest())
    registry.register(adapter)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(adapter)
    with pytest.raises(KeyError, match="No local adapter"):
        registry.infer("ocr", artifact_id="a", input_features={})


def test_reposted_photo_is_duplicate_and_never_queued_for_review() -> None:
    pipeline = MaterialVisualChangePipeline()
    original = _observation("old", exact_hash="same-bytes", phash="0f0f")
    repost = _observation("repost", exact_hash="same-bytes", phash="0f0f")

    result = pipeline.evaluate(repost, baseline=original)

    assert result.classification == "duplicate"
    assert result.review_status == "not_eligible"
    assert pipeline.review_queue == []


def test_near_duplicate_perceptual_hash_is_suppressed() -> None:
    pipeline = MaterialVisualChangePipeline()
    original = _observation("old", phash="000f")
    recompressed = _observation("recompressed", phash="000e")

    result = pipeline.evaluate(recompressed, baseline=original)

    assert result.classification == "duplicate"
    assert result.baseline_artifact_id == "old"


def test_same_site_visible_construction_advance_is_reviewable_candidate() -> None:
    pipeline = MaterialVisualChangePipeline()
    baseline = _observation("baseline", embedding=(1.0, 0.0), labels=("foundation",))
    advanced = _observation(
        "advanced",
        embedding=(0.0, 1.0),
        labels=("foundation", "crane", "steel", "building"),
        text="River bridge steel erection July 2026",
    )

    result = pipeline.evaluate(advanced, baseline=baseline)

    assert result.classification == "material_change_candidate"
    assert result.score >= 0.70
    assert result.review_status == "queued"
    assert result.review_packet is not None
    assert result.review_packet["candidate_artifact_id"] == "advanced"
    assert len(pipeline.review_queue) == 1


def test_matching_keyword_without_site_evidence_is_irrelevant() -> None:
    pipeline = MaterialVisualChangePipeline()
    unrelated = _observation(
        "unrelated",
        site_id=None,
        embedding=(0.0, 1.0),
        labels=("crane",),
        text="River Bridge construction update",
    )
    unrelated = VisualObservation(
        **{**unrelated.__dict__, "target_location_names": ("River Bridge",), "named_locations": ("Different City",), "geospatial_match": 0.0}
    )

    result = pipeline.evaluate(unrelated, baseline=_observation("baseline"))

    assert result.classification == "irrelevant"
    assert "no_named_or_geospatial_site_association" in result.reason_codes


def test_quota_exhaustion_keeps_material_candidate_queued_and_collection_continues() -> None:
    pipeline = MaterialVisualChangePipeline()
    baseline = _observation("baseline", embedding=(1.0, 0.0), labels=("foundation",))
    candidate = _observation("new", embedding=(0.0, 1.0), labels=("crane", "steel", "building"))

    exhausted = pipeline.evaluate(candidate, baseline=baseline, llm_quota_available=False)
    follow_up = pipeline.evaluate(_observation("duplicate", exact_hash="a"), historical=())

    assert exhausted.classification == "material_change_candidate"
    assert exhausted.review_status == "queued_quota_exhausted"
    assert pipeline.review_queue == [exhausted]
    assert follow_up.classification == "insufficient_context"
