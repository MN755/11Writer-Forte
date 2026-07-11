"""Offline, deterministic contracts for local inference and visual change triage.

This module deliberately contains no model downloader, HTTP client, subprocess call, or
GPU-specific import.  A deployment may register a locally installed model implementation,
but every adapter is audited as a versioned, network-disabled pure transform.  This makes
the fallback path usable in the base server image and keeps source evidence separate from
model-produced labels.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence


MODEL_KINDS = frozenset(
    {
        "object_detection",
        "image_embedding",
        "ocr",
        "speech_to_text",
        "acoustic_features",
        "structured_anomaly",
    }
)
CHANGE_CLASSIFICATIONS = frozenset(
    {
        "duplicate",
        "irrelevant",
        "insufficient_context",
        "possible_change",
        "material_change_candidate",
        "confirmed_change",
    }
)


def _canonical_hash(value: Any) -> str:
    """Return a stable SHA-256 hash for a JSON-compatible contract value."""

    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class ModelManifest:
    """Security and reproducibility proof required before a local model is enabled."""

    model_id: str
    version: str
    kind: str
    upstream_origin: str
    license_id: str
    artifact_sha256: str
    package_lock_sha256: str
    sbom_sha256: str
    cve_review_ref: str
    hardware_requirement: str
    test_fixture_ref: str
    config: Mapping[str, Any] = field(default_factory=dict)
    network_disabled: bool = True

    def __post_init__(self) -> None:
        if self.kind not in MODEL_KINDS:
            raise ValueError(f"Unsupported local inference kind: {self.kind}")
        required = (
            self.model_id,
            self.version,
            self.upstream_origin,
            self.license_id,
            self.artifact_sha256,
            self.package_lock_sha256,
            self.sbom_sha256,
            self.cve_review_ref,
            self.hardware_requirement,
            self.test_fixture_ref,
        )
        if not all(str(item).strip() for item in required):
            raise ValueError("Model manifest is missing a mandatory security-gate field.")
        if not self.network_disabled:
            raise ValueError("Runtime inference must be network-disabled.")

    @property
    def config_hash(self) -> str:
        return _canonical_hash(dict(self.config))

    @property
    def security_gate_passed(self) -> bool:
        return self.network_disabled and all(
            (
                self.artifact_sha256,
                self.package_lock_sha256,
                self.sbom_sha256,
                self.cve_review_ref,
                self.test_fixture_ref,
            )
        )


@dataclass(frozen=True)
class InferenceRecord:
    """Immutable local inference evidence; labels never replace source provenance."""

    artifact_id: str
    model_id: str
    model_version: str
    model_kind: str
    execution_device: str
    cpu_fallback: bool
    config_hash: str
    input_hash: str
    output_hash: str
    confidence: float
    reason_codes: tuple[str, ...]
    output: Mapping[str, Any]
    network_disabled: bool = True

    def __post_init__(self) -> None:
        if not self.artifact_id:
            raise ValueError("artifact_id is required for inference provenance")
        if not self.network_disabled:
            raise ValueError("An inference record cannot claim network-enabled runtime.")
        if self.execution_device not in {"cpu", "gpu"}:
            raise ValueError("execution_device must be cpu or gpu")


class LocalProcessor(Protocol):
    def __call__(self, payload: Mapping[str, Any], device: str) -> Mapping[str, Any]: ...


def _default_processor(payload: Mapping[str, Any], device: str) -> Mapping[str, Any]:
    """Deterministic fixture processor, useful until a security-gated model is installed."""

    # The caller controls supplied local features.  This deliberately does not pretend
    # it performed detection, OCR, or embedding inference.
    return {"features": dict(payload), "processor": "deterministic_contract", "device": device}


class LocalInferenceAdapter:
    """A versioned, local-only adapter with bounded GPU-to-CPU degradation."""

    def __init__(
        self,
        manifest: ModelManifest,
        processor: LocalProcessor | None = None,
        *,
        gpu_available: Callable[[], bool] | None = None,
    ) -> None:
        if not manifest.security_gate_passed:
            raise ValueError("Model did not pass the local security gate.")
        self.manifest = manifest
        self._processor = processor or _default_processor
        self._gpu_available = gpu_available or (lambda: False)

    def infer(
        self,
        *,
        artifact_id: str,
        input_features: Mapping[str, Any],
        prefer_gpu: bool = True,
    ) -> InferenceRecord:
        if not artifact_id:
            raise ValueError("artifact_id is required")
        gpu_selected = bool(prefer_gpu and self._gpu_available())
        device = "gpu" if gpu_selected else "cpu"
        # Processors receive data only; network primitives are intentionally absent
        # from this adapter's runtime contract.
        output = dict(self._processor(dict(input_features), device))
        confidence = _clamp(float(output.get("confidence", input_features.get("confidence", 0.0))))
        codes = output.get("reason_codes", input_features.get("reason_codes", ()))
        if isinstance(codes, str):
            codes = (codes,)
        reason_codes = tuple(sorted({str(code) for code in codes if str(code)}))
        if not gpu_selected and prefer_gpu:
            reason_codes = tuple(sorted(set(reason_codes) | {"gpu_unavailable_cpu_fallback"}))
        return InferenceRecord(
            artifact_id=artifact_id,
            model_id=self.manifest.model_id,
            model_version=self.manifest.version,
            model_kind=self.manifest.kind,
            execution_device=device,
            cpu_fallback=bool(prefer_gpu and not gpu_selected),
            config_hash=self.manifest.config_hash,
            input_hash=_canonical_hash(dict(input_features)),
            output_hash=_canonical_hash(output),
            confidence=confidence,
            reason_codes=reason_codes,
            output=output,
            network_disabled=True,
        )


class LocalInferenceRegistry:
    """One active adapter per inference kind, with explicit replacement policy."""

    def __init__(self) -> None:
        self._adapters: dict[str, LocalInferenceAdapter] = {}

    def register(self, adapter: LocalInferenceAdapter, *, replace: bool = False) -> None:
        kind = adapter.manifest.kind
        if kind in self._adapters and not replace:
            raise ValueError(f"An adapter is already registered for {kind}.")
        self._adapters[kind] = adapter

    def infer(self, kind: str, **kwargs: Any) -> InferenceRecord:
        if kind not in self._adapters:
            raise KeyError(f"No local adapter registered for {kind}.")
        return self._adapters[kind].infer(**kwargs)

    def manifests(self) -> list[ModelManifest]:
        return [self._adapters[kind].manifest for kind in sorted(self._adapters)]


@dataclass(frozen=True)
class VisualObservation:
    artifact_id: str
    source_uri: str
    source_excerpt: str
    source_credibility: float
    site_id: str | None = None
    exact_hash: str | None = None
    perceptual_hash: str | None = None
    capture_time: datetime | None = None
    named_locations: tuple[str, ...] = ()
    target_location_names: tuple[str, ...] = ()
    geospatial_match: float | None = None
    scene_embedding: tuple[float, ...] = ()
    object_labels: tuple[str, ...] = ()
    ocr_text: str = ""
    keyword_hits: tuple[str, ...] = ()
    derived_asset_ids: tuple[str, ...] = ()
    inference_records: tuple[InferenceRecord, ...] = ()

    def __post_init__(self) -> None:
        if not self.artifact_id or not self.source_uri:
            raise ValueError("artifact_id and source_uri are required evidence fields")
        object.__setattr__(self, "source_credibility", _clamp(self.source_credibility))
        object.__setattr__(self, "capture_time", _utc(self.capture_time))


@dataclass(frozen=True)
class VisualChangeCandidate:
    classification: str
    score: float
    reason_codes: tuple[str, ...]
    observation_artifact_id: str
    baseline_artifact_id: str | None
    review_status: str
    review_packet: Mapping[str, Any] | None
    score_components: Mapping[str, float]

    def __post_init__(self) -> None:
        if self.classification not in CHANGE_CLASSIFICATIONS:
            raise ValueError(f"Unknown visual-change classification: {self.classification}")


def _hash_distance(first: str | None, second: str | None) -> int | None:
    if not first or not second:
        return None
    try:
        return (int(first, 16) ^ int(second, 16)).bit_count()
    except ValueError:
        return None


def _cosine_similarity(first: Sequence[float], second: Sequence[float]) -> float | None:
    if not first or not second or len(first) != len(second):
        return None
    dot = sum(float(a) * float(b) for a, b in zip(first, second))
    magnitude = math.sqrt(sum(float(a) ** 2 for a in first) * sum(float(b) ** 2 for b in second))
    return _clamp((dot / magnitude + 1.0) / 2.0) if magnitude else None


def _tokens(*values: str) -> set[str]:
    return {
        word
        for value in values
        for word in value.casefold().replace("-", " ").split()
        if len(word) >= 3
    }


def _site_association(observation: VisualObservation) -> float:
    if observation.site_id:
        return 1.0
    named = _tokens(*observation.named_locations)
    targets = _tokens(*observation.target_location_names)
    if named and targets:
        return len(named & targets) / len(targets)
    return 0.0


def _semantic_progress(observation: VisualObservation, baseline: VisualObservation) -> float:
    labels = {label.casefold() for label in observation.object_labels}
    base_labels = {label.casefold() for label in baseline.object_labels}
    construction = {"crane", "excavator", "construction", "building", "foundation", "steel", "scaffolding"}
    gained = len((labels - base_labels) & construction) / max(1, len(construction & labels))
    similarity = _cosine_similarity(observation.scene_embedding, baseline.scene_embedding)
    visual_delta = 0.0 if similarity is None else 1.0 - similarity
    return _clamp(max(gained, visual_delta))


class MaterialVisualChangePipeline:
    """Rule-first triage; it queues exceptional review but never performs an LLM call."""

    def __init__(self, *, material_threshold: float = 0.70, possible_threshold: float = 0.45) -> None:
        self.material_threshold = material_threshold
        self.possible_threshold = possible_threshold
        self.review_queue: list[VisualChangeCandidate] = []

    def evaluate(
        self,
        observation: VisualObservation,
        *,
        baseline: VisualObservation | None = None,
        historical: Iterable[VisualObservation] = (),
        llm_quota_available: bool = True,
        confirmed: bool = False,
    ) -> VisualChangeCandidate:
        history = tuple(historical)
        duplicate_of = self._find_duplicate(observation, (baseline, *history))
        if duplicate_of is not None:
            return self._candidate(
                "duplicate", observation, duplicate_of, 0.0, ("exact_or_near_duplicate",), {}, None
            )

        association = _site_association(observation)
        geo = _clamp(observation.geospatial_match or 0.0)
        # A keyword is only a retrieval hint; without location or geospatial evidence,
        # it can never turn an unrelated image into an alert.
        if association < 0.35 and geo < 0.35:
            return self._candidate(
                "irrelevant", observation, baseline, 0.0, ("no_named_or_geospatial_site_association",), {}, None
            )
        if baseline is None or observation.capture_time is None or not observation.source_excerpt.strip():
            return self._candidate(
                "insufficient_context", observation, baseline, 0.0, ("baseline_capture_time_or_citable_context_missing",), {}, None
            )

        similarity = _cosine_similarity(observation.scene_embedding, baseline.scene_embedding)
        scene_change = 0.0 if similarity is None else 1.0 - similarity
        semantic = _semantic_progress(observation, baseline)
        ocr_change = float(_tokens(observation.ocr_text) != _tokens(baseline.ocr_text)) if observation.ocr_text else 0.0
        components = {
            "source_credibility": observation.source_credibility * 0.16,
            "site_association": association * 0.24,
            "geospatial_hints": geo * 0.08,
            "scene_change": scene_change * 0.28,
            "semantic_progress": semantic * 0.18,
            "ocr_change": ocr_change * 0.06,
        }
        score = round(sum(components.values()), 4)
        reason_codes = ["same_site_evidence", "baseline_compared"]
        if scene_change >= 0.20:
            reason_codes.append("material_scene_delta")
        if semantic >= 0.20:
            reason_codes.append("construction_semantic_delta")
        if confirmed:
            classification = "confirmed_change"
        elif score >= self.material_threshold:
            classification = "material_change_candidate"
        elif score >= self.possible_threshold:
            classification = "possible_change"
        else:
            classification = "insufficient_context"
            reason_codes.append("change_score_below_review_threshold")
        packet = None
        review_status = "not_eligible"
        if classification == "material_change_candidate":
            packet = self._review_packet(observation, baseline)
            review_status = "queued" if llm_quota_available else "queued_quota_exhausted"
        candidate = self._candidate(
            classification, observation, baseline, score, tuple(reason_codes), components, packet, review_status
        )
        if classification == "material_change_candidate":
            self.review_queue.append(candidate)
        return candidate

    @staticmethod
    def _find_duplicate(
        observation: VisualObservation, candidates: Iterable[VisualObservation | None]
    ) -> VisualObservation | None:
        for candidate in candidates:
            if candidate is None:
                continue
            if observation.exact_hash and observation.exact_hash == candidate.exact_hash:
                return candidate
            distance = _hash_distance(observation.perceptual_hash, candidate.perceptual_hash)
            if distance is not None and distance <= 4:
                return candidate
        return None

    @staticmethod
    def _review_packet(observation: VisualObservation, baseline: VisualObservation) -> Mapping[str, Any]:
        return {
            "candidate_artifact_id": observation.artifact_id,
            "baseline_artifact_id": baseline.artifact_id,
            "derived_asset_ids": list(observation.derived_asset_ids[:12]),
            "feature_records": [
                {
                    "artifact_id": record.artifact_id,
                    "model_id": record.model_id,
                    "version": record.model_version,
                    "output_hash": record.output_hash,
                    "reason_codes": list(record.reason_codes),
                }
                for record in observation.inference_records[:12]
            ],
            "source_excerpt": observation.source_excerpt[:2_000],
            "historical_baseline": {
                "source_uri": baseline.source_uri,
                "capture_time": baseline.capture_time.isoformat() if baseline.capture_time else None,
                "object_labels": list(baseline.object_labels[:50]),
            },
        }

    @staticmethod
    def _candidate(
        classification: str,
        observation: VisualObservation,
        baseline: VisualObservation | None,
        score: float,
        reason_codes: tuple[str, ...],
        components: Mapping[str, float],
        packet: Mapping[str, Any] | None,
        review_status: str = "not_eligible",
    ) -> VisualChangeCandidate:
        return VisualChangeCandidate(
            classification=classification,
            score=score,
            reason_codes=tuple(sorted(set(reason_codes))),
            observation_artifact_id=observation.artifact_id,
            baseline_artifact_id=baseline.artifact_id if baseline else None,
            review_status=review_status,
            review_packet=packet,
            score_components=dict(components),
        )


__all__ = [
    "CHANGE_CLASSIFICATIONS",
    "MODEL_KINDS",
    "InferenceRecord",
    "LocalInferenceAdapter",
    "LocalInferenceRegistry",
    "MaterialVisualChangePipeline",
    "ModelManifest",
    "VisualChangeCandidate",
    "VisualObservation",
]
