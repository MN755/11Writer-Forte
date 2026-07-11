"""Offline, approval-gated local intelligence runtime.

This module deliberately separates model setup from inference.  The registry records
an operator-approved, checksum-pinned local model and a benchmark report; inference
only accepts an artifact UID and never receives a file path or a model download URL.
Production inference invokes an operator-approved, checksum-pinned local command via
a JSON stdin/stdout feature contract.  A deterministic *non-semantic* hash extractor
exists solely for explicit development tests and is rejected by default.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
from typing import Any, Iterator

from src.services.artifact_store_service import ArtifactStore

REGISTRY_VERSION = 1
MANIFEST_VERSION = 1
BENCHMARK_VERSION = 1
SUPPORTED_TASKS = frozenset({
    "object_detection", "image_embedding", "ocr", "speech_to_text", "time_series_anomaly"
})
_NETWORK_GUARD_LOCK = threading.RLock()


class LocalIntelligenceError(RuntimeError):
    """Raised when an offline local-intelligence control cannot be verified."""


@dataclass(frozen=True)
class BenchmarkReport:
    benchmark_version: str
    corpus_manifest_sha256: str
    metrics: dict[str, float]
    measured_at: str
    environment: dict[str, str]

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "BenchmarkReport":
        metrics = value.get("metrics")
        if not isinstance(metrics, dict):
            raise LocalIntelligenceError("Benchmark report requires a metrics object.")
        required = {"precision", "recall", "false_alert_rate", "latency_ms"}
        missing = sorted(required - set(metrics))
        if missing:
            raise LocalIntelligenceError(f"Benchmark report is missing metrics: {', '.join(missing)}.")
        normalized: dict[str, float] = {}
        for name, raw in metrics.items():
            try:
                number = float(raw)
            except (TypeError, ValueError) as exc:
                raise LocalIntelligenceError(f"Benchmark metric {name} must be numeric.") from exc
            if number < 0 or (name != "latency_ms" and number > 1):
                raise LocalIntelligenceError(f"Benchmark metric {name} is out of range.")
            normalized[str(name)] = number
        corpus = str(value.get("corpus_manifest_sha256", ""))
        if not _is_sha256(corpus):
            raise LocalIntelligenceError("Benchmark corpus_manifest_sha256 must be a SHA-256 digest.")
        version = str(value.get("benchmark_version", ""))
        if not version.strip():
            raise LocalIntelligenceError("Benchmark report requires benchmark_version.")
        environment = value.get("environment", {})
        if not isinstance(environment, dict):
            raise LocalIntelligenceError("Benchmark environment must be an object.")
        return cls(
            benchmark_version=version,
            corpus_manifest_sha256=corpus,
            metrics=normalized,
            measured_at=str(value.get("measured_at") or _now()),
            environment={str(key): str(item) for key, item in environment.items()},
        )


@dataclass(frozen=True)
class ApprovalManifest:
    model_id: str
    model_version: str
    task: str
    runtime: str
    model_path: str
    model_sha256: str
    runner_executable_path: str | None
    runner_executable_sha256: str | None
    runner_asset_path: str | None
    runner_asset_sha256: str | None
    runner_arguments: tuple[str, ...]
    license_id: str
    upstream_origin: str
    sbom_sha256: str
    package_lock_sha256: str
    cve_review_ref: str
    benchmark: BenchmarkReport
    gpu_vram_mb: int
    cpu_fallback: bool
    timeout_seconds: float
    queue_depth_limit: int
    approved_by: str
    approved_at: str
    manifest_version: int = MANIFEST_VERSION
    network_disabled: bool = True

    @property
    def key(self) -> str:
        return f"{self.model_id}@{self.model_version}"

    @classmethod
    def from_mapping(
        cls, value: dict[str, Any], *, data_dir: Path, allow_development_runner: bool = False
    ) -> "ApprovalManifest":
        task = str(value.get("task", ""))
        if task not in SUPPORTED_TASKS:
            raise LocalIntelligenceError(f"Unsupported task '{task}'.")
        runtime = str(value.get("runtime", ""))
        if runtime not in {"approved_command_v1", "artifact_hash_features_v1"}:
            raise LocalIntelligenceError("Unsupported local-intelligence runner.")
        if runtime == "artifact_hash_features_v1" and not allow_development_runner:
            raise LocalIntelligenceError("The hash feature runner is development-only and forbidden in production.")
        # Fixtures should never get a quiet promotion to a production runtime. Cute demo,
        # catastrophic release gate.
        model_id = str(value.get("model_id", "")).strip()
        model_version = str(value.get("model_version", "")).strip()
        model_path_raw = str(value.get("model_path", ""))
        if (
            not model_id
            or not model_version
            or "fixture" in model_id.lower()
            or "fixture" in Path(model_path_raw).name.lower()
        ):
            raise LocalIntelligenceError("Fixture models are forbidden by the production registry.")
        model_path = Path(model_path_raw).expanduser().resolve()
        _require_under(model_path, data_dir, "Approved model path")
        if not model_path.is_file():
            raise LocalIntelligenceError("Approved model path must be an existing immutable local file.")
        model_sha256 = str(value.get("model_sha256", ""))
        if not _is_sha256(model_sha256) or _sha256_file(model_path) != model_sha256:
            raise LocalIntelligenceError("Approved model checksum does not match its local file.")
        digests = {field: str(value.get(field, "")) for field in ("sbom_sha256", "package_lock_sha256")}
        if any(not _is_sha256(digest) for digest in digests.values()):
            raise LocalIntelligenceError("Approval requires SHA-256 SBOM and package-lock records.")
        if not str(value.get("license_id", "")).strip() or not str(value.get("upstream_origin", "")).strip():
            raise LocalIntelligenceError("Approval requires a license and upstream origin.")
        if not str(value.get("cve_review_ref", "")).strip():
            raise LocalIntelligenceError("Approval requires a completed CVE review reference.")
        if value.get("network_disabled") is not True:
            raise LocalIntelligenceError("Approval must explicitly attest network_disabled=true.")
        try:
            gpu_vram_mb = int(value.get("gpu_vram_mb", 0))
            timeout_seconds = float(value.get("timeout_seconds", 60))
            queue_depth_limit = int(value.get("queue_depth_limit", 1))
        except (TypeError, ValueError) as exc:
            raise LocalIntelligenceError("GPU, timeout, and queue controls must be numeric.") from exc
        if gpu_vram_mb < 0 or timeout_seconds <= 0 or queue_depth_limit < 1:
            raise LocalIntelligenceError("GPU, timeout, and queue controls are out of range.")
        executable_path: str | None = None
        executable_sha256: str | None = None
        asset_path: str | None = None
        asset_sha256: str | None = None
        runner_arguments: tuple[str, ...] = ()
        if runtime == "approved_command_v1":
            executable = Path(str(value.get("runner_executable_path", ""))).expanduser().resolve()
            executable_sha256 = str(value.get("runner_executable_sha256", ""))
            raw_arguments = value.get("runner_arguments", [])
            if not executable.is_file() or not _is_sha256(executable_sha256):
                raise LocalIntelligenceError("Command runner requires an immutable executable and SHA-256.")
            if _sha256_file(executable) != executable_sha256:
                raise LocalIntelligenceError("Approved command executable checksum does not match.")
            asset = Path(str(value.get("runner_asset_path", ""))).expanduser().resolve()
            asset_sha256 = str(value.get("runner_asset_sha256", ""))
            _require_under(asset, data_dir, "Approved command runner asset")
            if not asset.is_file() or not _is_sha256(asset_sha256):
                raise LocalIntelligenceError("Command runner requires a local immutable asset and SHA-256.")
            if _sha256_file(asset) != asset_sha256:
                raise LocalIntelligenceError("Approved command runner asset checksum does not match.")
            if not isinstance(raw_arguments, list) or not all(isinstance(item, str) for item in raw_arguments):
                raise LocalIntelligenceError("runner_arguments must be a fixed list of strings.")
            if len(raw_arguments) > 32 or any(len(item) > 8192 for item in raw_arguments):
                raise LocalIntelligenceError("runner_arguments exceed the approved command bounds.")
            if str(asset) not in raw_arguments:
                raise LocalIntelligenceError("runner_arguments must invoke the checksum-pinned runner asset.")
            executable_path, asset_path, runner_arguments = str(executable), str(asset), tuple(raw_arguments)
        return cls(
            model_id=model_id, model_version=model_version, task=task, runtime=runtime,
            model_path=str(model_path), model_sha256=model_sha256,
            runner_executable_path=executable_path, runner_executable_sha256=executable_sha256,
            runner_asset_path=asset_path, runner_asset_sha256=asset_sha256,
            runner_arguments=runner_arguments,
            license_id=str(value["license_id"]), upstream_origin=str(value["upstream_origin"]),
            sbom_sha256=digests["sbom_sha256"], package_lock_sha256=digests["package_lock_sha256"],
            cve_review_ref=str(value["cve_review_ref"]),
            benchmark=BenchmarkReport.from_mapping(dict(value.get("benchmark", {}))),
            gpu_vram_mb=gpu_vram_mb, cpu_fallback=bool(value.get("cpu_fallback", True)),
            timeout_seconds=timeout_seconds, queue_depth_limit=queue_depth_limit,
            approved_by=str(value.get("approved_by", "operator")), approved_at=str(value.get("approved_at") or _now()),
            manifest_version=int(value.get("manifest_version", MANIFEST_VERSION)), network_disabled=True,
        )


@dataclass(frozen=True)
class InferenceRecord:
    artifact_id: str
    input_sha256: str
    model_id: str
    model_version: str
    task: str
    runtime: str
    device: str
    cpu_fallback: bool
    elapsed_ms: float
    confidence: float
    reason_codes: tuple[str, ...]
    features: dict[str, Any]
    transform_lineage: tuple[str, ...]
    network_guard: str = "blocked"


class ModelRegistry:
    """Versioned, atomic approval registry rooted in the local data directory."""

    def __init__(self, data_dir: str | Path, *, allow_development_runner: bool = False) -> None:
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.root = self.data_dir / "local_intelligence"
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "model_registry.json"
        self.allow_development_runner = allow_development_runner
        self._lock = threading.RLock()
        if not self.path.exists():
            self._write({"registry_version": REGISTRY_VERSION, "models": {}})

    def approve(self, payload: dict[str, Any]) -> ApprovalManifest:
        manifest = ApprovalManifest.from_mapping(
            payload, data_dir=self.data_dir, allow_development_runner=self.allow_development_runner
        )
        with self._lock:
            registry = self._read()
            registry["models"][manifest.key] = _manifest_dict(manifest)
            self._write(registry)
        return manifest

    def resolve(self, model_id: str, model_version: str | None = None) -> ApprovalManifest:
        with self._lock:
            models = self._read()["models"]
            matches = [raw for key, raw in models.items() if key == f"{model_id}@{model_version}"] if model_version else [
                raw for key, raw in models.items() if key.startswith(f"{model_id}@")
            ]
        if not matches:
            raise LocalIntelligenceError("No approved model matches the requested ID/version.")
        if len(matches) != 1:
            raise LocalIntelligenceError("model_version is required when multiple approved versions exist.")
        return ApprovalManifest.from_mapping(
            matches[0], data_dir=self.data_dir, allow_development_runner=self.allow_development_runner
        )

    def list(self) -> list[ApprovalManifest]:
        with self._lock:
            raw = list(self._read()["models"].values())
        return [
            ApprovalManifest.from_mapping(
                item, data_dir=self.data_dir, allow_development_runner=self.allow_development_runner
            )
            for item in raw
        ]

    def _read(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LocalIntelligenceError(f"Unreadable model registry: {exc}") from exc
        if value.get("registry_version") != REGISTRY_VERSION or not isinstance(value.get("models"), dict):
            raise LocalIntelligenceError("Unsupported model registry format.")
        return value

    def _write(self, value: dict[str, Any]) -> None:
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        os.replace(temporary, self.path)


class LocalIntelligenceRuntime:
    """Artifact-ID-only runner with deterministic GPU selection and offline execution."""

    def __init__(
        self,
        data_dir: str | Path,
        *,
        gpu_available: bool | None = None,
        allow_development_runner: bool = False,
    ) -> None:
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.registry = ModelRegistry(self.data_dir, allow_development_runner=allow_development_runner)
        self.artifacts = ArtifactStore(self.data_dir / "artifact_store")
        self._gpu_available = gpu_available
        self.allow_development_runner = allow_development_runner

    def run(self, *, artifact_id: str, model_id: str, model_version: str | None = None, prefer_gpu: bool = True) -> InferenceRecord:
        if not artifact_id.strip():
            raise LocalIntelligenceError("artifact_id is required; runtime file paths are not accepted.")
        manifest = self.registry.resolve(model_id, model_version)
        artifact = self.artifacts.get_artifact(artifact_id)
        blob = self.artifacts.root / artifact.blob_path
        if not blob.is_file() or _sha256_file(blob) != artifact.sha256:
            raise LocalIntelligenceError("Artifact bytes are missing or differ from the custody hash.")
        if _sha256_file(Path(manifest.model_path)) != manifest.model_sha256:
            raise LocalIntelligenceError("Approved model changed after approval; inference is blocked.")
        device, cpu_fallback = self._select_device(manifest, prefer_gpu)
        started = time.perf_counter()
        with _runner_slot(manifest.key, manifest.queue_depth_limit):
            with no_network_runtime_guard():
                if manifest.runtime == "approved_command_v1":
                    features, confidence, external_reasons = _run_approved_command(
                        manifest, artifact, artifact_store_root=self.artifacts.root
                    )
                elif self.allow_development_runner:
                    features, confidence = _artifact_hash_features(blob, artifact.sha256, manifest.task)
                    external_reasons = ("development_hash_runner",)
                else:  # Defensive: a registry written by a development process must not run in production.
                    raise LocalIntelligenceError("The hash feature runner is development-only and forbidden in production.")
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        if elapsed_ms > manifest.timeout_seconds * 1000:
            raise LocalIntelligenceError("Inference exceeded the approved runtime timeout.")
        return InferenceRecord(
            artifact_id=artifact_id, input_sha256=artifact.sha256, model_id=manifest.model_id,
            model_version=manifest.model_version, task=manifest.task, runtime=manifest.runtime,
            device=device, cpu_fallback=cpu_fallback, elapsed_ms=elapsed_ms, confidence=confidence,
            reason_codes=("approved_local_model", "offline_runtime_guard", *external_reasons),
            features=features, transform_lineage=(f"model:{manifest.key}", f"artifact:{artifact_id}"),
        )

    def _select_device(self, manifest: ApprovalManifest, prefer_gpu: bool) -> tuple[str, bool]:
        available = self._gpu_available
        if available is None:
            available = os.environ.get("ELEVENWRITER_LOCAL_INTELLIGENCE_GPU_AVAILABLE", "").lower() in {"1", "true", "yes"}
        if prefer_gpu and available and manifest.gpu_vram_mb > 0:
            return "cuda:0", False
        if manifest.cpu_fallback:
            return "cpu", bool(prefer_gpu)
        raise LocalIntelligenceError("Approved GPU is unavailable and this model has no CPU fallback.")


@contextmanager
def no_network_runtime_guard() -> Iterator[None]:
    """Fail closed for DNS and outbound socket connects during inference.

    This is a process-level guard, serialized so concurrent inference does not observe
    a half-installed patch.  A deployment network namespace remains the outer defence.
    """
    with _NETWORK_GUARD_LOCK:
        original_socket, original_connect, original_lookup = socket.socket, socket.create_connection, socket.getaddrinfo

        class OfflineSocket(original_socket):  # type: ignore[misc, valid-type]
            def connect(self, *args: Any, **kwargs: Any) -> None:
                raise LocalIntelligenceError("Outbound network is disabled for local inference.")

            def connect_ex(self, *args: Any, **kwargs: Any) -> int:
                raise LocalIntelligenceError("Outbound network is disabled for local inference.")

        def blocked(*args: Any, **kwargs: Any) -> Any:
            raise LocalIntelligenceError("Outbound network is disabled for local inference.")

        socket.socket, socket.create_connection, socket.getaddrinfo = OfflineSocket, blocked, blocked  # type: ignore[assignment]
        try:
            yield
        finally:
            socket.socket, socket.create_connection, socket.getaddrinfo = original_socket, original_connect, original_lookup  # type: ignore[assignment]


_RUNNER_SLOTS: dict[str, threading.BoundedSemaphore] = {}
_RUNNER_SLOTS_LOCK = threading.Lock()


@contextmanager
def _runner_slot(key: str, depth: int) -> Iterator[None]:
    with _RUNNER_SLOTS_LOCK:
        slot = _RUNNER_SLOTS.setdefault(key, threading.BoundedSemaphore(depth))
    if not slot.acquire(blocking=False):
        raise LocalIntelligenceError("Approved runner queue-depth limit is reached.")
    try:
        yield
    finally:
        slot.release()


def _run_approved_command(
    manifest: ApprovalManifest, artifact: Any, *, artifact_store_root: Path
) -> tuple[dict[str, Any], float, tuple[str, ...]]:
    """Run the checksum-pinned command over a JSON stdin/stdout feature contract.

    The command never receives an arbitrary caller-supplied path.  It gets only an
    artifact identifier and custody metadata; approved deployment configuration is
    supplied through environment variables for a sandboxed local worker to resolve.
    """
    if (
        not manifest.runner_executable_path
        or not manifest.runner_executable_sha256
        or not manifest.runner_asset_path
        or not manifest.runner_asset_sha256
    ):
        raise LocalIntelligenceError("Approved command runner is missing executable approval data.")
    executable = Path(manifest.runner_executable_path)
    if not executable.is_file() or _sha256_file(executable) != manifest.runner_executable_sha256:
        raise LocalIntelligenceError("Approved command executable changed after approval; inference is blocked.")
    asset = Path(manifest.runner_asset_path)
    if not asset.is_file() or _sha256_file(asset) != manifest.runner_asset_sha256:
        raise LocalIntelligenceError("Approved command runner asset changed after approval; inference is blocked.")
    request = {
        "schema_version": 1,
        "artifact_id": artifact.artifact_uid,
        "input_sha256": artifact.sha256,
        "byte_size": artifact.byte_size,
        "task": manifest.task,
        "model_id": manifest.model_id,
        "model_version": manifest.model_version,
    }
    environment = {
        **os.environ,
        "ELEVENWRITER_LOCAL_INTELLIGENCE_OFFLINE": "1",
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
        "NO_PROXY": "*",
        "ELEVENWRITER_ARTIFACT_STORE_ROOT": str(artifact_store_root),
        "ELEVENWRITER_APPROVED_MODEL_PATH": manifest.model_path,
    }
    try:
        completed = subprocess.run(
            [str(executable), *manifest.runner_arguments], input=json.dumps(request), text=True,
            capture_output=True, check=False, shell=False, cwd=str(Path(manifest.model_path).parent),
            env=environment, timeout=manifest.timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LocalIntelligenceError(f"Approved local command failed: {exc}") from exc
    if completed.returncode:
        detail = completed.stderr.strip()[:500] or f"exit code {completed.returncode}"
        raise LocalIntelligenceError(f"Approved local command failed: {detail}")
    try:
        output = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise LocalIntelligenceError("Approved local command did not emit JSON output.") from exc
    if not isinstance(output, dict) or set(output) - {"confidence", "features", "reason_codes", "output_artifact_id"}:
        raise LocalIntelligenceError("Approved local command emitted an unsupported output contract.")
    if not isinstance(output.get("features"), dict):
        raise LocalIntelligenceError("Approved local command output requires a features object.")
    try:
        confidence = float(output.get("confidence"))
    except (TypeError, ValueError) as exc:
        raise LocalIntelligenceError("Approved local command output requires numeric confidence.") from exc
    if not 0 <= confidence <= 1:
        raise LocalIntelligenceError("Approved local command confidence is out of range.")
    reasons = output.get("reason_codes", [])
    if not isinstance(reasons, list) or not all(isinstance(item, str) and len(item) <= 120 for item in reasons):
        raise LocalIntelligenceError("Approved local command reason_codes are invalid.")
    output_artifact_id = output.get("output_artifact_id")
    if output_artifact_id is not None and (
        not isinstance(output_artifact_id, str) or not output_artifact_id.strip()
    ):
        raise LocalIntelligenceError("output_artifact_id must be a non-empty artifact ID when supplied.")
    if len(json.dumps(output["features"])) > 100_000:
        raise LocalIntelligenceError("Approved local command feature output exceeds the bounded contract.")
    return output["features"], round(confidence, 6), tuple(reasons) or ("approved_command_runner",)


def _artifact_hash_features(path: Path, digest: str, task: str) -> tuple[dict[str, Any], float]:
    raw = bytes.fromhex(digest)
    confidence = round(raw[0] / 255, 6)
    if task == "image_embedding":
        return {"embedding": [round(byte / 255, 6) for byte in raw[:16]], "dimension": 16}, confidence
    if task == "time_series_anomaly":
        return {"artifact_digest_bucket": raw[0] % 16, "anomaly_score": confidence}, confidence
    return {"byte_size": path.stat().st_size, "artifact_digest_prefix": digest[:16]}, confidence


def _manifest_dict(manifest: ApprovalManifest) -> dict[str, Any]:
    result = asdict(manifest)
    result["benchmark"] = asdict(manifest.benchmark)
    return result


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _require_under(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise LocalIntelligenceError(f"{label} must be inside the configured data_dir.") from exc


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
