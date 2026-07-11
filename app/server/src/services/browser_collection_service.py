"""Fail-closed, operator-gated browser capture worker.

The web API never executes arbitrary browser commands.  Instead it hands an
already admitted HTTPS target to an operator-installed Playwright worker using
a small JSON-file contract.  The worker must create the fixed capture files in
the supplied output directory.  No HTTP fallback exists: without that worker,
collection is unavailable by design.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit
from uuid import uuid4

from src.services.discovery_fetch import DiscoveryFetchError, sanitize_response_headers
from src.services.research_fleet_service import (
    DEFAULT_PUBLIC_PROVIDER_REGISTRY,
    admit_static_collection_url,
)


class BrowserCollectionError(RuntimeError):
    """A collection request could not be safely run."""


class BrowserCollectionUnavailable(BrowserCollectionError):
    """The explicitly opted-in Playwright worker is not available."""


class BrowserCollectionRejected(BrowserCollectionError):
    """The request or worker output violated the capture contract."""


@dataclass(frozen=True)
class BrowserCollectionPolicy:
    enabled: bool = False
    command: tuple[str, ...] = ()
    max_redirects: int = 5
    max_response_bytes: int = 10 * 1024 * 1024
    max_wall_clock_seconds: float = 45.0
    max_concurrency: int = 1

    @classmethod
    def from_environment(cls) -> "BrowserCollectionPolicy":
        command = tuple(
            part for part in os.getenv("ELEVENWRITER_BROWSER_COLLECTION_COMMAND", "").split() if part
        )
        return cls(
            enabled=os.getenv("ELEVENWRITER_BROWSER_COLLECTION_ENABLED", "").strip().lower()
            in {"1", "true", "yes", "on"},
            command=command,
            max_redirects=_bounded_int("ELEVENWRITER_BROWSER_COLLECTION_MAX_REDIRECTS", 5, 0, 10),
            max_response_bytes=_bounded_int(
                "ELEVENWRITER_BROWSER_COLLECTION_MAX_BYTES", 10 * 1024 * 1024, 1024, 25 * 1024 * 1024
            ),
            max_wall_clock_seconds=_bounded_float(
                "ELEVENWRITER_BROWSER_COLLECTION_TIMEOUT_SECONDS", 45.0, 1.0, 120.0
            ),
            max_concurrency=_bounded_int("ELEVENWRITER_BROWSER_COLLECTION_MAX_CONCURRENCY", 1, 1, 4),
        )

    def normalized(self) -> "BrowserCollectionPolicy":
        return BrowserCollectionPolicy(
            enabled=bool(self.enabled),
            command=tuple(str(part) for part in self.command if str(part).strip()),
            max_redirects=max(0, min(int(self.max_redirects), 10)),
            max_response_bytes=max(1024, min(int(self.max_response_bytes), 25 * 1024 * 1024)),
            max_wall_clock_seconds=max(1.0, min(float(self.max_wall_clock_seconds), 120.0)),
            max_concurrency=max(1, min(int(self.max_concurrency), 4)),
        )


@dataclass(frozen=True)
class BrowserCapture:
    capture_id: str
    provider_id: str
    requested_url: str
    final_url: str
    status_code: int
    redirect_count: int
    elapsed_ms: float
    byte_size: int
    files: dict[str, str]
    manifest_path: str
    captured_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
_capture_state_lock = threading.Lock()
_active_captures = 0
_CAPTURE_FILES = {
    "dom": "dom.html",
    "text": "text.txt",
    "headers": "headers.json",
    "screenshot": "screenshot.png",
}


def browser_collection_status(policy: BrowserCollectionPolicy | None = None) -> dict[str, object]:
    active = (policy or BrowserCollectionPolicy.from_environment()).normalized()
    if not active.enabled:
        status, reason = "disabled", "operator_enablement_required"
    elif not active.command:
        status, reason = "unavailable", "playwright_worker_command_not_configured"
    else:
        status, reason = "ready", None
    return {
        "status": status,
        "reason": reason,
        "enabled": active.enabled,
        "worker": "playwright_cli_contract",
        "max_redirects": active.max_redirects,
        "max_response_bytes": active.max_response_bytes,
        "max_wall_clock_seconds": active.max_wall_clock_seconds,
        "max_concurrency": active.max_concurrency,
        "credentials": "disabled",
        "extensions": "disabled",
        "profile": "fresh_per_capture",
    }


def validate_browser_admission(admission: Mapping[str, object]) -> dict[str, object]:
    """Revalidate a research-fleet admission before a browser is ever started.

    A client-supplied admission is not trusted merely because it has the shape
    of one.  It is recomputed against the provider registry and all safety
    bounds are compared.  Browser targets are HTTPS-only and public-network
    only through ``admit_static_collection_url``.
    """

    provider_id = _required_text(admission.get("provider_id"), "provider_id")
    provider = next(
        (item for item in DEFAULT_PUBLIC_PROVIDER_REGISTRY if item.provider_id == provider_id), None
    )
    if provider is None:
        raise BrowserCollectionRejected("Admission provider is not configured.")
    url = _required_text(admission.get("canonical_url"), "canonical_url")
    try:
        recomputed = admit_static_collection_url(url, provider=provider)
    except (ValueError, DiscoveryFetchError) as exc:
        raise BrowserCollectionRejected(f"Admission no longer passes target policy: {exc}") from exc
    if urlsplit(str(recomputed["canonical_url"])).scheme != "https":
        raise BrowserCollectionRejected("Browser collection requires an HTTPS admission.")
    for field in ("canonical_url", "max_response_bytes", "max_concurrency", "max_requests"):
        if field in admission and admission[field] != recomputed[field]:
            raise BrowserCollectionRejected(f"Admission field {field} does not match policy.")
    return recomputed


def collect_browser_capture(
    *,
    admission: Mapping[str, object],
    data_dir: Path,
    policy: BrowserCollectionPolicy | None = None,
    command_runner: CommandRunner = subprocess.run,
    monotonic_fn: Callable[[], float] = time.monotonic,
) -> BrowserCapture:
    """Run a separately installed Playwright worker and validate its artifacts.

    The command receives paths and policy as arguments, never request headers,
    cookies, credentials, extension paths, or a reusable browser profile.  It
    must implement redirects and request interception internally; output is
    checked again before it becomes an evidence artifact.
    """

    active = (policy or BrowserCollectionPolicy.from_environment()).normalized()
    status = browser_collection_status(active)
    if status["status"] != "ready":
        raise BrowserCollectionUnavailable(str(status["reason"] or "browser_worker_unavailable"))
    approved = validate_browser_admission(admission)
    global _active_captures
    with _capture_state_lock:
        if _active_captures >= active.max_concurrency:
            raise BrowserCollectionUnavailable("browser_collection_concurrency_limit_reached")
        _active_captures += 1
    try:
        return _run_capture(
            approved=approved,
            data_dir=Path(data_dir),
            policy=active,
            command_runner=command_runner,
            monotonic_fn=monotonic_fn,
        )
    finally:
        with _capture_state_lock:
            _active_captures -= 1


def _run_capture(
    *,
    approved: Mapping[str, object],
    data_dir: Path,
    policy: BrowserCollectionPolicy,
    command_runner: CommandRunner,
    monotonic_fn: Callable[[], float],
) -> BrowserCapture:
    capture_id = uuid4().hex
    root = _safe_child(data_dir.resolve(), "browser-collection")
    root.mkdir(parents=True, exist_ok=True)
    final_dir = _safe_child(root, capture_id)
    started = monotonic_fn()
    with tempfile.TemporaryDirectory(prefix="browser-", dir=root) as temp:
        stage = Path(temp).resolve()
        request_path = _safe_child(stage, "request.json")
        request_path.write_text(
            json.dumps(
                {
                    "url": approved["canonical_url"],
                    "provider_id": approved["provider_id"],
                    "max_redirects": policy.max_redirects,
                    "max_response_bytes": min(int(approved["max_response_bytes"]), policy.max_response_bytes),
                    "timeout_ms": int(policy.max_wall_clock_seconds * 1000),
                    "profile": "fresh",
                    "credentials": "disabled",
                    "extensions": "disabled",
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        command = [
            *policy.command,
            "--input",
            str(request_path),
            "--output-dir",
            str(stage),
            "--fresh-profile",
            "--disable-extensions",
            "--no-credentials",
            "--max-redirects",
            str(policy.max_redirects),
            "--max-bytes",
            str(min(int(approved["max_response_bytes"]), policy.max_response_bytes)),
            "--timeout-ms",
            str(int(policy.max_wall_clock_seconds * 1000)),
        ]
        try:
            completed = command_runner(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=policy.max_wall_clock_seconds,
                shell=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise BrowserCollectionUnavailable(f"playwright_worker_failed_to_start_or_timed_out: {exc}") from exc
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "worker returned non-zero").strip()[:1000]
            raise BrowserCollectionUnavailable(f"playwright_worker_failed: {detail}")
        elapsed_ms = round(max(0.0, monotonic_fn() - started) * 1000.0, 3)
        if elapsed_ms > policy.max_wall_clock_seconds * 1000.0:
            raise BrowserCollectionRejected("Browser worker exceeded wall-clock limit.")
        manifest = _validate_stage(stage=stage, approved=approved, policy=policy)
        if final_dir.exists():  # UUID collision is fantastically unlikely; still no overwrite.
            raise BrowserCollectionRejected("Capture identifier collision.")
        shutil.move(str(stage), str(final_dir))

    files = {key: str(_safe_child(final_dir, filename)) for key, filename in _CAPTURE_FILES.items()}
    manifest_path = _safe_child(final_dir, "manifest.json")
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")
    return BrowserCapture(
        capture_id=capture_id,
        provider_id=str(approved["provider_id"]),
        requested_url=str(approved["canonical_url"]),
        final_url=str(manifest["final_url"]),
        status_code=int(manifest["status_code"]),
        redirect_count=int(manifest["redirect_count"]),
        elapsed_ms=elapsed_ms,
        byte_size=int(manifest["byte_size"]),
        files=files,
        manifest_path=str(manifest_path),
        captured_at=datetime.now(timezone.utc).isoformat(),
    )


def _validate_stage(
    *, stage: Path, approved: Mapping[str, object], policy: BrowserCollectionPolicy
) -> dict[str, object]:
    expected_names = {"request.json", "capture.json", *_CAPTURE_FILES.values()}
    actual_names = {item.name for item in stage.iterdir()}
    if actual_names != expected_names or any(item.is_dir() for item in stage.iterdir()):
        raise BrowserCollectionRejected("Browser worker created an unexpected output path.")
    raw_manifest = _read_json(_safe_child(stage, "capture.json"))
    final_url = _required_text(raw_manifest.get("final_url"), "capture.final_url")
    if urlsplit(final_url).scheme.lower() != "https":
        raise BrowserCollectionRejected("Browser worker final URL is not HTTPS.")
    # Revalidate the final redirect target, preventing trust in a worker that only
    # checked the initial URL.  This deliberately does another public DNS check.
    final_admission = dict(approved)
    final_admission["canonical_url"] = final_url
    validate_browser_admission(final_admission)
    redirects = raw_manifest.get("redirects", [])
    if not isinstance(redirects, list) or len(redirects) > policy.max_redirects:
        raise BrowserCollectionRejected("Browser worker exceeded redirect limit.")
    for item in redirects:
        if not isinstance(item, str) or urlsplit(item).scheme.lower() != "https":
            raise BrowserCollectionRejected("Browser worker reported an unsafe redirect.")
        redirected_admission = dict(approved)
        redirected_admission["canonical_url"] = item
        validate_browser_admission(redirected_admission)
    status_code = raw_manifest.get("status_code")
    if not isinstance(status_code, int) or not 200 <= status_code < 400:
        raise BrowserCollectionRejected("Browser worker did not capture a successful response.")
    headers_path = _safe_child(stage, _CAPTURE_FILES["headers"])
    headers = _read_json(headers_path)
    if not isinstance(headers, dict):
        raise BrowserCollectionRejected("Browser worker headers artifact is invalid.")
    safe_headers = sanitize_response_headers({str(key): str(value) for key, value in headers.items()})
    headers_path.write_text(json.dumps(safe_headers, sort_keys=True), encoding="utf-8")
    byte_size = 0
    hashes: dict[str, str] = {}
    for key, filename in _CAPTURE_FILES.items():
        path = _safe_child(stage, filename)
        if not path.is_file():
            raise BrowserCollectionRejected(f"Browser worker did not create {key} artifact.")
        size = path.stat().st_size
        byte_size += size
        hashes[key] = hashlib.sha256(path.read_bytes()).hexdigest()
    # Count the worker manifest too; the request file is written by this process
    # and has a fixed, small schema.  This avoids accepting an arbitrarily large
    # response disguised as capture metadata.
    byte_size += _safe_child(stage, "capture.json").stat().st_size
    if byte_size > min(int(approved["max_response_bytes"]), policy.max_response_bytes):
        raise BrowserCollectionRejected("Browser capture exceeds byte limit.")
    return {
        "schema_version": 1,
        "requested_url": approved["canonical_url"],
        "final_url": final_url,
        "provider_id": approved["provider_id"],
        "status_code": status_code,
        "redirect_count": len(redirects),
        "redirects": redirects,
        "headers": safe_headers,
        "byte_size": byte_size,
        "sha256": hashes,
        "worker": "playwright_cli_contract",
        "credentials": "disabled",
        "extensions": "disabled",
        "profile": "fresh_per_capture",
    }


def _safe_child(parent: Path, name: str) -> Path:
    result = (parent / name).resolve()
    try:
        result.relative_to(parent.resolve())
    except ValueError as exc:
        raise BrowserCollectionRejected("Unsafe capture artifact path.") from exc
    return result


def _read_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BrowserCollectionRejected("Browser worker manifest is missing or invalid.") from exc
    if not isinstance(raw, dict):
        raise BrowserCollectionRejected("Browser worker manifest must be an object.")
    return raw


def _required_text(value: object, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise BrowserCollectionRejected(f"{field} is required.")
    return result


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        return max(minimum, min(int(os.getenv(name, str(default))), maximum))
    except ValueError:
        return default


def _bounded_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        return max(minimum, min(float(os.getenv(name, str(default))), maximum))
    except ValueError:
        return default
