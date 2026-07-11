from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.routes.browser_collection import router
from src.services.browser_collection_service import (
    BrowserCollectionPolicy,
    BrowserCollectionRejected,
    BrowserCollectionUnavailable,
    browser_collection_status,
    collect_browser_capture,
)
from src.services.research_fleet_service import DEFAULT_PUBLIC_PROVIDER_REGISTRY, admit_static_collection_url


def _resolver(ip: str):
    def resolve(host: str, port: object, type: int):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0))]

    return resolve


def _admission() -> dict[str, object]:
    # Browser admission is re-resolved by the worker; use a public literal test
    # target so no external DNS is needed for the runner output test.
    return admit_static_collection_url(
        "https://93.184.216.34/article", provider=DEFAULT_PUBLIC_PROVIDER_REGISTRY[0]
    )


def _worker(command, **kwargs):  # type: ignore[no-untyped-def]
    stage = Path(command[command.index("--output-dir") + 1])
    (stage / "dom.html").write_text("<main>Evidence</main>", encoding="utf-8")
    (stage / "text.txt").write_text("Evidence", encoding="utf-8")
    (stage / "headers.json").write_text(json.dumps({"Content-Type": "text/html"}), encoding="utf-8")
    (stage / "screenshot.png").write_bytes(b"PNG")
    (stage / "capture.json").write_text(
        json.dumps(
            {
                "final_url": "https://93.184.216.34/article",
                "status_code": 200,
                "redirects": [],
            }
        ),
        encoding="utf-8",
    )
    return subprocess.CompletedProcess(command, 0, "", "")


def test_status_is_fail_closed_until_operator_enables_worker() -> None:
    status = browser_collection_status(BrowserCollectionPolicy())
    assert status["status"] == "disabled"
    assert status["credentials"] == "disabled"


def test_capture_requires_all_artifacts_and_writes_safe_manifest(tmp_path: Path) -> None:
    capture = collect_browser_capture(
        admission=_admission(),
        data_dir=tmp_path,
        policy=BrowserCollectionPolicy(enabled=True, command=("playwright-worker",)),
        command_runner=_worker,
    )
    capture_dir = tmp_path / "browser-collection" / capture.capture_id
    assert capture.final_url == "https://93.184.216.34/article"
    assert (capture_dir / "dom.html").is_file()
    assert (capture_dir / "text.txt").is_file()
    assert (capture_dir / "headers.json").is_file()
    assert (capture_dir / "screenshot.png").is_file()
    manifest = json.loads((capture_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["credentials"] == "disabled"
    assert manifest["sha256"]["screenshot"]


def test_capture_rejects_missing_screenshot_and_no_command_fallback(tmp_path: Path) -> None:
    def incomplete(command, **kwargs):  # type: ignore[no-untyped-def]
        stage = Path(command[command.index("--output-dir") + 1])
        (stage / "capture.json").write_text(
            '{"final_url":"https://93.184.216.34/article","status_code":200,"redirects":[]}',
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, "", "")

    with pytest.raises(BrowserCollectionRejected, match="unexpected output"):
        collect_browser_capture(
            admission=_admission(),
            data_dir=tmp_path,
            policy=BrowserCollectionPolicy(enabled=True, command=("playwright-worker",)),
            command_runner=incomplete,
        )
    with pytest.raises(BrowserCollectionUnavailable):
        collect_browser_capture(admission=_admission(), data_dir=tmp_path, policy=BrowserCollectionPolicy())


def test_route_exposes_fail_closed_health_without_a_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ELEVENWRITER_BROWSER_COLLECTION_ENABLED", raising=False)
    monkeypatch.delenv("ELEVENWRITER_BROWSER_COLLECTION_COMMAND", raising=False)
    app = FastAPI()
    app.include_router(router, prefix="/api")
    client = TestClient(app)
    assert client.get("/api/browser-collection/health").json()["status"] == "disabled"
    response = client.post("/api/browser-collection/collect", json={"admission": _admission()})
    assert response.status_code == 503
