from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from src.app import create_application
from src.config.settings import Settings, get_settings
from src.reference.ingest import cli as reference_ingest_cli
from src.reference.schemas import ReferenceRecord
from src.services.ops_audit_service import list_alert_records, list_provenance_events
from src.services.runtime_scheduler_service import RuntimeSchedulerCoordinator
from src.services.wave_monitor_service import WaveMonitorService


def _shared_settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        APP_ENV="test",
        APP_RUNTIME_MODE="backend-only",
        PRIMARY_DATABASE_URL=f"sqlite:///{(tmp_path / '11writer.db').as_posix()}",
        APP_CORS_ORIGINS="",
        WEBCAM_WORKER_ENABLED=False,
        WEBCAM_WORKER_RUN_ON_STARTUP=False,
    )


def test_reference_ingest_writes_backend_provenance_event(tmp_path: Path, monkeypatch, capsys) -> None:
    settings = _shared_settings(tmp_path)
    dataset_root = tmp_path / "fixture-dataset"
    dataset_root.mkdir()

    monkeypatch.setattr(reference_ingest_cli, "get_settings", lambda: settings)
    monkeypatch.setattr(reference_ingest_cli, "prepare_source", lambda manifest, staging_root: dataset_root)
    monkeypatch.setitem(
        reference_ingest_cli.PARSERS,
        "fixes",
        lambda source_path, version: [
            ReferenceRecord(
                ref_id="fix:test-1",
                object_type="fix",
                canonical_name="TESTFIX",
                primary_code="TESTFIX",
                source_dataset="faa-fixes",
                source_key="fixture-1",
                status="active",
                country_code="US",
                admin1_code="US-TX",
                centroid_lat=30.0,
                centroid_lon=-97.0,
                bbox_min_lat=30.0,
                bbox_min_lon=-97.0,
                bbox_max_lat=30.0,
                bbox_max_lon=-97.0,
                geometry_json=None,
                coverage_tier="baseline",
                source_version=version,
                detail={"ident": "TESTFIX", "fix_type": "named"},
            )
        ],
    )

    reference_ingest_cli.run_cli(
        [
            "fixes",
            str(dataset_root),
            "--database-url",
            settings.reference_database_url,
            "--version",
            "fixture-v1",
        ]
    )
    assert "Ingested 1 records from fixes." in capsys.readouterr().out

    provenance = list_provenance_events(settings, subsystem="reference")

    assert provenance.count == 1
    assert provenance.events[0].event_kind == "reference_dataset_ingest"
    assert provenance.events[0].subject_id == "fixes:fixture-v1"
    assert provenance.events[0].output_refs == ["reference_dataset_load:fixes:fixture-v1"]


def test_runtime_worker_failure_creates_alert_and_provenance(tmp_path: Path, monkeypatch) -> None:
    settings = _shared_settings(tmp_path)

    async def _boom(self) -> object:
        del self
        raise RuntimeError("wave worker exploded")

    monkeypatch.setattr(WaveMonitorService, "scheduler_tick", _boom)

    run = RuntimeSchedulerCoordinator(settings).run_worker_now("wave_monitor", requested_by="test-operator")
    alerts = list_alert_records(settings, subsystem="runtime_scheduler")
    provenance = list_provenance_events(settings, subsystem="runtime_scheduler")

    assert run.status == "failed"
    assert alerts.count == 1
    assert alerts.alerts[0].alert_type == "runtime_worker_failure"
    assert alerts.alerts[0].status == "open"
    assert "exploded" in alerts.alerts[0].summary
    assert {event.status for event in provenance.events[:2]} == {"failed", "started"}


def test_ops_routes_expose_wave_monitor_alerts_and_provenance(tmp_path: Path) -> None:
    settings = _shared_settings(tmp_path)
    app = create_application()
    app.dependency_overrides[get_settings] = lambda: settings
    client = TestClient(app)

    run_response = client.post("/api/tools/waves/wave:scam-ecosystem-watch/run-now")
    alerts_response = client.get("/api/ops/alerts", params={"subsystem": "wave_monitor"})
    provenance_response = client.get("/api/ops/provenance", params={"subsystem": "wave_monitor"})

    assert run_response.status_code == 200
    assert alerts_response.status_code == 200
    assert provenance_response.status_code == 200
    alerts_payload = alerts_response.json()
    provenance_payload = provenance_response.json()
    assert alerts_payload["count"] >= 1
    assert any(alert["alertType"] == "wave_monitor_signal" for alert in alerts_payload["alerts"])
    assert any(event["eventKind"] == "connector_run" for event in provenance_payload["events"])
