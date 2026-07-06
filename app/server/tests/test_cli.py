from __future__ import annotations

import json
from pathlib import Path

from src import cli
from src.config.settings import Settings


def test_build_doctor_report_marks_frontend_removed(tmp_path: Path) -> None:
    report = cli.build_doctor_report(
        repo_root=tmp_path,
        settings=Settings(_env_file=None, APP_RUNTIME_MODE="backend-only", APP_CORS_ORIGINS=""),
    )

    assert report["frontend_runtime_removed"] is True
    assert report["frontend_runtime_paths"] == {
        "app_client": False,
        "sevenpo8_frontend": False,
        "code_oss_reference": False,
    }


def test_build_doctor_report_detects_frontend_runtime_paths(tmp_path: Path) -> None:
    (tmp_path / "app" / "client").mkdir(parents=True)
    (tmp_path / "7Po8" / "apps" / "frontend").mkdir(parents=True)

    report = cli.build_doctor_report(
        repo_root=tmp_path,
        settings=Settings(_env_file=None, APP_RUNTIME_MODE="backend-only", APP_CORS_ORIGINS=""),
    )

    assert report["frontend_runtime_removed"] is False
    assert report["frontend_runtime_paths"]["app_client"] is True
    assert report["frontend_runtime_paths"]["sevenpo8_frontend"] is True


def test_main_dispatches_worker_command(monkeypatch) -> None:
    captured: dict[str, list[str]] = {}

    def fake_run_cli(argv: list[str]) -> None:
        captured["argv"] = argv

    monkeypatch.setattr(cli.runtime_worker, "run_cli", fake_run_cli)

    exit_code = cli.main(["worker", "--worker", "wave_monitor", "--loop"])

    assert exit_code == 0
    assert captured["argv"] == ["--worker", "wave_monitor", "--loop"]


def test_doctor_json_output(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_doctor_report",
        lambda: {
            "runtime_mode": "backend-only",
            "platform": {"python": "3.11.0", "system": "TestOS", "release": "1"},
            "api": {
                "reference_database_url": "sqlite:///./data/reference.db",
                "wave_monitor_database_url": "sqlite:///./data/wave_monitor.db",
                "source_discovery_database_url": "sqlite:///./data/source_discovery.db",
                "cors_origins": [],
            },
            "storage": {
                "storageMode": "persistent-sqlite",
                "primaryDatabaseUrl": None,
                "primaryDatabaseBackend": None,
                "primaryDatabasePostgisEnabled": False,
                "sharedStorage": False,
                "distinctDatabaseCount": 3,
                "bootstrapped": False,
                "components": [],
                "caveats": [],
            },
            "workers": {
                "webcam_worker_enabled": False,
                "source_discovery_scheduler_enabled": False,
                "wave_monitor_scheduler_enabled": False,
            },
            "frontend_runtime_removed": True,
            "frontend_runtime_paths": {
                "app_client": False,
                "sevenpo8_frontend": False,
                "code_oss_reference": False,
            },
            "repo_root": "/tmp/repo",
        },
    )

    exit_code = cli.main(["doctor", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["frontend_runtime_removed"] is True
    assert payload["runtime_mode"] == "backend-only"


def test_db_status_json_output(monkeypatch, capsys) -> None:
    class _FakeReport:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "storageMode": "persistent-postgis",
                "primaryDatabaseUrl": "postgresql+psycopg://user:pass@db/11writer",
                "primaryDatabaseBackend": "postgresql+postgis",
                "primaryDatabasePostgisEnabled": True,
                "sharedStorage": True,
                "distinctDatabaseCount": 1,
                "bootstrapped": False,
                "components": [
                    {
                        "component": "reference",
                        "backend": "postgresql+postgis",
                        "reachable": True,
                        "initialized": True,
                    }
                ],
                "caveats": [],
            }

    monkeypatch.setattr(cli, "build_storage_status", lambda settings: _FakeReport())
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(["db-status", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["storageMode"] == "persistent-postgis"
    assert payload["sharedStorage"] is True


def test_alerts_json_output(monkeypatch, capsys) -> None:
    class _FakeReport:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "count": 1,
                "alerts": [
                    {
                        "alertId": "alert:test",
                        "dedupeKey": "runtime-worker-failure:wave_monitor",
                        "subsystem": "runtime_scheduler",
                        "alertType": "runtime_worker_failure",
                        "severity": "high",
                        "status": "open",
                        "title": "Runtime worker failed: wave_monitor",
                        "summary": "boom",
                        "subjectType": "runtime_worker",
                        "subjectId": "wave_monitor",
                        "sourceEventId": "prov:test",
                        "firstObservedAt": "2026-01-01T00:00:00Z",
                        "lastObservedAt": "2026-01-01T00:00:00Z",
                        "occurrenceCount": 1,
                        "evidenceRefs": ["runtime_scheduler_run:test"],
                        "metadata": {},
                        "caveats": [],
                    }
                ],
            }

    monkeypatch.setattr(cli, "list_alert_records", lambda *args, **kwargs: _FakeReport())
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(["alerts", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["count"] == 1
    assert payload["alerts"][0]["alertType"] == "runtime_worker_failure"


def test_ready_json_output(monkeypatch, capsys) -> None:
    class _FakeReport:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "status": "ok",
                "ready": True,
                "runtimeMode": "backend-only",
                "storageMode": "persistent-postgis",
                "checks": [
                    {"name": "storage", "ready": True, "detail": "5/5 configured storage components are reachable and initialized."}
                ],
                "caveats": [],
            }

    monkeypatch.setattr(cli, "build_runtime_readiness_report", lambda settings: _FakeReport())
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(["ready", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["ready"] is True
    assert payload["storageMode"] == "persistent-postgis"


def test_ready_command_returns_nonzero_when_not_ready(monkeypatch, capsys) -> None:
    class _FakeReport:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "status": "degraded",
                "ready": False,
                "runtimeMode": "backend-only",
                "storageMode": "persistent-sqlite",
                "checks": [
                    {"name": "storage", "ready": False, "detail": "0/5 configured storage components are reachable and initialized."}
                ],
                "caveats": ["Run `11writer db-bootstrap` before treating the backend as ready."],
            }

    monkeypatch.setattr(cli, "build_runtime_readiness_report", lambda settings: _FakeReport())
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(["ready", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["ready"] is False


def test_event_report_json_output(monkeypatch, capsys) -> None:
    class _FakeDetail:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "artifact": {
                    "artifactId": "event-artifact:test",
                    "eventId": "source-event:test",
                    "provenanceEventId": "prov:test",
                    "artifactKind": "report",
                    "redactionLevel": "public",
                    "title": "Event Report :: Test",
                    "generatedBy": "11writer-cli",
                    "generatedAt": "2026-07-06T00:00:00Z",
                    "confidenceScore": 0.72,
                    "confidenceLabel": "medium",
                    "supportingSourceCount": 2,
                    "contradictionSourceCount": 1,
                    "correctiveSourceCount": 0,
                    "openQuestionCount": 0,
                    "citationCount": 2,
                    "summaryText": "Test summary",
                    "bodyText": "# Event Report",
                    "citations": [],
                    "chainOfCustody": [],
                    "metadata": {},
                    "caveats": [],
                },
                "caveats": [],
            }

    class _FakeService:
        def __init__(self, settings) -> None:
            self.settings = settings

        def generate_event_artifact(self, event_id, request):
            assert event_id == "source-event:test"
            assert request.artifact_kind == "report"
            return _FakeDetail()

    monkeypatch.setattr(cli, "SourceEventArtifactService", _FakeService)
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(["event-report", "source-event:test", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact"]["artifactKind"] == "report"
    assert payload["artifact"]["confidenceLabel"] == "medium"


def test_event_reports_json_output(monkeypatch, capsys) -> None:
    class _FakeList:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "count": 1,
                "artifacts": [
                    {
                        "artifactId": "event-artifact:test",
                        "eventId": "source-event:test",
                        "provenanceEventId": "prov:test",
                        "artifactKind": "cited_summary",
                        "redactionLevel": "restricted",
                        "title": "Cited Summary :: Test",
                        "generatedBy": "11writer-cli",
                        "generatedAt": "2026-07-06T00:00:00Z",
                        "confidenceScore": 0.61,
                        "confidenceLabel": "medium",
                        "supportingSourceCount": 2,
                        "contradictionSourceCount": 0,
                        "correctiveSourceCount": 0,
                        "openQuestionCount": 1,
                        "citationCount": 2,
                        "summaryText": "Test cited summary",
                    }
                ],
                "caveats": [],
            }

    class _FakeService:
        def __init__(self, settings) -> None:
            self.settings = settings

        def list_event_artifacts(self, event_id):
            assert event_id == "source-event:test"
            return _FakeList()

    monkeypatch.setattr(cli, "SourceEventArtifactService", _FakeService)
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(["event-reports", "source-event:test", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["count"] == 1
    assert payload["artifacts"][0]["artifactKind"] == "cited_summary"


def test_geofence_create_json_output(monkeypatch, capsys) -> None:
    class _FakeDetail:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "geofenceId": "geofence:test-circle",
                "name": "Test Circle",
                "shapeKind": "circle",
                "redactionLevel": "public",
                "enabled": True,
                "createdBy": "11writer-cli",
                "createdAt": "2026-07-06T00:00:00Z",
                "updatedAt": "2026-07-06T00:00:00Z",
                "description": None,
                "minLat": 29.9,
                "minLon": -98.1,
                "maxLat": 30.1,
                "maxLon": -97.9,
                "centerLat": 30.0,
                "centerLon": -98.0,
                "radiusM": 1500.0,
                "tags": ["fixture"],
                "geometryJson": None,
                "metadata": {},
                "caveats": [],
            }

    class _FakeService:
        def __init__(self, settings) -> None:
            self.settings = settings

        def create_geofence(self, request):
            assert request.geofence_id == "geofence:test-circle"
            assert request.shape_kind == "circle"
            assert request.radius_m == 1500.0
            return _FakeDetail()

    monkeypatch.setattr(cli, "GeofenceService", _FakeService)
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(
        [
            "geofence-create",
            "geofence:test-circle",
            "Test Circle",
            "--shape-kind",
            "circle",
            "--center-lat",
            "30.0",
            "--center-lon",
            "-98.0",
            "--radius-m",
            "1500",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["shapeKind"] == "circle"
    assert payload["geofenceId"] == "geofence:test-circle"


def test_geofence_check_json_output(monkeypatch, capsys) -> None:
    class _FakeResponse:
        def model_dump(self, *, mode: str, by_alias: bool) -> dict[str, object]:
            assert mode == "json"
            assert by_alias is True
            return {
                "geofence": {
                    "geofenceId": "geofence:test-box",
                    "name": "Test Box",
                    "shapeKind": "bbox",
                    "redactionLevel": "public",
                    "enabled": True,
                    "createdBy": "11writer-api",
                    "createdAt": "2026-07-06T00:00:00Z",
                    "updatedAt": "2026-07-06T00:00:00Z",
                    "description": None,
                    "minLat": 30.0,
                    "minLon": -98.0,
                    "maxLat": 30.5,
                    "maxLon": -97.5,
                    "centerLat": 30.25,
                    "centerLon": -97.75,
                    "radiusM": None,
                    "tags": [],
                },
                "evaluation": {
                    "evaluationId": "geofence-eval:test",
                    "geofenceId": "geofence:test-box",
                    "subjectType": "event",
                    "subjectId": "event:test-1",
                    "observationLabel": "Fixture observation",
                    "observedLat": 30.2,
                    "observedLon": -97.8,
                    "observedAt": "2026-07-06T00:00:00Z",
                    "matched": True,
                    "matchMethod": "bbox",
                    "distanceToCenterM": None,
                    "referenceMatchCount": 1,
                    "matchedReferenceObjects": [],
                    "alertId": "alert:test",
                    "provenanceEventId": "prov:test",
                    "metadata": {},
                    "caveats": [],
                },
                "caveats": [],
            }

    class _FakeService:
        def __init__(self, settings) -> None:
            self.settings = settings

        def evaluate_point(self, geofence_id, request):
            assert geofence_id == "geofence:test-box"
            assert request.subject_type == "event"
            assert request.lat == 30.2
            return _FakeResponse()

    monkeypatch.setattr(cli, "GeofenceService", _FakeService)
    monkeypatch.setattr(cli, "get_settings", lambda: Settings(_env_file=None))

    exit_code = cli.main(
        [
            "geofence-check",
            "geofence:test-box",
            "--lat",
            "30.2",
            "--lon",
            "-97.8",
            "--subject-type",
            "event",
            "--subject-id",
            "event:test-1",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["evaluation"]["matched"] is True
    assert payload["evaluation"]["alertId"] == "alert:test"
