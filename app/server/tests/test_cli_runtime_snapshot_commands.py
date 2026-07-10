from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from src.cli import app
from src.config import reset_settings_cache
from src.db import get_session_factory, init_db, reset_db_state
from src.models import StorageObjectORM


@pytest.fixture()
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CliRunner:
    database_path = tmp_path / "test.db"
    data_path = tmp_path / "var"
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(data_path))
    reset_settings_cache()
    reset_db_state()
    init_db()
    runner = CliRunner()
    yield runner
    reset_db_state()
    reset_settings_cache()


def test_export_runtime_snapshot_writes_manifest_and_storage_records(cli_env: CliRunner, tmp_path: Path) -> None:
    snapshot_path = tmp_path / "exports" / "runtime-snapshot.json"
    manifest_path = tmp_path / "exports" / "runtime-snapshot.manifest.json"

    export_result = cli_env.invoke(app, ["export-runtime-snapshot", str(snapshot_path)])
    assert export_result.exit_code == 0
    assert snapshot_path.exists()
    assert manifest_path.exists()
    assert "exported runtime snapshot to" in export_result.output
    assert "exported runtime snapshot manifest to" in export_result.output

    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["snapshot_file_name"] == snapshot_path.name
    assert manifest_payload["snapshot_storage_object_id"] is not None
    assert manifest_payload["snapshot_object_key"]

    verify_result = cli_env.invoke(app, ["verify-runtime-snapshot", str(snapshot_path)])
    assert verify_result.exit_code == 0
    assert "verified runtime snapshot artifact" in verify_result.output

    session = get_session_factory()()
    try:
        snapshot_rows = (
            session.query(StorageObjectORM)
            .filter_by(owner_type="runtime_snapshot", object_kind="runtime_snapshot_export")
            .all()
        )
        manifest_rows = (
            session.query(StorageObjectORM)
            .filter_by(owner_type="runtime_snapshot", object_kind="runtime_snapshot_manifest_export")
            .all()
        )
        assert len(snapshot_rows) == 1
        assert len(manifest_rows) == 1
        assert manifest_rows[0].metadata_json["snapshot_storage_object_id"] == snapshot_rows[0].storage_object_id
    finally:
        session.close()


def test_restore_runtime_snapshot_rejects_tampered_artifact(cli_env: CliRunner, tmp_path: Path) -> None:
    snapshot_path = tmp_path / "exports" / "runtime-snapshot.json"

    export_result = cli_env.invoke(app, ["export-runtime-snapshot", str(snapshot_path)])
    assert export_result.exit_code == 0

    snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot_payload["app_name"] = "Totally Fine Definitely Not Tampered"
    snapshot_path.write_text(json.dumps(snapshot_payload, indent=2), encoding="utf-8")

    restore_result = cli_env.invoke(app, ["restore-runtime-snapshot", str(snapshot_path), "--replace-existing"])
    assert restore_result.exit_code != 0
    assert "runtime snapshot artifact verification failed" in restore_result.output
    assert "snapshot_sha256" in restore_result.output
