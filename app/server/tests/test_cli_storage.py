from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from src.cli import app
from src.config import reset_settings_cache
from src.db import get_session_factory, init_db, reset_db_state
from src.models import StorageObjectORM
from src.services.storage_backends import hash_file


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


def test_cli_storage_lifecycle_commands_manage_local_artifact(cli_env: CliRunner, tmp_path: Path) -> None:
    artifact_path = tmp_path / "var" / "exports" / "casefile.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text('{"event":"harbor departure"}', encoding="utf-8")
    rehydrated_path = tmp_path / "rehydrated" / "casefile-copy.json"

    add_result = cli_env.invoke(
        app,
        [
            "add-storage-object",
            "manual:casefile:42",
            "runtime_snapshot_export",
            "event",
            "42",
            artifact_path.resolve().as_uri(),
            "--storage-tier",
            "hot",
            "--retention-class",
            "investigative",
            "--content-hash",
            hash_file(artifact_path),
            "--media-type",
            "application/json",
            "--byte-size",
            str(artifact_path.stat().st_size),
            "--metadata-json",
            '{"file_name":"casefile.json"}',
        ],
    )
    assert add_result.exit_code == 0
    assert "storage_object=" in add_result.stdout

    session = get_session_factory()()
    try:
        record = session.query(StorageObjectORM).filter_by(object_key="manual:casefile:42").one()
        storage_object_id = record.storage_object_id
    finally:
        session.close()

    manifest_result = cli_env.invoke(app, ["show-storage-manifest", str(storage_object_id)])
    assert manifest_result.exit_code == 0
    assert "transfer_status=ready" in manifest_result.stdout
    assert "archive_eligible=True" in manifest_result.stdout

    archive_result = cli_env.invoke(app, ["archive-storage-object", str(storage_object_id)])
    assert archive_result.exit_code == 0
    assert "action=archive" in archive_result.stdout
    assert "status=archived" in archive_result.stdout

    verify_result = cli_env.invoke(app, ["verify-storage-object", str(storage_object_id)])
    assert verify_result.exit_code == 0
    assert "action=verify" in verify_result.stdout
    assert "verified=True" in verify_result.stdout

    request_result = cli_env.invoke(app, ["request-storage-rehydrate", str(storage_object_id)])
    assert request_result.exit_code == 0
    assert "action=request_rehydrate" in request_result.stdout

    rehydrate_result = cli_env.invoke(
        app,
        [
            "rehydrate-storage-object",
            str(storage_object_id),
            "--target-path",
            str(rehydrated_path),
        ],
    )
    assert rehydrate_result.exit_code == 0
    assert "action=rehydrate" in rehydrate_result.stdout
    assert rehydrated_path.exists()
    assert rehydrated_path.read_text(encoding="utf-8") == artifact_path.read_text(encoding="utf-8")

    prune_result = cli_env.invoke(app, ["prune-storage-object", str(storage_object_id)])
    assert prune_result.exit_code == 0
    assert "action=prune" in prune_result.stdout
    assert not artifact_path.exists()

    quarantine_result = cli_env.invoke(
        app,
        ["quarantine-storage-object", str(storage_object_id), "integrity review"],
    )
    assert quarantine_result.exit_code == 0
    assert "action=quarantine" in quarantine_result.stdout
    assert "status=quarantined" in quarantine_result.stdout

    unquarantine_result = cli_env.invoke(
        app,
        ["unquarantine-storage-object", str(storage_object_id), "--note", "cleared"],
    )
    assert unquarantine_result.exit_code == 0
    assert "action=unquarantine" in unquarantine_result.stdout

    final_manifest_result = cli_env.invoke(app, ["show-storage-manifest", str(storage_object_id)])
    assert final_manifest_result.exit_code == 0
    assert "role=archive" in final_manifest_result.stdout
    assert "role=rehydrated" in final_manifest_result.stdout

    session = get_session_factory()()
    try:
        record = session.get(StorageObjectORM, storage_object_id)
        assert record is not None
        assert record.lifecycle_status == "archived"
    finally:
        session.close()


def test_cli_storage_lifecycle_command_accepts_operation_filter(cli_env: CliRunner) -> None:
    now = datetime.now(timezone.utc)
    session = get_session_factory()()
    try:
        session.add(
            StorageObjectORM(
                object_key="manual:expired:cli",
                object_kind="report_export",
                owner_type="event",
                owner_id="501",
                object_uri="file:///tmp/expired-cli.json",
                storage_tier="warm",
                retention_class="operational",
                lifecycle_status="active",
                observed_at=now - timedelta(days=2),
                expires_at=now - timedelta(hours=1),
                metadata_json={},
            )
        )
        session.commit()
    finally:
        session.close()

    result = cli_env.invoke(
        app,
        [
            "run-storage-lifecycle",
            "--retention-class",
            "operational",
            "--operation",
            "expire",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "operations=['expire']" in result.stdout
    assert "dry_run=True" in result.stdout
