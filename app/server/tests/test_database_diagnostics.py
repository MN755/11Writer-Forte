from __future__ import annotations

from fastapi.testclient import TestClient

from src.config import reset_settings_cache
from src.migrations import DatabaseRevisionStatus
from src.services import database_diagnostics_service


def test_database_diagnostics_endpoint_reports_sqlite_runtime(client: TestClient) -> None:
    response = client.get("/api/operations/database")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["database_backend"] == "sqlite"
    assert payload["database_connected"] is True
    assert payload["postgis_expected"] is False
    assert payload["postgis_extension_installed"] is None
    assert payload["warning_count"] == 0
    assert payload["migration"]["version_table_present"] is True
    assert payload["migration"]["schema_up_to_date"] is True
    assert any("SQLite/Python spatial fallback" in note for note in payload["notes"])
    assert any("Alembic revision status" in note for note in payload["notes"])
    table_counts = {item["table_name"]: item["row_count"] for item in payload["table_counts"]}
    assert table_counts["observations"] == 0
    assert table_counts["alerts"] == 0
    assert table_counts["storage_objects"] == 0


def test_database_diagnostics_marks_missing_postgis_prerequisites(monkeypatch) -> None:
    monkeypatch.setenv(
        "ELEVENWRITER_DATABASE_URL",
        "postgresql://operator:secret@localhost:5432/elevenwriter_forte",
    )
    reset_settings_cache()

    class FakeResult:
        def scalar_one(self) -> int:
            return 1

    class FakeDialect:
        name = "postgresql"

    class FakeEngine:
        dialect = FakeDialect()

    class FakeSession:
        def get_bind(self) -> FakeEngine:
            return FakeEngine()

        def execute(self, _statement) -> FakeResult:
            return FakeResult()

    monkeypatch.setattr(
        database_diagnostics_service,
        "collect_table_counts",
        lambda _session: [{"table_name": "observations", "row_count": 42}],
    )
    monkeypatch.setattr(
        database_diagnostics_service,
        "inspect_database_revision",
        lambda _engine: DatabaseRevisionStatus(
            current_revision="20260707_0000",
            head_revision="20260707_0001",
            head_revisions=("20260707_0001",),
            version_table_present=True,
            has_application_tables=True,
            schema_up_to_date=False,
        ),
    )
    monkeypatch.setattr(
        database_diagnostics_service,
        "fetch_postgis_extension_version",
        lambda _session: None,
    )
    monkeypatch.setattr(
        database_diagnostics_service,
        "collect_spatial_index_statuses",
        lambda _session: [
            {"index_name": "idx_observations_location_wkt_gist", "present": False},
            {"index_name": "idx_geofences_geometry_wkt_gist", "present": False},
        ],
    )

    payload = database_diagnostics_service.build_database_diagnostics(FakeSession())
    assert payload["status"] == "degraded"
    assert payload["database_backend"] == "postgresql"
    assert payload["database_connected"] is True
    assert payload["postgis_expected"] is True
    assert payload["postgis_extension_installed"] is False
    assert payload["warning_count"] == 3
    assert "Alembic head revision" in payload["warnings"][0]
    assert "PostGIS extension is not installed" in payload["warnings"][1]
    assert "idx_observations_location_wkt_gist" in payload["warnings"][2]

    reset_settings_cache()
