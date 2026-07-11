from fastapi.testclient import TestClient
from typer.testing import CliRunner

from src.cli import app as cli_app


def test_create_geofence_writes_custody_log(client: TestClient) -> None:
    response = client.post(
        "/api/geofences",
        json={
            "name": "Harbor Watch",
            "description": "Polygon around the harbor.",
            "geometry_geojson": {
                "type": "Polygon",
                "coordinates": [
                    [[-96.0, 29.0], [-94.0, 29.0], [-94.0, 31.0], [-96.0, 31.0], [-96.0, 29.0]]
                ],
            },
            "rule_expression": "observation enters polygon",
        },
    )
    assert response.status_code == 200
    payload = response.json()

    custody = client.get(
        "/api/custody/logs",
        params={
            "object_type": "geofence",
            "object_id": str(payload["geofence_id"]),
            "action": "geofence_created",
            "actor": "api_geofence",
            "limit": 5,
        },
    )
    assert custody.status_code == 200
    rows = custody.json()
    assert len(rows) == 1
    assert rows[0]["details_json"]["name"] == "Harbor Watch"
    assert (
        rows[0]["details_json"]["geometry_wkt"]
        == "POLYGON ((-96 29, -94 29, -94 31, -96 31, -96 29))"
    )


def test_real_typer_add_and_list_geofences(client: TestClient) -> None:
    runner = CliRunner()
    create = runner.invoke(
        cli_app,
        [
            "add-geofence",
            "Delta Watch",
            '{"type":"Polygon","coordinates":[[[-91.0,29.0],[-90.0,29.0],[-90.0,30.0],[-91.0,30.0],[-91.0,29.0]]]}',
            "--description",
            "CLI managed geofence",
            "--rule-expression",
            "delta coverage",
            "--no-enabled",
        ],
    )
    assert create.exit_code == 0, create.output
    assert "Delta Watch" in create.output

    listed = runner.invoke(cli_app, ["list-geofences"])
    assert listed.exit_code == 0, listed.output
    assert "Delta Watch" in listed.output
    assert "enabled=False" in listed.output

    api_rows = client.get("/api/geofences")
    assert api_rows.status_code == 200
    assert any(
        row["name"] == "Delta Watch"
        and row["enabled"] is False
        and row["rule_expression"] == "delta coverage"
        for row in api_rows.json()
    )
