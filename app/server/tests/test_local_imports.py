from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_local_json_import_applies_trust_profile(client: TestClient, tmp_path: Path) -> None:
    client.post(
        "/api/source-trust/profiles",
        json={
            "domain": "example.com",
            "trust_level": "trusted",
            "approval_policy": "auto_approve_stable",
            "integrity_source": True,
            "notes": "fixture",
        },
    )

    fixture = tmp_path / "input.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Port departure",
                    "url": "https://example.com/port/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )

    response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "marine-track"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["records_imported"] == 1
    assert payload["records_skipped"] == 0
    assert payload["observations"][0]["trust_level"] == "trusted"
    assert payload["observations"][0]["approval_policy"] == "auto_approve_stable"
    assert payload["observations"][0]["location_geojson"]["type"] == "Point"
    storage_response = client.get(
        "/api/storage/objects",
        params={"owner_type": "local_import_run", "owner_id": str(payload["import_run_id"])},
    )
    assert storage_response.status_code == 200
    storage_objects = storage_response.json()
    assert len(storage_objects) == 1
    assert storage_objects[0]["object_kind"] == "local_import_source"
    assert storage_objects[0]["retention_class"] == "investigative"
    assert storage_objects[0]["storage_tier"] == "warm"
    assert storage_objects[0]["source_uri"] == str(fixture)

    layers_response = client.get("/api/layers")
    assert layers_response.status_code == 200
    layers = layers_response.json()
    assert layers[0]["key"] == "marine-track"
    assert layers[0]["metadata_json"]["auto_created"] is True


def test_local_import_skips_duplicate_observations(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "duplicate.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Duplicate record",
                    "url": "https://example.com/dup/1",
                    "lat": 29.76,
                    "lon": -95.36,
                },
                {
                    "title": "Duplicate record",
                    "url": "https://example.com/dup/1",
                    "lat": 29.76,
                    "lon": -95.36,
                },
            ]
        ),
        encoding="utf-8",
    )

    first_response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "marine-track"},
    )
    assert first_response.status_code == 200
    first_payload = first_response.json()
    assert first_payload["records_seen"] == 2
    assert first_payload["records_imported"] == 1
    assert first_payload["records_skipped"] == 1
    assert len(first_payload["observations"]) == 1

    second_response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "marine-track"},
    )
    assert second_response.status_code == 200
    second_payload = second_response.json()
    assert second_payload["records_seen"] == 2
    assert second_payload["records_imported"] == 0
    assert second_payload["records_skipped"] == 2
    assert len(second_payload["observations"]) == 0

    observations_response = client.get("/api/observations")
    assert observations_response.status_code == 200
    observations = observations_response.json()
    assert len(observations) == 1

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    import_runs = imports_response.json()
    assert import_runs[0]["records_skipped"] == 2
    assert import_runs[1]["records_skipped"] == 1

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "local_import_run"
        and row["details_json"]["records_skipped"] == 2
        for row in custody_response.json()
    )
    assert sum(
        1
        for row in custody_response.json()
        if row["object_type"] == "storage_object"
        and row["action"] == "storage_registered"
    ) >= 2


def test_local_text_import_ingests_line_records(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "feed.txt"
    fixture.write_text(
        "\n".join(
            [
                "https://example.com/alpha",
                "",
                "https://example.com/bravo",
            ]
        ),
        encoding="utf-8",
    )

    response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "news-track"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["records_seen"] == 2
    assert payload["records_imported"] == 2
    assert payload["records_skipped"] == 0
    assert [row["record_format"] for row in payload["observations"]] == ["txt", "txt"]
    assert {row["source_domain"] for row in payload["observations"]} == {"example.com"}


@pytest.mark.parametrize(
    ("suffix", "language", "body_text"),
    [
        (".c", "c", "#include <stdio.h>\nint main(void) { return 0; }\n"),
        (".cpp", "cpp", "#include <iostream>\nint main() { std::cout << \"hi\"; }\n"),
        (".go", "go", "package main\nfunc main() { println(\"harbor\") }\n"),
        (".java", "java", "class HarborEvent { public static void main(String[] args) {} }\n"),
        (".php", "php", "<?php echo 'watch';\n"),
        (".r", "r", "harbor_events <- data.frame(name='alpha')\nprint(harbor_events)\n"),
    ],
)
def test_local_source_code_import_supports_requested_file_types(
    client: TestClient,
    tmp_path: Path,
    suffix: str,
    language: str,
    body_text: str,
) -> None:
    fixture = tmp_path / f"sample{suffix}"
    fixture.write_text(body_text, encoding="utf-8")

    response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": f"code-{language}"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["source_format"] == "source_code"
    assert payload["records_seen"] == 1
    assert payload["records_imported"] == 1
    assert payload["records_skipped"] == 0
    assert len(payload["observations"]) == 1

    observation = payload["observations"][0]
    assert observation["record_format"] == "source_code"
    assert observation["content_json"]["entry_kind"] == "source_code_file"
    assert observation["content_json"]["file_name"] == fixture.name
    assert observation["content_json"]["file_extension"] == suffix
    assert observation["content_json"]["language"] == language
    assert observation["content_json"]["line_count"] >= 1
    assert observation["content_json"]["character_count"] == len(body_text)
    assert observation["content_text"] == body_text


def test_local_directory_import_ingests_mixed_supported_files_with_provenance(
    client: TestClient,
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "notes.txt").write_text("https://example.com/alpha\n", encoding="utf-8")
    (bundle / "collector.go").write_text(
        "package main\nfunc main() { println(\"harbor bundle\") }\n",
        encoding="utf-8",
    )
    (bundle / "events.json").write_text(
        json.dumps([{"title": "Bundle Event", "url": "https://example.com/bundle-event"}]),
        encoding="utf-8",
    )
    (bundle / "ignored.bin").write_bytes(b"\x00\x01\x02")

    response = client.post(
        "/api/imports/local",
        json={"source_path": str(bundle), "layer_key": "bundle-track"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["source_format"] == "directory"
    assert payload["records_seen"] == 3
    assert payload["records_imported"] == 3
    assert payload["records_skipped"] == 0

    observations = payload["observations"]
    assert len(observations) == 3
    assert {row["record_format"] for row in observations} == {"txt", "json", "source_code"}
    assert {
        Path(row["content_json"]["local_source_path"]).name
        for row in observations
    } == {"notes.txt", "collector.go", "events.json"}
    assert all("local_source_name" in row["content_json"] for row in observations)
    assert any(row["content_json"].get("language") == "go" for row in observations)
    assert any(row["content_json"].get("local_source_line_number") == 1 for row in observations)

    storage_response = client.get(
        "/api/storage/objects",
        params={"owner_type": "local_import_run", "owner_id": str(payload["import_run_id"])},
    )
    assert storage_response.status_code == 200
    storage_objects = storage_response.json()
    assert len(storage_objects) == 1
    assert storage_objects[0]["metadata_json"]["is_directory_import"] is True
    assert storage_objects[0]["source_uri"] == str(bundle)


def test_local_directory_import_returns_structured_409_when_no_supported_files_exist(
    client: TestClient,
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "empty-bundle"
    bundle.mkdir()
    (bundle / "ignored.bin").write_bytes(b"\x00\x01\x02")

    response = client.post(
        "/api/imports/local",
        json={"source_path": str(bundle), "layer_key": "bundle-track"},
    )
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["source_path"] == str(bundle)
    assert detail["layer_key"] == "bundle-track"
    assert detail["error_type"] == "ValueError"
    assert "did not contain any supported files" in detail["message"]


def test_local_import_route_returns_structured_404_for_missing_file(client: TestClient, tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"

    response = client.post(
        "/api/imports/local",
        json={"source_path": str(missing), "layer_key": "news-track"},
    )
    assert response.status_code == 404
    detail = response.json()["detail"]
    assert detail["source_path"] == str(missing)
    assert detail["layer_key"] == "news-track"
    assert detail["error_type"] == "FileNotFoundError"
    assert "does not exist" in detail["message"]


def test_local_import_route_returns_structured_409_for_malformed_json(
    client: TestClient,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "broken.json"
    fixture.write_text('{"items":[', encoding="utf-8")

    response = client.post(
        "/api/imports/local",
        json={"source_path": str(fixture), "layer_key": "news-track"},
    )
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["source_path"] == str(fixture)
    assert detail["layer_key"] == "news-track"
    assert detail["error_type"] == "ValueError"
    assert "Could not parse json input" in detail["message"]
