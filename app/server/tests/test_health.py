from fastapi.testclient import TestClient


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["app_name"] == "11Writer Forte"
    assert payload["database_backend"] == "sqlite"
    assert payload["database_connected"] is True
    assert payload["spatial_backend"] == "python"
    assert payload["postgis_ready"] is True
    assert payload["warning_count"] == 0
