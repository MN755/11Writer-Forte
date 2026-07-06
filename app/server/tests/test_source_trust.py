from fastapi.testclient import TestClient


def test_seed_default_integrity_sources(client: TestClient) -> None:
    seed_response = client.post("/api/source-trust/seed-defaults")
    assert seed_response.status_code == 200
    assert seed_response.json()["created"] >= 4

    list_response = client.get("/api/source-trust/profiles")
    assert list_response.status_code == 200
    domains = [item["domain"] for item in list_response.json()]
    assert "nytimes.com" in domains
    assert "npr.org" in domains

