from fastapi.testclient import TestClient

from src.services.trust_service import normalize_domain


def test_seed_default_integrity_sources(client: TestClient) -> None:
    seed_response = client.post("/api/source-trust/seed-defaults")
    assert seed_response.status_code == 200
    assert seed_response.json()["created"] >= 4

    list_response = client.get("/api/source-trust/profiles")
    assert list_response.status_code == 200
    domains = [item["domain"] for item in list_response.json()]
    assert "nytimes.com" in domains
    assert "npr.org" in domains


def test_normalize_domain_strips_ports_and_credentials() -> None:
    assert normalize_domain("https://user:pass@127.0.0.1:54837/feed.xml") == "127.0.0.1"
    assert normalize_domain("example.com:8443") == "example.com"
