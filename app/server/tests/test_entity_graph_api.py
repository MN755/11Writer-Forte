from pathlib import Path

from fastapi.testclient import TestClient

from src.db import get_session_factory
from src.models import EntityORM


def test_cited_relationship_network_and_redaction(client: TestClient, tmp_path: Path) -> None:
    session = get_session_factory()()
    try:
        subject = EntityORM(slug="entity-subject", entity_type="person", canonical_name="Alex Rivera")
        organization = EntityORM(slug="entity-org", entity_type="organization", canonical_name="Public Org")
        session.add_all([subject, organization])
        session.commit()
        session.refresh(subject)
        session.refresh(organization)
    finally:
        session.close()

    artifact = client.post("/api/storage/objects", json={
        "object_key": "graph-fixture", "object_kind": "public_record", "owner_type": "fixture",
        "owner_id": "1", "object_uri": str(tmp_path / "record.html"), "source_uri": "https://court.example/record",
        "content_hash": "a" * 64,
    })
    assert artifact.status_code == 200
    citation = client.post("/api/entities/citations", json={
        "storage_object_id": artifact.json()["storage_object_id"], "source_uri": "https://court.example/record",
        "quote_text": "Alex Rivera served as a public officer of Public Org.", "source_reliability": "official",
        "jurisdiction": "US-MN",
    })
    assert citation.status_code == 200
    citation_id = citation.json()["entity_citation_id"]
    relationship_type = client.post("/api/entities/relationship-types", json={
        "relation_type": "public_officer_of", "display_name": "Public officer of",
        "allowed_subject_types_json": ["person"], "allowed_object_types_json": ["organization"],
    })
    assert relationship_type.status_code == 200
    created = client.post("/api/entities/relationships", json={
        "subject_entity_id": subject.entity_id, "object_entity_id": organization.entity_id,
        "relation_type": "public_officer_of", "jurisdiction": "US-MN", "confidence_score": 0.8,
        "citation_ids": [citation_id],
    })
    assert created.status_code == 200

    profile = client.get(f"/api/entities/{subject.entity_id}/profile")
    assert profile.status_code == 200
    assert profile.json()["evidence_count"] == 1
    assert profile.json()["relationships"][0]["citations"][0]["source_uri"] == "https://court.example/record"
    network = client.get(f"/api/entities/{subject.entity_id}/network?jurisdiction=US-MN")
    assert network.status_code == 200
    assert network.json()["status"] == "ok"
    assert network.json()["evidence_count"] == 1
    assert len(network.json()["entities"]) == 2
    snapshot = client.get("/api/operations/runtime/export")
    assert snapshot.status_code == 200
    assert len(snapshot.json()["entity_citations"]) == 1
    assert len(snapshot.json()["entity_relationship_assertions"]) == 1
    assert len(snapshot.json()["entity_relationship_assertion_citation_links"]) == 1
    restored = client.post("/api/operations/runtime/restore?replace_existing=true", json=snapshot.json())
    assert restored.status_code == 200
    empty = client.get(f"/api/entities/{subject.entity_id}/network?jurisdiction=CA-ON")
    assert empty.status_code == 200
    assert empty.json()["status"] == "insufficient_evidence"
