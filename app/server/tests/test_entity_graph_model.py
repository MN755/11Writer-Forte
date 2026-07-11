from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from src.models import (
    Base,
    EntityAliasCitationLinkORM,
    EntityAliasORM,
    EntityCandidateCitationLinkORM,
    EntityCandidateORM,
    EntityCitationORM,
    EntityIdentityAssertionCitationLinkORM,
    EntityIdentityAssertionORM,
    EntityORM,
    EntityRelationshipAssertionCitationLinkORM,
    EntityRelationshipAssertionORM,
    EntityRelationshipTypeORM,
    StorageObjectORM,
)
from src.schemas import (
    EntityCitationCreate,
    EntityRelationshipAssertionCreate,
    EntityRelationshipAssertionEvidenceRead,
    EntityRelationshipTypeCreate,
)


def test_graph_contract_rejects_bare_edges_and_invalid_vocabulary() -> None:
    with pytest.raises(ValidationError, match="citation_ids"):
        EntityRelationshipAssertionCreate(
            subject_entity_id=1,
            object_entity_id=2,
            relation_type="member_of",
        )

    with pytest.raises(ValidationError, match="storage_object_id or observation_id"):
        EntityCitationCreate(source_uri="https://public.example/record")

    with pytest.raises(ValidationError):
        EntityRelationshipTypeCreate(
            relation_type="member_of",
            display_name="Member of",
            allowed_subject_types_json=["person"],
            allowed_object_types_json=["unrecognized_type"],
        )


def test_graph_records_keep_conflicts_and_evidence_links() -> None:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        artifact = StorageObjectORM(
            object_key="public-record-a",
            object_kind="public_record",
            owner_type="entity_graph",
            owner_id="fixture",
            object_uri="file:///public-record-a.json",
        )
        alpha = EntityORM(
            slug="alpha",
            entity_type="person",
            canonical_name="Alpha Example",
        )
        bravo = EntityORM(
            slug="bravo-org",
            entity_type="organization",
            canonical_name="Bravo Organization",
        )
        relation_type = EntityRelationshipTypeORM(
            relation_type="member_of",
            relation_version=1,
            display_name="Member of",
            allowed_subject_types_json=["person"],
            allowed_object_types_json=["organization"],
        )
        session.add_all([artifact, alpha, bravo, relation_type])
        session.flush()

        citation = EntityCitationORM(
            storage_object_id=artifact.storage_object_id,
            source_uri="https://public.example/record",
            quote_text="Alpha was listed as a member of Bravo.",
            locator="p. 4",
        )
        candidate = EntityCandidateORM(
            entity_type="person",
            display_name="Alpha Example",
            normalized_name="alpha example",
            jurisdiction="US-MN",
        )
        accepted_alias = EntityAliasORM(
            entity_id=alpha.entity_id,
            entity_type="person",
            alias_text="A. Example",
            normalized_alias="a example",
            review_state="accepted",
        )
        disputed_alias = EntityAliasORM(
            entity_id=alpha.entity_id,
            entity_type="person",
            alias_text="A. Other",
            normalized_alias="a other",
            review_state="reviewed",
            contradiction_state="contradicted",
        )
        session.add_all([citation, candidate, accepted_alias, disputed_alias])
        session.flush()

        identity = EntityIdentityAssertionORM(
            entity_candidate_id=candidate.entity_candidate_id,
            entity_id=alpha.entity_id,
            assertion_kind="same_as",
            confidence_score=0.8,
        )
        relationship = EntityRelationshipAssertionORM(
            subject_entity_id=alpha.entity_id,
            object_entity_id=bravo.entity_id,
            relation_type="member_of",
            relation_version=1,
            jurisdiction="US-MN",
            valid_from=None,
            observed_from=None,
            confidence_score=0.75,
            review_state="reviewed",
        )
        session.add_all([identity, relationship])
        session.flush()
        session.add_all(
            [
                EntityCandidateCitationLinkORM(
                    entity_candidate_id=candidate.entity_candidate_id,
                    entity_citation_id=citation.entity_citation_id,
                ),
                EntityAliasCitationLinkORM(
                    entity_alias_id=accepted_alias.entity_alias_id,
                    entity_citation_id=citation.entity_citation_id,
                ),
                EntityAliasCitationLinkORM(
                    entity_alias_id=disputed_alias.entity_alias_id,
                    entity_citation_id=citation.entity_citation_id,
                    evidence_role="contradicting",
                ),
                EntityIdentityAssertionCitationLinkORM(
                    entity_identity_assertion_id=identity.entity_identity_assertion_id,
                    entity_citation_id=citation.entity_citation_id,
                ),
                EntityRelationshipAssertionCitationLinkORM(
                    entity_relationship_assertion_id=relationship.entity_relationship_assertion_id,
                    entity_citation_id=citation.entity_citation_id,
                ),
            ]
        )
        session.commit()

        aliases = list(session.scalars(select(EntityAliasORM).order_by(EntityAliasORM.entity_alias_id)))
        assert [alias.contradiction_state for alias in aliases] == ["none", "contradicted"]

        persisted_relationship = session.scalar(select(EntityRelationshipAssertionORM))
        assert persisted_relationship is not None
        assert persisted_relationship.relation_version == 1
        assert len(persisted_relationship.citation_links) == 1
        assert persisted_relationship.citation_links[0].citation.storage_object_id == artifact.storage_object_id

        evidence_view = EntityRelationshipAssertionEvidenceRead(
            entity_relationship_assertion_id=persisted_relationship.entity_relationship_assertion_id,
            subject_entity_id=alpha.entity_id,
            object_entity_id=bravo.entity_id,
            relation_type=persisted_relationship.relation_type,
            relation_version=persisted_relationship.relation_version,
            assertion_kind=persisted_relationship.assertion_kind,
            status=persisted_relationship.status,
            jurisdiction=persisted_relationship.jurisdiction,
            confidence_score=persisted_relationship.confidence_score,
            review_state=persisted_relationship.review_state,
            contradiction_state=persisted_relationship.contradiction_state,
            statement=persisted_relationship.statement,
            metadata_json=persisted_relationship.metadata_json,
            valid_from=persisted_relationship.valid_from,
            valid_to=persisted_relationship.valid_to,
            observed_from=persisted_relationship.observed_from,
            observed_to=persisted_relationship.observed_to,
            created_at=persisted_relationship.created_at,
            updated_at=persisted_relationship.updated_at,
            citation_ids=[citation.entity_citation_id],
            citations=[
                {
                    "entity_citation_id": citation.entity_citation_id,
                    "storage_object_id": artifact.storage_object_id,
                    "observation_id": None,
                    "source_uri": citation.source_uri,
                    "source_title": None,
                    "source_published_at": None,
                    "retrieved_at": citation.retrieved_at,
                    "content_hash": None,
                    "locator": citation.locator,
                    "quote_text": citation.quote_text,
                    "span_start": None,
                    "span_end": None,
                    "jurisdiction": None,
                    "source_reliability": citation.source_reliability,
                    "metadata_json": {},
                    "created_at": citation.created_at,
                    "updated_at": citation.updated_at,
                }
            ],
        )
        assert evidence_view.citations[0].quote_text == "Alpha was listed as a member of Bravo."
