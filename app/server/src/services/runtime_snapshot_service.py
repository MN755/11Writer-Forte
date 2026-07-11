from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, TypeAdapter
from sqlalchemy import delete, func, insert, select, text
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import (
    AlertORM,
    CandidateHealthCheckORM,
    CandidatePromotionDecisionORM,
    CandidateSuppressionORM,
    CameraInventoryORM,
    CameraSourceInventoryORM,
    CustodyLogORM,
    DataLayerORM,
    DiscoveryArtifactORM,
    DiscoveryCampaignORM,
    DiscoveryDomainPolicyORM,
    DiscoveryFrontierEntryORM,
    DiscoveryGraphEdgeORM,
    DiscoveryRunORM,
    EntityObservationLinkORM,
    EntityORM,
    EntityCandidateORM,
    EntityAliasORM,
    EntityRelationshipTypeORM,
    EntityCitationORM,
    EntityIdentityAssertionORM,
    EntityRelationshipAssertionORM,
    EntityCandidateCitationLinkORM,
    EntityAliasCitationLinkORM,
    EntityIdentityAssertionCitationLinkORM,
    EntityRelationshipAssertionCitationLinkORM,
    EventObservationLinkORM,
    EventORM,
    GeofenceORM,
    InvestigationDiscoveryAttemptORM,
    InvestigationEvidencePromotionORM,
    InvestigationORM,
    InvestigationReportVersionORM,
    LocalImportRunORM,
    ObservationORM,
    RobotsObservationORM,
    ResearchProviderORM,
    ResearchProviderRunORM,
    ScheduledTaskORM,
    ScheduledTaskRunORM,
    SituationProductORM,
    StorageObjectORM,
    SourceDefinitionORM,
    SourceCandidateORM,
    SourceCandidateRevisionORM,
    SourceRunORM,
    SourceTrustProfileORM,
)
from src.schemas import (
    AlertRead,
    CandidateHealthCheckRead,
    CandidatePromotionDecisionRead,
    CandidateSuppressionRead,
    CameraInventoryRead,
    CameraSourceInventoryRead,
    CustodyLogRead,
    DataLayerRead,
    DatabaseTableCountRead,
    DiscoveryArtifactRead,
    DiscoveryCampaignRead,
    DiscoveryDomainPolicyRead,
    DiscoveryFrontierEntryRead,
    DiscoveryGraphEdgeRead,
    DiscoveryRunRead,
    EntityObservationLinkRead,
    EntityRead,
    EntityCandidateRead,
    EntityAliasRead,
    EntityRelationshipTypeRead,
    EntityCitationRead,
    EntityIdentityAssertionRead,
    EntityRelationshipAssertionRead,
    EntityCandidateCitationLinkRead,
    EntityAliasCitationLinkRead,
    EntityIdentityAssertionCitationLinkRead,
    EntityRelationshipAssertionCitationLinkRead,
    EventObservationLinkRead,
    EventRead,
    GeofenceRead,
    InvestigationDiscoveryAttemptRead,
    InvestigationEvidencePromotionRead,
    InvestigationRead,
    InvestigationReportVersionRead,
    LocalImportRunSummaryRead,
    ObservationRead,
    RobotsObservationRead,
    ResearchProviderRead,
    ResearchProviderRunRead,
    RuntimeRestoreResultRead,
    RuntimeSnapshotRead,
    ScheduledTaskRead,
    ScheduledTaskRunRead,
    SituationProductRead,
    StorageObjectRead,
    SourceDefinitionRead,
    SourceCandidateRead,
    SourceCandidateRevisionRead,
    SourceRunRead,
    SourceTrustProfileRead,
)
from src.services.database_diagnostics_service import collect_table_counts


def snapshot_now() -> datetime:
    return datetime.now(timezone.utc)


SNAPSHOT_SECTIONS: tuple[tuple[str, object, type[BaseModel], object], ...] = (
    ("data_layers", DataLayerORM, DataLayerRead, DataLayerORM.layer_id),
    (
        "source_trust_profiles",
        SourceTrustProfileORM,
        SourceTrustProfileRead,
        SourceTrustProfileORM.trust_profile_id,
    ),
    (
        "discovery_domain_policies",
        DiscoveryDomainPolicyORM,
        DiscoveryDomainPolicyRead,
        DiscoveryDomainPolicyORM.domain_policy_id,
    ),
    (
        "discovery_campaigns",
        DiscoveryCampaignORM,
        DiscoveryCampaignRead,
        DiscoveryCampaignORM.campaign_id,
    ),
    (
        "discovery_runs",
        DiscoveryRunORM,
        DiscoveryRunRead,
        DiscoveryRunORM.discovery_run_id,
    ),
    ("geofences", GeofenceORM, GeofenceRead, GeofenceORM.geofence_id),
    (
        "source_definitions",
        SourceDefinitionORM,
        SourceDefinitionRead,
        SourceDefinitionORM.source_id,
    ),
    (
        "research_providers",
        ResearchProviderORM,
        ResearchProviderRead,
        ResearchProviderORM.research_provider_id,
    ),
    (
        "source_candidates",
        SourceCandidateORM,
        SourceCandidateRead,
        SourceCandidateORM.candidate_id,
    ),
    (
        "discovery_frontier_entries",
        DiscoveryFrontierEntryORM,
        DiscoveryFrontierEntryRead,
        DiscoveryFrontierEntryORM.frontier_entry_id,
    ),
    (
        "source_candidate_revisions",
        SourceCandidateRevisionORM,
        SourceCandidateRevisionRead,
        SourceCandidateRevisionORM.candidate_revision_id,
    ),
    (
        "discovery_graph_edges",
        DiscoveryGraphEdgeORM,
        DiscoveryGraphEdgeRead,
        DiscoveryGraphEdgeORM.graph_edge_id,
    ),
    (
        "candidate_health_checks",
        CandidateHealthCheckORM,
        CandidateHealthCheckRead,
        CandidateHealthCheckORM.health_check_id,
    ),
    (
        "candidate_suppressions",
        CandidateSuppressionORM,
        CandidateSuppressionRead,
        CandidateSuppressionORM.suppression_id,
    ),
    (
        "candidate_promotion_decisions",
        CandidatePromotionDecisionORM,
        CandidatePromotionDecisionRead,
        CandidatePromotionDecisionORM.promotion_decision_id,
    ),
    (
        "robots_observations",
        RobotsObservationORM,
        RobotsObservationRead,
        RobotsObservationORM.robots_observation_id,
    ),
    (
        "local_import_runs",
        LocalImportRunORM,
        LocalImportRunSummaryRead,
        LocalImportRunORM.import_run_id,
    ),
    ("events", EventORM, EventRead, EventORM.event_id),
    ("entities", EntityORM, EntityRead, EntityORM.entity_id),
    ("entity_candidates", EntityCandidateORM, EntityCandidateRead, EntityCandidateORM.entity_candidate_id),
    ("entity_aliases", EntityAliasORM, EntityAliasRead, EntityAliasORM.entity_alias_id),
    ("entity_relationship_types", EntityRelationshipTypeORM, EntityRelationshipTypeRead, EntityRelationshipTypeORM.relationship_type_id),
    ("entity_citations", EntityCitationORM, EntityCitationRead, EntityCitationORM.entity_citation_id),
    ("entity_identity_assertions", EntityIdentityAssertionORM, EntityIdentityAssertionRead, EntityIdentityAssertionORM.entity_identity_assertion_id),
    ("entity_relationship_assertions", EntityRelationshipAssertionORM, EntityRelationshipAssertionRead, EntityRelationshipAssertionORM.entity_relationship_assertion_id),
    ("entity_candidate_citation_links", EntityCandidateCitationLinkORM, EntityCandidateCitationLinkRead, EntityCandidateCitationLinkORM.entity_candidate_citation_link_id),
    ("entity_alias_citation_links", EntityAliasCitationLinkORM, EntityAliasCitationLinkRead, EntityAliasCitationLinkORM.entity_alias_citation_link_id),
    ("entity_identity_assertion_citation_links", EntityIdentityAssertionCitationLinkORM, EntityIdentityAssertionCitationLinkRead, EntityIdentityAssertionCitationLinkORM.entity_identity_assertion_citation_link_id),
    ("entity_relationship_assertion_citation_links", EntityRelationshipAssertionCitationLinkORM, EntityRelationshipAssertionCitationLinkRead, EntityRelationshipAssertionCitationLinkORM.entity_relationship_assertion_citation_link_id),
    ("observations", ObservationORM, ObservationRead, ObservationORM.observation_id),
    (
        "camera_inventory",
        CameraInventoryORM,
        CameraInventoryRead,
        CameraInventoryORM.camera_inventory_id,
    ),
    (
        "camera_source_inventory",
        CameraSourceInventoryORM,
        CameraSourceInventoryRead,
        CameraSourceInventoryORM.camera_source_inventory_id,
    ),
    ("storage_objects", StorageObjectORM, StorageObjectRead, StorageObjectORM.storage_object_id),
    ("investigations", InvestigationORM, InvestigationRead, InvestigationORM.investigation_id),
    (
        "investigation_discovery_attempts",
        InvestigationDiscoveryAttemptORM,
        InvestigationDiscoveryAttemptRead,
        InvestigationDiscoveryAttemptORM.investigation_attempt_id,
    ),
    (
        "investigation_report_versions",
        InvestigationReportVersionORM,
        InvestigationReportVersionRead,
        InvestigationReportVersionORM.investigation_report_version_id,
    ),
    (
        "investigation_evidence_promotions",
        InvestigationEvidencePromotionORM,
        InvestigationEvidencePromotionRead,
        InvestigationEvidencePromotionORM.investigation_evidence_promotion_id,
    ),
    (
        "research_provider_runs",
        ResearchProviderRunORM,
        ResearchProviderRunRead,
        ResearchProviderRunORM.research_provider_run_id,
    ),
    (
        "discovery_artifacts",
        DiscoveryArtifactORM,
        DiscoveryArtifactRead,
        DiscoveryArtifactORM.discovery_artifact_id,
    ),
    (
        "event_observation_links",
        EventObservationLinkORM,
        EventObservationLinkRead,
        EventObservationLinkORM.event_observation_link_id,
    ),
    (
        "entity_observation_links",
        EntityObservationLinkORM,
        EntityObservationLinkRead,
        EntityObservationLinkORM.entity_observation_link_id,
    ),
    ("alerts", AlertORM, AlertRead, AlertORM.alert_id),
    ("scheduled_tasks", ScheduledTaskORM, ScheduledTaskRead, ScheduledTaskORM.task_id),
    (
        "scheduled_task_runs",
        ScheduledTaskRunORM,
        ScheduledTaskRunRead,
        ScheduledTaskRunORM.task_run_id,
    ),
    ("source_runs", SourceRunORM, SourceRunRead, SourceRunORM.source_run_id),
    (
        "situation_products",
        SituationProductORM,
        SituationProductRead,
        SituationProductORM.product_id,
    ),
    ("custody_logs", CustodyLogORM, CustodyLogRead, CustodyLogORM.custody_log_id),
)

RESTORE_ORDER: tuple[tuple[str, object], ...] = (
    ("data_layers", DataLayerORM),
    ("source_trust_profiles", SourceTrustProfileORM),
    ("discovery_domain_policies", DiscoveryDomainPolicyORM),
    ("geofences", GeofenceORM),
    ("source_definitions", SourceDefinitionORM),
    ("research_providers", ResearchProviderORM),
    ("discovery_campaigns", DiscoveryCampaignORM),
    ("discovery_runs", DiscoveryRunORM),
    ("source_candidates", SourceCandidateORM),
    ("discovery_frontier_entries", DiscoveryFrontierEntryORM),
    ("source_candidate_revisions", SourceCandidateRevisionORM),
    ("discovery_graph_edges", DiscoveryGraphEdgeORM),
    ("candidate_health_checks", CandidateHealthCheckORM),
    ("candidate_suppressions", CandidateSuppressionORM),
    ("candidate_promotion_decisions", CandidatePromotionDecisionORM),
    ("robots_observations", RobotsObservationORM),
    ("local_import_runs", LocalImportRunORM),
    ("events", EventORM),
    ("entities", EntityORM),
    ("entity_relationship_types", EntityRelationshipTypeORM),
    ("observations", ObservationORM),
    ("camera_inventory", CameraInventoryORM),
    ("camera_source_inventory", CameraSourceInventoryORM),
    ("storage_objects", StorageObjectORM),
    ("investigations", InvestigationORM),
    ("investigation_discovery_attempts", InvestigationDiscoveryAttemptORM),
    ("investigation_report_versions", InvestigationReportVersionORM),
    ("investigation_evidence_promotions", InvestigationEvidencePromotionORM),
    ("research_provider_runs", ResearchProviderRunORM),
    ("entity_citations", EntityCitationORM),
    ("entity_candidates", EntityCandidateORM),
    ("entity_aliases", EntityAliasORM),
    ("entity_identity_assertions", EntityIdentityAssertionORM),
    ("entity_relationship_assertions", EntityRelationshipAssertionORM),
    ("entity_candidate_citation_links", EntityCandidateCitationLinkORM),
    ("entity_alias_citation_links", EntityAliasCitationLinkORM),
    ("entity_identity_assertion_citation_links", EntityIdentityAssertionCitationLinkORM),
    ("entity_relationship_assertion_citation_links", EntityRelationshipAssertionCitationLinkORM),
    ("discovery_artifacts", DiscoveryArtifactORM),
    ("event_observation_links", EventObservationLinkORM),
    ("entity_observation_links", EntityObservationLinkORM),
    ("alerts", AlertORM),
    ("scheduled_tasks", ScheduledTaskORM),
    ("source_runs", SourceRunORM),
    ("scheduled_task_runs", ScheduledTaskRunORM),
    ("situation_products", SituationProductORM),
    ("custody_logs", CustodyLogORM),
)


def build_runtime_snapshot(session: Session) -> dict[str, object]:
    settings = get_settings()
    row_counts_before = collect_table_counts(session)
    export_log = log_runtime_snapshot_export(session, row_counts=row_counts_before)
    try:
        snapshot = {
            "snapshot_version": 4,
            "exported_at": snapshot_now(),
            "app_name": settings.app_name,
            "app_version": settings.app_version,
            "database_backend": session.get_bind().dialect.name,
            "spatial_backend": settings.spatial_backend,
            "row_counts": collect_table_counts(session),
        }
        snapshot.update(serialize_snapshot_sections(session))
        if not any(
            log["custody_log_id"] == export_log.custody_log_id for log in snapshot["custody_logs"]
        ):
            snapshot["custody_logs"].append(serialize_row(export_log, CustodyLogRead))
        TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot)
        session.commit()
        return snapshot
    except Exception:
        session.rollback()
        raise


def restore_runtime_snapshot(
    session: Session,
    snapshot_payload: dict[str, object],
    *,
    replace_existing: bool = False,
) -> dict[str, object]:
    snapshot = TypeAdapter(RuntimeSnapshotRead).validate_python(snapshot_payload)
    if runtime_has_records(session):
        if not replace_existing:
            raise ValueError(
                "Runtime restore requires an empty database unless replace_existing is enabled."
            )
        clear_runtime_tables(session)

    for section_name, model in RESTORE_ORDER:
        rows = [row.model_dump(mode="python") for row in getattr(snapshot, section_name)]
        if not rows:
            continue
        session.execute(insert(model), rows)

    restored_at = snapshot_now()
    session.flush()
    reseed_postgresql_sequences(session)
    session.add(
        CustodyLogORM(
            object_type="runtime_snapshot",
            object_id=restored_at.isoformat(),
            action="runtime_restored",
            actor="runtime_restore",
            details_json={
                "snapshot_exported_at": snapshot.exported_at.isoformat(),
                "replaced_existing": replace_existing,
                "restored_total_records": sum(
                    len(getattr(snapshot, section_name)) for section_name, _ in RESTORE_ORDER
                ),
            },
        )
    )
    session.commit()

    row_counts = collect_table_counts(session)
    return (
        TypeAdapter(RuntimeRestoreResultRead)
        .validate_python(
            {
                "restored_at": restored_at,
                "database_backend": session.get_bind().dialect.name,
                "replaced_existing": replace_existing,
                "total_records": sum(item["row_count"] for item in row_counts),
                "row_counts": row_counts,
            }
        )
        .model_dump(mode="python")
    )


def serialize_snapshot_sections(session: Session) -> dict[str, list[dict[str, object]]]:
    payload: dict[str, list[dict[str, object]]] = {}
    for section_name, model, schema_cls, order_column in SNAPSHOT_SECTIONS:
        rows = list(session.scalars(select(model).order_by(order_column.asc())))
        payload[section_name] = [serialize_row(row, schema_cls) for row in rows]
    return payload


def serialize_row(row: object, schema_cls: type[BaseModel]) -> dict[str, object]:
    return schema_cls.model_validate(row).model_dump(mode="python")


def runtime_has_records(session: Session) -> bool:
    return any(
        int(session.scalar(select(func.count()).select_from(model)) or 0) > 0
        for _, model in RESTORE_ORDER
    )


def clear_runtime_tables(session: Session) -> None:
    for _, model in reversed(RESTORE_ORDER):
        session.execute(delete(model))
    session.flush()


def reseed_postgresql_sequences(session: Session) -> None:
    if session.get_bind().dialect.name != "postgresql":
        return
    for _, model in RESTORE_ORDER:
        table = model.__table__
        primary_key_columns = list(table.primary_key.columns)
        if len(primary_key_columns) != 1:
            continue
        primary_key = primary_key_columns[0]
        maximum = session.scalar(select(func.max(primary_key)))
        if maximum is None:
            continue
        sequence_name = session.scalar(
            text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
            {
                "table_name": table.fullname,
                "column_name": primary_key.name,
            },
        )
        if not sequence_name:
            continue
        session.execute(
            text("SELECT setval(CAST(:sequence_name AS regclass), :maximum, true)"),
            {
                "sequence_name": sequence_name,
                "maximum": int(maximum),
            },
        )


def log_runtime_snapshot_export(
    session: Session,
    *,
    row_counts: list[dict[str, object]],
) -> CustodyLogORM:
    normalized_row_counts = TypeAdapter(list[DatabaseTableCountRead]).validate_python(row_counts)
    record = CustodyLogORM(
        object_type="runtime_snapshot",
        object_id=snapshot_now().isoformat(),
        action="runtime_exported",
        actor="runtime_export",
        details_json={
            "row_counts": [row.model_dump(mode="json") for row in normalized_row_counts],
        },
    )
    session.add(record)
    session.flush()
    return record
