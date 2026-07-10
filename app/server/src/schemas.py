from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


TrustLevel = Literal["trusted", "neutral", "blocked"]
ApprovalPolicy = Literal["auto_approve_stable", "manual_review", "always_review", "auto_reject"]
StorageTier = Literal["hot", "warm", "archive", "permanent"]
RetentionClass = Literal["ephemeral", "operational", "investigative", "permanent"]
StorageLifecycleStatus = Literal["active", "promoted", "degraded", "archived", "expired", "quarantined"]
StorageTransferStatus = Literal[
    "ready",
    "archiving",
    "archived",
    "verified",
    "verification_failed",
    "rehydration_requested",
    "rehydrating",
    "rehydrated",
    "quarantined",
    "pruned",
    "failed",
]
CameraSourceStatus = Literal["candidate", "review", "ready", "graduated", "ignored", "retired"]
CameraSourceVerificationState = Literal["unknown", "observed", "reachable", "failed"]
SourceKind = Literal[
    "local_file",
    "sqlite_file",
    "http_json",
    "http_jsonl",
    "http_text",
    "http_xml",
    "rss",
    "web_search",
    "web_crawl",
    "web_discovery",
    "webhook_ingest",
    "sse_stream",
    "websocket_stream",
]


class ForteModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class HealthResponse(ForteModel):
    status: str
    app_name: str
    app_version: str
    database_url: str
    database_backend: str
    database_connected: bool
    spatial_backend: str
    postgis_ready: bool
    warning_count: int


class ReadinessProbeResponse(ForteModel):
    status: str
    ready: bool
    app_name: str
    app_version: str
    overall_status: str
    action_required_count: int
    warning_count: int
    check_count: int
    operator_actions: list[str]


class DatabaseColumnAuditRead(ForteModel):
    table_name: str
    column_name: str
    present: bool


class DatabaseIndexAuditRead(ForteModel):
    index_name: str
    present: bool


class DatabaseMigrationStatusRead(ForteModel):
    current_revision: str | None
    head_revision: str
    version_table_present: bool
    has_application_tables: bool
    schema_up_to_date: bool


class DatabaseTableCountRead(ForteModel):
    table_name: str
    row_count: int


class DatabaseDiagnosticsRead(ForteModel):
    status: str
    database_backend: str
    database_url: str
    database_connected: bool
    spatial_backend: str
    scheduler_poll_seconds: float
    postgis_expected: bool
    postgis_extension_installed: bool | None
    postgis_version: str | None
    warning_count: int
    warnings: list[str]
    notes: list[str]
    required_columns: list[DatabaseColumnAuditRead]
    migration: DatabaseMigrationStatusRead
    spatial_indexes: list[DatabaseIndexAuditRead]
    table_counts: list[DatabaseTableCountRead]


class RuntimeReadinessCheckRead(ForteModel):
    key: str
    status: str
    summary: str
    details_json: dict[str, Any]


class RuntimeReadinessRead(ForteModel):
    generated_at: datetime
    overall_status: str
    ready: bool
    check_count: int
    action_required_count: int
    warning_count: int
    operator_actions: list[str]
    checks: list[RuntimeReadinessCheckRead]


class ClickHouseDiagnosticsRead(ForteModel):
    status: str
    enabled: bool
    clickhouse_url: str
    clickhouse_database: str
    observation_table: str
    storage_object_table: str
    reachable: bool
    version: str | None
    current_database: str | None
    storage_policy: str | None
    storage_mode: str
    r2_configured: bool
    r2_endpoint: str | None
    r2_bucket: str | None
    r2_region: str | None
    r2_archive_root: str | None
    r2_storage_ready: bool
    r2_storage_bucket: str | None
    r2_storage_root: str | None
    warnings: list[str]
    notes: list[str]


class ClickHouseProvisionResultRead(ForteModel):
    provisioned_at: datetime
    clickhouse_database: str
    observation_table: str
    storage_object_table: str
    storage_policy: str | None
    storage_mode: str


class ClickHouseSyncResultRead(ForteModel):
    synced_at: datetime
    clickhouse_database: str
    observation_table: str
    storage_object_table: str
    filters_json: dict[str, Any]
    observation_count: int
    storage_object_count: int


class ClickHouseArchiveResultRead(ForteModel):
    archived_at: datetime
    clickhouse_database: str
    observation_table: str
    archive_root_url: str
    partition_strategy: str
    filters_json: dict[str, Any]
    exported_row_count: int
    sql: str


class ClickHouseR2ConfigRead(ForteModel):
    generated_at: datetime
    storage_mode: str
    archive_root_url: str
    storage_root_url: str
    storage_policy: str | None
    storage_xml: str
    create_table_sql: str
    archive_example_sql: str
    rehydrate_example_sql: str
    direct_query_example_sql: str
    docker_output_path: str


class ClickHouseRehydrateResultRead(ForteModel):
    rehydrated_at: datetime
    clickhouse_database: str
    observation_table: str
    archive_glob_url: str
    imported_row_count: int
    sql: str


class RuntimeSnapshotRead(ForteModel):
    exported_at: datetime
    app_name: str
    app_version: str
    database_backend: str
    spatial_backend: str
    database_revision: str | None = None
    database_head_revision: str | None = None
    row_counts: list[DatabaseTableCountRead]
    data_layers: list["DataLayerRead"]
    source_trust_profiles: list["SourceTrustProfileRead"]
    geofences: list["GeofenceRead"]
    source_definitions: list["SourceDefinitionRead"]
    source_checkpoints: list["SourceCheckpointRead"]
    local_import_runs: list["LocalImportRunSummaryRead"]
    events: list["EventRead"]
    entities: list["EntityRead"]
    observations: list["ObservationRead"]
    camera_inventory: list["CameraInventoryRead"]
    camera_source_inventory: list["CameraSourceInventoryRead"]
    storage_objects: list["StorageObjectRead"]
    event_observation_links: list["EventObservationLinkRead"]
    entity_observation_links: list["EntityObservationLinkRead"]
    alerts: list["AlertRead"]
    scheduled_tasks: list["ScheduledTaskRead"]
    scheduled_task_runs: list["ScheduledTaskRunRead"]
    source_runs: list["SourceRunRead"]
    source_dead_letters: list["SourceDeadLetterRead"]
    worker_statuses: list["WorkerStatusRead"] = Field(default_factory=list)
    situation_products: list["SituationProductRead"]
    custody_logs: list["CustodyLogRead"]


class RuntimeRestoreResultRead(ForteModel):
    restored_at: datetime
    database_backend: str
    replaced_existing: bool
    total_records: int
    row_counts: list[DatabaseTableCountRead]


class RuntimeSnapshotArtifactManifestRead(ForteModel):
    format_version: str = "runtime_snapshot_manifest.v1"
    generated_at: datetime
    snapshot_file_name: str
    snapshot_sha256: str
    snapshot_byte_size: int
    snapshot_storage_object_id: int | None = None
    snapshot_object_key: str | None = None
    exported_at: datetime
    app_name: str
    app_version: str
    database_backend: str
    spatial_backend: str
    database_revision: str | None = None
    database_head_revision: str | None = None
    section_counts: dict[str, int]
    row_counts: list[DatabaseTableCountRead]


class OperationsReportArtifactExportRequest(ForteModel):
    output_path: str
    hours: float | None = 24.0
    limit: int = Field(default=25, ge=1, le=200)


class ExportArtifactWriteResultRead(ForteModel):
    output_path: str
    storage_object: "StorageObjectRead"


class RuntimeSnapshotArtifactExportRequest(ForteModel):
    output_path: str
    manifest_path: str | None = None


class RuntimeSnapshotArtifactExportResultRead(ForteModel):
    output_path: str
    manifest_path: str
    snapshot_manifest: RuntimeSnapshotArtifactManifestRead
    snapshot_storage_object: "StorageObjectRead"
    manifest_storage_object: "StorageObjectRead"


class AlertSummaryArtifactExportRequest(ForteModel):
    output_path: str
    status: str | None = None
    geofence_id: int | None = None
    event_id: int | None = None
    severity: str | None = None
    stale_after_hours: float = 24.0
    alert_limit: int = Field(default=500, ge=1, le=5000)
    report_limit: int = Field(default=25, ge=1, le=200)
    stale_alert_limit: int = Field(default=25, ge=1, le=500)


class CameraSummaryArtifactExportRequest(ForteModel):
    output_path: str
    layer_key: str | None = None
    source_domain: str | None = None
    status: str | None = None
    active: bool | None = None
    min_lon: float | None = None
    min_lat: float | None = None
    max_lon: float | None = None
    max_lat: float | None = None
    stale_after_hours: float = 24.0
    camera_limit: int = Field(default=500, ge=1, le=5000)
    report_limit: int = Field(default=25, ge=1, le=200)
    stale_camera_limit: int = Field(default=25, ge=1, le=500)


class CameraSourceSummaryArtifactExportRequest(ForteModel):
    output_path: str
    layer_key: str | None = None
    source_domain: str | None = None
    endpoint_kind: str | None = None
    status: str | None = None
    verification_state: str | None = None
    active: bool | None = None
    stale_after_hours: float = 24.0
    source_limit: int = Field(default=500, ge=1, le=5000)
    report_limit: int = Field(default=25, ge=1, le=200)
    stale_source_limit: int = Field(default=25, ge=1, le=500)


class EventSummaryArtifactExportRequest(ForteModel):
    output_path: str
    status: str | None = None
    redaction_level: str | None = None
    stale_after_hours: float = 24.0
    event_limit: int = Field(default=500, ge=1, le=5000)
    report_limit: int = Field(default=25, ge=1, le=200)
    stale_event_limit: int = Field(default=25, ge=1, le=500)


class EntitySummaryArtifactExportRequest(ForteModel):
    output_path: str
    entity_type: str | None = None
    redaction_level: str | None = None
    min_confidence_score: float | None = None
    entity_limit: int = Field(default=500, ge=1, le=5000)
    report_limit: int = Field(default=25, ge=1, le=200)
    conflict_limit: int = Field(default=25, ge=1, le=500)


class SchedulerSummaryArtifactExportRequest(ForteModel):
    output_path: str
    task_limit: int = Field(default=500, ge=1, le=5000)
    report_limit: int = Field(default=25, ge=1, le=200)
    overdue_task_limit: int = Field(default=25, ge=1, le=500)


class SourceSummaryArtifactExportRequest(ForteModel):
    output_path: str
    stale_after_hours: float = 24.0
    source_limit: int = Field(default=500, ge=1, le=5000)
    report_limit: int = Field(default=25, ge=1, le=200)
    stale_source_limit: int = Field(default=25, ge=1, le=500)


class DataLayerCreate(ForteModel):
    key: str
    name: str
    description: str = ""
    temporal_resolution: str = "unknown"
    data_latency: str = "unknown"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class DataLayerRead(DataLayerCreate):
    layer_id: int
    created_at: datetime
    updated_at: datetime


class EventCreate(ForteModel):
    slug: str
    title: str
    summary: str = ""
    occurred_at: datetime | None = None
    status: str = "open"
    redaction_level: str = "public"
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class EventRead(EventCreate):
    event_id: int
    created_at: datetime
    updated_at: datetime


class EntityRead(ForteModel):
    entity_id: int
    slug: str
    entity_type: str
    canonical_name: str
    resolution_basis: str
    confidence_score: float
    redaction_level: str
    metadata_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class EntityObservationLinkRead(ForteModel):
    entity_observation_link_id: int
    entity_id: int
    observation_id: int
    match_basis: str
    confidence_contribution: float
    created_at: datetime


class CameraInventoryRead(ForteModel):
    camera_inventory_id: int
    camera_key: str
    observation_id: int | None
    external_id: str | None
    name: str
    source_domain: str | None
    layer_key: str
    provider: str
    road_name: str
    status: str
    active: bool
    image_url: str | None
    stream_url: str | None
    page_url: str | None
    location_geojson: dict[str, Any] | None
    confidence_score: float
    last_observed_at: datetime | None
    metadata_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class StorageObjectCreate(ForteModel):
    object_key: str
    object_kind: str
    owner_type: str
    owner_id: str
    object_uri: str
    content_hash: str | None = None
    media_type: str | None = None
    storage_tier: StorageTier = "hot"
    retention_class: RetentionClass = "operational"
    lifecycle_status: StorageLifecycleStatus = "active"
    source_uri: str | None = None
    byte_size: int | None = None
    observed_at: datetime | None = None
    expires_at: datetime | None = None
    degraded_from_storage_object_id: int | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class StorageObjectRead(StorageObjectCreate):
    storage_object_id: int
    promoted_by_type: str | None
    promoted_by_id: str | None
    created_at: datetime
    updated_at: datetime


class StorageReplicaRead(ForteModel):
    backend: str
    uri: str
    role: str
    status: str
    content_hash: str | None
    byte_size: int | None
    created_at: datetime | None = None
    archived_at: datetime | None = None
    verified_at: datetime | None = None
    rehydrated_at: datetime | None = None
    pruned_at: datetime | None = None
    last_error: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class StorageManifestRead(ForteModel):
    canonical_uri: str
    transfer_status: StorageTransferStatus | str
    failure_reason: str | None = None
    archive_eligible: bool
    prune_eligible: bool
    storage_managed: bool
    replicas: list[StorageReplicaRead]
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class StorageActionResultRead(ForteModel):
    action: str
    message: str
    verified: bool
    storage_object: StorageObjectRead
    manifest: StorageManifestRead


class StorageObjectPromoteRequest(ForteModel):
    storage_tier: StorageTier = "warm"
    retention_class: RetentionClass | None = None
    promoted_by_type: str
    promoted_by_id: str
    expires_at: datetime | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class StorageObjectTransitionRequest(ForteModel):
    lifecycle_status: StorageLifecycleStatus
    storage_tier: StorageTier | None = None
    expires_at: datetime | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class StorageArchiveRequest(ForteModel):
    prune_local: bool = False


class StorageRehydrateRequest(ForteModel):
    target_path: str | None = None
    replace_existing: bool = False


class StorageQuarantineRequest(ForteModel):
    reason: str


class StorageUnquarantineRequest(ForteModel):
    note: str | None = None


class StorageInventoryBucketRead(ForteModel):
    key: str
    total_count: int
    expired_count: int
    active_count: int


class StorageReportRead(ForteModel):
    generated_at: datetime
    total_count: int
    active_count: int
    expired_count: int
    promoted_count: int
    archived_count: int
    quarantined_count: int = 0
    archive_pending_count: int = 0
    verification_failure_count: int = 0
    rehydration_pending_count: int = 0
    next_expiration_at: datetime | None
    oldest_expired_at: datetime | None
    retention_class_counts: list[StorageInventoryBucketRead]
    storage_tier_counts: list[StorageInventoryBucketRead]
    lifecycle_status_counts: list[StorageInventoryBucketRead]
    transfer_status_counts: list[StorageInventoryBucketRead] = Field(default_factory=list)
    expiring_objects: list[StorageObjectRead]
    problem_objects: list[StorageObjectRead] = Field(default_factory=list)


class StorageLifecycleSweepResultRead(ForteModel):
    swept_at: datetime
    dry_run: bool
    filters_json: dict[str, Any]
    expired_candidate_count: int
    transitioned_count: int
    processed_count: int = 0
    failed_count: int = 0
    operation_results: list[dict[str, Any]] = Field(default_factory=list)
    candidates: list[StorageObjectRead]


class CameraMaterializationRequest(ForteModel):
    layer_key: str | None = None
    source_domain: str | None = None
    limit: int = Field(default=500, ge=1, le=5000)


class CameraMaterializationResponse(ForteModel):
    created_count: int
    updated_count: int
    scanned_count: int
    source_created_count: int = 0
    source_updated_count: int = 0
    source_scanned_endpoint_count: int = 0
    cameras: list[CameraInventoryRead]


class CameraInventorySummaryBucketRead(ForteModel):
    key: str
    total_count: int
    active_count: int
    inactive_count: int
    stale_count: int


class CameraInventorySummaryRead(ForteModel):
    generated_at: datetime
    stale_before: datetime
    total_count: int
    active_count: int
    inactive_count: int
    stale_count: int
    layer_counts: list[CameraInventorySummaryBucketRead]
    source_domain_counts: list[CameraInventorySummaryBucketRead]
    provider_counts: list[CameraInventorySummaryBucketRead]
    status_counts: list[CameraInventorySummaryBucketRead]


class CameraInventoryOpsDetailRead(ForteModel):
    camera: CameraInventoryRead
    latest_observation: ObservationRead | None
    latest_import_run: LocalImportRunSummaryRead | None
    custody_logs: list["CustodyLogRead"]
    refresh_tasks: list["ScheduledTaskRead"]


class CameraSourceInventoryRead(ForteModel):
    camera_source_inventory_id: int
    candidate_key: str
    camera_inventory_id: int | None
    observation_id: int | None
    external_id: str | None
    name: str
    source_domain: str | None
    layer_key: str
    provider: str
    endpoint_kind: str
    endpoint_url: str
    status: CameraSourceStatus
    verification_state: CameraSourceVerificationState
    active: bool
    last_observed_at: datetime | None
    last_checked_at: datetime | None
    confidence_score: float
    graduation_score: float
    metadata_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class CameraSourceMaterializationRequest(ForteModel):
    layer_key: str | None = None
    source_domain: str | None = None
    active: bool | None = None
    limit: int = Field(default=500, ge=1, le=5000)


class CameraSourceMaterializationResponse(ForteModel):
    created_count: int
    updated_count: int
    scanned_camera_count: int
    scanned_endpoint_count: int
    sources: list[CameraSourceInventoryRead]


class CameraSourceVerificationRequest(ForteModel):
    camera_source_inventory_id: int | None = None
    layer_key: str | None = None
    source_domain: str | None = None
    endpoint_kind: str | None = None
    status: str | None = None
    verification_state: str | None = None
    active: bool | None = None
    limit: int = Field(default=200, ge=1, le=5000)
    timeout_seconds: float = Field(default=5.0, gt=0.0, le=30.0)
    max_payload_bytes: int | None = Field(default=None, ge=1, le=20_000_000)
    allow_private_networks: bool | None = None
    min_request_interval_seconds: float | None = Field(default=None, ge=0.0, le=30.0)


class CameraSourceVerificationResponse(ForteModel):
    verified_count: int
    reachable_count: int
    failed_count: int
    sources: list[CameraSourceInventoryRead]


class CameraSourceSummaryBucketRead(ForteModel):
    key: str
    total_count: int
    active_count: int
    ready_count: int
    review_count: int


class CameraSourceSummaryRead(ForteModel):
    generated_at: datetime
    total_count: int
    active_count: int
    ready_count: int
    review_count: int
    candidate_count: int
    graduated_count: int
    source_domain_counts: list[CameraSourceSummaryBucketRead]
    endpoint_kind_counts: list[CameraSourceSummaryBucketRead]
    status_counts: list[CameraSourceSummaryBucketRead]
    verification_state_counts: list[CameraSourceSummaryBucketRead]


class CameraSourceOpsDetailRead(ForteModel):
    source: CameraSourceInventoryRead
    camera: CameraInventoryRead | None
    latest_observation: ObservationRead | None
    custody_logs: list["CustodyLogRead"]


class CameraSourceOpsReportIndexRead(ForteModel):
    generated_at: datetime
    stale_after_hours: float
    latest_materialization_at: datetime | None
    inventory_summary: CameraSourceSummaryRead
    refresh_task_count: int
    refresh_run_count: int
    refresh_failure_count: int
    refresh_tasks: list["ScheduledTaskRead"]
    recent_refresh_runs: list["CameraRefreshTaskRunRead"]
    verification_task_count: int
    verification_run_count: int
    verification_failure_count: int
    verification_tasks: list["ScheduledTaskRead"]
    recent_verification_runs: list["CameraRefreshTaskRunRead"]
    recent_materializations: list["CustodyLogRead"]
    recent_verifications: list["CustodyLogRead"]
    stale_sources: list[CameraSourceInventoryRead]


class CameraSourceOpsExportSummaryRead(ForteModel):
    generated_at: datetime
    filters_json: dict[str, Any]
    report_index: CameraSourceOpsReportIndexRead
    sources: list[CameraSourceInventoryRead]


class EventObservationLinkRead(ForteModel):
    event_observation_link_id: int
    event_id: int
    observation_id: int
    relationship_type: str
    confidence_contribution: float
    created_at: datetime


class SituationProductRead(ForteModel):
    product_id: int
    event_id: int
    product_type: str
    redaction_level: str
    title: str
    body_text: str
    citations_json: list[dict[str, Any]]
    generated_by: str
    created_at: datetime
    updated_at: datetime


class EventSummaryBucketRead(ForteModel):
    key: str
    total_count: int
    open_count: int
    closed_count: int
    stale_open_count: int


class EventInventorySummaryRead(ForteModel):
    generated_at: datetime
    stale_before: datetime
    total_count: int
    open_count: int
    closed_count: int
    stale_open_count: int
    alert_scoped_count: int
    product_covered_count: int
    status_counts: list[EventSummaryBucketRead]
    redaction_level_counts: list[EventSummaryBucketRead]
    confidence_band_counts: list[EventSummaryBucketRead]


class EventOpsReportIndexRead(ForteModel):
    generated_at: datetime
    stale_after_hours: float
    latest_event_at: datetime | None
    inventory_summary: EventInventorySummaryRead
    event_fusion_task_count: int
    event_fusion_run_count: int
    event_fusion_failure_count: int
    event_fusion_tasks: list["ScheduledTaskRead"]
    recent_fusion_runs: list["ScheduledTaskRunRead"]
    recent_events: list[EventRead]
    stale_open_events: list[EventRead]
    recent_products: list[SituationProductRead]
    recent_alerts: list[AlertRead]


class EventOpsExportSummaryRead(ForteModel):
    generated_at: datetime
    filters_json: dict[str, Any]
    report_index: EventOpsReportIndexRead
    events: list[EventRead]


class EntitySummaryBucketRead(ForteModel):
    key: str
    total_count: int
    high_confidence_count: int
    signal_conflict_count: int


class EntityInventorySummaryRead(ForteModel):
    generated_at: datetime
    total_count: int
    high_confidence_count: int
    medium_confidence_count: int
    low_confidence_count: int
    signal_conflict_count: int
    entity_type_counts: list[EntitySummaryBucketRead]
    redaction_level_counts: list[EntitySummaryBucketRead]
    confidence_band_counts: list[EntitySummaryBucketRead]
    evidence_strength_counts: list[EntitySummaryBucketRead]


class EntityOpsReportIndexRead(ForteModel):
    generated_at: datetime
    latest_entity_at: datetime | None
    inventory_summary: EntityInventorySummaryRead
    entity_resolution_task_count: int
    entity_resolution_run_count: int
    entity_resolution_failure_count: int
    entity_resolution_tasks: list["ScheduledTaskRead"]
    recent_resolution_runs: list["ScheduledTaskRunRead"]
    recent_entities: list[EntityRead]
    conflicting_entities: list[EntityRead]


class EntityOpsExportSummaryRead(ForteModel):
    generated_at: datetime
    filters_json: dict[str, Any]
    report_index: EntityOpsReportIndexRead
    entities: list[EntityRead]


class GeofenceCreate(ForteModel):
    name: str
    description: str = ""
    geometry_geojson: dict[str, Any]
    rule_expression: str = ""
    enabled: bool = True


class GeofenceRead(GeofenceCreate):
    geofence_id: int
    created_at: datetime
    updated_at: datetime


class AlertCreate(ForteModel):
    event_id: int | None = None
    geofence_id: int | None = None
    severity: str = "info"
    status: str = "open"
    dedupe_key: str | None = None
    message: str
    trigger_basis_json: dict[str, Any] = Field(default_factory=dict)


class AlertUpdate(ForteModel):
    status: Literal["open", "acknowledged", "closed"]
    severity: str | None = None
    disposition_note: str = ""


class AlertRead(AlertCreate):
    alert_id: int
    disposition_note: str
    created_at: datetime
    updated_at: datetime


class AlertSummaryBucketRead(ForteModel):
    key: str
    total_count: int
    open_count: int
    acknowledged_count: int
    closed_count: int
    stale_open_count: int


class AlertInventorySummaryRead(ForteModel):
    generated_at: datetime
    stale_before: datetime
    total_count: int
    open_count: int
    acknowledged_count: int
    closed_count: int
    stale_open_count: int
    geofence_scoped_count: int
    event_scoped_count: int
    unscoped_count: int
    severity_counts: list[AlertSummaryBucketRead]
    status_counts: list[AlertSummaryBucketRead]
    geofence_counts: list[AlertSummaryBucketRead]


class AlertOpsReportIndexRead(ForteModel):
    generated_at: datetime
    stale_after_hours: float
    latest_alert_at: datetime | None
    inventory_summary: AlertInventorySummaryRead
    geofence_scan_task_count: int
    geofence_scan_run_count: int
    geofence_scan_failure_count: int
    geofence_scan_tasks: list["ScheduledTaskRead"]
    observation_watch_task_count: int = 0
    observation_watch_run_count: int = 0
    observation_watch_failure_count: int = 0
    observation_watch_tasks: list["ScheduledTaskRead"] = Field(default_factory=list)
    recent_alerts: list[AlertRead]
    stale_open_alerts: list[AlertRead]
    unscoped_alerts: list[AlertRead]


class AlertOpsExportSummaryRead(ForteModel):
    generated_at: datetime
    filters_json: dict[str, Any]
    report_index: AlertOpsReportIndexRead
    alerts: list[AlertRead]


class SourceTrustProfileCreate(ForteModel):
    domain: str
    trust_level: TrustLevel = "neutral"
    approval_policy: ApprovalPolicy = "manual_review"
    integrity_source: bool = False
    notes: str = ""


class SourceTrustProfileRead(SourceTrustProfileCreate):
    trust_profile_id: int
    created_at: datetime
    updated_at: datetime


class SourceDefinitionCreate(ForteModel):
    name: str
    source_kind: SourceKind
    layer_key: str
    target_uri: str
    enabled: bool = True
    integrity_source: bool = False
    notes: str = ""
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class SourceDefinitionRead(SourceDefinitionCreate):
    source_id: int
    created_at: datetime
    updated_at: datetime


class SourceDefinitionUpdate(ForteModel):
    name: str | None = None
    source_kind: SourceKind | None = None
    layer_key: str | None = None
    target_uri: str | None = None
    enabled: bool | None = None
    integrity_source: bool | None = None
    notes: str | None = None
    metadata_json: dict[str, Any] | None = None


class WebSearchProviderRead(ForteModel):
    provider_name: str
    default_target_uri: str | None = None
    query_param: str
    redirect_query_param: str | None = None
    search_page_param: str | None = None
    search_page_start: int
    search_page_step: int
    skip_provider_domain: bool


class WebSourceRunRequest(ForteModel):
    name: str
    source_kind: Literal["web_search", "web_crawl", "web_discovery"]
    layer_key: str
    target_uri: str
    enabled: bool = True
    integrity_source: bool = False
    notes: str = ""
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    upsert_existing: bool = True


class WebSourceRunResponse(ForteModel):
    action: Literal["created", "updated"]
    source: "SourceDefinitionRead"
    source_run: "SourceRunRead"


class SourceCheckpointRead(ForteModel):
    source_checkpoint_id: int
    source_id: int
    adapter_kind: str
    fetch_mode: str
    status: str
    cursor_text: str | None
    last_event_id: str | None
    last_offset: int | None
    checkpoint_json: dict[str, Any]
    last_seen_at: datetime | None
    last_success_at: datetime | None
    last_failure_at: datetime | None
    failure_count: int
    created_at: datetime
    updated_at: datetime


class SourceDeadLetterRead(ForteModel):
    source_dead_letter_id: int
    source_id: int
    source_run_id: int | None
    adapter_kind: str
    source_kind: str
    stage: str
    status: str
    failure_reason: str
    record_key: str | None
    record_hash: str | None
    raw_payload_text: str | None
    payload_json: dict[str, Any]
    cursor_text: str | None
    checkpoint_json: dict[str, Any]
    replay_count: int
    last_replayed_at: datetime | None
    last_error_text: str | None
    created_at: datetime
    updated_at: datetime


class SourceRunRead(ForteModel):
    source_run_id: int
    source_id: int
    import_run_id: int | None
    status: str
    started_at: datetime
    finished_at: datetime | None
    adapter_kind: str
    fetch_mode: str
    records_seen: int
    records_imported: int
    records_skipped: int
    records_failed: int
    cursor_text: str | None
    last_event_id: str | None
    last_offset: int | None
    checkpoint_json: dict[str, Any]
    error_text: str | None
    output_json: dict[str, Any]


class SourceOpsDetailRead(ForteModel):
    source: SourceDefinitionRead
    report_status: SourceOpsStatusRead | None = None
    recent_runs: list[SourceRunRead]
    checkpoint: SourceCheckpointRead | None = None
    dead_letters: list[SourceDeadLetterRead] = Field(default_factory=list)
    storage_objects: list[StorageObjectRead]
    custody_logs: list["CustodyLogRead"]


class SourceOpsStatusRead(ForteModel):
    source: SourceDefinitionRead
    latest_run: SourceRunRead | None
    checkpoint: SourceCheckpointRead | None = None
    has_schedule: bool
    next_run_at: datetime | None
    latest_success_at: datetime | None
    is_stale: bool
    is_failing: bool
    runtime_state: str = "idle"
    fetch_mode: str = "pull"
    pending_dead_letter_count: int = 0
    replayed_dead_letter_count: int = 0
    last_dead_letter_at: datetime | None = None
    storage_object_count: int
    last_storage_observed_at: datetime | None
    web_collection_enabled: bool = False
    web_collection_kind: str | None = None
    web_collection_stats: dict[str, Any] = Field(default_factory=dict)


class SourceSummaryBucketRead(ForteModel):
    key: str
    total_count: int
    enabled_count: int
    disabled_count: int
    stale_count: int
    failing_count: int


class WebCollectionFleetSummaryRead(ForteModel):
    total_source_count: int = 0
    enabled_source_count: int = 0
    search_source_count: int = 0
    crawl_source_count: int = 0
    discovery_source_count: int = 0
    latest_run_count: int = 0
    problem_source_count: int = 0
    robots_blocked_source_count: int = 0
    private_network_blocked_source_count: int = 0
    fetch_error_source_count: int = 0
    total_search_page_count: int = 0
    total_search_result_candidate_count: int = 0
    total_search_page_fetch_count: int = 0
    total_search_page_fetch_error_count: int = 0
    total_search_result_crawl_page_count: int = 0
    total_sitemap_fetch_count: int = 0
    total_crawl_page_count: int = 0
    total_crawl_queued_count: int = 0
    total_crawl_fetch_error_count: int = 0
    total_robots_blocked_count: int = 0
    total_private_network_blocked_count: int = 0


class SourceInventorySummaryRead(ForteModel):
    generated_at: datetime
    stale_before: datetime
    total_count: int
    enabled_count: int
    disabled_count: int
    stale_count: int
    failing_count: int
    runtime_active_count: int = 0
    runtime_degraded_count: int = 0
    dead_letter_pending_count: int = 0
    scheduled_count: int
    unscheduled_count: int
    source_kind_counts: list[SourceSummaryBucketRead]
    fetch_mode_counts: list[SourceSummaryBucketRead] = Field(default_factory=list)
    layer_counts: list[SourceSummaryBucketRead]
    latest_status_counts: list[SourceSummaryBucketRead]
    web_collection_summary: WebCollectionFleetSummaryRead = Field(default_factory=WebCollectionFleetSummaryRead)


class SourceOpsReportIndexRead(ForteModel):
    generated_at: datetime
    stale_after_hours: float
    latest_run_at: datetime | None
    inventory_summary: SourceInventorySummaryRead
    sync_task_count: int
    sync_run_count: int
    sync_failure_count: int
    pending_dead_letter_count: int = 0
    runtime_degraded_count: int = 0
    sync_tasks: list["ScheduledTaskRead"]
    recent_runs: list[SourceRunRead]
    stale_sources: list[SourceOpsStatusRead]
    failing_sources: list[SourceOpsStatusRead]
    unscheduled_sources: list[SourceOpsStatusRead]
    runtime_degraded_sources: list[SourceOpsStatusRead] = Field(default_factory=list)
    pending_dead_letter_sources: list[SourceOpsStatusRead] = Field(default_factory=list)
    web_collection_summary: WebCollectionFleetSummaryRead = Field(default_factory=WebCollectionFleetSummaryRead)
    web_collection_issue_sources: list[SourceOpsStatusRead] = Field(default_factory=list)


class SourceOpsExportSummaryRead(ForteModel):
    generated_at: datetime
    filters_json: dict[str, Any]
    report_index: SourceOpsReportIndexRead
    sources: list[SourceDefinitionRead]


class IntegritySeedResponse(ForteModel):
    created: int
    domains: list[str]


class ObservationRead(ForteModel):
    observation_id: int
    import_run_id: int | None
    event_id: int | None
    layer_key: str
    source_domain: str | None
    source_type: str
    record_format: str
    trust_level: TrustLevel
    approval_policy: ApprovalPolicy
    confidence_score: float
    observed_at: datetime | None = None
    location_geojson: dict[str, Any] | None
    content_text: str
    content_json: dict[str, Any]
    raw_hash: str
    created_at: datetime
    updated_at: datetime

    @field_validator("observed_at", "created_at", "updated_at", mode="before")
    @classmethod
    def normalize_datetime_fields(cls, value: datetime | None) -> datetime | None:
        if value is None or not isinstance(value, datetime):
            return value
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


class ObservationSearchResultRead(ForteModel):
    observation: ObservationRead
    search_score: float
    matched_terms: list[str]
    title: str | None = None
    source_url: str | None = None
    snippet: str | None = None


class CrossVerificationSummaryRead(ForteModel):
    cluster_id: str
    observation_ids: list[int]
    observation_count: int
    source_domain_count: int
    source_domains: list[str]
    layer_count: int
    layer_keys: list[str]
    independent_signal_count: int
    verification_score: float
    trusted_observation_count: int
    integrity_source_count: int
    ground_truth_count: int
    time_span_minutes: float
    started_at: datetime
    ended_at: datetime
    centroid_geojson: dict[str, Any]


class EventFusionRequest(ForteModel):
    layer_key: str | None = None
    source_domain: str | None = None
    trust_level: str | None = None
    min_lon: float | None = None
    min_lat: float | None = None
    max_lon: float | None = None
    max_lat: float | None = None
    since: datetime | None = None
    until: datetime | None = None
    limit: int = Field(default=500, ge=1, le=2000)
    time_window_minutes: int = Field(default=60, ge=1, le=1440)
    distance_km: float = Field(default=25.0, gt=0.0, le=500.0)
    min_independent_signals: int = Field(default=2, ge=2, le=10)
    redaction_level: str = "public"


class EventFusionResultRead(ForteModel):
    event_id: int
    slug: str
    title: str
    observation_count: int
    product_count: int
    verification_score: float


class EventFusionResponse(ForteModel):
    created_event_count: int
    event_results: list[EventFusionResultRead]


class EntityResolutionRequest(ForteModel):
    layer_key: str | None = None
    source_domain: str | None = None
    trust_level: str | None = None
    min_lon: float | None = None
    min_lat: float | None = None
    max_lon: float | None = None
    max_lat: float | None = None
    since: datetime | None = None
    until: datetime | None = None
    limit: int = Field(default=500, ge=1, le=2000)
    min_observations: int = Field(default=2, ge=1, le=100)
    entity_type: str | None = None
    redaction_level: str = "public"


class EntityResolutionResultRead(ForteModel):
    entity_id: int
    slug: str
    entity_type: str
    canonical_name: str
    observation_count: int
    signal_count: int
    confidence_score: float
    created_new: bool


class EntityResolutionResponse(ForteModel):
    created_entity_count: int
    entity_results: list[EntityResolutionResultRead]


class LocalImportRequest(ForteModel):
    source_path: str
    layer_key: str = "unassigned"
    notes: str = ""


class LocalImportRunRead(ForteModel):
    import_run_id: int
    source_path: str
    source_format: str
    layer_key: str
    status: str
    records_seen: int
    records_imported: int
    records_skipped: int
    notes: str
    chain_of_custody_json: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime
    observations: list[ObservationRead] = Field(default_factory=list)


class LocalImportRunSummaryRead(ForteModel):
    import_run_id: int
    source_path: str
    source_format: str
    layer_key: str
    status: str
    records_seen: int
    records_imported: int
    records_skipped: int
    notes: str
    chain_of_custody_json: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime


class CustodyLogRead(ForteModel):
    custody_log_id: int
    object_type: str
    object_id: str
    action: str
    actor: str
    details_json: dict[str, Any]
    created_at: datetime


class ScheduledTaskCreate(ForteModel):
    name: str
    task_type: Literal[
        "local_import",
        "geofence_scan",
        "integrity_seed",
        "source_sync",
        "source_maintenance",
        "source_health_scan",
        "storage_lifecycle",
        "runtime_snapshot_export",
        "clickhouse_sync",
        "clickhouse_archive",
        "camera_inventory_refresh",
        "camera_source_verification",
        "observation_watch_scan",
        "entity_resolution_refresh",
        "event_fusion_refresh",
    ]
    interval_seconds: int = Field(ge=60)
    enabled: bool = True
    retry_attempts: int = Field(default=1, ge=1, le=10)
    retry_backoff_seconds: float = Field(default=0.0, ge=0.0, le=300.0)
    source_id: int | None = None
    target_path: str | None = None
    layer_key: str | None = None
    geofence_id: int | None = None
    notes: str = ""
    payload_json: dict[str, Any] = Field(default_factory=dict)


class ScheduledTaskRead(ScheduledTaskCreate):
    task_id: int
    last_run_at: datetime | None
    next_run_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ScheduledTaskUpdate(ForteModel):
    name: str | None = None
    enabled: bool | None = None
    interval_seconds: int | None = Field(default=None, ge=60)
    retry_attempts: int | None = Field(default=None, ge=1, le=10)
    retry_backoff_seconds: float | None = Field(default=None, ge=0.0, le=300.0)
    source_id: int | None = None
    target_path: str | None = None
    layer_key: str | None = None
    geofence_id: int | None = None
    notes: str | None = None
    payload_json: dict[str, Any] | None = None


class ScheduledTaskRunRead(ForteModel):
    task_run_id: int
    task_id: int
    status: str
    started_at: datetime
    finished_at: datetime | None
    records_affected: int
    error_text: str | None
    output_json: dict[str, Any]


class ScheduledTaskOpsStatusRead(ForteModel):
    task: ScheduledTaskRead
    latest_run: ScheduledTaskRunRead | None
    is_due: bool
    is_overdue: bool
    is_failing: bool


class ScheduledTaskSummaryBucketRead(ForteModel):
    key: str
    total_count: int
    enabled_count: int
    disabled_count: int
    due_count: int
    failing_count: int


class ScheduledTaskRunSummaryBucketRead(ForteModel):
    key: str
    total_count: int
    completed_count: int
    failure_count: int


class SchedulerInventorySummaryRead(ForteModel):
    generated_at: datetime
    reference_time: datetime
    total_count: int
    enabled_count: int
    disabled_count: int
    due_count: int
    overdue_count: int
    failing_count: int
    maintenance_task_count: int
    task_type_counts: list[ScheduledTaskSummaryBucketRead]
    latest_status_counts: list[ScheduledTaskSummaryBucketRead]


class SchedulerOpsReportIndexRead(ForteModel):
    generated_at: datetime
    latest_run_at: datetime | None
    inventory_summary: SchedulerInventorySummaryRead
    task_run_count: int
    task_run_failure_count: int
    maintenance_run_count: int
    maintenance_failure_count: int
    task_type_run_counts: list[ScheduledTaskRunSummaryBucketRead]
    recent_runs: list[ScheduledTaskRunRead]
    overdue_tasks: list[ScheduledTaskOpsStatusRead]
    failing_tasks: list[ScheduledTaskOpsStatusRead]
    maintenance_tasks: list[ScheduledTaskOpsStatusRead]


class SchedulerOpsExportSummaryRead(ForteModel):
    generated_at: datetime
    filters_json: dict[str, Any]
    report_index: SchedulerOpsReportIndexRead
    tasks: list[ScheduledTaskRead]


class CameraRefreshTaskRunRead(ForteModel):
    task_run_id: int
    task_id: int
    task_name: str
    layer_key: str | None
    source_domain: str | None
    status: str
    started_at: datetime
    finished_at: datetime | None
    records_affected: int
    error_text: str | None
    output_json: dict[str, Any]


class CameraOpsReportIndexRead(ForteModel):
    generated_at: datetime
    stale_after_hours: float
    latest_materialization_at: datetime | None
    inventory_summary: CameraInventorySummaryRead
    refresh_task_count: int
    refresh_run_count: int
    refresh_failure_count: int
    refresh_tasks: list[ScheduledTaskRead]
    recent_refresh_runs: list[CameraRefreshTaskRunRead]
    recent_materializations: list[CustodyLogRead]
    stale_cameras: list[CameraInventoryRead]


class CameraOpsExportSummaryRead(ForteModel):
    generated_at: datetime
    filters_json: dict[str, Any]
    report_index: CameraOpsReportIndexRead
    cameras: list[CameraInventoryRead]


class SchedulerKickResponse(ForteModel):
    runs_created: int
    task_run_ids: list[int]


class WorkerStatusRead(ForteModel):
    worker_status_id: int
    worker_key: str
    worker_type: str
    actor: str
    status: str
    process_token: str | None
    last_started_at: datetime | None
    last_seen_at: datetime | None
    last_iteration_at: datetime | None
    last_stopped_at: datetime | None
    failure_count: int
    metadata_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class WorkerStatusSummaryBucketRead(ForteModel):
    key: str
    total_count: int
    active_count: int
    stale_count: int


class WorkerStatusSummaryRead(ForteModel):
    generated_at: datetime
    stale_before: datetime
    total_count: int
    active_count: int
    stale_count: int
    status_counts: list[WorkerStatusSummaryBucketRead]
    worker_type_counts: list[WorkerStatusSummaryBucketRead]
    workers: list[WorkerStatusRead]


class PlatformRuntimeCycleSourceRuntimeRead(ForteModel):
    source_count: int
    source_run_ids: list[int]
    records_seen: int
    records_imported: int
    records_failed: int


class PlatformRuntimeCycleScheduleRunRead(ForteModel):
    runs_created: int
    task_run_ids: list[int]
    completed_count: int
    failed_count: int
    task_types: list[str]


class PlatformRuntimeCycleRead(ForteModel):
    executed_at: datetime
    include_stream_runtime: bool
    include_enabled_schedules: bool
    task_type_filters: list[str]
    source_runtime: PlatformRuntimeCycleSourceRuntimeRead
    schedules: PlatformRuntimeCycleScheduleRunRead


class OperationsSummaryRead(ForteModel):
    import_run_count: int
    imported_record_count: int
    skipped_record_count: int
    source_run_count: int
    source_run_failure_count: int
    scheduled_task_run_count: int
    scheduled_task_run_failure_count: int
    alert_count: int
    open_alert_count: int
    acknowledged_alert_count: int
    closed_alert_count: int
    event_count: int
    entity_count: int
    observation_count: int


class OperationsReportRead(ForteModel):
    generated_at: datetime
    scope_since: datetime | None
    scope_until: datetime | None
    summary: OperationsSummaryRead
    runtime_readiness: RuntimeReadinessRead
    alert_inventory_summary: AlertInventorySummaryRead
    alert_report_index: AlertOpsReportIndexRead
    event_inventory_summary: EventInventorySummaryRead
    event_report_index: EventOpsReportIndexRead
    entity_inventory_summary: EntityInventorySummaryRead
    entity_report_index: EntityOpsReportIndexRead
    storage_report: StorageReportRead
    clickhouse_diagnostics: ClickHouseDiagnosticsRead
    scheduler_inventory_summary: SchedulerInventorySummaryRead
    scheduler_report_index: SchedulerOpsReportIndexRead
    worker_status_summary: WorkerStatusSummaryRead
    source_inventory_summary: SourceInventorySummaryRead
    source_report_index: SourceOpsReportIndexRead
    camera_inventory_summary: CameraInventorySummaryRead
    camera_report_index: CameraOpsReportIndexRead
    camera_source_inventory_summary: CameraSourceSummaryRead
    camera_source_report_index: CameraSourceOpsReportIndexRead
    import_runs: list[LocalImportRunSummaryRead]
    source_runs: list[SourceRunRead]
    scheduled_task_runs: list[ScheduledTaskRunRead]
    alerts: list[AlertRead]
    custody_logs: list[CustodyLogRead]


class EventExportBundleRead(ForteModel):
    exported_at: datetime
    event: EventRead
    observation_links: list[EventObservationLinkRead]
    observations: list[ObservationRead]
    entities: list[EntityRead]
    entity_observation_links: list[EntityObservationLinkRead]
    alerts: list[AlertRead]
    import_runs: list[LocalImportRunSummaryRead]
    source_runs: list[SourceRunRead]
    source_definitions: list[SourceDefinitionRead]
    scheduled_tasks: list[ScheduledTaskRead]
    scheduled_task_runs: list[ScheduledTaskRunRead]
    products: list[SituationProductRead]
    custody_logs: list[CustodyLogRead]
    citations_json: list[dict[str, Any]]
