from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


TrustLevel = Literal["trusted", "neutral", "blocked"]
ApprovalPolicy = Literal["auto_approve_stable", "manual_review", "always_review", "auto_reject"]
StorageTier = Literal["hot", "warm", "archive"]
RetentionClass = Literal["ephemeral", "operational", "investigative", "permanent"]
StorageLifecycleStatus = Literal["active", "promoted", "degraded", "archived", "expired"]
CameraSourceStatus = Literal["candidate", "review", "ready", "graduated", "ignored", "retired"]
CameraSourceVerificationState = Literal["unknown", "observed", "reachable", "failed"]


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


class DatabaseColumnAuditRead(ForteModel):
    table_name: str
    column_name: str
    present: bool


class DatabaseIndexAuditRead(ForteModel):
    index_name: str
    present: bool


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
    spatial_indexes: list[DatabaseIndexAuditRead]
    table_counts: list[DatabaseTableCountRead]


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
    row_counts: list[DatabaseTableCountRead]
    data_layers: list["DataLayerRead"]
    source_trust_profiles: list["SourceTrustProfileRead"]
    geofences: list["GeofenceRead"]
    source_definitions: list["SourceDefinitionRead"]
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
    situation_products: list["SituationProductRead"]
    custody_logs: list["CustodyLogRead"]


class RuntimeRestoreResultRead(ForteModel):
    restored_at: datetime
    database_backend: str
    replaced_existing: bool
    total_records: int
    row_counts: list[DatabaseTableCountRead]


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
    next_expiration_at: datetime | None
    oldest_expired_at: datetime | None
    retention_class_counts: list[StorageInventoryBucketRead]
    storage_tier_counts: list[StorageInventoryBucketRead]
    lifecycle_status_counts: list[StorageInventoryBucketRead]
    expiring_objects: list[StorageObjectRead]


class StorageLifecycleSweepResultRead(ForteModel):
    swept_at: datetime
    dry_run: bool
    filters_json: dict[str, Any]
    expired_candidate_count: int
    transitioned_count: int
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


class CameraSourceOpsDetailRead(ForteModel):
    source: CameraSourceInventoryRead
    camera: CameraInventoryRead | None
    latest_observation: ObservationRead | None
    custody_logs: list["CustodyLogRead"]


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
    source_kind: Literal["local_file", "http_json", "http_text", "http_xml"]
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
    layer_key: str | None = None
    target_uri: str | None = None
    enabled: bool | None = None
    integrity_source: bool | None = None
    notes: str | None = None
    metadata_json: dict[str, Any] | None = None


class SourceRunRead(ForteModel):
    source_run_id: int
    source_id: int
    import_run_id: int | None
    status: str
    started_at: datetime
    finished_at: datetime | None
    records_imported: int
    error_text: str | None
    output_json: dict[str, Any]


class SourceOpsDetailRead(ForteModel):
    source: SourceDefinitionRead
    recent_runs: list[SourceRunRead]
    storage_objects: list[StorageObjectRead]
    custody_logs: list["CustodyLogRead"]


class SourceOpsStatusRead(ForteModel):
    source: SourceDefinitionRead
    latest_run: SourceRunRead | None
    has_schedule: bool
    next_run_at: datetime | None
    latest_success_at: datetime | None
    is_stale: bool
    is_failing: bool
    storage_object_count: int
    last_storage_observed_at: datetime | None


class SourceSummaryBucketRead(ForteModel):
    key: str
    total_count: int
    enabled_count: int
    disabled_count: int
    stale_count: int
    failing_count: int


class SourceInventorySummaryRead(ForteModel):
    generated_at: datetime
    stale_before: datetime
    total_count: int
    enabled_count: int
    disabled_count: int
    stale_count: int
    failing_count: int
    scheduled_count: int
    unscheduled_count: int
    source_kind_counts: list[SourceSummaryBucketRead]
    layer_counts: list[SourceSummaryBucketRead]
    latest_status_counts: list[SourceSummaryBucketRead]


class SourceOpsReportIndexRead(ForteModel):
    generated_at: datetime
    stale_after_hours: float
    latest_run_at: datetime | None
    inventory_summary: SourceInventorySummaryRead
    sync_task_count: int
    sync_run_count: int
    sync_failure_count: int
    sync_tasks: list["ScheduledTaskRead"]
    recent_runs: list[SourceRunRead]
    stale_sources: list[SourceOpsStatusRead]
    failing_sources: list[SourceOpsStatusRead]
    unscheduled_sources: list[SourceOpsStatusRead]


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
    location_geojson: dict[str, Any] | None
    content_text: str
    content_json: dict[str, Any]
    raw_hash: str
    created_at: datetime
    updated_at: datetime


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
        "storage_lifecycle",
        "camera_inventory_refresh",
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
    source_inventory_summary: SourceInventorySummaryRead
    source_report_index: SourceOpsReportIndexRead
    camera_inventory_summary: CameraInventorySummaryRead
    camera_report_index: CameraOpsReportIndexRead
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
