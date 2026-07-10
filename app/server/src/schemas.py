from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


TrustLevel = Literal["trusted", "neutral", "blocked"]
ApprovalPolicy = Literal["auto_approve_stable", "manual_review", "always_review", "auto_reject"]
StorageTier = Literal["hot", "warm", "archive"]
RetentionClass = Literal["ephemeral", "operational", "investigative", "permanent"]
StorageLifecycleStatus = Literal["active", "promoted", "degraded", "archived", "expired"]
CameraSourceStatus = Literal["candidate", "review", "ready", "graduated", "ignored", "retired"]
CameraSourceVerificationState = Literal["unknown", "observed", "reachable", "failed"]


class ForteModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


DISCOVERY_SNAPSHOT_ROW_SCHEMAS = {
    "discovery_domain_policies": "DiscoveryDomainPolicyRead",
    "discovery_campaigns": "DiscoveryCampaignRead",
    "discovery_runs": "DiscoveryRunRead",
    "source_candidates": "SourceCandidateRead",
    "discovery_frontier_entries": "DiscoveryFrontierEntryRead",
    "source_candidate_revisions": "SourceCandidateRevisionRead",
    "discovery_graph_edges": "DiscoveryGraphEdgeRead",
    "candidate_health_checks": "CandidateHealthCheckRead",
    "candidate_suppressions": "CandidateSuppressionRead",
    "candidate_promotion_decisions": "CandidatePromotionDecisionRead",
    "robots_observations": "RobotsObservationRead",
    "discovery_artifacts": "DiscoveryArtifactRead",
}


def validate_discovery_crawl_policy(value: dict[str, Any]) -> dict[str, Any]:
    numeric_bounds: dict[str, tuple[float, float, bool]] = {
        "crawl_delay_seconds": (0.0, 3600.0, False),
        "max_concurrency": (1, 20, True),
        "max_depth": (0, 10, True),
        "max_pages_per_run": (1, 10_000, True),
        "max_pages_per_domain": (1, 10_000, True),
        "max_response_bytes": (1024, 100_000_000, True),
        "request_timeout_seconds": (0.1, 300.0, False),
        "retry_attempts": (1, 10, True),
        "retry_backoff_seconds": (0.0, 300.0, False),
        "max_seconds": (1.0, 86_400.0, False),
        "robots_ttl_seconds": (300, 2_592_000, True),
    }
    for key, (minimum, maximum, integer_only) in numeric_bounds.items():
        if key not in value:
            continue
        candidate = value[key]
        if isinstance(candidate, bool) or not isinstance(candidate, (int, float)):
            raise ValueError(f"Discovery crawl policy {key} must be numeric.")
        if integer_only and not isinstance(candidate, int):
            raise ValueError(f"Discovery crawl policy {key} must be an integer.")
        if candidate < minimum or candidate > maximum:
            raise ValueError(
                f"Discovery crawl policy {key} must be between {minimum} and {maximum}."
            )
    for key in (
        "robots_aware",
        "allow_private_networks",
        "allow_cross_domain_links",
        "store_artifacts",
    ):
        if key in value and not isinstance(value[key], bool):
            raise ValueError(f"Discovery crawl policy {key} must be a boolean.")
    if "user_agent" in value and (
        not isinstance(value["user_agent"], str) or len(value["user_agent"]) > 500
    ):
        raise ValueError("Discovery crawl policy user_agent must be a string of 500 characters or fewer.")
    return value


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
    model_config = ConfigDict(from_attributes=True, extra="forbid")

    snapshot_version: int = 1
    exported_at: datetime
    app_name: str
    app_version: str
    database_backend: str
    spatial_backend: str
    row_counts: list[DatabaseTableCountRead]
    data_layers: list["DataLayerRead"]
    source_trust_profiles: list["SourceTrustProfileRead"]
    discovery_domain_policies: list["DiscoveryDomainPolicyRead"] = Field(default_factory=list)
    discovery_campaigns: list["DiscoveryCampaignRead"] = Field(default_factory=list)
    discovery_runs: list["DiscoveryRunRead"] = Field(default_factory=list)
    source_candidates: list["SourceCandidateRead"] = Field(default_factory=list)
    discovery_frontier_entries: list["DiscoveryFrontierEntryRead"] = Field(default_factory=list)
    source_candidate_revisions: list["SourceCandidateRevisionRead"] = Field(default_factory=list)
    discovery_graph_edges: list["DiscoveryGraphEdgeRead"] = Field(default_factory=list)
    candidate_health_checks: list["CandidateHealthCheckRead"] = Field(default_factory=list)
    candidate_suppressions: list["CandidateSuppressionRead"] = Field(default_factory=list)
    candidate_promotion_decisions: list["CandidatePromotionDecisionRead"] = Field(default_factory=list)
    robots_observations: list["RobotsObservationRead"] = Field(default_factory=list)
    discovery_artifacts: list["DiscoveryArtifactRead"] = Field(default_factory=list)
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

    @model_validator(mode="before")
    @classmethod
    def validate_snapshot_contract(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        version = int(value.get("snapshot_version", 1))
        if version not in {1, 2}:
            raise ValueError(f"Unsupported runtime snapshot version: {version}.")
        if version < 2:
            return value

        raw_counts = value.get("row_counts", [])
        row_counts = {
            str(item.get("table_name")): int(item.get("row_count", 0))
            for item in raw_counts
            if isinstance(item, dict) and item.get("table_name") is not None
        }
        for section_name, schema_name in DISCOVERY_SNAPSHOT_ROW_SCHEMAS.items():
            if section_name in row_counts and section_name not in value:
                raise ValueError(
                    f"Runtime snapshot section '{section_name}' is missing despite row_counts metadata."
                )
            rows = value.get(section_name, [])
            if not isinstance(rows, list):
                raise ValueError(f"Runtime snapshot section '{section_name}' must be a list.")
            if section_name in row_counts and len(rows) != row_counts[section_name]:
                raise ValueError(
                    f"Runtime snapshot section '{section_name}' contains {len(rows)} rows; "
                    f"row_counts declares {row_counts[section_name]}."
                )
            schema_cls = globals().get(schema_name)
            if schema_cls is None:
                continue
            expected_fields = set(schema_cls.model_fields)
            for row_index, row in enumerate(rows):
                if not isinstance(row, dict):
                    raise ValueError(
                        f"Runtime snapshot section '{section_name}' row {row_index} must be an object."
                    )
                actual_fields = set(row)
                missing_fields = sorted(expected_fields - actual_fields)
                unknown_fields = sorted(actual_fields - expected_fields)
                if missing_fields or unknown_fields:
                    raise ValueError(
                        f"Runtime snapshot section '{section_name}' row {row_index} has an "
                        f"invalid field set; missing={missing_fields}, unknown={unknown_fields}."
                    )
        return value


class RuntimeRestoreResultRead(ForteModel):
    restored_at: datetime
    database_backend: str
    replaced_existing: bool
    total_records: int
    row_counts: list[DatabaseTableCountRead]


class RuntimeBundleRestoreResultRead(RuntimeRestoreResultRead):
    restored_file_count: int
    bundle_sha256: str
    bundle_format_version: int
    data_dir: str


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
    recent_materializations: list["CustodyLogRead"]
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


class SourceTrustProfileUpdate(ForteModel):
    domain: str | None = None
    trust_level: TrustLevel | None = None
    approval_policy: ApprovalPolicy | None = None
    integrity_source: bool | None = None
    notes: str | None = None


class SourceTrustProfileRead(SourceTrustProfileCreate):
    trust_profile_id: int
    created_at: datetime
    updated_at: datetime


class SourceDefinitionCreate(ForteModel):
    name: str
    source_kind: Literal[
        "local_file",
        "http_json",
        "http_jsonl",
        "http_text",
        "http_xml",
        "rss",
        "web_search",
        "web_crawl",
        "web_discovery",
        "websocket_stream",
        "sse_stream",
        "webhook_ingest",
        "camera_image",
        "camera_stream",
    ]
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
        "clickhouse_sync",
        "clickhouse_archive",
        "camera_inventory_refresh",
        "entity_resolution_refresh",
        "event_fusion_refresh",
        "discovery_campaign",
        "discovery_health_scan",
        "discovery_revisit",
        "watch_evaluate",
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

WatchType = Literal["source_delta", "image_change", "observation_rule", "source_health"]
WatchState = Literal["enabled", "paused"]
WatchSeverity = Literal["info", "warning", "critical"]
WatchRunStatus = Literal["running", "completed", "failed"]
WatchRunOutcome = Literal["pending", "baseline", "no_change", "change", "failure"]


class ForteModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class StrictForteModel(ForteModel):
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class NotificationPolicy(StrictForteModel):
    api_enabled: bool = True
    rss_enabled: bool = True
    analysis_on_change: bool = False

    @model_validator(mode="after")
    def require_api_for_rss(self) -> "NotificationPolicy":
        if self.rss_enabled and not self.api_enabled:
            raise ValueError("rss_enabled requires api_enabled because RSS is backed by local alerts")
        return self


class SourceDeltaRule(StrictForteModel):
    mode: Literal["source_delta"] = "source_delta"
    run_source: bool = True
    change_basis: Literal["payload_sha256"] = "payload_sha256"
    alert_on_initial: bool = False


class ImageChangeRule(StrictForteModel):
    mode: Literal["image_change"] = "image_change"
    comparison: Literal["sha256"] = "sha256"
    alert_on_initial: bool = False
    accepted_media_types: list[str] = Field(
        default_factory=lambda: ["image/jpeg", "image/png", "image/webp", "image/gif"],
        min_length=1,
        max_length=16,
    )
    retention_class: Literal["investigative", "permanent"] = "permanent"

    @field_validator("accepted_media_types")
    @classmethod
    def validate_accepted_media_types(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        for media_type in value:
            candidate = media_type.strip().lower()
            if not candidate.startswith("image/") or len(candidate) > 120:
                raise ValueError("accepted_media_types entries must be image media types")
            if candidate not in normalized:
                normalized.append(candidate)
        if not normalized:
            raise ValueError("accepted_media_types must contain at least one image media type")
        return normalized


ObservationPredicateOperator = Literal[
    "eq",
    "ne",
    "in",
    "contains",
    "exists",
    "gt",
    "gte",
    "lt",
    "lte",
]
ObservationPredicateValue = str | int | float | bool | None | list[str | int | float | bool | None]


class ObservationPredicate(StrictForteModel):
    field: str = Field(min_length=1, max_length=160)
    operator: ObservationPredicateOperator
    value: ObservationPredicateValue = None

    @field_validator("field")
    @classmethod
    def normalize_field(cls, value: str) -> str:
        return value.strip()


class ObservationRule(StrictForteModel):
    mode: Literal["observation_rule"] = "observation_rule"
    source_domain: str | None = Field(default=None, max_length=255)
    min_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    trust_levels: list[TrustLevel] = Field(default_factory=list, max_length=3)
    time_window_seconds: int | None = Field(default=None, gt=0)
    include_existing_on_first_run: bool = False
    predicates: list[ObservationPredicate] = Field(default_factory=list, max_length=50)
    max_results: int = Field(default=200, ge=1, le=2000)

    @field_validator("source_domain")
    @classmethod
    def normalize_source_domain(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        return normalized or None

    @field_validator("trust_levels")
    @classmethod
    def dedupe_trust_levels(cls, value: list[TrustLevel]) -> list[TrustLevel]:
        return list(dict.fromkeys(value))


WatchHealthState = Literal["failed", "stale", "disabled", "never_run"]


class SourceHealthRule(StrictForteModel):
    mode: Literal["source_health"] = "source_health"
    stale_after_seconds: int = Field(default=3600, gt=0)
    alert_states: list[WatchHealthState] = Field(
        default_factory=lambda: ["failed", "stale", "disabled", "never_run"],
        min_length=1,
        max_length=4,
    )
    alert_on_recovery: bool = False
    alert_on_initial_unhealthy: bool = True

    @field_validator("alert_states")
    @classmethod
    def dedupe_alert_states(cls, value: list[WatchHealthState]) -> list[WatchHealthState]:
        return list(dict.fromkeys(value))


WatchRule = Annotated[
    SourceDeltaRule | ImageChangeRule | ObservationRule | SourceHealthRule,
    Field(discriminator="mode"),
]


def ensure_watch_rule_matches_type(watch_type: WatchType, rule: WatchRule) -> None:
    if rule.mode != watch_type:
        raise ValueError(f"rule_json mode {rule.mode!r} must match watch_type {watch_type!r}")


class WatchCreate(StrictForteModel):
    name: str = Field(min_length=1, max_length=160)
    slug: str = Field(min_length=1, max_length=160, pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    objective: str = Field(min_length=1)
    description: str = ""
    watch_type: WatchType
    state: WatchState = "enabled"
    rule_json: WatchRule
    source_id: int | None = Field(default=None, gt=0)
    camera_inventory_id: int | None = Field(default=None, gt=0)
    camera_source_inventory_id: int | None = Field(default=None, gt=0)
    layer_key: str | None = Field(default=None, min_length=1, max_length=80)
    event_id: int | None = Field(default=None, gt=0)
    geofence_id: int | None = Field(default=None, gt=0)
    scheduled_task_id: int | None = Field(default=None, gt=0)
    interval_seconds: int | None = Field(default=None, ge=60)
    severity: WatchSeverity = "info"
    notification_policy_json: NotificationPolicy = Field(default_factory=NotificationPolicy)
    metadata_json: dict[str, Any] = Field(default_factory=dict)
    provenance_json: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_rule_mode(self) -> "WatchCreate":
        ensure_watch_rule_matches_type(self.watch_type, self.rule_json)
        return self


class WatchUpdate(StrictForteModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    slug: str | None = Field(
        default=None,
        min_length=1,
        max_length=160,
        pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$",
    )
    objective: str | None = Field(default=None, min_length=1)
    description: str | None = None
    watch_type: WatchType | None = None
    state: WatchState | None = None
    rule_json: WatchRule | None = None
    source_id: int | None = Field(default=None, gt=0)
    camera_inventory_id: int | None = Field(default=None, gt=0)
    camera_source_inventory_id: int | None = Field(default=None, gt=0)
    layer_key: str | None = Field(default=None, min_length=1, max_length=80)
    event_id: int | None = Field(default=None, gt=0)
    geofence_id: int | None = Field(default=None, gt=0)
    scheduled_task_id: int | None = Field(default=None, gt=0)
    interval_seconds: int | None = Field(default=None, ge=60)
    severity: WatchSeverity | None = None
    notification_policy_json: NotificationPolicy | None = None
    metadata_json: dict[str, Any] | None = None
    provenance_json: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_rule_mode(self) -> "WatchUpdate":
        if self.watch_type is not None and self.rule_json is not None:
            ensure_watch_rule_matches_type(self.watch_type, self.rule_json)
        return self


class WatchRead(WatchCreate):
    watch_id: int
    baseline_json: dict[str, Any]
    dedupe_json: dict[str, Any]
    last_evaluated_at: datetime | None
    last_changed_at: datetime | None
    next_run_at: datetime | None
    created_at: datetime
    updated_at: datetime


class WatchRunRead(StrictForteModel):
    watch_run_id: int
    watch_id: int
    scheduled_task_run_id: int | None
    source_run_id: int | None
    alert_id: int | None
    storage_object_id: int | None
    status: WatchRunStatus
    outcome: WatchRunOutcome
    started_at: datetime
    finished_at: datetime | None
    change_detected: bool
    baseline_initialized: bool
    dedupe_key: str | None
    evidence_json: dict[str, Any]
    checkpoint_before_json: dict[str, Any]
    checkpoint_after_json: dict[str, Any]
    output_summary: str
    error_text: str | None
    metadata_json: dict[str, Any]


class WatchScheduleCreate(StrictForteModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    interval_seconds: int = Field(ge=60)
    enabled: bool = True
    retry_attempts: int = Field(default=1, ge=1, le=10)
    retry_backoff_seconds: float = Field(default=0.0, ge=0.0, le=300.0)
    notes: str = ""


class WatchEvaluateRequest(StrictForteModel):
    force: bool = False
    actor: str | None = Field(default=None, min_length=1, max_length=120)


class WatchEvaluateTaskPayload(StrictForteModel):
    watch_id: int = Field(gt=0)
    force: bool = False


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
    storage_report: StorageReportRead
    clickhouse_diagnostics: ClickHouseDiagnosticsRead
    scheduler_inventory_summary: SchedulerInventorySummaryRead
    scheduler_report_index: SchedulerOpsReportIndexRead
    source_inventory_summary: SourceInventorySummaryRead
    source_report_index: SourceOpsReportIndexRead
    camera_inventory_summary: CameraInventorySummaryRead
    camera_report_index: CameraOpsReportIndexRead
    camera_source_inventory_summary: CameraSourceSummaryRead
    camera_source_report_index: CameraSourceOpsReportIndexRead
    discovery_ops_summary: "DiscoveryOpsSummaryRead | None" = None
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


class DiscoveryCampaignCreate(ForteModel):
    name: str = Field(min_length=1, max_length=160)
    description: str = ""
    mode: str = Field(default="query_seeded", max_length=40)
    status: str = Field(default="draft", max_length=30)
    enabled: bool = True
    layer_key: str | None = Field(default=None, max_length=80)
    query_text: str = ""
    modes_json: list[str] = Field(default_factory=list)
    query_strings_json: list[str] = Field(default_factory=list)
    search_templates_json: list[str] = Field(default_factory=list)
    format_targets_json: list[str] = Field(default_factory=list)
    seed_urls_json: list[str] = Field(default_factory=list)
    locale_variants_json: list[str] = Field(default_factory=list)
    language_variants_json: list[str] = Field(default_factory=list)
    domain_allowlist_json: list[str] = Field(default_factory=list)
    domain_denylist_json: list[str] = Field(default_factory=list)
    target_geography_json: dict[str, Any] = Field(default_factory=dict)
    entity_seeds_json: list[dict[str, Any]] = Field(default_factory=list)
    historical_backfill: bool = False
    recency_days: int | None = Field(default=None, ge=0)
    max_depth: int = Field(default=2, ge=0, le=10)
    max_pages: int = Field(default=100, ge=1, le=10_000)
    max_candidates: int = Field(default=1000, ge=1, le=100_000)
    request_json: dict[str, Any] = Field(default_factory=dict)
    crawl_policy_json: dict[str, Any] = Field(default_factory=dict)
    scoring_weights_json: dict[str, Any] = Field(default_factory=dict)
    schedule_json: dict[str, Any] = Field(default_factory=dict)
    last_run_at: datetime | None = None
    last_completed_at: datetime | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)

    @field_validator("crawl_policy_json")
    @classmethod
    def validate_crawl_policy(cls, value: dict[str, Any]) -> dict[str, Any]:
        return validate_discovery_crawl_policy(value)

    @field_validator("request_json")
    @classmethod
    def validate_request_crawl_policy(cls, value: dict[str, Any]) -> dict[str, Any]:
        nested = value.get("crawl_policy")
        if nested is None:
            return value
        if not isinstance(nested, dict):
            raise ValueError("Discovery request crawl_policy must be a JSON object.")
        validate_discovery_crawl_policy(nested)
        return value


class DiscoveryCampaignUpdate(ForteModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    description: str | None = None
    mode: str | None = Field(default=None, max_length=40)
    status: str | None = Field(default=None, max_length=30)
    enabled: bool | None = None
    layer_key: str | None = Field(default=None, max_length=80)
    query_text: str | None = None
    modes_json: list[str] | None = None
    query_strings_json: list[str] | None = None
    search_templates_json: list[str] | None = None
    format_targets_json: list[str] | None = None
    seed_urls_json: list[str] | None = None
    locale_variants_json: list[str] | None = None
    language_variants_json: list[str] | None = None
    domain_allowlist_json: list[str] | None = None
    domain_denylist_json: list[str] | None = None
    target_geography_json: dict[str, Any] | None = None
    entity_seeds_json: list[dict[str, Any]] | None = None
    historical_backfill: bool | None = None
    recency_days: int | None = Field(default=None, ge=0)
    max_depth: int | None = Field(default=None, ge=0, le=10)
    max_pages: int | None = Field(default=None, ge=1, le=10_000)
    max_candidates: int | None = Field(default=None, ge=1, le=100_000)
    request_json: dict[str, Any] | None = None
    crawl_policy_json: dict[str, Any] | None = None
    scoring_weights_json: dict[str, Any] | None = None
    schedule_json: dict[str, Any] | None = None
    last_run_at: datetime | None = None
    last_completed_at: datetime | None = None
    metadata_json: dict[str, Any] | None = None

    @field_validator("crawl_policy_json")
    @classmethod
    def validate_crawl_policy(
        cls,
        value: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        return validate_discovery_crawl_policy(value) if value is not None else None

    @field_validator("request_json")
    @classmethod
    def validate_request_crawl_policy(
        cls,
        value: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if value is None:
            return None
        nested = value.get("crawl_policy")
        if nested is None:
            return value
        if not isinstance(nested, dict):
            raise ValueError("Discovery request crawl_policy must be a JSON object.")
        validate_discovery_crawl_policy(nested)
        return value


class DiscoveryCampaignRead(DiscoveryCampaignCreate):
    campaign_id: int
    created_at: datetime
    updated_at: datetime


class DiscoveryRunCreate(ForteModel):
    campaign_id: int
    mode: str = Field(default="query_seeded", max_length=40)
    status: str = Field(default="queued", max_length=30)
    trigger_kind: str = Field(default="manual", max_length=30)
    actor: str = Field(default="discovery_engine", max_length=80)
    resumed_from_run_id: int | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    pages_queued: int = Field(default=0, ge=0)
    pages_fetched: int = Field(default=0, ge=0)
    candidates_discovered: int = Field(default=0, ge=0)
    candidates_updated: int = Field(default=0, ge=0)
    error_count: int = Field(default=0, ge=0)
    request_snapshot_json: dict[str, Any] = Field(default_factory=dict)
    policy_snapshot_json: dict[str, Any] = Field(default_factory=dict)
    frontier_checkpoint_json: dict[str, Any] = Field(default_factory=dict)
    stats_json: dict[str, Any] = Field(default_factory=dict)
    output_json: dict[str, Any] = Field(default_factory=dict)
    error_text: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class DiscoveryRunUpdate(ForteModel):
    mode: str | None = Field(default=None, max_length=40)
    status: str | None = Field(default=None, max_length=30)
    trigger_kind: str | None = Field(default=None, max_length=30)
    actor: str | None = Field(default=None, max_length=80)
    resumed_from_run_id: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    pages_queued: int | None = Field(default=None, ge=0)
    pages_fetched: int | None = Field(default=None, ge=0)
    candidates_discovered: int | None = Field(default=None, ge=0)
    candidates_updated: int | None = Field(default=None, ge=0)
    error_count: int | None = Field(default=None, ge=0)
    request_snapshot_json: dict[str, Any] | None = None
    policy_snapshot_json: dict[str, Any] | None = None
    frontier_checkpoint_json: dict[str, Any] | None = None
    stats_json: dict[str, Any] | None = None
    output_json: dict[str, Any] | None = None
    error_text: str | None = None
    metadata_json: dict[str, Any] | None = None


class DiscoveryRunRead(DiscoveryRunCreate):
    discovery_run_id: int
    started_at: datetime
    created_at: datetime
    updated_at: datetime


class DiscoveryRunRequest(ForteModel):
    campaign_id: int | None = None
    actor: str = Field(default="operator", max_length=80)
    resume: bool = True
    resume_run_id: int | None = None
    max_pages: int | None = Field(default=None, ge=1, le=10_000)
    max_candidates: int | None = Field(default=None, ge=1, le=100_000)
    max_seconds: float | None = Field(default=None, gt=0.0, le=86_400.0)
    dry_run: bool = False


class DiscoveryRunResultRead(ForteModel):
    run: DiscoveryRunRead
    frontier_queued_count: int = 0
    frontier_completed_count: int = 0
    frontier_dead_letter_count: int = 0
    candidate_ids: list[int] = Field(default_factory=list)


class DiscoveryDomainPolicyCreate(ForteModel):
    normalized_domain: str = Field(min_length=1, max_length=255)
    policy: str = Field(default="allow", max_length=30)
    robots_mode: str = Field(default="respect", max_length=30)
    enabled: bool = True
    allow_subdomains: bool = True
    crawl_delay_seconds: float = Field(default=1.0, ge=0.0, le=3600.0)
    max_concurrency: int = Field(default=1, ge=1, le=20)
    max_depth: int = Field(default=2, ge=0, le=10)
    max_pages_per_run: int = Field(default=100, ge=1, le=10_000)
    max_response_bytes: int = Field(default=5_000_000, ge=1024, le=100_000_000)
    request_timeout_seconds: float = Field(default=20.0, ge=0.1, le=300.0)
    retry_attempts: int = Field(default=2, ge=1, le=10)
    retry_backoff_seconds: float = Field(default=1.0, ge=0.0, le=300.0)
    allowed_path_patterns_json: list[str] = Field(default_factory=list)
    denied_path_patterns_json: list[str] = Field(default_factory=list)
    allowed_content_types_json: list[str] = Field(default_factory=list)
    notes: str = ""
    last_fetch_at: datetime | None = None
    next_allowed_at: datetime | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class DiscoveryDomainPolicyUpdate(ForteModel):
    policy: str | None = Field(default=None, max_length=30)
    robots_mode: str | None = Field(default=None, max_length=30)
    enabled: bool | None = None
    allow_subdomains: bool | None = None
    crawl_delay_seconds: float | None = Field(default=None, ge=0.0, le=3600.0)
    max_concurrency: int | None = Field(default=None, ge=1, le=20)
    max_depth: int | None = Field(default=None, ge=0, le=10)
    max_pages_per_run: int | None = Field(default=None, ge=1, le=10_000)
    max_response_bytes: int | None = Field(default=None, ge=1024, le=100_000_000)
    request_timeout_seconds: float | None = Field(default=None, ge=0.1, le=300.0)
    retry_attempts: int | None = Field(default=None, ge=1, le=10)
    retry_backoff_seconds: float | None = Field(default=None, ge=0.0, le=300.0)
    allowed_path_patterns_json: list[str] | None = None
    denied_path_patterns_json: list[str] | None = None
    allowed_content_types_json: list[str] | None = None
    notes: str | None = None
    last_fetch_at: datetime | None = None
    next_allowed_at: datetime | None = None
    metadata_json: dict[str, Any] | None = None


class DiscoveryDomainPolicyRead(DiscoveryDomainPolicyCreate):
    domain_policy_id: int
    created_at: datetime
    updated_at: datetime


class SourceCandidateCreate(ForteModel):
    canonical_url_hash: str = Field(min_length=64, max_length=64)
    canonical_url: str
    discovered_url: str
    normalized_domain: str = Field(min_length=1, max_length=255)
    path_pattern: str | None = Field(default=None, max_length=500)
    first_campaign_id: int | None = None
    last_campaign_id: int | None = None
    first_run_id: int | None = None
    last_run_id: int | None = None
    parent_url: str | None = None
    discovery_method: str = Field(default="unknown", max_length=50)
    candidate_type: str = Field(default="unknown", max_length=60)
    format_hint: str = Field(default="unknown", max_length=60)
    footprint_kind: str = Field(default="unknown", max_length=40)
    footprint_geojson: dict[str, Any] | None = None
    geo_hints_json: dict[str, Any] = Field(default_factory=dict)
    temporal_hints_json: dict[str, Any] = Field(default_factory=dict)
    format_hints_json: dict[str, Any] = Field(default_factory=dict)
    trust_hints_json: dict[str, Any] = Field(default_factory=dict)
    operational_hints_json: dict[str, Any] = Field(default_factory=dict)
    promotion_json: dict[str, Any] = Field(default_factory=dict)
    status: str = Field(default="candidate", max_length=40)
    score: float = Field(default=0.0, ge=0.0, le=100.0)
    score_bucket: str = Field(default="keep_candidate", max_length=40)
    score_breakdown_json: dict[str, Any] = Field(default_factory=dict)
    content_hash: str | None = Field(default=None, max_length=64)
    schema_hash: str | None = Field(default=None, max_length=64)
    first_seen_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_changed_at: datetime | None = None
    last_checked_at: datetime | None = None
    last_revisited_at: datetime | None = None
    next_revisit_at: datetime | None = None
    revisit_count: int = Field(default=0, ge=0)
    failure_count: int = Field(default=0, ge=0)
    last_failure_at: datetime | None = None
    last_error_text: str | None = None
    promoted_source_id: int | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class SourceCandidateUpdate(ForteModel):
    canonical_url_hash: str | None = Field(default=None, min_length=64, max_length=64)
    canonical_url: str | None = None
    discovered_url: str | None = None
    normalized_domain: str | None = Field(default=None, min_length=1, max_length=255)
    path_pattern: str | None = Field(default=None, max_length=500)
    first_campaign_id: int | None = None
    last_campaign_id: int | None = None
    first_run_id: int | None = None
    last_run_id: int | None = None
    parent_url: str | None = None
    discovery_method: str | None = Field(default=None, max_length=50)
    candidate_type: str | None = Field(default=None, max_length=60)
    format_hint: str | None = Field(default=None, max_length=60)
    footprint_kind: str | None = Field(default=None, max_length=40)
    footprint_geojson: dict[str, Any] | None = None
    geo_hints_json: dict[str, Any] | None = None
    temporal_hints_json: dict[str, Any] | None = None
    format_hints_json: dict[str, Any] | None = None
    trust_hints_json: dict[str, Any] | None = None
    operational_hints_json: dict[str, Any] | None = None
    promotion_json: dict[str, Any] | None = None
    status: str | None = Field(default=None, max_length=40)
    score: float | None = Field(default=None, ge=0.0, le=100.0)
    score_bucket: str | None = Field(default=None, max_length=40)
    score_breakdown_json: dict[str, Any] | None = None
    content_hash: str | None = Field(default=None, max_length=64)
    schema_hash: str | None = Field(default=None, max_length=64)
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    last_changed_at: datetime | None = None
    last_checked_at: datetime | None = None
    last_revisited_at: datetime | None = None
    next_revisit_at: datetime | None = None
    revisit_count: int | None = Field(default=None, ge=0)
    failure_count: int | None = Field(default=None, ge=0)
    last_failure_at: datetime | None = None
    last_error_text: str | None = None
    promoted_source_id: int | None = None
    metadata_json: dict[str, Any] | None = None


class SourceCandidateRead(SourceCandidateCreate):
    candidate_id: int
    first_seen_at: datetime
    last_seen_at: datetime
    created_at: datetime
    updated_at: datetime


class DiscoveryFrontierEntryCreate(ForteModel):
    discovery_run_id: int
    campaign_id: int
    candidate_id: int | None = None
    canonical_url_hash: str = Field(min_length=64, max_length=64)
    canonical_url: str
    discovered_url: str
    priority: float = 0.0
    state: str = Field(default="queued", max_length=30)
    attempt_count: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=3, ge=1, le=20)
    depth: int = Field(default=0, ge=0, le=100)
    parent_url: str | None = None
    parent_candidate_id: int | None = None
    discovery_method: str = Field(default="unknown", max_length=50)
    next_attempt_at: datetime | None = None
    last_attempt_at: datetime | None = None
    claimed_at: datetime | None = None
    fetched_at: datetime | None = None
    completed_at: datetime | None = None
    dead_lettered_at: datetime | None = None
    checkpoint_json: dict[str, Any] = Field(default_factory=dict)
    last_error_text: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class DiscoveryFrontierEntryUpdate(ForteModel):
    candidate_id: int | None = None
    priority: float | None = None
    state: str | None = Field(default=None, max_length=30)
    attempt_count: int | None = Field(default=None, ge=0)
    max_attempts: int | None = Field(default=None, ge=1, le=20)
    depth: int | None = Field(default=None, ge=0, le=100)
    parent_url: str | None = None
    parent_candidate_id: int | None = None
    discovery_method: str | None = Field(default=None, max_length=50)
    next_attempt_at: datetime | None = None
    last_attempt_at: datetime | None = None
    claimed_at: datetime | None = None
    fetched_at: datetime | None = None
    completed_at: datetime | None = None
    dead_lettered_at: datetime | None = None
    checkpoint_json: dict[str, Any] | None = None
    last_error_text: str | None = None
    metadata_json: dict[str, Any] | None = None


class DiscoveryFrontierEntryRead(DiscoveryFrontierEntryCreate):
    frontier_entry_id: int
    created_at: datetime
    updated_at: datetime


class SourceCandidateRevisionCreate(ForteModel):
    candidate_id: int
    campaign_id: int | None = None
    discovery_run_id: int | None = None
    revision_number: int = Field(ge=1)
    revision_kind: str = Field(default="observed", max_length=40)
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = Field(default="candidate", max_length=40)
    score: float = Field(default=0.0, ge=0.0, le=100.0)
    score_bucket: str = Field(default="keep_candidate", max_length=40)
    score_breakdown_json: dict[str, Any] = Field(default_factory=dict)
    content_hash: str | None = Field(default=None, max_length=64)
    schema_hash: str | None = Field(default=None, max_length=64)
    changed: bool = False
    reason: str = ""
    snapshot_json: dict[str, Any] = Field(default_factory=dict)
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class SourceCandidateRevisionUpdate(ForteModel):
    revision_kind: str | None = Field(default=None, max_length=40)
    observed_at: datetime | None = None
    status: str | None = Field(default=None, max_length=40)
    score: float | None = Field(default=None, ge=0.0, le=100.0)
    score_bucket: str | None = Field(default=None, max_length=40)
    score_breakdown_json: dict[str, Any] | None = None
    content_hash: str | None = Field(default=None, max_length=64)
    schema_hash: str | None = Field(default=None, max_length=64)
    changed: bool | None = None
    reason: str | None = None
    snapshot_json: dict[str, Any] | None = None
    metadata_json: dict[str, Any] | None = None


class SourceCandidateRevisionRead(SourceCandidateRevisionCreate):
    candidate_revision_id: int
    observed_at: datetime
    created_at: datetime


class DiscoveryGraphEdgeCreate(ForteModel):
    edge_key: str = Field(min_length=64, max_length=64)
    campaign_id: int
    discovery_run_id: int
    parent_candidate_id: int | None = None
    child_candidate_id: int
    parent_url: str | None = None
    child_url: str
    edge_type: str = Field(default="discovered_from", max_length=40)
    discovery_method: str = Field(default="unknown", max_length=50)
    depth: int = Field(default=0, ge=0)
    evidence_json: dict[str, Any] = Field(default_factory=dict)
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class DiscoveryGraphEdgeUpdate(ForteModel):
    edge_type: str | None = Field(default=None, max_length=40)
    discovery_method: str | None = Field(default=None, max_length=50)
    depth: int | None = Field(default=None, ge=0)
    evidence_json: dict[str, Any] | None = None
    metadata_json: dict[str, Any] | None = None


class DiscoveryGraphEdgeRead(DiscoveryGraphEdgeCreate):
    graph_edge_id: int
    created_at: datetime


class CandidateHealthCheckCreate(ForteModel):
    candidate_id: int
    discovery_run_id: int | None = None
    checked_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = Field(default="unknown", max_length=30)
    reachable: bool = False
    http_status: int | None = None
    latency_ms: float | None = Field(default=None, ge=0.0)
    content_type: str | None = Field(default=None, max_length=160)
    content_length: int | None = Field(default=None, ge=0)
    content_hash: str | None = Field(default=None, max_length=64)
    schema_hash: str | None = Field(default=None, max_length=64)
    changed: bool = False
    redirect_url: str | None = None
    robots_allowed: bool | None = None
    error_type: str | None = Field(default=None, max_length=80)
    error_text: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class CandidateHealthCheckUpdate(ForteModel):
    checked_at: datetime | None = None
    status: str | None = Field(default=None, max_length=30)
    reachable: bool | None = None
    http_status: int | None = None
    latency_ms: float | None = Field(default=None, ge=0.0)
    content_type: str | None = Field(default=None, max_length=160)
    content_length: int | None = Field(default=None, ge=0)
    content_hash: str | None = Field(default=None, max_length=64)
    schema_hash: str | None = Field(default=None, max_length=64)
    changed: bool | None = None
    redirect_url: str | None = None
    robots_allowed: bool | None = None
    error_type: str | None = Field(default=None, max_length=80)
    error_text: str | None = None
    metadata_json: dict[str, Any] | None = None


class CandidateHealthCheckRead(CandidateHealthCheckCreate):
    health_check_id: int
    checked_at: datetime
    created_at: datetime


class CandidateHealthScanRequest(ForteModel):
    candidate_id: int | None = None
    normalized_domain: str | None = None
    campaign_id: int | None = None
    limit: int = Field(default=100, ge=1, le=5000)
    actor: str = Field(default="operator", max_length=80)


class CandidateHealthScanResultRead(ForteModel):
    checked_at: datetime
    checked_count: int
    reachable_count: int
    failing_count: int
    changed_count: int
    checks: list[CandidateHealthCheckRead] = Field(default_factory=list)


class CandidateSuppressionCreate(ForteModel):
    candidate_id: int | None = None
    normalized_domain: str | None = Field(default=None, max_length=255)
    scope: str = Field(default="candidate", max_length=30)
    status: str = Field(default="active", max_length=30)
    reason_code: str = Field(default="operator_suppressed", max_length=60)
    reason: str = ""
    actor: str = Field(default="system", max_length=80)
    expires_at: datetime | None = None
    revoked_at: datetime | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class CandidateSuppressionUpdate(ForteModel):
    status: str | None = Field(default=None, max_length=30)
    reason_code: str | None = Field(default=None, max_length=60)
    reason: str | None = None
    actor: str | None = Field(default=None, max_length=80)
    expires_at: datetime | None = None
    revoked_at: datetime | None = None
    metadata_json: dict[str, Any] | None = None


class CandidateSuppressionRead(CandidateSuppressionCreate):
    suppression_id: int
    created_at: datetime


class CandidateSuppressionRequest(ForteModel):
    candidate_id: int | None = None
    normalized_domain: str | None = Field(default=None, max_length=255)
    scope: str = Field(default="candidate", max_length=30)
    reason_code: str = Field(default="operator_suppressed", max_length=60)
    reason: str = ""
    actor: str = Field(default="operator", max_length=80)
    expires_at: datetime | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class CandidatePromotionDecisionCreate(ForteModel):
    candidate_id: int
    source_id: int | None = None
    discovery_run_id: int | None = None
    decision: str = Field(default="deferred", max_length=40)
    recommended_source_kind: str | None = Field(default=None, max_length=40)
    recommended_schedule_json: dict[str, Any] = Field(default_factory=dict)
    score: float = Field(default=0.0, ge=0.0, le=100.0)
    score_bucket: str = Field(default="keep_candidate", max_length=40)
    score_breakdown_json: dict[str, Any] = Field(default_factory=dict)
    reason: str = ""
    trust_reasoning_json: dict[str, Any] = Field(default_factory=dict)
    integrity_reasoning_json: dict[str, Any] = Field(default_factory=dict)
    health_risks_json: dict[str, Any] = Field(default_factory=dict)
    geo_relevance_json: dict[str, Any] = Field(default_factory=dict)
    evidence_json: dict[str, Any] = Field(default_factory=dict)
    actor: str = Field(default="system", max_length=80)
    decided_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class CandidatePromotionDecisionUpdate(ForteModel):
    source_id: int | None = None
    decision: str | None = Field(default=None, max_length=40)
    recommended_source_kind: str | None = Field(default=None, max_length=40)
    recommended_schedule_json: dict[str, Any] | None = None
    score: float | None = Field(default=None, ge=0.0, le=100.0)
    score_bucket: str | None = Field(default=None, max_length=40)
    score_breakdown_json: dict[str, Any] | None = None
    reason: str | None = None
    trust_reasoning_json: dict[str, Any] | None = None
    integrity_reasoning_json: dict[str, Any] | None = None
    health_risks_json: dict[str, Any] | None = None
    geo_relevance_json: dict[str, Any] | None = None
    evidence_json: dict[str, Any] | None = None
    actor: str | None = Field(default=None, max_length=80)
    decided_at: datetime | None = None
    metadata_json: dict[str, Any] | None = None


class CandidatePromotionDecisionRead(CandidatePromotionDecisionCreate):
    promotion_decision_id: int
    decided_at: datetime


class CandidatePromotionRequest(ForteModel):
    candidate_id: int | None = None
    source_kind: str | None = Field(default=None, max_length=40)
    recommended_source_kind: str | None = Field(default=None, max_length=40)
    source_name: str | None = Field(default=None, max_length=160)
    layer_key: str | None = Field(default=None, max_length=80)
    enabled: bool = True
    integrity_source: bool = False
    create_schedule: bool = True
    schedule_interval_seconds: int | None = Field(default=None, ge=60)
    reason: str = ""
    actor: str = Field(default="operator", max_length=80)
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class CandidatePromotionResultRead(ForteModel):
    candidate: SourceCandidateRead
    decision: CandidatePromotionDecisionRead
    source: SourceDefinitionRead | None = None
    scheduled_task: ScheduledTaskRead | None = None


class RobotsObservationCreate(ForteModel):
    domain_policy_id: int | None = None
    discovery_run_id: int | None = None
    normalized_domain: str = Field(max_length=255)
    robots_url: str
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime | None = None
    status: str = Field(default="unknown", max_length=30)
    http_status: int | None = None
    allowed: bool | None = None
    crawl_delay_seconds: float | None = Field(default=None, ge=0.0)
    sitemap_urls_json: list[str] = Field(default_factory=list)
    rules_json: dict[str, Any] = Field(default_factory=dict)
    content_hash: str | None = Field(default=None, max_length=64)
    error_text: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class RobotsObservationUpdate(ForteModel):
    domain_policy_id: int | None = None
    discovery_run_id: int | None = None
    fetched_at: datetime | None = None
    expires_at: datetime | None = None
    status: str | None = Field(default=None, max_length=30)
    http_status: int | None = None
    allowed: bool | None = None
    crawl_delay_seconds: float | None = Field(default=None, ge=0.0)
    sitemap_urls_json: list[str] | None = None
    rules_json: dict[str, Any] | None = None
    content_hash: str | None = Field(default=None, max_length=64)
    error_text: str | None = None
    metadata_json: dict[str, Any] | None = None


class RobotsObservationRead(RobotsObservationCreate):
    robots_observation_id: int
    fetched_at: datetime
    created_at: datetime


class DiscoveryArtifactCreate(ForteModel):
    candidate_id: int | None = None
    discovery_run_id: int | None = None
    frontier_entry_id: int | None = None
    storage_object_id: int | None = None
    artifact_kind: str = Field(default="fetched_document", max_length=50)
    source_url: str
    media_type: str | None = Field(default=None, max_length=160)
    content_hash: str | None = Field(default=None, max_length=64)
    byte_size: int | None = Field(default=None, ge=0)
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    object_uri: str | None = None
    metadata_json: dict[str, Any] = Field(default_factory=dict)


class DiscoveryArtifactUpdate(ForteModel):
    storage_object_id: int | None = None
    artifact_kind: str | None = Field(default=None, max_length=50)
    media_type: str | None = Field(default=None, max_length=160)
    content_hash: str | None = Field(default=None, max_length=64)
    byte_size: int | None = Field(default=None, ge=0)
    fetched_at: datetime | None = None
    object_uri: str | None = None
    metadata_json: dict[str, Any] | None = None


class DiscoveryArtifactRead(DiscoveryArtifactCreate):
    discovery_artifact_id: int
    fetched_at: datetime
    created_at: datetime


class CandidateScoreExplanationRead(ForteModel):
    candidate_id: int
    canonical_url: str
    score: float = Field(ge=0.0, le=100.0)
    score_bucket: str
    score_breakdown_json: dict[str, Any] = Field(default_factory=dict)
    reasons: list[str] = Field(default_factory=list)
    evaluated_at: datetime


class SourceCandidateDetailRead(ForteModel):
    candidate: SourceCandidateRead
    revisions: list[SourceCandidateRevisionRead] = Field(default_factory=list)
    incoming_edges: list[DiscoveryGraphEdgeRead] = Field(default_factory=list)
    outgoing_edges: list[DiscoveryGraphEdgeRead] = Field(default_factory=list)
    health_checks: list[CandidateHealthCheckRead] = Field(default_factory=list)
    suppressions: list[CandidateSuppressionRead] = Field(default_factory=list)
    promotion_decisions: list[CandidatePromotionDecisionRead] = Field(default_factory=list)
    artifacts: list[DiscoveryArtifactRead] = Field(default_factory=list)


class DiscoveryLineageSummaryRead(ForteModel):
    candidate: SourceCandidateRead
    campaigns: list[DiscoveryCampaignRead] = Field(default_factory=list)
    runs: list[DiscoveryRunRead] = Field(default_factory=list)
    incoming_edges: list[DiscoveryGraphEdgeRead] = Field(default_factory=list)
    outgoing_edges: list[DiscoveryGraphEdgeRead] = Field(default_factory=list)
    ancestor_candidate_ids: list[int] = Field(default_factory=list)
    descendant_candidate_ids: list[int] = Field(default_factory=list)


class DiscoveryLineageRead(DiscoveryLineageSummaryRead):
    pass


class SourceCandidateInventorySummaryRead(ForteModel):
    generated_at: datetime
    total_count: int = 0
    active_count: int = 0
    promoted_count: int = 0
    suppressed_count: int = 0
    failing_count: int = 0
    stale_count: int = 0
    due_revisit_count: int = 0
    status_counts: dict[str, int] = Field(default_factory=dict)
    score_bucket_counts: dict[str, int] = Field(default_factory=dict)
    type_counts: dict[str, int] = Field(default_factory=dict)
    format_counts: dict[str, int] = Field(default_factory=dict)
    domain_counts: dict[str, int] = Field(default_factory=dict)


class DiscoveryHealthSummaryRead(ForteModel):
    generated_at: datetime
    status: str = "ok"
    campaign_count: int = 0
    active_campaign_count: int = 0
    running_run_count: int = 0
    candidate_count: int = 0
    reachable_candidate_count: int = 0
    failing_candidate_count: int = 0
    stale_candidate_count: int = 0
    due_revisit_count: int = 0
    queued_frontier_count: int = 0
    dead_letter_frontier_count: int = 0
    robots_block_count: int = 0
    warning_count: int = 0
    warnings: list[str] = Field(default_factory=list)


class DiscoveryOpsSummaryRead(ForteModel):
    generated_at: datetime
    health_summary: DiscoveryHealthSummaryRead
    inventory_summary: SourceCandidateInventorySummaryRead
    campaign_status_counts: dict[str, int] = Field(default_factory=dict)
    run_status_counts: dict[str, int] = Field(default_factory=dict)
    frontier_state_counts: dict[str, int] = Field(default_factory=dict)
    candidate_status_counts: dict[str, int] = Field(default_factory=dict)
    score_bucket_counts: dict[str, int] = Field(default_factory=dict)
    domain_candidate_counts: dict[str, int] = Field(default_factory=dict)
    failing_candidates: list[SourceCandidateRead] = Field(default_factory=list)
    stale_candidates: list[SourceCandidateRead] = Field(default_factory=list)
    due_revisit_candidates: list[SourceCandidateRead] = Field(default_factory=list)
    recent_runs: list[DiscoveryRunRead] = Field(default_factory=list)


class DiscoveryExportSummaryRead(ForteModel):
    generated_at: datetime
    filters_json: dict[str, Any] = Field(default_factory=dict)
    ops_summary: DiscoveryOpsSummaryRead
    campaigns: list[DiscoveryCampaignRead] = Field(default_factory=list)
    runs: list[DiscoveryRunRead] = Field(default_factory=list)
    candidates: list[SourceCandidateRead] = Field(default_factory=list)
    promotion_decisions: list[CandidatePromotionDecisionRead] = Field(default_factory=list)


class DiscoveryInventoryDiffRead(ForteModel):
    generated_at: datetime
    from_at: datetime | None = None
    to_at: datetime | None = None
    added_count: int = 0
    changed_count: int = 0
    promoted_count: int = 0
    suppressed_count: int = 0
    disappeared_count: int = 0
    added_candidate_ids: list[int] = Field(default_factory=list)
    changed_candidate_ids: list[int] = Field(default_factory=list)
    promoted_candidate_ids: list[int] = Field(default_factory=list)
    suppressed_candidate_ids: list[int] = Field(default_factory=list)
    disappeared_candidate_ids: list[int] = Field(default_factory=list)


class DiscoveryRevisitRequest(ForteModel):
    candidate_id: int | None = None
    normalized_domain: str | None = Field(default=None, max_length=255)
    campaign_id: int | None = None
    force: bool = False
    include_suppressed: bool = False
    priority: float = 0.0
    actor: str = Field(default="operator", max_length=80)


class DiscoveryRevisitResultRead(ForteModel):
    requested_at: datetime
    queued_count: int
    candidate_ids: list[int] = Field(default_factory=list)
    frontier_entry_ids: list[int] = Field(default_factory=list)
    skipped_candidate_ids: list[int] = Field(default_factory=list)
