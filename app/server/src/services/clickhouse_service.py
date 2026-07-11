from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import get_settings
from src.models import CustodyLogORM, ObservationORM, StorageObjectORM
from src.services.observation_service import ObservationQueryRecord, extract_observation_timestamp

logger = logging.getLogger(__name__)


def clickhouse_now() -> datetime:
    return datetime.now(timezone.utc)


def build_clickhouse_diagnostics() -> dict[str, object]:
    settings = get_settings()
    warnings: list[str] = []
    notes: list[str] = []
    reachable = False
    version: str | None = None
    current_database: str | None = None
    r2_archive_root = build_r2_archive_root() if settings.clickhouse_r2_configured else None
    r2_storage_root = build_r2_storage_root() if settings.clickhouse_r2_storage_configured else None

    if not settings.clickhouse_enabled:
        notes.append("ClickHouse integration is disabled.")
        return {
            "status": "disabled",
            "enabled": False,
            "clickhouse_url": settings.clickhouse_url,
            "clickhouse_database": settings.clickhouse_database,
            "observation_table": settings.clickhouse_observation_table,
            "storage_object_table": settings.clickhouse_storage_object_table,
            "reachable": False,
            "version": None,
            "current_database": None,
            "storage_policy": settings.clickhouse_effective_storage_policy,
            "storage_mode": settings.clickhouse_r2_storage_mode,
            "r2_configured": settings.clickhouse_r2_configured,
            "r2_endpoint": settings.clickhouse_r2_endpoint,
            "r2_bucket": settings.clickhouse_r2_bucket,
            "r2_region": settings.clickhouse_r2_region,
            "r2_archive_root": r2_archive_root,
            "r2_storage_ready": settings.clickhouse_r2_storage_configured,
            "r2_storage_bucket": settings.clickhouse_r2_storage_bucket_effective,
            "r2_storage_root": r2_storage_root,
            "warnings": warnings,
            "notes": notes,
        }

    if not settings.clickhouse_r2_configured:
        warnings.append("ClickHouse is enabled, but Cloudflare R2 archive settings are incomplete.")
    if settings.clickhouse_r2_storage_mode == "hybrid":
        notes.append(
            "Hybrid mode keeps hot ClickHouse tables local and expects R2 for archive query/rehydration."
        )
    if settings.clickhouse_r2_storage_mode == "r2_disk":
        if settings.clickhouse_r2_storage_configured:
            notes.append(
                "R2 disk mode will provision ClickHouse tables against the configured remote storage policy."
            )
        else:
            warnings.append(
                "ClickHouse R2 disk mode is selected, but the R2 storage settings are incomplete."
            )

    try:
        ping_clickhouse()
        reachable = True
        metadata_rows = execute_clickhouse_query_json(
            "SELECT version() AS version, currentDatabase() AS current_database FORMAT JSONEachRow"
        )
        if metadata_rows:
            version = str(metadata_rows[0].get("version") or "")
            current_database = str(metadata_rows[0].get("current_database") or "")
    except RuntimeError as exc:
        logger.warning("ClickHouse diagnostics request failed: %s", exc)
        warnings.append("ClickHouse endpoint is unreachable or returned an invalid response.")

    return {
        "status": "ok" if reachable else "degraded",
        "enabled": True,
        "clickhouse_url": settings.clickhouse_url,
        "clickhouse_database": settings.clickhouse_database,
        "observation_table": settings.clickhouse_observation_table,
        "storage_object_table": settings.clickhouse_storage_object_table,
        "reachable": reachable,
        "version": version,
        "current_database": current_database,
        "storage_policy": settings.clickhouse_effective_storage_policy,
        "storage_mode": settings.clickhouse_r2_storage_mode,
        "r2_configured": settings.clickhouse_r2_configured,
        "r2_endpoint": settings.clickhouse_r2_endpoint,
        "r2_bucket": settings.clickhouse_r2_bucket,
        "r2_region": settings.clickhouse_r2_region,
        "r2_archive_root": r2_archive_root,
        "r2_storage_ready": settings.clickhouse_r2_storage_configured,
        "r2_storage_bucket": settings.clickhouse_r2_storage_bucket_effective,
        "r2_storage_root": r2_storage_root,
        "warnings": warnings,
        "notes": notes,
    }


def provision_clickhouse_backend(
    session: Session | None = None,
    *,
    actor: str = "clickhouse_operator",
) -> dict[str, object]:
    settings = get_settings()
    ensure_clickhouse_enabled()
    ensure_clickhouse_storage_mode_ready()
    ping_clickhouse()
    execute_clickhouse_sql(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")
    execute_clickhouse_sql(build_observation_table_sql())
    execute_clickhouse_sql(build_storage_object_table_sql())
    result = {
        "provisioned_at": clickhouse_now(),
        "clickhouse_database": settings.clickhouse_database,
        "observation_table": settings.clickhouse_observation_table,
        "storage_object_table": settings.clickhouse_storage_object_table,
        "storage_policy": settings.clickhouse_effective_storage_policy,
        "storage_mode": settings.clickhouse_r2_storage_mode,
    }
    if session is not None:
        session.add(
            CustodyLogORM(
                object_type="clickhouse_backend",
                object_id=settings.clickhouse_database,
                action="clickhouse_provisioned",
                actor=actor,
                details_json={
                    "observation_table": settings.clickhouse_observation_table,
                    "storage_object_table": settings.clickhouse_storage_object_table,
                    "storage_policy": settings.clickhouse_effective_storage_policy,
                    "storage_mode": settings.clickhouse_r2_storage_mode,
                },
            )
        )
        session.commit()
    return result


def sync_runtime_to_clickhouse(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    limit: int = 1000,
    actor: str = "clickhouse_operator",
) -> dict[str, object]:
    provision_clickhouse_backend()
    observations = query_observations_for_clickhouse(
        session,
        layer_key=layer_key,
        source_domain=source_domain,
        limit=limit,
    )
    storage_objects = query_storage_objects_for_clickhouse(session, limit=limit)
    observation_rows = [serialize_observation_row(row) for row in observations]
    storage_object_rows = [serialize_storage_object_row(row) for row in storage_objects]
    if observation_rows:
        execute_clickhouse_json_insert(
            table_name=get_settings().clickhouse_observation_table,
            rows=observation_rows,
        )
    if storage_object_rows:
        execute_clickhouse_json_insert(
            table_name=get_settings().clickhouse_storage_object_table,
            rows=storage_object_rows,
        )

    result = {
        "synced_at": clickhouse_now(),
        "clickhouse_database": get_settings().clickhouse_database,
        "observation_table": get_settings().clickhouse_observation_table,
        "storage_object_table": get_settings().clickhouse_storage_object_table,
        "filters_json": {
            "layer_key": layer_key,
            "source_domain": source_domain,
            "limit": limit,
        },
        "observation_count": len(observation_rows),
        "storage_object_count": len(storage_object_rows),
    }
    session.add(
        CustodyLogORM(
            object_type="clickhouse_backend",
            object_id=get_settings().clickhouse_database,
            action="clickhouse_synced",
            actor=actor,
            details_json={
                "layer_key": layer_key,
                "source_domain": source_domain,
                "limit": limit,
                "observation_count": len(observation_rows),
                "storage_object_count": len(storage_object_rows),
            },
        )
    )
    session.commit()
    return result


def archive_clickhouse_observations_to_r2(
    session: Session,
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    limit: int | None = None,
    actor: str = "clickhouse_operator",
) -> dict[str, object]:
    settings = get_settings()
    ensure_clickhouse_enabled()
    if not settings.clickhouse_r2_configured:
        raise ValueError("ClickHouse R2 archive settings are incomplete.")

    archive_root_url = build_r2_archive_root()
    sql = build_archive_observations_sql(
        archive_root_url=archive_root_url,
        layer_key=layer_key,
        source_domain=source_domain,
        limit=limit,
    )
    execute_clickhouse_sql(
        sql,
        settings_map={"file_like_engine_default_partition_strategy": "wildcard"},
    )
    exported_row_count = count_clickhouse_observation_rows(
        layer_key=layer_key,
        source_domain=source_domain,
        limit=limit,
    )
    result = {
        "archived_at": clickhouse_now(),
        "clickhouse_database": settings.clickhouse_database,
        "observation_table": settings.clickhouse_observation_table,
        "archive_root_url": archive_root_url,
        "partition_strategy": "wildcard",
        "filters_json": {
            "layer_key": layer_key,
            "source_domain": source_domain,
            "limit": limit,
        },
        "exported_row_count": exported_row_count,
        "sql": sql,
    }
    session.add(
        CustodyLogORM(
            object_type="clickhouse_backend",
            object_id=settings.clickhouse_database,
            action="clickhouse_archived_to_r2",
            actor=actor,
            details_json={
                "archive_root_url": archive_root_url,
                "layer_key": layer_key,
                "source_domain": source_domain,
                "limit": limit,
                "exported_row_count": exported_row_count,
            },
        )
    )
    session.commit()
    return result


def build_clickhouse_r2_config_preview() -> dict[str, object]:
    settings = get_settings()
    if not settings.clickhouse_r2_configured:
        raise ValueError("ClickHouse R2 settings are incomplete.")
    archive_root_url = build_r2_archive_root()
    storage_root_url = build_r2_storage_root()
    sample_archive_glob = build_r2_archive_glob_url()
    storage_xml = render_clickhouse_r2_storage_xml()
    return {
        "generated_at": clickhouse_now(),
        "storage_mode": settings.clickhouse_r2_storage_mode,
        "archive_root_url": archive_root_url,
        "storage_root_url": storage_root_url,
        "storage_policy": settings.clickhouse_effective_storage_policy,
        "storage_xml": storage_xml,
        "create_table_sql": build_observation_table_sql(
            storage_policy_override=settings.clickhouse_r2_storage_policy
        ),
        "archive_example_sql": build_archive_observations_sql(archive_root_url=archive_root_url),
        "rehydrate_example_sql": build_rehydrate_observations_sql(sample_archive_glob),
        "direct_query_example_sql": build_r2_direct_query_example_sql(sample_archive_glob),
        "docker_output_path": str(default_clickhouse_r2_config_path()),
    }


def rehydrate_clickhouse_observations_from_r2(
    session: Session,
    *,
    archive_glob_url: str,
    actor: str = "clickhouse_operator",
) -> dict[str, object]:
    settings = get_settings()
    ensure_clickhouse_enabled()
    if not settings.clickhouse_r2_configured:
        raise ValueError("ClickHouse R2 settings are incomplete.")
    validate_r2_archive_glob_url(archive_glob_url)
    provision_clickhouse_backend()
    imported_row_count = count_r2_archive_rows(archive_glob_url)
    sql = build_rehydrate_observations_sql(archive_glob_url)
    execute_clickhouse_sql(sql)
    session.add(
        CustodyLogORM(
            object_type="clickhouse_backend",
            object_id=settings.clickhouse_database,
            action="clickhouse_rehydrated_from_r2",
            actor=actor,
            details_json={
                "archive_glob_url": archive_glob_url,
                "imported_row_count": imported_row_count,
            },
        )
    )
    session.commit()
    return {
        "rehydrated_at": clickhouse_now(),
        "clickhouse_database": settings.clickhouse_database,
        "observation_table": settings.clickhouse_observation_table,
        "archive_glob_url": archive_glob_url,
        "imported_row_count": imported_row_count,
        "sql": sql,
    }


def query_clickhouse_observations(
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    trust_level: str | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 200,
    backend: str = "clickhouse",
    archive_glob_url: str | None = None,
) -> list[ObservationQueryRecord]:
    settings = get_settings()
    ensure_clickhouse_enabled()
    normalized_backend = normalize_clickhouse_query_backend(backend)
    if normalized_backend == "r2_archive":
        archive_source = archive_glob_url or build_r2_archive_glob_url()
        validate_r2_archive_glob_url(archive_source)
        from_clause = (
            "s3("
            f"{to_clickhouse_string(archive_source)}, "
            f"{to_clickhouse_string(settings.clickhouse_r2_access_key_id or '')}, "
            f"{to_clickhouse_string(settings.clickhouse_r2_secret_access_key or '')}, "
            "'Parquet'"
            ")"
        )
    else:
        from_clause = f"{settings.clickhouse_database}.{settings.clickhouse_observation_table}"

    where_clauses = ["1 = 1"]
    if layer_key is not None:
        where_clauses.append(f"layer_key = {to_clickhouse_string(layer_key)}")
    if source_domain is not None:
        where_clauses.append(f"source_domain = {to_clickhouse_string(source_domain)}")
    if trust_level is not None:
        where_clauses.append(f"trust_level = {to_clickhouse_string(trust_level)}")
    if since is not None:
        where_clauses.append(
            "observed_at >= parseDateTime64BestEffort("
            f"{to_clickhouse_string(isoformat_millis(since))})"
        )
    if until is not None:
        where_clauses.append(
            "observed_at <= parseDateTime64BestEffort("
            f"{to_clickhouse_string(isoformat_millis(until))})"
        )
    if None not in {min_lon, min_lat, max_lon, max_lat}:
        where_clauses.extend(
            [
                f"longitude >= {min_lon}",
                f"longitude <= {max_lon}",
                f"latitude >= {min_lat}",
                f"latitude <= {max_lat}",
            ]
        )

    query = (
        "SELECT observation_id, import_run_id, event_id, layer_key, source_domain, source_type, "
        "record_format, trust_level, approval_policy, confidence_score, longitude, latitude, "
        "observed_at, created_at, updated_at, raw_hash, content_text, content_json_json "
        f"FROM {from_clause} "
        f"WHERE {' AND '.join(where_clauses)} "
        "ORDER BY observed_at DESC, observation_id DESC "
        f"LIMIT {max(limit, 1)} FORMAT JSONEachRow"
    )
    rows = execute_clickhouse_query_json(query)
    return [deserialize_clickhouse_observation_row(row) for row in rows]


def ping_clickhouse() -> None:
    response = perform_clickhouse_http_request(path="/ping", method="GET")
    if response.strip() != "Ok.":
        raise RuntimeError(f"ClickHouse ping returned an unexpected response: {response!r}")


def execute_clickhouse_query_json(query: str) -> list[dict[str, Any]]:
    payload = execute_clickhouse_sql(query)
    rows = [line for line in payload.splitlines() if line.strip()]
    return [json.loads(line) for line in rows]


def execute_clickhouse_json_insert(
    *,
    table_name: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    body = (
        f"INSERT INTO {get_settings().clickhouse_database}.{table_name} FORMAT JSONEachRow\n"
        + "\n".join(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in rows)
    )
    execute_clickhouse_sql(body)


def execute_clickhouse_sql(
    query: str,
    *,
    settings_map: dict[str, str] | None = None,
) -> str:
    settings = get_settings()
    ensure_clickhouse_enabled()
    params = {"database": settings.clickhouse_database}
    if settings_map:
        params.update(settings_map)
    return perform_clickhouse_http_request(
        path="/",
        method="POST",
        params=params,
        data=query.encode("utf-8"),
    )


def perform_clickhouse_http_request(
    *,
    path: str,
    method: str,
    params: dict[str, str] | None = None,
    data: bytes | None = None,
) -> str:
    settings = get_settings()
    url = settings.clickhouse_url.rstrip("/") + path
    if params:
        url = f"{url}?{urlencode(params)}"
    request = Request(url, data=data, method=method)
    request.add_header("X-ClickHouse-User", settings.clickhouse_user)
    request.add_header("X-ClickHouse-Key", settings.clickhouse_password)
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urlopen(request, timeout=settings.clickhouse_timeout_seconds) as response:
            return response.read().decode("utf-8")
    except Exception as exc:  # pragma: no cover - platform/network details vary
        raise RuntimeError(f"ClickHouse request failed: {exc}") from exc


def build_observation_table_sql(*, storage_policy_override: str | None = None) -> str:
    table_settings = build_storage_policy_clause(storage_policy_override)
    settings_clause = f"\nSETTINGS {table_settings}" if table_settings else ""
    settings = get_settings()
    return (
        f"CREATE TABLE IF NOT EXISTS {settings.clickhouse_database}.{settings.clickhouse_observation_table} ("
        "observation_id UInt64,"
        "import_run_id Nullable(UInt64),"
        "event_id Nullable(UInt64),"
        "layer_key LowCardinality(String),"
        "source_domain Nullable(String),"
        "source_type LowCardinality(String),"
        "record_format LowCardinality(String),"
        "trust_level LowCardinality(String),"
        "approval_policy LowCardinality(String),"
        "confidence_score Float64,"
        "longitude Nullable(Float64),"
        "latitude Nullable(Float64),"
        "observed_at DateTime64(3, 'UTC'),"
        "created_at DateTime64(3, 'UTC'),"
        "updated_at DateTime64(3, 'UTC'),"
        "raw_hash String,"
        "content_text String,"
        "content_json_json String"
        ") ENGINE = ReplacingMergeTree(updated_at) "
        "PARTITION BY toYYYYMM(observed_at) "
        "ORDER BY (layer_key, observed_at, observation_id)"
        f"{settings_clause}"
    )


def build_storage_object_table_sql(*, storage_policy_override: str | None = None) -> str:
    table_settings = build_storage_policy_clause(storage_policy_override)
    settings_clause = f"\nSETTINGS {table_settings}" if table_settings else ""
    settings = get_settings()
    return (
        f"CREATE TABLE IF NOT EXISTS {settings.clickhouse_database}.{settings.clickhouse_storage_object_table} ("
        "storage_object_id UInt64,"
        "object_key String,"
        "object_kind LowCardinality(String),"
        "owner_type LowCardinality(String),"
        "owner_id String,"
        "content_hash Nullable(String),"
        "media_type Nullable(String),"
        "storage_tier LowCardinality(String),"
        "retention_class LowCardinality(String),"
        "lifecycle_status LowCardinality(String),"
        "source_uri Nullable(String),"
        "object_uri String,"
        "byte_size Nullable(Int64),"
        "observed_at Nullable(DateTime64(3, 'UTC')),"
        "expires_at Nullable(DateTime64(3, 'UTC')),"
        "promoted_by_type Nullable(String),"
        "promoted_by_id Nullable(String),"
        "degraded_from_storage_object_id Nullable(UInt64),"
        "created_at DateTime64(3, 'UTC'),"
        "updated_at DateTime64(3, 'UTC'),"
        "metadata_json_json String"
        ") ENGINE = ReplacingMergeTree(updated_at) "
        "PARTITION BY toYYYYMM(created_at) "
        "ORDER BY (owner_type, owner_id, storage_object_id)"
        f"{settings_clause}"
    )


def build_storage_policy_clause(storage_policy_override: str | None = None) -> str | None:
    storage_policy = storage_policy_override or get_settings().clickhouse_effective_storage_policy
    if not storage_policy:
        return None
    return f"storage_policy = '{storage_policy}'"


def build_archive_observations_sql(
    *,
    archive_root_url: str,
    layer_key: str | None = None,
    source_domain: str | None = None,
    limit: int | None = None,
) -> str:
    settings = get_settings()
    structure = (
        "observation_id UInt64, import_run_id Nullable(UInt64), event_id Nullable(UInt64), "
        "layer_key String, source_domain Nullable(String), source_type String, record_format String, "
        "trust_level String, approval_policy String, confidence_score Float64, "
        "longitude Nullable(Float64), latitude Nullable(Float64), observed_at DateTime64(3, 'UTC'), "
        "created_at DateTime64(3, 'UTC'), updated_at DateTime64(3, 'UTC'), raw_hash String, "
        "content_text String, content_json_json String"
    )
    where_clauses = ["1 = 1"]
    if layer_key is not None:
        where_clauses.append(f"layer_key = {to_clickhouse_string(layer_key)}")
    if source_domain is not None:
        where_clauses.append(f"source_domain = {to_clickhouse_string(source_domain)}")
    where_sql = " AND ".join(where_clauses)
    limit_sql = f"\nLIMIT {max(limit, 0)}" if limit is not None else ""
    return (
        "INSERT INTO FUNCTION s3("
        f"'{archive_root_url.rstrip('/')}/observations/{{_partition_id}}/part.parquet', "
        f"'{settings.clickhouse_r2_access_key_id}', "
        f"'{settings.clickhouse_r2_secret_access_key}', "
        f"'Parquet', '{structure}'"
        ")\n"
        "PARTITION BY concat("
        "'layer=', replaceRegexpAll(layer_key, '[^A-Za-z0-9_-]+', '_'), "
        "'/date=', formatDateTime(observed_at, '%Y-%m-%d')"
        ")\n"
        "SELECT observation_id, import_run_id, event_id, layer_key, source_domain, source_type, "
        "record_format, trust_level, approval_policy, confidence_score, longitude, latitude, "
        "observed_at, created_at, updated_at, raw_hash, content_text, content_json_json "
        f"FROM {settings.clickhouse_database}.{settings.clickhouse_observation_table}\n"
        f"WHERE {where_sql}"
        f"{limit_sql}"
    )


def build_rehydrate_observations_sql(archive_glob_url: str) -> str:
    settings = get_settings()
    return (
        f"INSERT INTO {settings.clickhouse_database}.{settings.clickhouse_observation_table}\n"
        "SELECT observation_id, import_run_id, event_id, layer_key, source_domain, source_type, "
        "record_format, trust_level, approval_policy, confidence_score, longitude, latitude, "
        "observed_at, created_at, updated_at, raw_hash, content_text, content_json_json\n"
        "FROM s3("
        f"{to_clickhouse_string(archive_glob_url)}, "
        f"{to_clickhouse_string(settings.clickhouse_r2_access_key_id or '')}, "
        f"{to_clickhouse_string(settings.clickhouse_r2_secret_access_key or '')}, "
        "'Parquet'"
        ")"
    )


def build_r2_direct_query_example_sql(archive_glob_url: str) -> str:
    settings = get_settings()
    return (
        "SELECT layer_key, count(*) AS row_count\n"
        "FROM s3("
        f"{to_clickhouse_string(archive_glob_url)}, "
        f"{to_clickhouse_string(settings.clickhouse_r2_access_key_id or '')}, "
        f"{to_clickhouse_string(settings.clickhouse_r2_secret_access_key or '')}, "
        "'Parquet'"
        ")\n"
        "GROUP BY layer_key\n"
        "ORDER BY row_count DESC\n"
        "LIMIT 100"
    )


def count_r2_archive_rows(archive_glob_url: str) -> int:
    rows = execute_clickhouse_query_json(
        "SELECT count(*) AS row_count FROM s3("
        f"{to_clickhouse_string(archive_glob_url)}, "
        f"{to_clickhouse_string(get_settings().clickhouse_r2_access_key_id or '')}, "
        f"{to_clickhouse_string(get_settings().clickhouse_r2_secret_access_key or '')}, "
        "'Parquet'"
        ") FORMAT JSONEachRow"
    )
    return int(rows[0]["row_count"]) if rows else 0


def count_clickhouse_observation_rows(
    *,
    layer_key: str | None = None,
    source_domain: str | None = None,
    limit: int | None = None,
) -> int:
    settings = get_settings()
    where_clauses = ["1 = 1"]
    if layer_key is not None:
        where_clauses.append(f"layer_key = {to_clickhouse_string(layer_key)}")
    if source_domain is not None:
        where_clauses.append(f"source_domain = {to_clickhouse_string(source_domain)}")
    where_sql = " AND ".join(where_clauses)
    limit_sql = f" LIMIT {max(limit, 0)}" if limit is not None else ""
    rows = execute_clickhouse_query_json(
        "SELECT count(*) AS row_count FROM ("
        "SELECT observation_id "
        f"FROM {settings.clickhouse_database}.{settings.clickhouse_observation_table} "
        f"WHERE {where_sql}{limit_sql}"
        ") FORMAT JSONEachRow"
    )
    return int(rows[0]["row_count"]) if rows else 0


def query_observations_for_clickhouse(
    session: Session,
    *,
    layer_key: str | None,
    source_domain: str | None,
    limit: int,
) -> list[ObservationORM]:
    statement = select(ObservationORM).order_by(ObservationORM.updated_at.desc())
    if layer_key:
        statement = statement.where(ObservationORM.layer_key == layer_key)
    if source_domain:
        statement = statement.where(ObservationORM.source_domain == source_domain)
    statement = statement.limit(limit)
    return list(session.scalars(statement))


def query_storage_objects_for_clickhouse(session: Session, *, limit: int) -> list[StorageObjectORM]:
    statement = select(StorageObjectORM).order_by(StorageObjectORM.updated_at.desc()).limit(limit)
    return list(session.scalars(statement))


def serialize_observation_row(row: ObservationORM) -> dict[str, Any]:
    coordinates = (row.location_geojson or {}).get("coordinates")
    longitude = (
        float(coordinates[0]) if isinstance(coordinates, list) and len(coordinates) >= 2 else None
    )
    latitude = (
        float(coordinates[1]) if isinstance(coordinates, list) and len(coordinates) >= 2 else None
    )
    observed_at = extract_observation_timestamp(row) or row.created_at
    return {
        "observation_id": row.observation_id,
        "import_run_id": row.import_run_id,
        "event_id": row.event_id,
        "layer_key": row.layer_key,
        "source_domain": row.source_domain,
        "source_type": row.source_type,
        "record_format": row.record_format,
        "trust_level": row.trust_level,
        "approval_policy": row.approval_policy,
        "confidence_score": row.confidence_score,
        "longitude": longitude,
        "latitude": latitude,
        "observed_at": isoformat_millis(observed_at),
        "created_at": isoformat_millis(row.created_at),
        "updated_at": isoformat_millis(row.updated_at),
        "raw_hash": row.raw_hash,
        "content_text": row.content_text,
        "content_json_json": json.dumps(row.content_json, separators=(",", ":"), sort_keys=True),
    }


def serialize_storage_object_row(row: StorageObjectORM) -> dict[str, Any]:
    return {
        "storage_object_id": row.storage_object_id,
        "object_key": row.object_key,
        "object_kind": row.object_kind,
        "owner_type": row.owner_type,
        "owner_id": row.owner_id,
        "content_hash": row.content_hash,
        "media_type": row.media_type,
        "storage_tier": row.storage_tier,
        "retention_class": row.retention_class,
        "lifecycle_status": row.lifecycle_status,
        "source_uri": row.source_uri,
        "object_uri": row.object_uri,
        "byte_size": row.byte_size,
        "observed_at": isoformat_millis_nullable(row.observed_at),
        "expires_at": isoformat_millis_nullable(row.expires_at),
        "promoted_by_type": row.promoted_by_type,
        "promoted_by_id": row.promoted_by_id,
        "degraded_from_storage_object_id": row.degraded_from_storage_object_id,
        "created_at": isoformat_millis(row.created_at),
        "updated_at": isoformat_millis(row.updated_at),
        "metadata_json_json": json.dumps(row.metadata_json, separators=(",", ":"), sort_keys=True),
    }


def isoformat_millis(value: datetime) -> str:
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def isoformat_millis_nullable(value: datetime | None) -> str | None:
    return isoformat_millis(value) if value is not None else None


def build_r2_archive_root() -> str:
    settings = get_settings()
    if not settings.clickhouse_r2_configured:
        raise ValueError("ClickHouse R2 archive settings are incomplete.")
    return (
        f"{settings.clickhouse_r2_endpoint.rstrip('/')}/"
        f"{settings.clickhouse_r2_bucket}/"
        f"{settings.clickhouse_r2_archive_prefix.strip('/')}"
    )


def build_r2_storage_root() -> str:
    settings = get_settings()
    if not settings.clickhouse_r2_storage_configured:
        raise ValueError("ClickHouse R2 storage settings are incomplete.")
    return (
        f"{settings.clickhouse_r2_endpoint.rstrip('/')}/"
        f"{settings.clickhouse_r2_storage_bucket_effective}/"
        f"{settings.clickhouse_r2_storage_prefix.strip('/')}"
    )


def build_r2_archive_glob_url() -> str:
    return f"{build_r2_archive_root().rstrip('/')}/observations/layer=*/date=*/*.parquet"


def render_clickhouse_r2_storage_xml() -> str:
    settings = get_settings()
    storage_root_url = build_r2_storage_root()
    return (
        "<clickhouse>\n"
        "  <storage_configuration>\n"
        "    <disks>\n"
        "      <r2_disk>\n"
        "        <type>object_storage</type>\n"
        "        <object_storage_type>s3</object_storage_type>\n"
        "        <metadata_type>local</metadata_type>\n"
        f"        <endpoint>{storage_root_url.rstrip('/')}/</endpoint>\n"
        f"        <access_key_id>{settings.clickhouse_r2_access_key_id}</access_key_id>\n"
        f"        <secret_access_key>{settings.clickhouse_r2_secret_access_key}</secret_access_key>\n"
        f"        <region>{settings.clickhouse_r2_region}</region>\n"
        "        <metadata_path>/var/lib/clickhouse/disks/r2_disk/</metadata_path>\n"
        "      </r2_disk>\n"
        "      <r2_cache>\n"
        "        <type>cache</type>\n"
        "        <disk>r2_disk</disk>\n"
        "        <path>/var/lib/clickhouse/disks/r2_cache/</path>\n"
        f"        <max_size>{settings.clickhouse_r2_cache_size}</max_size>\n"
        "      </r2_cache>\n"
        "    </disks>\n"
        "    <policies>\n"
        f"      <{settings.clickhouse_r2_storage_policy}>\n"
        "        <volumes>\n"
        "          <main>\n"
        "            <disk>r2_cache</disk>\n"
        "          </main>\n"
        "        </volumes>\n"
        f"      </{settings.clickhouse_r2_storage_policy}>\n"
        "    </policies>\n"
        "  </storage_configuration>\n"
        "</clickhouse>"
    )


def default_clickhouse_r2_config_path() -> Path:
    repo_root_candidate = Path.cwd() / "app" / "server" / "11writer-r2-storage.xml"
    if (Path.cwd() / "docker-compose.yml").exists() and repo_root_candidate.parent.exists():
        return repo_root_candidate
    return Path("./11writer-r2-storage.xml")


def validate_r2_archive_glob_url(archive_glob_url: str) -> None:
    if not archive_glob_url.strip():
        raise ValueError("archive_glob_url is required.")
    endpoint = (get_settings().clickhouse_r2_endpoint or "").rstrip("/")
    if endpoint and not archive_glob_url.startswith(f"{endpoint}/"):
        raise ValueError("archive_glob_url must target the configured Cloudflare R2 endpoint.")


def ensure_clickhouse_storage_mode_ready() -> None:
    settings = get_settings()
    if (
        settings.clickhouse_r2_storage_mode == "r2_disk"
        and not settings.clickhouse_r2_storage_configured
    ):
        raise ValueError("ClickHouse R2 disk mode requires complete R2 storage settings.")


def ensure_clickhouse_enabled() -> None:
    if not get_settings().clickhouse_configured:
        raise ValueError("ClickHouse integration is not enabled or not fully configured.")


def normalize_clickhouse_query_backend(backend: str) -> str:
    normalized = backend.strip().lower()
    if normalized not in {"clickhouse", "r2_archive"}:
        raise ValueError("ClickHouse observation backend must be 'clickhouse' or 'r2_archive'.")
    if normalized == "r2_archive" and not get_settings().clickhouse_r2_configured:
        raise ValueError("ClickHouse R2 archive settings are incomplete.")
    return normalized


def deserialize_clickhouse_observation_row(row: dict[str, Any]) -> ObservationQueryRecord:
    longitude = normalize_clickhouse_float(row.get("longitude"))
    latitude = normalize_clickhouse_float(row.get("latitude"))
    location_geojson = None
    if longitude is not None and latitude is not None:
        location_geojson = {"type": "Point", "coordinates": [longitude, latitude]}

    raw_content_json = row.get("content_json_json")
    content_json = (
        json.loads(raw_content_json)
        if isinstance(raw_content_json, str) and raw_content_json
        else {}
    )
    if not isinstance(content_json, dict):
        content_json = {}

    return ObservationQueryRecord(
        observation_id=int(row["observation_id"]),
        import_run_id=normalize_clickhouse_int(row.get("import_run_id")),
        event_id=normalize_clickhouse_int(row.get("event_id")),
        layer_key=str(row.get("layer_key") or ""),
        source_domain=str(row["source_domain"]) if row.get("source_domain") is not None else None,
        source_type=str(row.get("source_type") or ""),
        record_format=str(row.get("record_format") or ""),
        trust_level=str(row.get("trust_level") or "neutral"),
        approval_policy=str(row.get("approval_policy") or "manual_review"),
        confidence_score=normalize_clickhouse_float(row.get("confidence_score")) or 0.0,
        location_geojson=location_geojson,
        content_text=str(row.get("content_text") or ""),
        content_json=content_json,
        raw_hash=str(row.get("raw_hash") or ""),
        created_at=parse_clickhouse_datetime(row.get("created_at")),
        updated_at=parse_clickhouse_datetime(row.get("updated_at")),
    )


def normalize_clickhouse_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def normalize_clickhouse_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def parse_clickhouse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        raise ValueError("ClickHouse observation row is missing a timestamp field.")
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def to_clickhouse_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
