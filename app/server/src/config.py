from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

APP_SERVER_ROOT = Path(__file__).resolve().parents[1]


def _resolve_runtime_path(path: Path, *, base_dir: Path | None = None) -> Path:
    resolved_base_dir = (base_dir or APP_SERVER_ROOT).resolve()
    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded.resolve()
    return (resolved_base_dir / expanded).resolve()


def _resolve_sqlite_url(database_url: str, *, base_dir: Path | None = None) -> str:
    for prefix in ("sqlite:///", "sqlite+pysqlite:///"):
        if not database_url.startswith(prefix):
            continue
        raw_path = database_url[len(prefix) :]
        if raw_path == ":memory:" or raw_path.startswith("file:"):
            return database_url
        return f"{prefix}{_resolve_runtime_path(Path(raw_path), base_dir=base_dir).as_posix()}"
    return database_url


class Settings(BaseSettings):
    app_name: str = "11Writer Forte"
    app_version: str = "0.1.0"
    app_env: str = "local"
    api_prefix: str = "/api"
    database_url: str = "sqlite:///./var/11writer_forte.db"
    database_auto_migrate: bool = False
    data_dir: Path = Field(default=Path("./var"))
    storage_archive_backend: Literal["local", "r2"] = "local"
    storage_archive_dir: Path = Field(default=Path("artifacts/archive"))
    storage_rehydrate_dir: Path = Field(default=Path("artifacts/rehydrated"))
    storage_s3_endpoint: str | None = None
    storage_s3_bucket: str | None = None
    storage_s3_access_key_id: str | None = None
    storage_s3_secret_access_key: str | None = None
    storage_s3_region: str = "auto"
    storage_s3_prefix: str = "11writer-artifacts"
    import_row_limit: int = 5000
    scheduler_poll_seconds: float = 30.0
    source_fetch_max_payload_bytes: int = 5_000_000
    source_fetch_allow_private_networks: bool = True
    source_fetch_min_request_interval_seconds: float = 0.0
    runtime_snapshot_max_age_hours: float = 168.0
    allowed_origins: list[str] = Field(default_factory=list)
    clickhouse_enabled: bool = False
    clickhouse_url: str = "http://127.0.0.1:8123"
    clickhouse_database: str = "elevenwriter"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_timeout_seconds: float = 30.0
    clickhouse_observation_table: str = "observation_facts"
    clickhouse_storage_object_table: str = "storage_object_facts"
    clickhouse_storage_policy: str | None = None
    clickhouse_r2_endpoint: str | None = None
    clickhouse_r2_bucket: str | None = None
    clickhouse_r2_access_key_id: str | None = None
    clickhouse_r2_secret_access_key: str | None = None
    clickhouse_r2_region: str = "auto"
    clickhouse_r2_archive_prefix: str = "11writer-archive"
    clickhouse_r2_storage_mode: Literal["archive_only", "hybrid", "r2_disk"] = "archive_only"
    clickhouse_r2_storage_bucket: str | None = None
    clickhouse_r2_storage_prefix: str = "11writer-clickhouse"
    clickhouse_r2_storage_policy: str = "r2_main"
    clickhouse_r2_cache_size: str = "10Gi"

    model_config = SettingsConfigDict(
        env_prefix="ELEVENWRITER_",
        env_file=".env",
        extra="ignore",
    )

    def ensure_runtime_dirs(self) -> None:
        self.data_dir_effective.mkdir(parents=True, exist_ok=True)
        self.storage_archive_dir_effective.mkdir(parents=True, exist_ok=True)
        self.storage_rehydrate_dir_effective.mkdir(parents=True, exist_ok=True)

    @property
    def sqlalchemy_connect_args(self) -> dict[str, object]:
        if self.database_url_effective.startswith("sqlite"):
            return {"check_same_thread": False}
        return {}

    @property
    def uses_postgres(self) -> bool:
        return self.database_url_effective.startswith("postgresql")

    @property
    def data_dir_effective(self) -> Path:
        return _resolve_runtime_path(self.data_dir)

    @property
    def database_url_effective(self) -> str:
        return _resolve_sqlite_url(self.database_url)

    @property
    def spatial_backend(self) -> str:
        return "postgis" if self.uses_postgres else "python"

    @property
    def clickhouse_configured(self) -> bool:
        return self.clickhouse_enabled and bool(self.clickhouse_url.strip()) and bool(
            self.clickhouse_database.strip()
        )

    @property
    def clickhouse_r2_configured(self) -> bool:
        return (
            self.clickhouse_configured
            and bool(self.clickhouse_r2_endpoint)
            and bool(self.clickhouse_r2_bucket)
            and bool(self.clickhouse_r2_access_key_id)
            and bool(self.clickhouse_r2_secret_access_key)
        )

    @property
    def clickhouse_r2_storage_bucket_effective(self) -> str | None:
        return self.clickhouse_r2_storage_bucket or self.clickhouse_r2_bucket

    @property
    def clickhouse_r2_storage_configured(self) -> bool:
        return self.clickhouse_r2_configured and bool(self.clickhouse_r2_storage_bucket_effective)

    @property
    def clickhouse_effective_storage_policy(self) -> str | None:
        if self.clickhouse_storage_policy:
            return self.clickhouse_storage_policy
        if self.clickhouse_r2_storage_mode == "r2_disk" and self.clickhouse_r2_storage_configured:
            return self.clickhouse_r2_storage_policy
        return None

    @property
    def storage_archive_dir_effective(self) -> Path:
        if self.storage_archive_dir.is_absolute():
            return self.storage_archive_dir.resolve()
        return (self.data_dir_effective / self.storage_archive_dir).resolve()

    @property
    def storage_rehydrate_dir_effective(self) -> Path:
        if self.storage_rehydrate_dir.is_absolute():
            return self.storage_rehydrate_dir.resolve()
        return (self.data_dir_effective / self.storage_rehydrate_dir).resolve()

    @property
    def storage_s3_endpoint_effective(self) -> str | None:
        return self.storage_s3_endpoint or self.clickhouse_r2_endpoint

    @property
    def storage_s3_bucket_effective(self) -> str | None:
        return self.storage_s3_bucket or self.clickhouse_r2_bucket

    @property
    def storage_s3_access_key_id_effective(self) -> str | None:
        return self.storage_s3_access_key_id or self.clickhouse_r2_access_key_id

    @property
    def storage_s3_secret_access_key_effective(self) -> str | None:
        return self.storage_s3_secret_access_key or self.clickhouse_r2_secret_access_key

    @property
    def storage_s3_region_effective(self) -> str:
        return self.storage_s3_region or self.clickhouse_r2_region

    @property
    def storage_r2_configured(self) -> bool:
        return bool(
            self.storage_s3_endpoint_effective
            and self.storage_s3_bucket_effective
            and self.storage_s3_access_key_id_effective
            and self.storage_s3_secret_access_key_effective
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
