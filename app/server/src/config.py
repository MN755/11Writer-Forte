from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "11Writer Forte"
    app_version: str = "0.1.0"
    app_env: str = "local"
    api_prefix: str = "/api"
    database_url: str = "sqlite:///./var/11writer_forte.db"
    data_dir: Path = Field(default=Path("./var"))
    import_row_limit: int = 5000
    scheduler_poll_seconds: float = 30.0
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

    model_config = SettingsConfigDict(
        env_prefix="ELEVENWRITER_",
        env_file=".env",
        extra="ignore",
    )

    def ensure_runtime_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @property
    def sqlalchemy_connect_args(self) -> dict[str, object]:
        if self.database_url.startswith("sqlite"):
            return {"check_same_thread": False}
        return {}

    @property
    def uses_postgres(self) -> bool:
        return self.database_url.startswith("postgresql")

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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
