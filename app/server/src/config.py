from __future__ import annotations

from functools import lru_cache
from pathlib import Path
<<<<<<< HEAD
=======
from typing import Literal
>>>>>>> 05aeee6 (chore: initialize repository)

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
<<<<<<< HEAD
=======
    clickhouse_r2_storage_mode: Literal["archive_only", "hybrid", "r2_disk"] = "archive_only"
    clickhouse_r2_storage_bucket: str | None = None
    clickhouse_r2_storage_prefix: str = "11writer-clickhouse"
    clickhouse_r2_storage_policy: str = "r2_main"
    clickhouse_r2_cache_size: str = "10Gi"
>>>>>>> 05aeee6 (chore: initialize repository)

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

<<<<<<< HEAD
=======
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

>>>>>>> 05aeee6 (chore: initialize repository)

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
