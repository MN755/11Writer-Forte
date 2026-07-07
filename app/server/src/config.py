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
    allowed_origins: list[str] = Field(default_factory=list)

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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
