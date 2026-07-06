from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel

from src.config.settings import get_settings


class ForteSettings(BaseModel):
    app_name: str = "11Writer Forte Source Ops API"
    app_version: str = "0.1.0"
    api_prefix: str = "/api/forte"
    database_url: str
    allowed_origins: list[str]

    def ensure_data_dir(self) -> None:
        if self.database_url.startswith("sqlite:///./"):
            Path("data").mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_forte_settings() -> ForteSettings:
    runtime = get_settings()
    return ForteSettings(
        database_url=runtime.database_url,
        allowed_origins=runtime.cors_origins,
    )


settings = get_forte_settings()
