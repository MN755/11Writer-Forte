from __future__ import annotations

from pydantic import Field

from src.types.api import CamelModel


class BackendDatabaseComponentStatus(CamelModel):
    component_key: str
    database_url: str
    database_identity: str
    shared_with: list[str] = Field(default_factory=list)
    dialect: str
    reachable: bool
    bootstrap_strategy: str
    table_count: int = 0
    required_tables: list[str] = Field(default_factory=list)
    missing_tables: list[str] = Field(default_factory=list)
    migration_version: str | None = None
    spatial_backend: str | None = None
    postgis_available: bool | None = None
    caveats: list[str] = Field(default_factory=list)


class BackendDatabaseStatusResponse(CamelModel):
    generated_at: str
    component_count: int
    reachable_component_count: int
    distinct_database_count: int
    bootstrapped_components: list[str] = Field(default_factory=list)
    components: list[BackendDatabaseComponentStatus] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
