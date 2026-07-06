from __future__ import annotations

from typing import Literal

from pydantic import Field

from src.types.api import CamelModel
from src.types.source_discovery import SourceClass, SourceDiscoveryMemory


LocalDatasetImportFormat = Literal["auto", "json", "jsonl", "txt", "sqlite"]
LocalDatasetImportStatus = Literal["running", "completed", "failed"]
LocalDatasetImportSourceKind = Literal[
    "historical_source",
    "data_feed_source",
    "data_source",
    "integrity_source",
]


class LocalDatasetImportTableSummary(CamelModel):
    table_name: str
    imported_row_count: int


class LocalDatasetImportRequest(CamelModel):
    file_path: str
    source_id: str | None = None
    title: str | None = None
    format: LocalDatasetImportFormat = "auto"
    source_kind: LocalDatasetImportSourceKind = "historical_source"
    source_class: SourceClass = "dataset"
    requested_by: str = "11writer-api"
    encoding: str = "utf-8"
    table_names: list[str] = Field(default_factory=list)
    max_records: int = 100
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)
    caveats: list[str] = Field(default_factory=list)


class LocalDatasetImportRunSummary(CamelModel):
    import_run_id: str
    source_id: str | None = None
    file_path: str
    file_format: LocalDatasetImportFormat | str
    source_kind: LocalDatasetImportSourceKind | str
    requested_by: str
    status: LocalDatasetImportStatus | str
    file_sha256: str | None = None
    file_size_bytes: int | None = None
    imported_record_count: int = 0
    snapshot_count: int = 0
    duplicate_snapshot_count: int = 0
    geospatial_record_count: int = 0
    tables: list[LocalDatasetImportTableSummary] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)
    error_summary: str | None = None
    provenance_event_id: str | None = None
    started_at: str
    finished_at: str | None = None
    caveats: list[str] = Field(default_factory=list)


class LocalDatasetImportResponse(CamelModel):
    run: LocalDatasetImportRunSummary
    memory: SourceDiscoveryMemory
    caveats: list[str] = Field(default_factory=list)


class LocalDatasetImportListResponse(CamelModel):
    count: int
    runs: list[LocalDatasetImportRunSummary] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
