from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db import apply_schema_migrations
from src.models import Base, ResearchProviderORM, ResearchProviderRunORM
from src.services.runtime_snapshot_service import build_runtime_snapshot, restore_runtime_snapshot


def _provider() -> ResearchProviderORM:
    return ResearchProviderORM(
        provider_key="federal-register-api",
        display_name="Federal Register API",
        provider_kind="structured_dataset",
        health_status="healthy",
        base_urls_json=["https://www.federalregister.gov/api/v1/"],
        terms_url="https://www.federalregister.gov/developers/documentation/api/v1",
        license_note="Public API metadata",
        request_budget_json={
            "max_requests_per_run": 10,
            "max_requests_per_day": 100,
            "max_response_bytes": 5_000_000,
        },
        languages_json=["en"],
        capabilities_json=["search", "parse_document"],
        coverage_gaps_json={"jurisdiction:state": {"status": "open"}},
    )


def test_durable_provider_run_persists_lease_budget_and_idempotency() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        provider = _provider()
        session.add(provider)
        session.flush()
        run = ResearchProviderRunORM(
            research_provider_id=provider.research_provider_id,
            idempotency_key="investigation:7:federal-register:query-hash",
            status="leased",
            worker_class="search",
            priority=75,
            attempt_count=1,
            max_attempts=3,
            lease_owner="worker-a",
            lease_acquired_at=datetime.now(timezone.utc),
            lease_expires_at=datetime.now(timezone.utc),
            heartbeat_at=datetime.now(timezone.utc),
            normalized_query="harbor logistics",
            response_hash="a" * 64,
            candidate_urls_json=["https://example.gov/record"],
            coverage_gaps_json={"source:primary": {"status": "open"}},
            budget_json={"investigation_request_remaining": 9},
            request_count=1,
            bytes_collected=128,
        )
        session.add(run)
        session.commit()

        stored = session.scalar(select(ResearchProviderRunORM))
        assert stored is not None
        assert stored.lease_owner == "worker-a"
        assert stored.budget_json["investigation_request_remaining"] == 9
        assert stored.provider.provider_key == "federal-register-api"

        session.add(
            ResearchProviderRunORM(
                research_provider_id=provider.research_provider_id,
                idempotency_key=run.idempotency_key,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()


def test_snapshot_round_trip_includes_provider_and_run_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        provider = _provider()
        session.add(provider)
        session.flush()
        session.add(
            ResearchProviderRunORM(
                research_provider_id=provider.research_provider_id,
                idempotency_key="provider-run:one",
                response_hash="b" * 64,
                coverage_gaps_json={"provider:federal-register-api": {"status": "open"}},
            )
        )
        session.commit()

        snapshot = build_runtime_snapshot(session)
        assert snapshot["snapshot_version"] == 4
        assert snapshot["research_providers"][0]["provider_key"] == "federal-register-api"
        assert snapshot["research_provider_runs"][0]["idempotency_key"] == "provider-run:one"

        restore_runtime_snapshot(session, snapshot, replace_existing=True)
        assert session.scalar(select(ResearchProviderORM).where(ResearchProviderORM.provider_key == "federal-register-api"))
        restored_run = session.scalar(select(ResearchProviderRunORM))
        assert restored_run is not None
        assert restored_run.response_hash == "b" * 64


def test_migration_v2_creates_fleet_tables_once() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        for table_name, columns in {
            "geofences": "geofence_id INTEGER PRIMARY KEY",
            "observations": "observation_id INTEGER PRIMARY KEY",
            "local_import_runs": "local_import_run_id INTEGER PRIMARY KEY",
            "scheduled_tasks": "task_id INTEGER PRIMARY KEY",
            "alerts": "alert_id INTEGER PRIMARY KEY",
            "watches": "watch_id INTEGER PRIMARY KEY",
            "investigations": "investigation_id INTEGER PRIMARY KEY",
        }.items():
            connection.exec_driver_sql(f"CREATE TABLE {table_name} ({columns})")

    assert apply_schema_migrations(engine) == [1, 2]
    assert apply_schema_migrations(engine) == []
    with engine.connect() as connection:
        tables = {row[0] for row in connection.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"research_providers", "research_provider_runs"}.issubset(tables)
