from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import select

from src.db import get_session_factory
from src.models import SituationProductORM
from src.services.alert_service import build_alert_ops_export_summary
from src.services.camera_source_service import build_camera_source_ops_export_summary
from src.services.camera_service import build_camera_ops_export_summary
from src.services.entity_service import build_entity_ops_export_summary
from src.services.event_export_service import build_event_export_bundle
from src.services.event_service import build_event_ops_export_summary
from src.services.export_artifact_service import write_json_export_artifact, write_text_export_artifact
from src.services.operations_report_service import build_operations_report
from src.services.runtime_snapshot_service import build_runtime_snapshot
from src.services.scheduler_service import build_scheduler_ops_export_summary
from src.services.source_service import build_source_ops_export_summary


def test_export_artifact_service_registers_storage_objects(
    client: TestClient,
    tmp_path: Path,
) -> None:
    source_fixture = tmp_path / "export-artifact-source.json"
    source_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Export artifact source",
                    "url": "https://export-artifact.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                    "vessel_name": "MV Export Artifact",
                    "mmsi": "111222333",
                }
            ]
        ),
        encoding="utf-8",
    )
    corroboration_fixture = tmp_path / "export-artifact-corroboration.json"
    corroboration_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Export artifact corroboration",
                    "url": "https://export-artifact-two.example.com/1",
                    "lat": 29.77,
                    "lon": -95.35,
                    "vessel_name": "MV Export Artifact",
                    "mmsi": "111222333",
                }
            ]
        ),
        encoding="utf-8",
    )
    camera_fixture = tmp_path / "export-artifact-cameras.json"
    camera_fixture.write_text(
        json.dumps(
            [
                {
                    "camera_id": "export-artifact-cam-1",
                    "camera_name": "Export Artifact Camera",
                    "provider": "MnDOT",
                    "road_name": "I-35W",
                    "status": "online",
                    "image_url": "https://cams.export-artifact.example.com/cam-1.jpg",
                    "page_url": "https://511mn.org/camera/export-artifact-1",
                    "lat": 44.9482,
                    "lon": -93.2701,
                    "observed_at": "2026-07-07T01:00:00Z",
                }
            ]
        ),
        encoding="utf-8",
    )

    assert client.post(
        "/api/imports/local",
        json={"source_path": str(source_fixture), "layer_key": "marine-track"},
    ).status_code == 200
    assert client.post(
        "/api/imports/local",
        json={"source_path": str(corroboration_fixture), "layer_key": "news-track"},
    ).status_code == 200
    assert client.post(
        "/api/imports/local",
        json={"source_path": str(camera_fixture), "layer_key": "traffic-camera-feed"},
    ).status_code == 200
    source_definition = client.post(
        "/api/sources",
        json={
            "name": "export-artifact-source-def",
            "source_kind": "local_file",
            "layer_key": "marine-track",
            "target_uri": str(source_fixture),
        },
    )
    assert source_definition.status_code == 200
    assert client.post(
        "/api/cameras/materialize",
        json={"layer_key": "traffic-camera-feed", "limit": 25},
    ).status_code == 200
    fused = client.post(
        "/api/events/fuse",
        json={
            "min_lon": -96.0,
            "min_lat": 29.0,
            "max_lon": -94.0,
            "max_lat": 31.0,
            "distance_km": 10,
            "time_window_minutes": 120,
            "redaction_level": "public",
        },
    )
    assert fused.status_code == 200
    event_id = fused.json()["event_results"][0]["event_id"]
    alert_response = client.post(
        "/api/alerts",
        json={
            "event_id": event_id,
            "severity": "warning",
            "status": "open",
            "message": "Export artifact alert",
        },
    )
    assert alert_response.status_code == 200

    exports_dir = tmp_path / "exports"
    session = get_session_factory()()
    try:
        bundle = build_event_export_bundle(session, event_id, max_redaction_level="public")
        bundle_payload = json.loads(json.dumps(bundle, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "event-bundle.json",
            payload=bundle_payload,
            object_kind="event_bundle_export",
            owner_type="event",
            owner_id=str(event_id),
            source_uri=f"/api/events/{event_id}/export",
            observed_at=bundle["exported_at"],
            metadata_json={
                "requested_redaction_level": "public",
                "observation_count": len(bundle_payload["observations"]),
                "entity_count": len(bundle_payload["entities"]),
                "product_count": len(bundle_payload["products"]),
            },
        )

        product = session.scalar(
            select(SituationProductORM).where(
                SituationProductORM.event_id == event_id,
                SituationProductORM.product_type == "cited_summary",
            )
        )
        assert product is not None
        write_text_export_artifact(
            session,
            output_path=exports_dir / "event-summary.txt",
            body_text=product.body_text,
            object_kind="situation_product_export",
            owner_type="situation_product",
            owner_id=str(product.product_id),
            source_uri=f"/api/events/{event_id}/products",
            observed_at=product.updated_at,
            metadata_json={
                "event_id": event_id,
                "product_type": product.product_type,
                "redaction_level": product.redaction_level,
                "requested_redaction_level": "public",
            },
        )

        camera_summary = build_camera_ops_export_summary(session, layer_key="traffic-camera-feed")
        camera_summary_payload = json.loads(json.dumps(camera_summary, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "camera-summary.json",
            payload=camera_summary_payload,
            object_kind="camera_summary_export",
            owner_type="camera_export",
            owner_id="traffic-camera-feed",
            source_uri="/api/cameras/export/summary",
            observed_at=camera_summary["generated_at"],
            metadata_json=camera_summary_payload["filters_json"],
        )

        camera_source_summary = build_camera_source_ops_export_summary(session, layer_key="traffic-camera-feed")
        camera_source_payload = json.loads(json.dumps(camera_source_summary, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "camera-source-summary.json",
            payload=camera_source_payload,
            object_kind="camera_source_summary_export",
            owner_type="camera_source_export",
            owner_id="traffic-camera-feed",
            source_uri="/api/camera-sources/export/summary",
            observed_at=camera_source_summary["generated_at"],
            metadata_json=camera_source_payload["filters_json"],
        )

        operations_report = build_operations_report(session, since=None, limit=10)
        operations_payload = json.loads(json.dumps(operations_report, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "operations-report.json",
            payload=operations_payload,
            object_kind="operations_report_export",
            owner_type="operations_report",
            owner_id="scoped",
            source_uri="/api/operations/report",
            observed_at=operations_report["generated_at"],
            metadata_json={
                "scope_since": operations_payload["scope_since"],
                "scope_until": operations_payload["scope_until"],
                "limit": 10,
                "hours": 24.0,
            },
        )

        runtime_snapshot = build_runtime_snapshot(session)
        runtime_payload = json.loads(json.dumps(runtime_snapshot, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "runtime-snapshot.json",
            payload=runtime_payload,
            object_kind="runtime_snapshot_export",
            owner_type="runtime_snapshot",
            owner_id=runtime_payload["exported_at"],
            source_uri="/api/operations/runtime/export",
            observed_at=runtime_snapshot["exported_at"],
            metadata_json={
                "database_backend": runtime_payload["database_backend"],
                "spatial_backend": runtime_payload["spatial_backend"],
            },
        )

        event_summary = build_event_ops_export_summary(session, event_limit=50, report_limit=25, stale_event_limit=25)
        event_summary_payload = json.loads(json.dumps(event_summary, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "event-summary.json",
            payload=event_summary_payload,
            object_kind="event_summary_export",
            owner_type="event_export",
            owner_id="scoped",
            source_uri="/api/events/export/summary",
            observed_at=event_summary["generated_at"],
            metadata_json=event_summary_payload["filters_json"],
        )

        entity_summary = build_entity_ops_export_summary(session, entity_limit=50, report_limit=25, conflict_limit=25)
        entity_summary_payload = json.loads(json.dumps(entity_summary, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "entity-summary.json",
            payload=entity_summary_payload,
            object_kind="entity_summary_export",
            owner_type="entity_export",
            owner_id="scoped",
            source_uri="/api/entities/export/summary",
            observed_at=entity_summary["generated_at"],
            metadata_json=entity_summary_payload["filters_json"],
        )

        scheduler_summary = build_scheduler_ops_export_summary(session, task_limit=50, report_limit=25, overdue_task_limit=25)
        scheduler_payload = json.loads(json.dumps(scheduler_summary, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "scheduler-summary.json",
            payload=scheduler_payload,
            object_kind="scheduler_summary_export",
            owner_type="scheduler_export",
            owner_id="scoped",
            source_uri="/api/scheduler/export/summary",
            observed_at=scheduler_summary["generated_at"],
            metadata_json=scheduler_payload["filters_json"],
        )

        source_summary = build_source_ops_export_summary(session, source_limit=50, report_limit=25, stale_source_limit=25)
        source_payload = json.loads(json.dumps(source_summary, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "source-summary.json",
            payload=source_payload,
            object_kind="source_summary_export",
            owner_type="source_export",
            owner_id="scoped",
            source_uri="/api/sources/export/summary",
            observed_at=source_summary["generated_at"],
            metadata_json=source_payload["filters_json"],
        )

        alert_summary = build_alert_ops_export_summary(session, alert_limit=50, report_limit=25, stale_alert_limit=25)
        alert_payload = json.loads(json.dumps(alert_summary, default=str))
        write_json_export_artifact(
            session,
            output_path=exports_dir / "alert-summary.json",
            payload=alert_payload,
            object_kind="alert_summary_export",
            owner_type="alert_export",
            owner_id="scoped",
            source_uri="/api/alerts/export/summary",
            observed_at=alert_summary["generated_at"],
            metadata_json=alert_payload["filters_json"],
        )
    finally:
        session.close()

    event_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "event", "owner_id": str(event_id), "object_kind": "event_bundle_export"},
    ).json()
    assert len(event_rows) == 1
    assert event_rows[0]["metadata_json"]["requested_redaction_level"] == "public"

    product_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "situation_product", "object_kind": "situation_product_export"},
    ).json()
    assert len(product_rows) == 1
    assert product_rows[0]["media_type"] == "text/plain"

    camera_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "camera_export", "owner_id": "traffic-camera-feed", "object_kind": "camera_summary_export"},
    ).json()
    assert len(camera_rows) == 1
    assert camera_rows[0]["metadata_json"]["layer_key"] == "traffic-camera-feed"

    camera_source_rows = client.get(
        "/api/storage/objects",
        params={
            "owner_type": "camera_source_export",
            "owner_id": "traffic-camera-feed",
            "object_kind": "camera_source_summary_export",
        },
    ).json()
    assert len(camera_source_rows) == 1
    assert camera_source_rows[0]["metadata_json"]["layer_key"] == "traffic-camera-feed"

    operations_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "operations_report", "object_kind": "operations_report_export"},
    ).json()
    assert len(operations_rows) == 1
    assert operations_rows[0]["metadata_json"]["hours"] == 24.0

    runtime_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "runtime_snapshot", "object_kind": "runtime_snapshot_export"},
    ).json()
    assert len(runtime_rows) == 1
    assert runtime_rows[0]["metadata_json"]["database_backend"] == "sqlite"

    event_summary_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "event_export", "object_kind": "event_summary_export"},
    ).json()
    assert len(event_summary_rows) == 1
    assert event_summary_rows[0]["metadata_json"]["event_limit"] == 50

    entity_summary_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "entity_export", "object_kind": "entity_summary_export"},
    ).json()
    assert len(entity_summary_rows) == 1
    assert entity_summary_rows[0]["metadata_json"]["entity_limit"] == 50

    scheduler_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "scheduler_export", "object_kind": "scheduler_summary_export"},
    ).json()
    assert len(scheduler_rows) == 1
    assert scheduler_rows[0]["metadata_json"]["task_limit"] == 50

    source_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "source_export", "object_kind": "source_summary_export"},
    ).json()
    assert len(source_rows) == 1
    assert source_rows[0]["metadata_json"]["source_limit"] == 50

    alert_rows = client.get(
        "/api/storage/objects",
        params={"owner_type": "alert_export", "owner_id": "scoped", "object_kind": "alert_summary_export"},
    ).json()
    assert len(alert_rows) == 1
    assert alert_rows[0]["metadata_json"]["alert_limit"] == 50
