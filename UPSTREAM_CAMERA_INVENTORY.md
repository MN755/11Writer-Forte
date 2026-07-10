# Upstream Camera Inventory

This workspace is not carrying the full upstream `MN755/11Writer` camera and webcam stack in its original shape.

That is the actual state as of July 6, 2026:

- local `11Writer Forte` is a backend-only snapshot/continuation
- local `.git` has no commits and no configured remote
- local `app/` currently contains 54 filesystem entries
- local camera-related work is limited to generic source ingestion plus MnDOT feed notes in [MNDOT_FEEDS.md](MNDOT_FEEDS.md)

Upstream `MN755/11Writer` does contain substantial camera/webcam functionality. The most relevant paths are below.

## Upstream client paths

- `app/client/src/features/layers/WebcamOperationsPanel.tsx`
- `app/client/src/features/webcams/webcamClustering.ts`
- `app/client/src/features/webcams/webcamSourceLifecycleSummary.ts`
- `app/client/src/hooks/useCameraPolling.ts`
- `app/client/src/layers/CameraLayer.tsx`
- `app/client/src/lib/cameraPresets.ts`

## Upstream docs

- `app/docs/webcams.md`
- `app/docs/webcam-source-lifecycle-policy.md`
- `app/docs/webcam-finland-digitraffic-fixture-plan.md`
- `app/docs/webcam-global-camera-candidate-batch-2026-05.md`

## Upstream backend routes and services

- `app/server/src/routes/cameras.py`
- `app/server/src/services/camera_service.py`
- `app/server/src/services/camera_registry.py`
- `app/server/src/services/camera_endpoint_evaluator.py`
- `app/server/src/services/camera_candidate_graduation_plan.py`
- `app/server/src/services/finland_digitraffic_service.py`

## Upstream webcam worker/runtime module

- `app/server/src/webcam/__init__.py`
- `app/server/src/webcam/db.py`
- `app/server/src/webcam/models.py`
- `app/server/src/webcam/refresh.py`
- `app/server/src/webcam/repository.py`
- `app/server/src/webcam/worker.py`

## Upstream backend migrations and fixtures

- `app/server/alembic/versions/20260404_0002_create_webcam_schema.py`
- `app/server/alembic/versions/20260404_0005_add_webcam_worker_state.py`
- `app/server/alembic/versions/20260404_0006_add_webcam_validation_run_fields.py`
- `app/server/alembic/versions/20260404_0007_add_camera_source_inventory.py`
- `app/server/alembic/versions/20260429_0011_add_endpoint_verification_fields_to_camera_source_inventory.py`
- `app/server/data/baton_rouge_traffic_cameras_fixture.json`
- `app/server/data/caltrans_cctv_cameras_fixture.json`
- `app/server/data/fingal_traffic_cameras_fixture.json`
- `app/server/data/finland_digitraffic_weathercam_fixture.json`
- `app/server/data/maryland_chart_traffic_cameras_fixture.json`
- `app/server/data/nsw_live_traffic_cameras_fixture.json`
- `app/server/data/quebec_mtmd_traffic_cameras_fixture.json`

## Upstream camera tests

- `app/server/tests/test_webcam_module.py`
- `app/server/tests/test_camera_candidate_endpoint_report.py`
- `app/server/tests/test_camera_candidate_graduation_plan.py`
- `app/server/tests/test_camera_endpoint_evaluator.py`
- `app/server/tests/test_camera_sandbox_validation_report.py`
- `app/server/tests/test_camera_source_ops_detail.py`
- `app/server/tests/test_camera_source_ops_export_summary.py`
- `app/server/tests/test_camera_source_ops_report_index.py`
- `app/server/tests/test_finland_digitraffic.py`

## What Forte currently has instead

Forte currently has:

- generic managed-source ingestion
- local/import/scheduler/event infrastructure
- `/api/cameras`, `/api/cameras/materialize`, `/api/cameras/summary`, and `/api/cameras/{id}/ops`
- `/api/camera-sources`, `/api/camera-sources/materialize`, `/api/camera-sources/summary`, and `/api/camera-sources/{id}/ops`
- persisted camera inventory records with provenance, custody logs, and scheduler-native refresh tasks
- persisted camera source candidate records with rule-based graduation scores, verification state, and custody logs
- active camera source endpoint verification for image/stream/page URLs, including persisted reachability state and scheduler-native verification tasks
- MnDOT FEU-g XML support
- MnDOT live camera access notes and verified public HLS/JPEG endpoints in [MNDOT_FEEDS.md](MNDOT_FEEDS.md)

Forte does not currently have the upstream:

- webcam worker/runtime module
- the upstream camera source lifecycle/report stack in its original form
- frontend webcam layer and operations panel
- camera-specific migrations, fixtures, and tests

Forte does now have a backend-native local equivalent for the operational path that matters most here:

- camera-shaped local upstream feeds that can be ingested through the generic source runtime
- scheduler-native camera inventory refresh and camera source verification
- local camera image/stream/page verification targets for end-to-end runtime smoke coverage
- backend camera inventory, candidate source inventory, reporting, custody, and export surfaces

## Why this file exists

This repo needed an in-tree answer to a simple question:

"Does Forte already include the upstream traffic camera/webcam stack?"

Right now, not wholesale. Upstream still has the bigger webcam runtime and lifecycle stack plus the UI. Forte has the backend-only camera runtime path needed for unattended collection and verification, but not the old upstream camera subsystem verbatim.
