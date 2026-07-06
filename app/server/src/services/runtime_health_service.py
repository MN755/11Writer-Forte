from __future__ import annotations

from src.config.settings import Settings
from src.services.runtime_scheduler_service import build_runtime_status
from src.services.storage_profile_service import build_storage_status
from src.types.api import RuntimeHealthCheck, RuntimeReadinessResponse


def build_runtime_readiness_report(settings: Settings) -> RuntimeReadinessResponse:
    storage = build_storage_status(settings)
    total_components = len(storage.components)
    ready_components = sum(1 for component in storage.components if component.reachable and component.initialized)
    storage_ready = total_components > 0 and ready_components == total_components
    checks = [
        RuntimeHealthCheck(
            name="storage",
            ready=storage_ready,
            detail=(
                f"{ready_components}/{total_components} configured storage components are reachable and initialized."
            ),
        )
    ]
    caveats = list(storage.caveats)

    runtime_ready = False
    try:
        runtime_status = build_runtime_status(settings)
        runtime_ready = True
        checks.append(
            RuntimeHealthCheck(
                name="runtime_scheduler",
                ready=True,
                detail=(
                    f"Runtime status loaded for {len(runtime_status.workers)} workers; "
                    f"recommended deployment is {runtime_status.recommended_runtime_deployment}."
                ),
            )
        )
        if runtime_status.recommended_runtime_deployment != "os-managed-service":
            caveats.append(
                "No supported OS service manager was detected on this machine; long-running workers still need explicit process supervision."
            )
    except Exception as exc:  # noqa: BLE001
        checks.append(
            RuntimeHealthCheck(
                name="runtime_scheduler",
                ready=False,
                detail=f"Runtime status could not be loaded: {str(exc)[:200]}",
            )
        )
        caveats.append("Runtime scheduler status must load successfully before the backend should be treated as operationally ready.")

    ready = storage_ready and runtime_ready
    if not storage_ready:
        caveats.append("Run `11writer db-bootstrap` before treating the backend as ready.")

    return RuntimeReadinessResponse(
        status="ok" if ready else "degraded",
        ready=ready,
        runtime_mode=settings.app_runtime_mode,
        storage_mode=storage.storage_mode,
        checks=checks,
        caveats=caveats,
    )
