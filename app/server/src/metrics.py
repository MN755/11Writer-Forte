from __future__ import annotations

from collections import defaultdict
from threading import Lock
from typing import Any

from sqlalchemy.orm import Session

from src.auth import build_api_auth_diagnostics
from src.config import get_settings

DEFAULT_HISTOGRAM_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

METRIC_HELP: dict[str, str] = {
    "elevenwriter_http_requests_total": "Total HTTP requests handled by the API.",
    "elevenwriter_http_request_duration_seconds": "HTTP request duration in seconds.",
    "elevenwriter_source_runs_total": "Total source runs by source kind and terminal status.",
    "elevenwriter_source_fetch_failures_total": "Total HTTP source fetch failures by source kind and error type.",
    "elevenwriter_source_runtime_worker_iterations_total": "Total source-runtime worker loop iterations.",
    "elevenwriter_scheduler_runs_total": "Total scheduler task executions by task type and terminal status.",
    "elevenwriter_scheduler_worker_iterations_total": "Total scheduler worker loop iterations.",
    "elevenwriter_scheduler_worker_runs_created_total": "Total task runs created by the scheduler worker.",
    "elevenwriter_source_runtime_sources_total": "Source runtime source counts by runtime state.",
    "elevenwriter_source_dead_letters_total": "Source dead-letter counts by state.",
    "elevenwriter_source_fetch_modes_total": "Source counts by fetch mode.",
    "elevenwriter_storage_sweeps_total": "Total storage lifecycle sweeps by retention class and status.",
    "elevenwriter_clickhouse_operations_total": "Total ClickHouse operations by operation and status.",
    "elevenwriter_auth_enabled": "Whether API key authentication is active.",
    "elevenwriter_auth_misconfigured": "Whether auth is required but not fully configured.",
    "elevenwriter_scheduler_tasks_total": "Scheduler task counts by state.",
    "elevenwriter_sources_total": "Source counts by state.",
    "elevenwriter_storage_objects_total": "Storage object counts by state.",
    "elevenwriter_clickhouse_status": "ClickHouse status exposed as a one-hot gauge.",
    "elevenwriter_build_info": "Static build metadata for the running service.",
}

COUNTERS: dict[str, dict[tuple[tuple[str, str], ...], float]] = defaultdict(lambda: defaultdict(float))
GAUGES: dict[str, dict[tuple[tuple[str, str], ...], float]] = defaultdict(dict)
HISTOGRAMS: dict[str, dict[tuple[tuple[str, str], ...], dict[str, Any]]] = defaultdict(dict)
REGISTRY_LOCK = Lock()


def reset_metrics_registry() -> None:
    with REGISTRY_LOCK:
        COUNTERS.clear()
        GAUGES.clear()
        HISTOGRAMS.clear()


def inc_counter(name: str, value: float = 1.0, **labels: object) -> None:
    with REGISTRY_LOCK:
        COUNTERS[name][normalize_labels(labels)] += value


def set_gauge(name: str, value: float, **labels: object) -> None:
    with REGISTRY_LOCK:
        GAUGES[name][normalize_labels(labels)] = value


def observe_histogram(
    name: str,
    value: float,
    *,
    buckets: tuple[float, ...] = DEFAULT_HISTOGRAM_BUCKETS,
    **labels: object,
) -> None:
    normalized = normalize_labels(labels)
    with REGISTRY_LOCK:
        sample = HISTOGRAMS[name].setdefault(
            normalized,
            {
                "buckets": buckets,
                "bucket_counts": {bucket: 0 for bucket in buckets},
                "count": 0,
                "sum": 0.0,
            },
        )
        for bucket in sample["buckets"]:
            if value <= bucket:
                sample["bucket_counts"][bucket] += 1
        sample["count"] += 1
        sample["sum"] += value


def render_metrics(session: Session | None = None) -> str:
    lines: list[str] = []
    metric_names = sorted(set(METRIC_HELP) | set(COUNTERS) | set(GAUGES) | set(HISTOGRAMS))

    with REGISTRY_LOCK:
        counter_snapshot = {name: dict(values) for name, values in COUNTERS.items()}
        gauge_snapshot = {name: dict(values) for name, values in GAUGES.items()}
        histogram_snapshot = {name: dict(values) for name, values in HISTOGRAMS.items()}

    add_runtime_gauges(gauge_snapshot, session)

    for name in metric_names:
        if name in counter_snapshot:
            lines.extend(render_counter(name, counter_snapshot[name]))
        if name in gauge_snapshot:
            lines.extend(render_gauge(name, gauge_snapshot[name]))
        if name in histogram_snapshot:
            lines.extend(render_histogram(name, histogram_snapshot[name]))

    return "\n".join(lines) + "\n"


def add_runtime_gauges(
    gauge_snapshot: dict[str, dict[tuple[tuple[str, str], ...], float]],
    session: Session | None,
) -> None:
    from src.services.clickhouse_service import build_clickhouse_diagnostics
    from src.services.scheduler_service import build_scheduler_inventory_summary
    from src.services.source_service import build_source_inventory_summary
    from src.services.storage_service import build_storage_report

    settings = get_settings()
    auth = build_api_auth_diagnostics()
    store_gauge_snapshot(
        gauge_snapshot,
        "elevenwriter_auth_enabled",
        float(bool(auth["enabled"])),
    )
    store_gauge_snapshot(
        gauge_snapshot,
        "elevenwriter_auth_misconfigured",
        float(bool(auth["misconfigured"])),
    )
    store_gauge_snapshot(
        gauge_snapshot,
        "elevenwriter_build_info",
        1.0,
        version=settings.app_version,
        app_env=settings.app_env,
    )

    if session is None:
        return

    scheduler = build_scheduler_inventory_summary(session)
    for state in ("enabled", "disabled", "due", "overdue", "failing"):
        store_gauge_snapshot(
            gauge_snapshot,
            "elevenwriter_scheduler_tasks_total",
            float(scheduler[f"{state}_count"]),
            state=state,
        )

    sources = build_source_inventory_summary(session)
    for state in ("enabled", "disabled", "stale", "failing", "scheduled", "unscheduled"):
        store_gauge_snapshot(
            gauge_snapshot,
            "elevenwriter_sources_total",
            float(sources[f"{state}_count"]),
            state=state,
        )
    for state in ("active", "degraded"):
        store_gauge_snapshot(
            gauge_snapshot,
            "elevenwriter_source_runtime_sources_total",
            float(sources[f"runtime_{state}_count"]),
            state=state,
        )
    store_gauge_snapshot(
        gauge_snapshot,
        "elevenwriter_source_dead_letters_total",
        float(sources["dead_letter_pending_count"]),
        state="pending",
    )
    for item in sources["fetch_mode_counts"]:
        store_gauge_snapshot(
            gauge_snapshot,
            "elevenwriter_source_fetch_modes_total",
            float(item["total_count"]),
            fetch_mode=item["key"],
        )

    storage = build_storage_report(session, limit=10)
    store_gauge_snapshot(gauge_snapshot, "elevenwriter_storage_objects_total", float(storage.total_count), state="total")
    store_gauge_snapshot(gauge_snapshot, "elevenwriter_storage_objects_total", float(storage.active_count), state="active")
    store_gauge_snapshot(gauge_snapshot, "elevenwriter_storage_objects_total", float(storage.expired_count), state="expired")
    store_gauge_snapshot(gauge_snapshot, "elevenwriter_storage_objects_total", float(storage.promoted_count), state="promoted")
    store_gauge_snapshot(gauge_snapshot, "elevenwriter_storage_objects_total", float(storage.archived_count), state="archived")

    clickhouse = build_clickhouse_diagnostics()
    for status in ("disabled", "degraded", "ok"):
        store_gauge_snapshot(
            gauge_snapshot,
            "elevenwriter_clickhouse_status",
            1.0 if clickhouse["status"] == status else 0.0,
            status=status,
        )


def store_gauge_snapshot(
    gauge_snapshot: dict[str, dict[tuple[tuple[str, str], ...], float]],
    name: str,
    value: float,
    **labels: object,
) -> None:
    gauge_snapshot.setdefault(name, {})[normalize_labels(labels)] = value


def render_counter(name: str, samples: dict[tuple[tuple[str, str], ...], float]) -> list[str]:
    lines = [f"# HELP {name} {METRIC_HELP.get(name, name)}", f"# TYPE {name} counter"]
    lines.extend(render_samples(name, samples))
    return lines


def render_gauge(name: str, samples: dict[tuple[tuple[str, str], ...], float]) -> list[str]:
    lines = [f"# HELP {name} {METRIC_HELP.get(name, name)}", f"# TYPE {name} gauge"]
    lines.extend(render_samples(name, samples))
    return lines


def render_histogram(
    name: str,
    samples: dict[tuple[tuple[str, str], ...], dict[str, Any]],
) -> list[str]:
    lines = [f"# HELP {name} {METRIC_HELP.get(name, name)}", f"# TYPE {name} histogram"]
    for labels, sample in sorted(samples.items(), key=lambda item: item[0]):
        cumulative = 0
        for bucket in sample["buckets"]:
            cumulative += int(sample["bucket_counts"][bucket])
            bucket_labels = dict(labels)
            bucket_labels["le"] = format_float(bucket)
            lines.append(f"{name}_bucket{format_label_string(bucket_labels)} {cumulative}")
        inf_labels = dict(labels)
        inf_labels["le"] = "+Inf"
        lines.append(f"{name}_bucket{format_label_string(inf_labels)} {sample['count']}")
        lines.append(f"{name}_sum{format_label_string(dict(labels))} {format_float(sample['sum'])}")
        lines.append(f"{name}_count{format_label_string(dict(labels))} {sample['count']}")
    return lines


def render_samples(name: str, samples: dict[tuple[tuple[str, str], ...], float]) -> list[str]:
    return [
        f"{name}{format_label_string(dict(labels))} {format_float(value)}"
        for labels, value in sorted(samples.items(), key=lambda item: item[0])
    ]


def normalize_labels(labels: dict[str, object]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(key), str(value)) for key, value in labels.items()))


def format_label_string(labels: dict[str, object]) -> str:
    if not labels:
        return ""
    rendered = ",".join(
        f'{key}="{escape_label_value(str(value))}"' for key, value in sorted(labels.items())
    )
    return f"{{{rendered}}}"


def escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def format_float(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")
