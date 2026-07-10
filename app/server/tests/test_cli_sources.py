from __future__ import annotations

import json
import socket
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import urlopen

import pytest
import uvicorn
from typer.testing import CliRunner

from src.cli import app
from src.config import reset_settings_cache
from src.db import get_session_factory, init_db, reset_db_state
from src.local_upstreams import app as local_upstreams_app
from src.models import (
    LocalImportRunORM,
    ObservationORM,
    ScheduledTaskORM,
    SourceDeadLetterORM,
    SourceDefinitionORM,
)
from src.schemas import SourceDefinitionCreate
from src.services.source_service import create_source_definition


@pytest.fixture()
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CliRunner:
    database_path = tmp_path / "test.db"
    data_path = tmp_path / "var"
    monkeypatch.setenv("ELEVENWRITER_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.setenv("ELEVENWRITER_DATA_DIR", str(data_path))
    reset_settings_cache()
    reset_db_state()
    runner = CliRunner()
    yield runner
    reset_db_state()
    reset_settings_cache()


@contextmanager
def cli_html_discovery_server():
    pages = {
        "/sites/briefs/harbor-departure": {
            "title": "Harbor Departure Confirmed",
            "description": "Cargo vessel departure confirmed by local observers.",
            "paragraphs": ["Observers reported a vessel clearing the berth shortly after dawn."],
            "links": ["/sites/reference/local-osint-overview"],
        },
        "/sites/briefs/rail-delay": {
            "title": "Rail Delay Bulletin",
            "description": "Dispatch notes describing a junction slowdown.",
            "paragraphs": ["Dispatch logged a temporary rail slowdown near the yard lead."],
            "links": ["/sites/reference/local-osint-overview"],
        },
        "/sites/reference/local-osint-overview": {
            "title": "Local OSINT Overview",
            "description": "Reference page linking the local mock site graph together.",
            "paragraphs": ["This page helps the crawler traverse more than one page."],
            "links": ["/sites/briefs/harbor-departure", "/sites/briefs/rail-delay"],
        },
    }
    state = {"requests": []}

    def render_html(title: str, description: str, paragraphs: list[str], links: list[tuple[str, str]]) -> str:
        paragraph_markup = "".join(f"<p>{paragraph}</p>" for paragraph in paragraphs)
        link_markup = "".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
        return (
            "<!DOCTYPE html><html><head>"
            f"<title>{title}</title>"
            f'<meta name="description" content="{description}" />'
            f"</head><body><h1>{title}</h1><p>{description}</p>{paragraph_markup}<ul>{link_markup}</ul></body></html>"
        )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            state["requests"].append(parsed.path)
            root_url = f"http://127.0.0.1:{server.server_port}"
            if parsed.path == "/search/html":
                query = parse_qs(parsed.query).get("q", [""])[0].lower()
                if "rail" in query:
                    links = [(f"/search/redirect?target={quote('/sites/briefs/rail-delay', safe='/')}", "Rail Delay Bulletin")]
                else:
                    links = [
                        (
                            f"/search/redirect?target={quote('/sites/briefs/harbor-departure', safe='/')}",
                            "Harbor Departure Confirmed",
                        )
                    ]
                html = render_html(
                    "CLI Search Results",
                    "Local HTML search provider used for CLI discovery tests.",
                    [f"Search query: {query or 'all'}"],
                    links,
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(html.encode("utf-8"))
                return
            if parsed.path == "/search/redirect":
                target = parse_qs(parsed.query).get("target", ["/"])[0]
                self.send_response(307)
                self.send_header("Location", target)
                self.end_headers()
                return
            if parsed.path == "/robots.txt":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(f"User-agent: *\nAllow: /\nSitemap: {root_url}/sitemap.xml\n".encode("utf-8"))
                return
            if parsed.path == "/sitemap.xml":
                xml = (
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                    f"<url><loc>{root_url}/sites/briefs/harbor-departure</loc></url>"
                    f"<url><loc>{root_url}/sites/reference/local-osint-overview</loc></url>"
                    "</urlset>"
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.end_headers()
                self.wfile.write(xml.encode("utf-8"))
                return
            if parsed.path == "/crawl/root":
                html = render_html(
                    "Local Crawl Root",
                    "Seed page for CLI crawl and discovery tests.",
                    ["This page fans out into a small local HTML graph."],
                    [("/sites/reference/local-osint-overview", "Local OSINT Overview")],
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(html.encode("utf-8"))
                return
            page = pages.get(parsed.path)
            if page is None:
                self.send_response(404)
                self.end_headers()
                return
            html = render_html(
                str(page["title"]),
                str(page["description"]),
                list(page["paragraphs"]),
                [(href, href.rsplit("/", 1)[-1].replace("-", " ").title()) for href in page["links"]],
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def cli_runtime_probe_server(
    *,
    ready: bool = True,
    include_worker: bool = True,
    ready_after_ready_checks: int = 0,
    database_backend: str = "postgresql",
    spatial_backend: str = "postgis",
    database_status: str = "ok",
    postgis_extension_installed: bool | None = True,
    schema_up_to_date: bool = True,
    version_table_present: bool = True,
):
    state = {"ready_checks": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            currently_ready = ready and state["ready_checks"] >= ready_after_ready_checks
            currently_includes_worker = include_worker and currently_ready
            if parsed.path == "/health":
                payload = {
                    "status": "ok",
                    "app_name": "11Writer Forte",
                    "app_version": "0.1.0",
                    "database_url": "postgresql+psycopg://elevenwriter:***@db:5432/elevenwriter",
                    "database_backend": "postgresql",
                    "database_connected": True,
                    "spatial_backend": "postgis",
                    "postgis_ready": True,
                    "warning_count": 0,
                }
                self.send_response(200)
            elif parsed.path == "/ready":
                state["ready_checks"] += 1
                currently_ready = ready and state["ready_checks"] >= ready_after_ready_checks
                currently_includes_worker = include_worker and currently_ready
                payload = {
                    "status": "ok" if currently_ready else "not_ready",
                    "ready": currently_ready,
                    "app_name": "11Writer Forte",
                    "app_version": "0.1.0",
                    "overall_status": "ready" if currently_ready else "not_ready",
                    "action_required_count": 0 if currently_ready else 1,
                    "warning_count": 0,
                    "check_count": 3,
                    "operator_actions": [] if currently_ready else ["Run platform-runtime-worker under a real supervisor."],
                }
                self.send_response(200 if currently_ready else 503)
            elif parsed.path == "/api/operations/workers/summary":
                payload = {
                    "generated_at": "2026-07-08T12:00:00Z",
                    "stale_before": "2026-07-08T11:55:00Z",
                    "total_count": 1 if currently_includes_worker else 0,
                    "active_count": 1 if currently_includes_worker else 0,
                    "stale_count": 0,
                    "status_counts": [{"key": "active", "total_count": 1 if currently_includes_worker else 0, "active_count": 1 if currently_includes_worker else 0, "stale_count": 0}],
                    "worker_type_counts": [{"key": "platform_runtime_worker", "total_count": 1 if currently_includes_worker else 0, "active_count": 1 if currently_includes_worker else 0, "stale_count": 0}],
                    "workers": (
                        [
                            {
                                "worker_status_id": 1,
                                "worker_key": "platform-runtime-worker",
                                "worker_type": "platform_runtime_worker",
                                "actor": "cli_platform_runtime_worker",
                                "status": "active",
                                "process_token": "token",
                                "last_started_at": "2026-07-08T11:59:00Z",
                                "last_seen_at": "2026-07-08T12:00:00Z",
                                "last_iteration_at": "2026-07-08T12:00:00Z",
                                "last_stopped_at": None,
                                "failure_count": 0,
                                "metadata_json": {},
                                "created_at": "2026-07-08T11:59:00Z",
                                "updated_at": "2026-07-08T12:00:00Z",
                            }
                        ]
                        if currently_includes_worker
                        else []
                    ),
                }
                self.send_response(200)
            elif parsed.path == "/api/operations/database":
                payload = {
                    "status": database_status,
                    "database_backend": database_backend,
                    "database_url": f"{database_backend}://elevenwriter:***@db:5432/elevenwriter",
                    "database_connected": True,
                    "spatial_backend": spatial_backend,
                    "scheduler_poll_seconds": 5.0,
                    "postgis_expected": spatial_backend == "postgis",
                    "postgis_extension_installed": postgis_extension_installed,
                    "postgis_version": "3.4" if postgis_extension_installed else None,
                    "warning_count": 0 if database_status == "ok" else 1,
                    "warnings": [] if database_status == "ok" else ["synthetic diagnostics warning"],
                    "notes": [],
                    "required_columns": [],
                    "migration": {
                        "current_revision": "head",
                        "head_revision": "head",
                        "version_table_present": version_table_present,
                        "has_application_tables": True,
                        "schema_up_to_date": schema_up_to_date,
                    },
                    "spatial_indexes": [],
                    "table_counts": [],
                }
                self.send_response(200)
            else:
                payload = {"status": "not_found"}
                self.send_response(404)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode("utf-8"))

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def cli_local_upstreams_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        _, port = sock.getsockname()

    config = uvicorn.Config(local_upstreams_app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.time() + 10.0
        while time.time() < deadline:
            try:
                with urlopen(f"{base_url}/health", timeout=1.0) as response:
                    if response.status == 200:
                        break
            except Exception:
                time.sleep(0.05)
        else:
            raise RuntimeError("Local upstreams test server did not become ready in time.")
        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def test_cli_push_source_webhook_creates_import_and_checkpoint(cli_env: CliRunner) -> None:
    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="cli-webhook-source",
                source_kind="webhook_ingest",
                layer_key="cli-inbound",
                target_uri="webhook://local",
            ),
        )
        source_id = source.source_id
    finally:
        session.close()

    result = cli_env.invoke(
        app,
        [
            "push-source-webhook",
            str(source_id),
            "--payload-json",
            '{"title":"CLI webhook","url":"https://example.com/cli-webhook","lat":44.95,"lon":-93.27}',
        ],
    )
    assert result.exit_code == 0
    assert f"source={source_id}" in result.stdout
    assert "status=completed" in result.stdout

    checkpoint_result = cli_env.invoke(app, ["list-source-checkpoints", "--source-id", str(source_id)])
    assert checkpoint_result.exit_code == 0
    assert f"source={source_id}" in checkpoint_result.stdout
    assert "status=idle" in checkpoint_result.stdout

    session = get_session_factory()()
    try:
        import_runs = list(session.query(LocalImportRunORM).all())
        observations = list(session.query(ObservationORM).all())
        assert len(import_runs) == 1
        assert import_runs[0].source_path == f"webhook://source/{source_id}"
        assert len(observations) == 1
        assert observations[0].layer_key == "cli-inbound"
    finally:
        session.close()


def test_cli_replays_source_dead_letter(cli_env: CliRunner) -> None:
    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="cli-dead-letter-source",
                source_kind="webhook_ingest",
                layer_key="cli-replay",
                target_uri="webhook://local",
            ),
        )
        record = SourceDeadLetterORM(
            source_id=source.source_id,
            source_run_id=None,
            adapter_kind="webhook_ingest",
            source_kind="webhook_ingest",
            stage="webhook_ingest",
            status="pending",
            failure_reason="synthetic test failure",
            payload_json={
                "title": "Replay payload",
                "url": "https://example.com/replay",
                "lat": 44.98,
                "lon": -93.24,
            },
            checkpoint_json={},
        )
        session.add(record)
        session.commit()
        dead_letter_id = record.source_dead_letter_id
        source_id = source.source_id
    finally:
        session.close()

    list_result = cli_env.invoke(app, ["list-source-dead-letters", "--source-id", str(source_id)])
    assert list_result.exit_code == 0
    assert f"{dead_letter_id} | source={source_id}" in list_result.stdout
    assert "status=pending" in list_result.stdout

    replay_result = cli_env.invoke(app, ["replay-source-dead-letter", str(dead_letter_id)])
    assert replay_result.exit_code == 0
    assert f"dead_letter={dead_letter_id}" in replay_result.stdout
    assert "status=replayed" in replay_result.stdout

    session = get_session_factory()()
    try:
        record = session.get(SourceDeadLetterORM, dead_letter_id)
        observations = list(session.query(ObservationORM).all())
        assert record is not None
        assert record.status == "replayed"
        assert record.replay_count == 1
        assert len(observations) == 1
        assert observations[0].layer_key == "cli-replay"
    finally:
        session.close()


def test_cli_add_source_web_discovery_persists_campaign_metadata(cli_env: CliRunner) -> None:
    result = cli_env.invoke(
        app,
        [
            "add-source-web-discovery",
            "cli-discovery-source",
            "cli-discovery-layer",
            "--query",
            "harbor departure",
            "--provider",
            "generic_html",
            "--provider-target",
            "generic_html=http://127.0.0.1:8010/search/html?page_size=1",
            "--seed-url",
            "http://127.0.0.1:8010/crawl/root",
            "--sitemap-url",
            "http://127.0.0.1:8010/sitemap.xml",
            "--robots-user-agent",
            "11Writer-Forte",
            "--max-payload-bytes",
            "2048",
            "--min-request-interval-seconds",
            "0.25",
            "--crawl-page-limit",
            "25",
        ],
    )
    assert result.exit_code == 0
    assert "created for discover://web" in result.stdout

    init_db()
    session = get_session_factory()()
    try:
        source = session.query(SourceDefinitionORM).filter_by(name="cli-discovery-source").one()
        assert source.source_kind == "web_discovery"
        assert source.layer_key == "cli-discovery-layer"
        assert source.metadata_json["search_providers"] == ["generic_html"]
        assert source.metadata_json["search_provider_targets"]["generic_html"].endswith("/search/html?page_size=1")
        assert source.metadata_json["seed_urls"] == ["http://127.0.0.1:8010/crawl/root"]
        assert source.metadata_json["sitemap_urls"] == ["http://127.0.0.1:8010/sitemap.xml"]
        assert source.metadata_json["respect_robots_txt"] is True
        assert source.metadata_json["robots_user_agents"] == ["11Writer-Forte"]
        assert source.metadata_json["max_payload_bytes"] == 2048
        assert source.metadata_json["min_request_interval_seconds"] == 0.25
    finally:
        session.close()


def test_cli_lists_supported_web_search_providers(cli_env: CliRunner) -> None:
    result = cli_env.invoke(app, ["list-web-search-providers"])
    assert result.exit_code == 0
    assert "generic_html" in result.stdout
    assert "duckduckgo_html" in result.stdout
    assert "bing_html" in result.stdout


def test_cli_run_web_discovery_now_upserts_and_executes(cli_env: CliRunner) -> None:
    with cli_html_discovery_server() as (base_url, state):
        first_result = cli_env.invoke(
            app,
            [
                "run-web-discovery-now",
                "cli-discovery-now",
                "cli-discovery-live",
                "--query",
                "harbor departure",
                "--provider",
                "generic_html",
                "--provider-target",
                f"generic_html={base_url}/search/html",
                "--seed-url",
                f"{base_url}/crawl/root",
                "--discover-sitemaps-from-seeds",
                "--crawl-depth",
                "1",
                "--crawl-page-limit",
                "3",
                "--same-domain-only",
                "--include-url-pattern",
                "/sites/",
                "--include-url-pattern",
                "/crawl/root",
                "--upsert-existing",
            ],
        )
        assert first_result.exit_code == 0
        assert "action=created" in first_result.stdout
        assert "status=completed" in first_result.stdout

        second_result = cli_env.invoke(
            app,
            [
                "run-web-discovery-now",
                "cli-discovery-now",
                "cli-discovery-live",
                "--query",
                "rail delay",
                "--provider",
                "generic_html",
                "--provider-target",
                f"generic_html={base_url}/search/html",
                "--no-discover-sitemaps-from-seeds",
                "--crawl-depth",
                "0",
                "--crawl-page-limit",
                "1",
                "--include-url-pattern",
                "/sites/",
                "--upsert-existing",
            ],
        )
        assert second_result.exit_code == 0
        assert "action=updated" in second_result.stdout
        assert "status=completed" in second_result.stdout

        init_db()
        session = get_session_factory()()
        try:
            source = session.query(SourceDefinitionORM).filter_by(name="cli-discovery-now").one()
            observations = list(
                session.query(ObservationORM)
                .filter_by(layer_key="cli-discovery-live")
                .order_by(ObservationORM.observation_id.asc())
            )
            assert source.source_kind == "web_discovery"
            assert any("/sites/briefs/harbor-departure" in row.content_json.get("page_url", "") for row in observations)
            assert any("/sites/briefs/rail-delay" in row.content_json.get("page_url", "") for row in observations)
        finally:
            session.close()

        assert "/search/html" in state["requests"]


def test_cli_bootstrap_local_runtime_is_idempotent(cli_env: CliRunner) -> None:
    first_result = cli_env.invoke(
        app,
        [
            "bootstrap-local-runtime",
            "--base-url",
            "http://127.0.0.1:8010",
            "--layer-prefix",
            "bootstrap-live",
            "--schedule-prefix",
            "bootstrap-runtime",
        ],
    )
    assert first_result.exit_code == 0
    assert "local sources | created=5 updated=0" in first_result.stdout
    assert "local schedules | created=12 updated=0" in first_result.stdout
    assert "platform-runtime-worker --poll-seconds 5" in first_result.stdout

    second_result = cli_env.invoke(
        app,
        [
            "bootstrap-local-runtime",
            "--base-url",
            "http://127.0.0.1:8010",
            "--layer-prefix",
            "bootstrap-live",
            "--schedule-prefix",
            "bootstrap-runtime",
        ],
    )
    assert second_result.exit_code == 0
    assert "local sources | created=0 updated=5" in second_result.stdout
    assert "local schedules | created=0 updated=12" in second_result.stdout
    assert "platform-runtime-worker --poll-seconds 5" in second_result.stdout

    init_db()
    session = get_session_factory()()
    try:
        sources = list(session.query(SourceDefinitionORM).order_by(SourceDefinitionORM.name.asc()))
        tasks = list(session.query(ScheduledTaskORM).order_by(ScheduledTaskORM.name.asc()))
        assert len(sources) == 5
        assert len(tasks) == 12
        assert any(source.source_kind == "web_discovery" for source in sources)
        assert any(task.task_type == "source_maintenance" for task in tasks)
        assert any(task.task_type == "source_health_scan" for task in tasks)
        assert any(task.task_type == "entity_resolution_refresh" for task in tasks)
        assert any(task.task_type == "event_fusion_refresh" for task in tasks)
        assert any(task.task_type == "runtime_snapshot_export" for task in tasks)
        assert any(task.task_type == "camera_inventory_refresh" for task in tasks)
        assert any(task.task_type == "camera_source_verification" for task in tasks)
        assert sum(1 for task in tasks if task.task_type == "source_sync") == 3
    finally:
        session.close()

    readiness_result = cli_env.invoke(app, ["show-runtime-readiness"])
    assert readiness_result.exit_code == 0
    assert "status=not_ready ready=False" in readiness_result.stdout
    assert "scheduler_executor_coverage: action_required" in readiness_result.stdout
    assert "source_runtime_executor_coverage: action_required" in readiness_result.stdout

    worker_result = cli_env.invoke(
        app,
        [
            "platform-runtime-worker",
            "--once",
            "--max-iterations",
            "1",
            "--no-include-stream-runtime",
            "--no-include-enabled-schedules",
        ],
    )
    assert worker_result.exit_code == 0

    readiness_result = cli_env.invoke(app, ["show-runtime-readiness"])
    assert readiness_result.exit_code == 0
    assert "status=not_ready ready=False" in readiness_result.stdout
    assert "scheduler_executor_coverage: action_required" in readiness_result.stdout
    assert "source_runtime_executor_coverage: action_required" in readiness_result.stdout
    assert "source_sync_coverage: pass" in readiness_result.stdout


def test_cli_bootstrap_local_runtime_can_run_initial_cycle(cli_env: CliRunner, tmp_path: Path) -> None:
    fixture = tmp_path / "bootstrap-cycle-source.json"
    fixture.write_text(
        '{"title":"Bootstrap cycle source","url":"https://bootstrap-cycle.example.com/1","lat":44.95,"lon":-93.27}',
        encoding="utf-8",
    )

    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="bootstrap-cycle-source",
                source_kind="local_file",
                layer_key="bootstrap-cycle-layer",
                target_uri=str(fixture),
            ),
        )
        session.add(
            ScheduledTaskORM(
                name="bootstrap-cycle-source-sync",
                task_type="source_sync",
                interval_seconds=300,
                source_id=source.source_id,
                enabled=True,
                payload_json={},
            )
        )
        session.commit()
    finally:
        session.close()

    result = cli_env.invoke(
        app,
        [
            "bootstrap-local-runtime",
            "--no-with-demo-sources",
            "--no-with-default-schedules",
            "--run-initial-cycle",
            "--no-include-stream-runtime-in-initial-cycle",
            "--initial-cycle-task-type",
            "source_sync",
        ],
    )
    assert result.exit_code == 0
    assert "local sources | skipped" in result.stdout
    assert "local schedules | skipped" in result.stdout
    assert "initial_cycle streams=0" in result.stdout
    assert "schedules=1" in result.stdout

    session = get_session_factory()()
    try:
        observations = list(
            session.query(ObservationORM)
            .filter_by(layer_key="bootstrap-cycle-layer")
            .order_by(ObservationORM.observation_id.asc())
        )
        assert len(observations) == 1
    finally:
        session.close()


def test_cli_smoke_test_local_runtime_reports_ready_for_minimal_runtime(
    cli_env: CliRunner,
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "smoke-runtime-source.json"
    fixture.write_text(
        '{"title":"Smoke runtime source","url":"https://smoke-runtime.example.com/1","lat":44.95,"lon":-93.27}',
        encoding="utf-8",
    )

    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="smoke-runtime-source",
                source_kind="local_file",
                layer_key="smoke-runtime-layer",
                target_uri=str(fixture),
            ),
        )
        session.add_all(
            [
                ScheduledTaskORM(
                    name="smoke-runtime-source-sync",
                    task_type="source_sync",
                    interval_seconds=300,
                    source_id=source.source_id,
                    enabled=True,
                    payload_json={},
                ),
                ScheduledTaskORM(
                    name="smoke-runtime-source-maintenance",
                    task_type="source_maintenance",
                    interval_seconds=600,
                    enabled=True,
                    payload_json={"stale_after_hours": 24.0, "source_limit": 25, "dead_letter_limit": 50},
                ),
                ScheduledTaskORM(
                    name="smoke-runtime-source-health",
                    task_type="source_health_scan",
                    interval_seconds=600,
                    enabled=True,
                    payload_json={"stale_after_hours": 24.0, "source_limit": 50},
                ),
                ScheduledTaskORM(
                    name="smoke-runtime-storage-lifecycle",
                    task_type="storage_lifecycle",
                    interval_seconds=3600,
                    enabled=True,
                    payload_json={"limit": 100},
                ),
                ScheduledTaskORM(
                    name="smoke-runtime-entity-resolution",
                    task_type="entity_resolution_refresh",
                    interval_seconds=900,
                    enabled=True,
                    payload_json={"limit": 100, "min_observations": 2},
                ),
                ScheduledTaskORM(
                    name="smoke-runtime-event-fusion",
                    task_type="event_fusion_refresh",
                    interval_seconds=900,
                    enabled=True,
                    payload_json={
                        "limit": 100,
                        "time_window_minutes": 120,
                        "distance_km": 10.0,
                        "min_independent_signals": 2,
                    },
                ),
                ScheduledTaskORM(
                    name="smoke-runtime-snapshot",
                    task_type="runtime_snapshot_export",
                    interval_seconds=21600,
                    enabled=True,
                    payload_json={"file_prefix": "smoke-runtime-snapshot"},
                ),
            ]
        )
        session.commit()
    finally:
        session.close()

    result = cli_env.invoke(
        app,
        [
            "smoke-test-local-runtime",
            "--no-with-demo-sources",
            "--no-with-default-schedules",
            "--no-run-initial-cycle",
            "--worker-task-type",
            "source_sync",
            "--worker-task-type",
            "runtime_snapshot_export",
            "--no-worker-include-stream-runtime",
        ],
    )
    assert result.exit_code == 0
    assert "local sources | skipped" in result.stdout
    assert "local schedules | skipped" in result.stdout
    assert "worker_once iterations=1" in result.stdout
    assert "readiness status=ready ready=True" in result.stdout
    assert "analytics_runtime events=0 entities=0 products=0" in result.stdout
    assert "backup_runtime snapshots=1 manifests=1" in result.stdout
    assert "backup_coverage status=pass" in result.stdout


def test_cli_smoke_test_local_runtime_validates_demo_web_collection(cli_env: CliRunner) -> None:
    with cli_local_upstreams_server() as base_url:
        result = cli_env.invoke(
            app,
            [
                "smoke-test-local-runtime",
                "--base-url",
                base_url,
                "--no-run-initial-cycle",
            ],
        )

    assert result.exit_code == 0
    assert "readiness status=ready ready=True" in result.stdout
    assert "web_runtime web_search_obs=" in result.stdout
    assert "web_discovery_obs=" in result.stdout
    assert "web_crawl_obs=" in result.stdout
    assert "source_runs=" in result.stdout
    assert "checkpoints=" in result.stdout

    init_db()
    session = get_session_factory()()
    try:
        web_search_count = session.query(ObservationORM).filter_by(layer_key="local-live-web-search").count()
        web_discovery_count = session.query(ObservationORM).filter_by(layer_key="local-live-web-discovery").count()
        web_crawl_count = session.query(ObservationORM).filter_by(layer_key="local-live-web-crawl").count()
        assert web_search_count >= 1
        assert web_discovery_count >= 1
        assert web_crawl_count >= 1
    finally:
        session.close()


def test_cli_verify_local_runtime_stack_reports_ready_runtime(cli_env: CliRunner) -> None:
    with cli_runtime_probe_server(ready=True, include_worker=True) as base_url:
        result = cli_env.invoke(
            app,
            [
                "verify-local-runtime-stack",
                "--api-base-url",
                base_url,
                "--upstreams-base-url",
                base_url,
            ],
        )
    assert result.exit_code == 0
    assert "api_health http=200 status=ok" in result.stdout
    assert "api_ready http=200 status=ready ready=True" in result.stdout
    assert "workers http=200 expected_type=platform_runtime_worker matched=1 active=1" in result.stdout
    assert "local_upstreams http=200 status=ok" in result.stdout
    assert "runtime_stack ok=True" in result.stdout


def test_cli_verify_local_runtime_stack_can_enforce_postgres_postgis_runtime(cli_env: CliRunner) -> None:
    with cli_runtime_probe_server(
        ready=True,
        include_worker=True,
        database_backend="postgresql",
        spatial_backend="postgis",
        database_status="ok",
        postgis_extension_installed=True,
        schema_up_to_date=True,
        version_table_present=True,
    ) as base_url:
        result = cli_env.invoke(
            app,
            [
                "verify-local-runtime-stack",
                "--api-base-url",
                base_url,
                "--upstreams-base-url",
                base_url,
                "--check-database-diagnostics",
                "--expected-database-backend",
                "postgresql",
                "--expected-spatial-backend",
                "postgis",
                "--require-postgis-ready",
                "--require-database-status-ok",
                "--require-schema-current",
            ],
        )
    assert result.exit_code == 0
    assert "database http=200 status=ok backend=postgresql spatial=postgis connected=True postgis_ready=True schema_current=True" in result.stdout
    assert "runtime_stack ok=True" in result.stdout


def test_cli_verify_local_runtime_stack_fails_when_database_runtime_contract_is_wrong(cli_env: CliRunner) -> None:
    with cli_runtime_probe_server(
        ready=True,
        include_worker=True,
        database_backend="sqlite",
        spatial_backend="python",
        database_status="ok",
        postgis_extension_installed=None,
        schema_up_to_date=False,
        version_table_present=False,
    ) as base_url:
        result = cli_env.invoke(
            app,
            [
                "verify-local-runtime-stack",
                "--api-base-url",
                base_url,
                "--upstreams-base-url",
                base_url,
                "--check-database-diagnostics",
                "--expected-database-backend",
                "postgresql",
                "--expected-spatial-backend",
                "postgis",
                "--require-postgis-ready",
                "--require-database-status-ok",
                "--require-schema-current",
            ],
        )
    assert result.exit_code == 1
    assert "database http=200 status=ok backend=sqlite spatial=python connected=True postgis_ready=True schema_current=False" in result.stdout
    assert "failed_checks:" in result.stdout
    assert "database_diagnostics" in result.stdout


def test_cli_verify_local_runtime_stack_fails_when_runtime_not_ready(cli_env: CliRunner) -> None:
    with cli_runtime_probe_server(ready=False, include_worker=False) as base_url:
        result = cli_env.invoke(
            app,
            [
                "verify-local-runtime-stack",
                "--api-base-url",
                base_url,
                "--upstreams-base-url",
                base_url,
            ],
        )
    assert result.exit_code == 1
    assert "api_ready http=503 status=not_ready ready=False" in result.stdout
    assert "runtime_stack ok=False" in result.stdout
    assert "failed_checks:" in result.stdout
    assert "operator_actions:" in result.stdout


def test_cli_verify_local_runtime_stack_can_wait_for_runtime_readiness(cli_env: CliRunner) -> None:
    with cli_runtime_probe_server(
        ready=True,
        include_worker=True,
        ready_after_ready_checks=2,
    ) as base_url:
        result = cli_env.invoke(
            app,
            [
                "verify-local-runtime-stack",
                "--api-base-url",
                base_url,
                "--upstreams-base-url",
                base_url,
                "--wait-seconds",
                "1",
                "--poll-seconds",
                "0.1",
            ],
        )
    assert result.exit_code == 0
    assert "runtime_stack ok=True attempts=" in result.stdout


def test_cli_run_platform_cycle_executes_enabled_schedules(cli_env: CliRunner, tmp_path: Path) -> None:
    fixture = tmp_path / "platform-cycle-source.json"
    fixture.write_text(
        '{"title":"Platform cycle source","url":"https://platform-cycle.example.com/1","lat":44.95,"lon":-93.27}',
        encoding="utf-8",
    )

    init_db()
    session = get_session_factory()()
    try:
        source = create_source_definition(
            session,
            SourceDefinitionCreate(
                name="platform-cycle-source",
                source_kind="local_file",
                layer_key="platform-cycle-layer",
                target_uri=str(fixture),
            ),
        )
        session.add(
            ScheduledTaskORM(
                name="platform-cycle-source-sync",
                task_type="source_sync",
                interval_seconds=300,
                source_id=source.source_id,
                enabled=True,
                payload_json={},
            )
        )
        session.commit()
    finally:
        session.close()

    result = cli_env.invoke(
        app,
        [
            "run-platform-cycle",
            "--task-type",
            "source_sync",
            "--no-include-stream-runtime",
        ],
    )
    assert result.exit_code == 0
    assert "runtime_cycle streams=0" in result.stdout
    assert "schedules=1" in result.stdout

    session = get_session_factory()()
    try:
        observations = list(session.query(ObservationORM).all())
        assert len(observations) == 1
        assert observations[0].layer_key == "platform-cycle-layer"
    finally:
        session.close()
