from __future__ import annotations

import base64
import hashlib
import json
import socketserver
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

import pytest
from fastapi.testclient import TestClient

from src.db import get_session_factory
from src.services.platform_runtime_service import run_platform_runtime_cycle
from src.services.source_runtime_service import run_source_runtime_worker
from src.services.source_service import SourceExecutionError, run_source_definition, run_source_runtime_cycle


@contextmanager
def flaky_json_server(payload: list[dict[str, object]]):
    state = {"requests": 0, "last_user_agent": None, "last_header": None}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            state["requests"] += 1
            state["last_user_agent"] = self.headers.get("User-Agent")
            state["last_header"] = self.headers.get("X-Test-Token")
            if state["requests"] == 1:
                self.send_response(503)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"temporary failure")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode("utf-8"))

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/feed.json", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def json_server(payload: object, *, content_type: str = "application/json"):
    state = {"requests": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            state["requests"] += 1
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.end_headers()
            if isinstance(payload, bytes):
                body = payload
            elif isinstance(payload, str):
                body = payload.encode("utf-8")
            else:
                body = json.dumps(payload).encode("utf-8")
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/feed.json", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def basic_auth_xml_server(payload: str, *, username: str, password: str):
    expected_token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    state = {"requests": 0, "last_authorization": None}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            state["requests"] += 1
            state["last_authorization"] = self.headers.get("Authorization")
            if state["last_authorization"] != f"Basic {expected_token}":
                self.send_response(401)
                self.send_header("WWW-Authenticate", 'Basic realm="test"')
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.end_headers()
            self.wfile.write(payload.encode("utf-8"))

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/feed.xml", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def sse_server(events: list[dict[str, object]]):
    state = {"last_event_id": None}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            state["last_event_id"] = self.headers.get("Last-Event-ID")
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            for event in events:
                if "id" in event:
                    self.wfile.write(f"id: {event['id']}\n".encode("utf-8"))
                if "event" in event:
                    self.wfile.write(f"event: {event['event']}\n".encode("utf-8"))
                self.wfile.write(f"data: {json.dumps(event['data'])}\n\n".encode("utf-8"))
                self.wfile.flush()

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/stream", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def websocket_frame(payload: bytes, opcode: int = 0x1) -> bytes:
    header = bytes([0x80 | opcode])
    size = len(payload)
    if size < 126:
        return header + bytes([size]) + payload
    if size <= 0xFFFF:
        return header + bytes([126]) + size.to_bytes(2, "big") + payload
    return header + bytes([127]) + size.to_bytes(8, "big") + payload


@contextmanager
def websocket_server(messages: list[dict[str, object]]):
    state = {"connections": 0}

    class Handler(socketserver.BaseRequestHandler):
        def handle(self) -> None:
            state["connections"] += 1
            buffer = b""
            while b"\r\n\r\n" not in buffer:
                chunk = self.request.recv(4096)
                if not chunk:
                    return
                buffer += chunk
            request_text = buffer.decode("utf-8", errors="replace")
            key = None
            for line in request_text.split("\r\n"):
                if line.lower().startswith("sec-websocket-key:"):
                    key = line.split(":", 1)[1].strip()
                    break
            if not key:
                return
            accept = base64.b64encode(
                hashlib.sha1(f"{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11".encode("utf-8")).digest()
            ).decode("ascii")
            response = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            )
            self.request.sendall(response.encode("utf-8"))
            time.sleep(0.05)
            for message in messages:
                self.request.sendall(websocket_frame(json.dumps(message).encode("utf-8")))
            self.request.sendall(websocket_frame(b"", opcode=0x8))

    server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"ws://127.0.0.1:{server.server_address[1]}/stream", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def camera_endpoint_server():
    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self) -> None:  # noqa: N802
            if self.path == "/camera.jpg":
                self.send_response(200)
                self.send_header("Content-Type", "image/jpeg")
                self.end_headers()
                return
            if self.path == "/camera.m3u8":
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.end_headers()
                return
            if self.path == "/camera-page":
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                return
            self.send_response(404)
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/camera.jpg":
                self.send_response(200)
                self.send_header("Content-Type", "image/jpeg")
                self.end_headers()
                self.wfile.write(b"camera-jpeg")
                return
            if self.path == "/camera.m3u8":
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.end_headers()
                self.wfile.write(b"#EXTM3U\n#EXTINF:6.0,\nsegment.ts\n#EXT-X-ENDLIST\n")
                return
            if self.path == "/camera-page":
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"<html><body>camera page</body></html>")
                return
            self.send_response(404)
            self.end_headers()

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
def html_discovery_server():
    pages = {
        "/sites/briefs/harbor-departure": {
            "title": "Harbor Departure Confirmed",
            "description": "Cargo vessel departure confirmed by local observers.",
            "latitude": 44.9778,
            "longitude": -93.2650,
            "observed_at": "2026-07-07T05:45:00Z",
            "paragraphs": [
                "Observers reported a vessel clearing the berth shortly after dawn.",
                "The page exists to test result-page fetch and import.",
            ],
            "links": [
                "/sites/reference/local-osint-overview",
                "/sites/briefs/harbor-manifest",
            ],
        },
        "/sites/briefs/harbor-manifest": {
            "title": "Harbor Manifest Notes",
            "description": "Manifest notes supporting the harbor movement.",
            "latitude": 44.9784,
            "longitude": -93.2641,
            "observed_at": "2026-07-07T05:52:00Z",
            "paragraphs": [
                "The manifest references outbound cargo and tug escort activity.",
            ],
            "links": [
                "/sites/reference/local-osint-overview",
            ],
        },
        "/sites/briefs/rail-delay": {
            "title": "Rail Delay Bulletin",
            "description": "Dispatch notes describing a junction slowdown.",
            "latitude": 44.9867,
            "longitude": -93.2581,
            "observed_at": "2026-07-07T22:10:00Z",
            "paragraphs": [
                "Dispatch logged a temporary rail slowdown near the yard lead.",
            ],
            "links": [
                "/sites/reference/local-osint-overview",
            ],
        },
        "/sites/reference/local-osint-overview": {
            "title": "Local OSINT Overview",
            "description": "Reference page linking the local mock site graph together.",
            "latitude": 44.9815,
            "longitude": -93.2619,
            "observed_at": "2026-07-07T06:05:00Z",
            "paragraphs": [
                "This reference page helps the crawler traverse more than one page.",
            ],
            "links": [
                "/sites/briefs/harbor-departure",
                "/sites/briefs/rail-delay",
            ],
        },
    }
    state = {"requests": []}

    def render_html(
        title: str,
        description: str,
        paragraphs: list[str],
        links: list[tuple[str, str]],
        *,
        latitude: float | None = None,
        longitude: float | None = None,
        observed_at: str | None = None,
    ) -> str:
        paragraph_markup = "".join(f"<p>{paragraph}</p>" for paragraph in paragraphs)
        link_markup = "".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
        meta_markup = ""
        if latitude is not None and longitude is not None:
            meta_markup = (
                f'<meta name="geo.position" content="{latitude};{longitude}" />'
                f'<meta property="article:published_time" content="{observed_at or ""}" />'
            )
        time_markup = f'<time datetime="{observed_at}">{observed_at}</time>' if observed_at else ""
        return (
            "<!DOCTYPE html><html><head>"
            f"<title>{title}</title>"
            f'<meta name="description" content="{description}" />'
            f"{meta_markup}</head><body>"
            f"<h1>{title}</h1><p>{description}</p>{time_markup}{paragraph_markup}<ul>{link_markup}</ul>"
            "</body></html>"
        )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            state["requests"].append(parsed.path)
            root_url = f"http://127.0.0.1:{server.server_port}"
            if parsed.path == "/search/html":
                query = parse_qs(parsed.query).get("q", [""])[0].lower()
                page_number = max(1, int(parse_qs(parsed.query).get("page", ["1"])[0]))
                page_size = max(1, int(parse_qs(parsed.query).get("page_size", ["20"])[0]))
                harbor_target = quote("/sites/briefs/harbor-departure", safe="/")
                manifest_target = quote("/sites/briefs/harbor-manifest", safe="/")
                rail_target = quote("/sites/briefs/rail-delay", safe="/")
                if "rail" in query:
                    links = [
                        (f"/search/redirect?target={rail_target}", "Rail Delay Bulletin"),
                        ("/search/about", "Search Harness About"),
                    ]
                else:
                    all_links = [
                        (f"/search/redirect?target={harbor_target}", "Harbor Departure Confirmed"),
                        (f"/search/redirect?target={manifest_target}", "Harbor Manifest Notes"),
                        ("/search/about", "Search Harness About"),
                    ]
                    start_index = max(0, (page_number - 1) * page_size)
                    links = all_links[start_index : start_index + page_size]
                    if start_index + page_size < len(all_links) - 1:
                        links.append((f"/search/html?q={quote(query)}&page={page_number + 1}&page_size={page_size}", "Next Results"))
                html = render_html(
                    f"Local Search Results Page {page_number}",
                    "Local HTML search provider used for source discovery tests.",
                    [f"Search query: {query or 'all'}", f"Page number: {page_number}"],
                    links,
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(html.encode("utf-8"))
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
                    '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                    f"<sitemap><loc>{root_url}/sitemaps/local.xml</loc></sitemap>"
                    "</sitemapindex>"
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.end_headers()
                self.wfile.write(xml.encode("utf-8"))
                return
            if parsed.path == "/sitemaps/local.xml":
                page_entries = "".join(
                    f"<url><loc>{root_url}{path}</loc></url>"
                    for path in sorted(pages.keys())
                )
                xml = (
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                    f"{page_entries}</urlset>"
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.end_headers()
                self.wfile.write(xml.encode("utf-8"))
                return
            if parsed.path == "/search/about":
                html = render_html(
                    "Search Harness About",
                    "Provider-owned helper page that search filters should ignore.",
                    ["This page is not meant to be imported as a result page."],
                    [("/sites/reference/local-osint-overview", "Local OSINT Overview")],
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
            if parsed.path == "/crawl/root":
                html = render_html(
                    "Local Crawl Root",
                    "Seed page for crawl-source tests.",
                    ["This page fans out into a small local HTML graph."],
                    [
                        ("/sites/reference/local-osint-overview", "Local OSINT Overview"),
                        ("/sites/briefs/harbor-departure", "Harbor Departure Confirmed"),
                    ],
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(html.encode("utf-8"))
                return
            page = pages.get(parsed.path)
            if page is None:
                self.send_response(404)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"not found")
                return
            html = render_html(
                str(page["title"]),
                str(page["description"]),
                list(page["paragraphs"]),
                [(href, href.rsplit("/", 1)[-1].replace("-", " ").title()) for href in page["links"]],
                latitude=float(page["latitude"]),
                longitude=float(page["longitude"]),
                observed_at=str(page["observed_at"]),
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
def robots_guarded_discovery_server():
    pages = {
        "/allowed/article": {
            "title": "Allowed Article",
            "description": "This page is allowed by robots.txt.",
            "paragraphs": ["Allowed article body."],
            "links": [],
        },
        "/blocked/article": {
            "title": "Blocked Article",
            "description": "This page should never be fetched.",
            "paragraphs": ["Blocked article body."],
            "links": [],
        },
    }
    state = {"requests": [], "request_log": []}

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
            state["request_log"].append((parsed.path, time.perf_counter()))
            root_url = f"http://127.0.0.1:{server.server_port}"
            allowed_target = quote("/allowed/article", safe="/")
            blocked_target = quote("/blocked/article", safe="/")
            if parsed.path == "/search/html":
                html = render_html(
                    "Guarded Search",
                    "Search page for robots policy tests.",
                    ["One result is allowed and one is blocked."],
                    [
                        (f"/search/redirect?target={allowed_target}", "Allowed Article"),
                        (f"/search/redirect?target={blocked_target}", "Blocked Article"),
                    ],
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
                self.wfile.write(
                    (
                        "User-agent: *\n"
                        "Crawl-delay: 0.02\n"
                        "Allow: /allowed/\n"
                        "Disallow: /blocked/\n"
                        f"Sitemap: {root_url}/sitemap.xml\n"
                    ).encode("utf-8")
                )
                return
            if parsed.path == "/sitemap.xml":
                xml = (
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
                    f"<url><loc>{root_url}/allowed/article</loc></url>"
                    f"<url><loc>{root_url}/blocked/article</loc></url>"
                    "</urlset>"
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.end_headers()
                self.wfile.write(xml.encode("utf-8"))
                return
            if parsed.path == "/crawl/root":
                html = render_html(
                    "Crawl Root",
                    "Seed page for robots tests.",
                    ["This root links to both pages."],
                    [
                        ("/allowed/article", "Allowed Article"),
                        ("/blocked/article", "Blocked Article"),
                    ],
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(html.encode("utf-8"))
                return
            page = pages.get(parsed.path)
            if page is None:
                self.send_response(404)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"not found")
                return
            html = render_html(
                str(page["title"]),
                str(page["description"]),
                list(page["paragraphs"]),
                [(href, label) for href, label in page["links"]],
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


def test_source_definition_run_creates_import_and_history(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "source-file.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Source catalog record",
                    "url": "https://source.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "harbor-source",
            "source_kind": "local_file",
            "layer_key": "marine-track",
            "target_uri": str(fixture),
            "integrity_source": True,
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    run_response = client.post(f"/api/sources/{source_id}/run")
    assert run_response.status_code == 200
    payload = run_response.json()
    assert payload["status"] == "completed"
    assert payload["records_imported"] == 1
    assert payload["import_run_id"] is not None

    runs_response = client.get("/api/sources/runs")
    assert runs_response.status_code == 200
    assert runs_response.json()[0]["source_id"] == source_id

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    assert imports_response.json()[0]["records_imported"] == 1

    ops_response = client.get(f"/api/sources/{source_id}/ops")
    assert ops_response.status_code == 200
    ops_payload = ops_response.json()
    assert ops_payload["source"]["source_id"] == source_id
    assert ops_payload["recent_runs"][0]["source_run_id"] == payload["source_run_id"]
    assert len(ops_payload["storage_objects"]) == 1
    assert ops_payload["storage_objects"][0]["object_kind"] == "source_local_payload"
    assert ops_payload["storage_objects"][0]["source_uri"] == str(fixture)
    assert ops_payload["storage_objects"][0]["metadata_json"]["import_run_id"] == payload["import_run_id"]

    layers_response = client.get("/api/layers")
    assert layers_response.status_code == 200
    assert any(layer["key"] == "marine-track" for layer in layers_response.json())

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(
        row["object_type"] == "source_definition"
        and row["object_id"] == str(source_id)
        and row["action"] == "source_created"
        for row in custody_rows
    )
    assert any(
        row["object_type"] == "source_run"
        and row["action"] == "source_run_started"
        for row in custody_rows
    )
    assert any(
        row["object_type"] == "source_run"
        and row["action"] == "source_run_completed"
        for row in custody_rows
    )
    assert any(
        row["object_type"] == "storage_object"
        and row["action"] in {"storage_registered", "storage_refreshed"}
        for row in custody_rows
    )


def test_source_sync_schedule_runs_source_definition(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "scheduled-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Scheduled source sync",
                    "url": "https://sync.example.com/1",
                    "lat": 30.0,
                    "lon": -95.0,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "scheduled-source",
            "source_kind": "local_file",
            "layer_key": "ops-feed",
            "target_uri": str(fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "source-sync-task",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": source_id,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]

    run_response = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert run_response.status_code == 200
    assert run_response.json()["records_affected"] == 1

    source_runs = client.get("/api/sources/runs")
    assert source_runs.status_code == 200
    assert source_runs.json()[0]["records_imported"] == 1

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    custody_rows = custody_response.json()
    assert any(
        row["object_type"] == "scheduled_task"
        and row["object_id"] == str(task_id)
        and row["action"] == "task_created"
        for row in custody_rows
    )


def test_http_source_retries_and_records_fetch_metadata(client: TestClient) -> None:
    payload = [
        {
            "title": "Remote source record",
            "url": "https://remote.example.com/1",
            "lat": 29.81,
            "lon": -95.41,
        }
    ]
    with flaky_json_server(payload) as (target_uri, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "remote-json-source",
                "source_kind": "http_json",
                "layer_key": "remote-feed",
                "target_uri": target_uri,
                "metadata_json": {
                    "retry_attempts": 2,
                    "retry_backoff_seconds": 0,
                    "request_timeout_seconds": 5,
                    "headers": {"X-Test-Token": "forte"},
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        run_response = client.post(f"/api/sources/{source_id}/run")
        assert run_response.status_code == 200
        run_payload = run_response.json()
        assert run_payload["status"] == "completed"
        assert run_payload["records_imported"] == 1
        assert run_payload["output_json"]["attempt_count"] == 2
        assert run_payload["output_json"]["http_status"] == 200
        assert run_payload["output_json"]["content_type"] == "application/json"
        assert run_payload["output_json"]["headers"]["X-Test-Token"] == "forte"
        assert run_payload["output_json"]["cached_path"].endswith(".json")

        assert state["requests"] == 2
        assert state["last_header"] == "forte"
        assert state["last_user_agent"] == "11Writer-Forte/0.1 (+headless-source-fetch)"

        ops_response = client.get(f"/api/sources/{source_id}/ops")
        assert ops_response.status_code == 200
        ops_payload = ops_response.json()
        assert ops_payload["storage_objects"][0]["object_kind"] == "source_cached_payload"
        assert ops_payload["storage_objects"][0]["object_uri"].endswith(".json")
        assert ops_payload["storage_objects"][0]["metadata_json"]["attempt_count"] == 2

        custody_response = client.get("/api/custody/logs")
        assert custody_response.status_code == 200
        custody_rows = custody_response.json()
        assert any(
            row["object_type"] == "source_run"
            and row["action"] == "source_payload_materialized"
            and row["details_json"]["attempt_count"] == 2
            for row in custody_rows
        )


def test_source_run_skips_unchanged_payloads(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "unchanged-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Stable payload",
                    "url": "https://stable.example.com/1",
                    "lat": 29.7,
                    "lon": -95.3,
                }
            ]
        ),
        encoding="utf-8",
    )

    source_response = client.post(
        "/api/sources",
        json={
            "name": "stable-source",
            "source_kind": "local_file",
            "layer_key": "stable-feed",
            "target_uri": str(fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    first_run = client.post(f"/api/sources/{source_id}/run")
    assert first_run.status_code == 200
    assert first_run.json()["status"] == "completed"
    first_import_run_id = first_run.json()["import_run_id"]

    second_run = client.post(f"/api/sources/{source_id}/run")
    assert second_run.status_code == 200
    second_payload = second_run.json()
    assert second_payload["status"] == "skipped"
    assert second_payload["import_run_id"] is None
    assert second_payload["output_json"]["skip_reason"] == "payload_unchanged"
    assert second_payload["output_json"]["payload_sha256"]

    imports_response = client.get("/api/imports/runs")
    assert imports_response.status_code == 200
    import_runs = imports_response.json()
    assert len(import_runs) == 1
    assert import_runs[0]["import_run_id"] == first_import_run_id

    source_runs_response = client.get("/api/sources/runs")
    assert source_runs_response.status_code == 200
    source_runs = source_runs_response.json()
    assert source_runs[0]["status"] == "skipped"
    assert source_runs[1]["status"] == "completed"

    ops_response = client.get(f"/api/sources/{source_id}/ops")
    assert ops_response.status_code == 200
    ops_payload = ops_response.json()
    assert len(ops_payload["storage_objects"]) == 2
    assert {row["metadata_json"]["run_status"] for row in ops_payload["storage_objects"]} == {
        "skipped",
        "completed",
    }

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "source_run"
        and row["action"] == "source_run_skipped"
        for row in custody_response.json()
    )


def test_web_search_source_discovers_and_imports_result_pages(client: TestClient) -> None:
    with html_discovery_server() as (base_url, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "html-search-source",
                "source_kind": "web_search",
                "layer_key": "web-discovery-search",
                "target_uri": f"{base_url}/search/html",
                "metadata_json": {
                    "query": "harbor departure",
                    "request_timeout_seconds": 5,
                    "search_result_limit": 10,
                    "page_fetch_limit": 5,
                    "fetch_result_pages": True,
                    "skip_provider_domain": False,
                    "result_redirect_query_param": "target",
                    "include_url_patterns": ["/sites/"],
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        run_response = client.post(f"/api/sources/{source_id}/run")
        assert run_response.status_code == 200
        run_payload = run_response.json()
        assert run_payload["status"] == "completed"
        assert run_payload["records_imported"] == 2
        assert run_payload["output_json"]["discovery_kind"] == "web_search"
        assert run_payload["output_json"]["search_query_count"] == 1
        assert run_payload["output_json"]["search_page_count"] == 1
        assert run_payload["output_json"]["search_result_candidate_count"] == 2
        assert run_payload["output_json"]["search_page_fetch_count"] == 2
        assert run_payload["output_json"]["cached_path"].endswith(".json")

        observations_response = client.get("/api/observations", params={"layer_key": "web-discovery-search"})
        assert observations_response.status_code == 200
        observations = observations_response.json()
        assert len(observations) == 2
        assert any("Cargo vessel departure confirmed by local observers." in row["content_text"] for row in observations)
        assert any("Manifest notes supporting the harbor movement." in row["content_text"] for row in observations)
        assert any(row["observed_at"] == "2026-07-07T05:45:00Z" for row in observations)
        assert any(row["location_geojson"]["coordinates"] == [-93.265, 44.9778] for row in observations)
        assert "/search/html" in state["requests"]
        assert "/sites/briefs/harbor-departure" in state["requests"]


def test_source_run_route_returns_structured_404_for_missing_local_source_input(
    client: TestClient,
    tmp_path: Path,
) -> None:
    missing_fixture = tmp_path / "missing-source-run.json"
    source_response = client.post(
        "/api/sources",
        json={
            "name": "missing-local-run-source",
            "source_kind": "local_file",
            "layer_key": "missing-source-run-layer",
            "target_uri": str(missing_fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    run_response = client.post(f"/api/sources/{source_id}/run")
    assert run_response.status_code == 404
    payload = run_response.json()
    assert payload["detail"]["source_id"] == source_id
    assert payload["detail"]["source_kind"] == "local_file"
    assert payload["detail"]["error_type"] == "FileNotFoundError"
    assert payload["detail"]["message"]

    runs_response = client.get("/api/sources/runs")
    assert runs_response.status_code == 200
    latest_run = next(row for row in runs_response.json() if row["source_id"] == source_id)
    assert latest_run["status"] == "failed"
    assert latest_run["source_run_id"] == payload["detail"]["source_run_id"]
    assert latest_run["error_text"]


def test_web_crawl_source_walks_local_html_graph(client: TestClient) -> None:
    with html_discovery_server() as (base_url, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "html-crawl-source",
                "source_kind": "web_crawl",
                "layer_key": "web-discovery-crawl",
                "target_uri": f"{base_url}/crawl/root",
                "metadata_json": {
                    "request_timeout_seconds": 5,
                    "crawl_depth": 2,
                    "crawl_page_limit": 10,
                    "crawl_link_limit": 10,
                    "same_domain_only": True,
                    "include_url_patterns": ["/sites/", "/crawl/root"],
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        run_response = client.post(f"/api/sources/{source_id}/run")
        assert run_response.status_code == 200
        run_payload = run_response.json()
        assert run_payload["status"] == "completed"
        assert run_payload["records_imported"] >= 3
        assert run_payload["output_json"]["discovery_kind"] == "web_crawl"
        assert run_payload["output_json"]["crawl_depth"] == 2
        assert run_payload["output_json"]["crawl_page_count"] >= 3
        assert "127.0.0.1" in run_payload["output_json"]["crawled_hosts"]

        observations_response = client.get("/api/observations", params={"layer_key": "web-discovery-crawl"})
        assert observations_response.status_code == 200
        observations = observations_response.json()
        assert len(observations) >= 3
        assert any("Seed page for crawl-source tests." in row["content_text"] for row in observations)
        assert any("Reference page linking the local mock site graph together." in row["content_text"] for row in observations)
        assert any(row["observed_at"] == "2026-07-07T06:05:00Z" for row in observations)
        assert any(row["location_geojson"]["coordinates"] == [-93.2619, 44.9815] for row in observations if row["location_geojson"])
        assert "/crawl/root" in state["requests"]
        assert "/sites/reference/local-osint-overview" in state["requests"]


def test_source_runtime_route_returns_structured_502_for_stream_runtime_failure(client: TestClient) -> None:
    source_response = client.post(
        "/api/sources",
        json={
            "name": "runtime-route-failing-stream-source",
            "source_kind": "websocket_stream",
            "layer_key": "runtime-route-layer",
            "target_uri": "ws://127.0.0.1:1/runtime-route",
            "metadata_json": {
                "request_timeout_seconds": 1,
                "stream_idle_timeout_seconds": 0.1,
                "stream_max_records": 5,
            },
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    runtime_response = client.post(f"/api/sources/{source_id}/runtime")
    assert runtime_response.status_code == 502
    payload = runtime_response.json()
    assert payload["detail"]["source_id"] == source_id
    assert payload["detail"]["source_kind"] == "websocket_stream"
    assert payload["detail"]["error_type"] in {
        "RuntimeError",
        "ConnectionRefusedError",
        "OSError",
        "TimeoutError",
    }
    assert payload["detail"]["message"]

    runs_response = client.get("/api/sources/runs")
    assert runs_response.status_code == 200
    latest_run = next(row for row in runs_response.json() if row["source_id"] == source_id)
    assert latest_run["status"] == "failed"
    assert latest_run["source_run_id"] == payload["detail"]["source_run_id"]


def test_web_search_provider_paginates_and_crawls_from_results(client: TestClient) -> None:
    with html_discovery_server() as (base_url, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "provider-search-source",
                "source_kind": "web_search",
                "layer_key": "web-discovery-provider",
                "target_uri": "search://web/generic_html",
                "metadata_json": {
                    "query": "harbor departure",
                    "search_target_uri": f"{base_url}/search/html?page_size=1",
                    "request_timeout_seconds": 5,
                    "search_page_param": "page",
                    "search_page_start": 1,
                    "search_page_step": 1,
                    "search_page_limit": 2,
                    "search_result_limit": 5,
                    "page_fetch_limit": 2,
                    "fetch_result_pages": True,
                    "crawl_from_results": True,
                    "result_crawl_depth": 1,
                    "result_crawl_page_limit": 5,
                    "result_crawl_link_limit": 10,
                    "result_crawl_same_domain_only": True,
                    "skip_provider_domain": False,
                    "result_redirect_query_param": "target",
                    "include_url_patterns": ["/sites/"],
                    "crawl_include_url_patterns": ["/sites/"],
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        run_response = client.post(f"/api/sources/{source_id}/run")
        assert run_response.status_code == 200
        run_payload = run_response.json()
        assert run_payload["status"] == "completed"
        assert run_payload["output_json"]["search_provider"] == "generic_html"
        assert run_payload["output_json"]["search_page_count"] == 2
        assert run_payload["output_json"]["search_provider_page_limit"] == 2
        assert run_payload["output_json"]["search_result_candidate_count"] == 2
        assert run_payload["output_json"]["search_page_fetch_count"] == 2
        assert run_payload["output_json"]["search_result_crawl_enabled"] is True
        assert run_payload["output_json"]["search_result_crawl_page_count"] >= 1

        observations_response = client.get("/api/observations", params={"layer_key": "web-discovery-provider"})
        assert observations_response.status_code == 200
        observations = observations_response.json()
        assert len(observations) >= 3
        assert any(row["content_json"].get("search_page_number") == 1 for row in observations)
        assert any(row["content_json"].get("search_page_number") == 2 for row in observations)
        assert any(row["content_json"].get("discovery_kind") == "web_search_crawl_page" for row in observations)
        assert any("/sites/reference/local-osint-overview" in row["content_json"].get("page_url", "") for row in observations)
        assert state["requests"].count("/search/html") == 2


def test_web_discovery_source_resumes_frontier_and_uses_sitemaps(client: TestClient) -> None:
    with html_discovery_server() as (base_url, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "generic-discovery-source",
                "source_kind": "web_discovery",
                "layer_key": "web-discovery-generic",
                "target_uri": "discover://web",
                "metadata_json": {
                    "query": "harbor departure",
                    "search_providers": ["generic_html"],
                    "search_provider_targets": {"generic_html": f"{base_url}/search/html?page_size=1"},
                    "request_timeout_seconds": 5,
                    "search_page_param": "page",
                    "search_page_start": 1,
                    "search_page_step": 1,
                    "search_page_limit": 2,
                    "search_result_limit": 5,
                    "page_fetch_limit": 1,
                    "fetch_result_pages": True,
                    "seed_urls": [f"{base_url}/crawl/root"],
                    "discover_sitemaps_from_seeds": True,
                    "sitemap_fetch_limit": 5,
                    "sitemap_url_limit": 10,
                    "crawl_depth": 1,
                    "crawl_page_limit": 2,
                    "crawl_link_limit": 10,
                    "same_domain_only": True,
                    "include_url_patterns": ["/sites/", "/crawl/root"],
                    "skip_provider_domain": False,
                    "result_redirect_query_param": "target",
                    "resume_frontier": True,
                    "skip_unchanged": True,
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        first_run_response = client.post(f"/api/sources/{source_id}/run")
        assert first_run_response.status_code == 200
        first_payload = first_run_response.json()
        assert first_payload["status"] == "completed"
        assert first_payload["output_json"]["discovery_kind"] == "web_discovery"
        assert first_payload["output_json"]["search_page_count"] == 2
        assert first_payload["output_json"]["sitemap_fetch_count"] >= 1
        assert first_payload["output_json"]["crawl_page_count"] == 2
        assert first_payload["output_json"]["source_checkpoint_json"]["pending_frontier"]

        second_run_response = client.post(f"/api/sources/{source_id}/run")
        assert second_run_response.status_code == 200
        second_payload = second_run_response.json()
        assert second_payload["status"] == "completed"
        assert second_payload["output_json"]["discovery_kind"] == "web_discovery"
        assert second_payload["output_json"]["checkpoint_state_sha256"] != first_payload["output_json"]["checkpoint_state_sha256"]

        source_runs_response = client.get("/api/sources/runs")
        assert source_runs_response.status_code == 200
        source_runs = [row for row in source_runs_response.json() if row["source_id"] == source_id]
        assert source_runs[0]["status"] == "completed"
        assert source_runs[1]["status"] == "completed"

        observations_response = client.get("/api/observations", params={"layer_key": "web-discovery-generic"})
        assert observations_response.status_code == 200
        observations = observations_response.json()
        assert len(observations) >= 4
        assert any(row["content_json"].get("discovery_kind") == "web_discovery_page" for row in observations)
        assert any("search_provider" in row["content_json"] for row in observations)
        assert "/robots.txt" in state["requests"]
        assert "/sitemap.xml" in state["requests"]


def test_web_discovery_respects_robots_txt_policy(client: TestClient) -> None:
    with robots_guarded_discovery_server() as (base_url, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "robots-guarded-discovery-source",
                "source_kind": "web_discovery",
                "layer_key": "robots-guarded-layer",
                "target_uri": "discover://web",
                "metadata_json": {
                    "query": "guarded article",
                    "search_providers": ["generic_html"],
                    "search_provider_targets": {"generic_html": f"{base_url}/search/html"},
                    "request_timeout_seconds": 5,
                    "fetch_result_pages": True,
                    "search_result_limit": 10,
                    "page_fetch_limit": 10,
                    "seed_urls": [f"{base_url}/crawl/root"],
                    "discover_sitemaps_from_seeds": True,
                    "crawl_depth": 1,
                    "crawl_page_limit": 10,
                    "crawl_link_limit": 10,
                    "same_domain_only": True,
                    "include_url_patterns": ["/allowed/", "/blocked/", "/crawl/root"],
                    "skip_provider_domain": False,
                    "result_redirect_query_param": "target",
                    "respect_robots_txt": True,
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        run_response = client.post(f"/api/sources/{source_id}/run")
        assert run_response.status_code == 200
        payload = run_response.json()
        assert payload["output_json"]["robots_policy_enabled"] is True
        assert payload["output_json"]["robots_origin_count"] >= 1
        assert payload["output_json"]["robots_blocked_count"] >= 1
        assert any("/blocked/article" in item for item in payload["output_json"]["robots_blocked_urls_sample"])

        observations_response = client.get("/api/observations", params={"layer_key": "robots-guarded-layer"})
        assert observations_response.status_code == 200
        observations = observations_response.json()
        assert any("/allowed/article" in row["content_json"].get("page_url", "") for row in observations)
        assert not any("/blocked/article" in row["content_json"].get("page_url", "") for row in observations)
        assert "/robots.txt" in state["requests"]
        assert "/allowed/article" in state["requests"]
        assert "/blocked/article" not in state["requests"]


def test_source_reporting_summarizes_web_collection_fleet_telemetry(client: TestClient) -> None:
    with robots_guarded_discovery_server() as (base_url, _state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "web-collection-reporting-source",
                "source_kind": "web_discovery",
                "layer_key": "web-collection-reporting-layer",
                "target_uri": "discover://web",
                "metadata_json": {
                    "query": "guarded article",
                    "search_providers": ["generic_html"],
                    "search_provider_targets": {"generic_html": f"{base_url}/search/html"},
                    "request_timeout_seconds": 5,
                    "fetch_result_pages": True,
                    "search_result_limit": 10,
                    "page_fetch_limit": 10,
                    "seed_urls": [f"{base_url}/crawl/root", f"{base_url}/missing"],
                    "discover_sitemaps_from_seeds": True,
                    "crawl_depth": 1,
                    "crawl_page_limit": 10,
                    "crawl_link_limit": 10,
                    "same_domain_only": True,
                    "include_url_patterns": ["/allowed/", "/blocked/", "/crawl/root", "/missing"],
                    "skip_provider_domain": False,
                    "result_redirect_query_param": "target",
                    "respect_robots_txt": True,
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        run_response = client.post(f"/api/sources/{source_id}/run")
        assert run_response.status_code == 200
        run_payload = run_response.json()
        assert run_payload["status"] == "completed"
        assert run_payload["output_json"]["discovery_kind"] == "web_discovery"
        assert run_payload["output_json"]["robots_blocked_count"] >= 1
        assert run_payload["output_json"]["crawl_fetch_error_count"] >= 1

        summary_response = client.get("/api/sources/summary", params={"stale_after_hours": 24})
        assert summary_response.status_code == 200
        summary = summary_response.json()
        web_summary = summary["web_collection_summary"]
        assert web_summary["total_source_count"] == 1
        assert web_summary["discovery_source_count"] == 1
        assert web_summary["problem_source_count"] == 1
        assert web_summary["robots_blocked_source_count"] == 1
        assert web_summary["fetch_error_source_count"] == 1
        assert web_summary["total_robots_blocked_count"] >= 1
        assert web_summary["total_crawl_fetch_error_count"] >= 1
        assert web_summary["total_search_page_count"] >= 1
        assert web_summary["total_crawl_page_count"] >= 1

        report_response = client.get(
            "/api/sources/report-index",
            params={"stale_after_hours": 24, "limit": 10, "stale_source_limit": 10},
        )
        assert report_response.status_code == 200
        report = report_response.json()
        assert report["web_collection_summary"]["problem_source_count"] == 1
        issue_source = next(
            row for row in report["web_collection_issue_sources"] if row["source"]["source_id"] == source_id
        )
        assert issue_source["web_collection_enabled"] is True
        assert issue_source["web_collection_kind"] == "web_discovery"
        assert issue_source["web_collection_stats"]["robots_blocked_count"] >= 1
        assert issue_source["web_collection_stats"]["crawl_fetch_error_count"] >= 1
        assert issue_source["web_collection_stats"]["has_issue"] is True


def test_web_discovery_applies_crawl_delay_request_pacing(client: TestClient) -> None:
    with robots_guarded_discovery_server() as (base_url, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "paced-discovery-source",
                "source_kind": "web_discovery",
                "layer_key": "paced-discovery-layer",
                "target_uri": "discover://web",
                "metadata_json": {
                    "query": "guarded article",
                    "search_providers": ["generic_html"],
                    "search_provider_targets": {"generic_html": f"{base_url}/search/html"},
                    "request_timeout_seconds": 5,
                    "fetch_result_pages": True,
                    "search_result_limit": 10,
                    "page_fetch_limit": 1,
                    "discover_sitemaps_from_seeds": False,
                    "crawl_depth": 0,
                    "crawl_page_limit": 1,
                    "crawl_link_limit": 10,
                    "same_domain_only": True,
                    "include_url_patterns": ["/allowed/", "/blocked/"],
                    "skip_provider_domain": False,
                    "result_redirect_query_param": "target",
                    "respect_robots_txt": True,
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        run_response = client.post(f"/api/sources/{source_id}/run")
        assert run_response.status_code == 200
        payload = run_response.json()
        assert payload["output_json"]["fetch_paced_request_count"] >= 1
        assert payload["output_json"]["fetch_pacing_delay_seconds"] > 0

        request_times = {path: ts for path, ts in state["request_log"]}
        assert "/search/html" in request_times
        assert "/robots.txt" in request_times
        assert "/allowed/article" in request_times
        assert request_times["/allowed/article"] - request_times["/search/html"] >= 0.015


def test_web_run_endpoint_upserts_and_executes_discovery_source(client: TestClient) -> None:
    with html_discovery_server() as (base_url, state):
        response = client.post(
            "/api/sources/web/run",
            json={
                "name": "immediate-discovery-source",
                "source_kind": "web_discovery",
                "layer_key": "web-immediate-layer",
                "target_uri": "discover://web",
                "metadata_json": {
                    "query": "harbor departure",
                    "search_providers": ["generic_html"],
                    "search_provider_targets": {"generic_html": f"{base_url}/search/html?page_size=1"},
                    "request_timeout_seconds": 5,
                    "search_page_param": "page",
                    "search_page_start": 1,
                    "search_page_step": 1,
                    "search_page_limit": 2,
                    "search_result_limit": 5,
                    "page_fetch_limit": 1,
                    "fetch_result_pages": True,
                    "seed_urls": [f"{base_url}/crawl/root"],
                    "discover_sitemaps_from_seeds": True,
                    "sitemap_fetch_limit": 5,
                    "sitemap_url_limit": 10,
                    "crawl_depth": 1,
                    "crawl_page_limit": 2,
                    "crawl_link_limit": 10,
                    "same_domain_only": True,
                    "include_url_patterns": ["/sites/", "/crawl/root"],
                    "skip_provider_domain": False,
                    "result_redirect_query_param": "target",
                    "resume_frontier": True,
                    "skip_unchanged": True,
                },
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["action"] == "created"
        assert payload["source"]["source_kind"] == "web_discovery"
        assert payload["source_run"]["status"] == "completed"
        assert payload["source_run"]["output_json"]["discovery_kind"] == "web_discovery"
        assert payload["source_run"]["records_imported"] >= 2

        second_response = client.post(
            "/api/sources/web/run",
            json={
                "name": "immediate-discovery-source",
                "source_kind": "web_discovery",
                "layer_key": "web-immediate-layer",
                "target_uri": "discover://web",
                "metadata_json": {
                    "query": "rail delay",
                    "search_providers": ["generic_html"],
                    "search_provider_targets": {"generic_html": f"{base_url}/search/html"},
                    "request_timeout_seconds": 5,
                    "fetch_result_pages": True,
                    "search_result_limit": 5,
                    "page_fetch_limit": 1,
                    "crawl_depth": 0,
                    "crawl_page_limit": 1,
                    "crawl_link_limit": 5,
                    "same_domain_only": True,
                    "include_url_patterns": ["/sites/"],
                    "skip_provider_domain": False,
                    "result_redirect_query_param": "target",
                    "discover_sitemaps_from_seeds": False,
                    "resume_frontier": False,
                },
            },
        )
        assert second_response.status_code == 200
        second_payload = second_response.json()
        assert second_payload["action"] == "updated"
        assert second_payload["source"]["source_id"] == payload["source"]["source_id"]
        assert second_payload["source_run"]["status"] == "completed"

        observations_response = client.get("/api/observations", params={"layer_key": "web-immediate-layer"})
        assert observations_response.status_code == 200
        observations = observations_response.json()
        assert any("/sites/briefs/harbor-departure" in row["content_json"].get("page_url", "") for row in observations)
        assert any("/sites/briefs/rail-delay" in row["content_json"].get("page_url", "") for row in observations)
        assert "/search/html" in state["requests"]


def test_source_webhook_route_returns_structured_409_for_invalid_json_payload(client: TestClient) -> None:
    source_response = client.post(
        "/api/sources",
        json={
            "name": "invalid-webhook-source",
            "source_kind": "webhook_ingest",
            "layer_key": "invalid-webhook-layer",
            "target_uri": "webhook://local",
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    webhook_response = client.post(
        f"/api/sources/{source_id}/webhook",
        content="{not-json",
        headers={"Content-Type": "application/json"},
    )
    assert webhook_response.status_code == 409
    payload = webhook_response.json()
    assert payload["detail"]["source_id"] == source_id
    assert payload["detail"]["source_kind"] == "webhook_ingest"
    assert payload["detail"]["error_type"] in {"JSONDecodeError", "ValueError"}
    assert payload["detail"]["message"]

    runs_response = client.get("/api/sources/runs")
    assert runs_response.status_code == 200
    latest_run = next(row for row in runs_response.json() if row["source_id"] == source_id)
    assert latest_run["status"] == "failed"
    assert latest_run["source_run_id"] == payload["detail"]["source_run_id"]
    assert latest_run["error_text"]

    dead_letters_response = client.get("/api/sources/dead-letters", params={"source_id": source_id})
    assert dead_letters_response.status_code == 200
    assert dead_letters_response.json()


def test_web_run_route_returns_structured_502_for_failed_discovery_execution(client: TestClient) -> None:
    response = client.post(
        "/api/sources/web/run",
        json={
            "name": "failing-web-run-source",
            "source_kind": "web_discovery",
            "layer_key": "failing-web-run-layer",
            "target_uri": "discover://web",
            "metadata_json": {
                "query": "harbor departure",
                "search_providers": ["generic_html"],
                "search_provider_targets": {"generic_html": "http://127.0.0.1:1/search/html"},
                "request_timeout_seconds": 1,
                "fetch_result_pages": True,
                "search_result_limit": 5,
                "page_fetch_limit": 1,
            },
        },
    )
    assert response.status_code == 502
    payload = response.json()
    assert payload["detail"]["source_kind"] == "web_discovery"
    assert payload["detail"]["error_type"] in {"RuntimeError", "ConnectionRefusedError", "TimeoutError", "OSError"}
    assert payload["detail"]["source_id"] >= 1
    assert payload["detail"]["source_run_id"] >= 1

    sources_response = client.get("/api/sources")
    assert sources_response.status_code == 200
    assert any(row["source_id"] == payload["detail"]["source_id"] for row in sources_response.json())

    runs_response = client.get("/api/sources/runs")
    assert runs_response.status_code == 200
    latest_run = next(row for row in runs_response.json() if row["source_id"] == payload["detail"]["source_id"])
    assert latest_run["status"] == "failed"
    assert latest_run["source_run_id"] == payload["detail"]["source_run_id"]


def test_source_provider_catalog_endpoint_lists_supported_search_profiles(client: TestClient) -> None:
    response = client.get("/api/sources/web-search/providers")
    assert response.status_code == 200
    payload = response.json()
    assert any(row["provider_name"] == "generic_html" for row in payload)
    assert any(row["provider_name"] == "duckduckgo_html" for row in payload)
    assert any(row["provider_name"] == "bing_html" for row in payload)


def test_source_update_can_disable_and_retarget_layer(client: TestClient, tmp_path: Path) -> None:
    fixture = tmp_path / "lifecycle-source.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Lifecycle source",
                    "url": "https://lifecycle.example.com/1",
                    "lat": 29.72,
                    "lon": -95.22,
                }
            ]
        ),
        encoding="utf-8",
    )
    source_response = client.post(
        "/api/sources",
        json={
            "name": "lifecycle-source",
            "source_kind": "local_file",
            "layer_key": "initial-feed",
            "target_uri": str(fixture),
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    update_response = client.patch(
        f"/api/sources/{source_id}",
        json={
            "enabled": False,
            "layer_key": "retargeted-feed",
            "notes": "Disabled for review",
            "metadata_json": {"skip_unchanged": False},
        },
    )
    assert update_response.status_code == 200
    payload = update_response.json()
    assert payload["enabled"] is False
    assert payload["layer_key"] == "retargeted-feed"
    assert payload["notes"] == "Disabled for review"
    assert payload["metadata_json"]["skip_unchanged"] is False

    layers_response = client.get("/api/layers")
    assert layers_response.status_code == 200
    assert any(layer["key"] == "retargeted-feed" for layer in layers_response.json())

    blocked_run = client.post(f"/api/sources/{source_id}/run")
    assert blocked_run.status_code == 409
    assert "disabled" in blocked_run.json()["detail"]

    custody_response = client.get("/api/custody/logs")
    assert custody_response.status_code == 200
    assert any(
        row["object_type"] == "source_definition"
        and row["object_id"] == str(source_id)
        and row["action"] == "source_updated"
        for row in custody_response.json()
    )


def test_http_xml_source_uses_env_basic_auth_and_parses_records(
    client: TestClient,
    monkeypatch,
) -> None:
    xml_payload = """<?xml version="1.0" encoding="UTF-8"?>
<FEUMessages>
  <feu:full-event-update xmlns:feu="http://www.northamericanhub.org">
    <message-header>
      <message-time-stamp>
        <date>20260706</date>
        <time>204544</time>
        <utc-offset>-0500</utc-offset>
      </message-time-stamp>
    </message-header>
    <event-reference>
      <event-id>MNSEG-5145378</event-id>
      <update>316</update>
    </event-reference>
    <event-indicators>
      <event-indicator><status>updated</status></event-indicator>
    </event-indicators>
    <headline><headline><mdss-conditions>normal driving conditions</mdss-conditions></headline></headline>
    <details>
      <detail>
        <locations>
          <location>
            <location-on-link>
              <route-designator>MN 5</route-designator>
              <primary-location>
                <geo-location>
                  <latitude>44801479</latitude>
                  <longitude>-93891350</longitude>
                </geo-location>
              </primary-location>
            </location-on-link>
          </location>
        </locations>
      </detail>
    </details>
  </feu:full-event-update>
</FEUMessages>
"""
    monkeypatch.setenv("TEST_MNDOT_FEED_PASSWORD", "secret-pass")
    with basic_auth_xml_server(
        xml_payload,
        username="mn-user",
        password="secret-pass",
    ) as (target_uri, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "remote-xml-source",
                "source_kind": "http_xml",
                "layer_key": "mndot-loop-feed",
                "target_uri": target_uri,
                "metadata_json": {
                    "retry_attempts": 1,
                    "request_timeout_seconds": 5,
                    "basic_auth_username": "mn-user",
                    "basic_auth_password_env": "TEST_MNDOT_FEED_PASSWORD",
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        run_response = client.post(f"/api/sources/{source_id}/run")
        assert run_response.status_code == 200
        run_payload = run_response.json()
        assert run_payload["status"] == "completed"
        assert run_payload["records_imported"] == 1
        assert run_payload["output_json"]["content_type"] == "application/xml"
        assert run_payload["output_json"]["materialized_content_type"] == "application/json"
        assert run_payload["output_json"]["cached_record_count"] == 1
        assert run_payload["output_json"]["cached_path"].endswith(".json")

        observations_response = client.get("/api/observations", params={"layer_key": "mndot-loop-feed"})
        assert observations_response.status_code == 200
        observations = observations_response.json()
        assert len(observations) == 1
        observation = observations[0]
        assert observation["source_domain"].startswith("127.0.0.1")
        assert observation["content_json"]["event_id"] == "MNSEG-5145378"
        assert observation["content_json"]["route_designator"] == "MN 5"
        assert observation["content_json"]["observed_at"] == "2026-07-06T20:45:44-05:00"
        assert observation["location_geojson"]["coordinates"] == [-93.89135, 44.801479]

        assert state["requests"] == 1
        assert state["last_authorization"] is not None


def test_http_json_source_blocks_private_network_targets_when_disabled(client: TestClient) -> None:
    with json_server([{"title": "Blocked target", "url": "https://example.com/blocked"}]) as (target_uri, _):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "private-network-blocked-source",
                "source_kind": "http_json",
                "layer_key": "private-network-blocked-layer",
                "target_uri": target_uri,
                "metadata_json": {
                    "request_timeout_seconds": 5,
                    "retry_attempts": 1,
                    "allow_private_networks": False,
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        session = get_session_factory()()
        try:
            with pytest.raises(SourceExecutionError, match="allow_private_networks=false") as exc_info:
                run_source_definition(session, source_id)
            assert isinstance(exc_info.value.cause, RuntimeError)
        finally:
            session.close()

        runs_response = client.get("/api/sources/runs")
        assert runs_response.status_code == 200
        latest_run = next(row for row in runs_response.json() if row["source_id"] == source_id)
        assert latest_run["status"] == "failed"
        assert "allow_private_networks=false" in (latest_run["error_text"] or "")


def test_http_json_source_enforces_max_payload_bytes(client: TestClient) -> None:
    oversized_payload = [{"title": "Large payload", "blob": "x" * 4096, "url": "https://example.com/large"}]
    with json_server(oversized_payload) as (target_uri, _):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "max-payload-source",
                "source_kind": "http_json",
                "layer_key": "max-payload-layer",
                "target_uri": target_uri,
                "metadata_json": {
                    "request_timeout_seconds": 5,
                    "retry_attempts": 1,
                    "max_payload_bytes": 256,
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        session = get_session_factory()()
        try:
            with pytest.raises(SourceExecutionError, match="max_payload_bytes=256") as exc_info:
                run_source_definition(session, source_id)
            assert isinstance(exc_info.value.cause, RuntimeError)
        finally:
            session.close()

        runs_response = client.get("/api/sources/runs")
        assert runs_response.status_code == 200
        latest_run = next(row for row in runs_response.json() if row["source_id"] == source_id)
        assert latest_run["status"] == "failed"
        assert "max_payload_bytes=256" in (latest_run["error_text"] or "")


def test_source_summary_and_report_index_capture_stale_failing_and_unscheduled_sources(
    client: TestClient,
    tmp_path: Path,
) -> None:
    healthy_fixture = tmp_path / "healthy-source.json"
    healthy_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Healthy source",
                    "url": "https://healthy-source.example.com/1",
                    "lat": 29.76,
                    "lon": -95.36,
                }
            ]
        ),
        encoding="utf-8",
    )
    unscheduled_fixture = tmp_path / "unscheduled-source.json"
    unscheduled_fixture.write_text(
        json.dumps(
            [
                {
                    "title": "Unscheduled source",
                    "url": "https://unscheduled-source.example.com/1",
                    "lat": 29.77,
                    "lon": -95.35,
                }
            ]
        ),
        encoding="utf-8",
    )

    healthy_source = client.post(
        "/api/sources",
        json={
            "name": "healthy-source",
            "source_kind": "local_file",
            "layer_key": "marine-track",
            "target_uri": str(healthy_fixture),
        },
    )
    assert healthy_source.status_code == 200
    healthy_source_id = healthy_source.json()["source_id"]

    schedule_response = client.post(
        "/api/scheduler/tasks",
        json={
            "name": "healthy-source-sync",
            "task_type": "source_sync",
            "interval_seconds": 300,
            "source_id": healthy_source_id,
        },
    )
    assert schedule_response.status_code == 200
    task_id = schedule_response.json()["task_id"]
    scheduled_run = client.post(f"/api/scheduler/tasks/{task_id}/run")
    assert scheduled_run.status_code == 200

    failing_source = client.post(
        "/api/sources",
        json={
            "name": "failing-source",
            "source_kind": "http_json",
            "layer_key": "remote-feed",
            "target_uri": "http://127.0.0.1:1/failing.json",
            "metadata_json": {
                "retry_attempts": 1,
                "request_timeout_seconds": 1,
            },
        },
    )
    assert failing_source.status_code == 200
    failing_source_id = failing_source.json()["source_id"]
    session = get_session_factory()()
    try:
        try:
            run_source_definition(session, failing_source_id)
        except SourceExecutionError as exc:
            assert isinstance(exc.cause, RuntimeError)
        else:
            raise AssertionError("Expected source run to fail.")
    finally:
        session.close()

    unscheduled_source = client.post(
        "/api/sources",
        json={
            "name": "unscheduled-source",
            "source_kind": "local_file",
            "layer_key": "news-track",
            "target_uri": str(unscheduled_fixture),
        },
    )
    assert unscheduled_source.status_code == 200
    unscheduled_source_id = unscheduled_source.json()["source_id"]

    disabled_source = client.post(
        "/api/sources",
        json={
            "name": "disabled-source",
            "source_kind": "local_file",
            "layer_key": "disabled-feed",
            "target_uri": str(unscheduled_fixture),
            "enabled": False,
        },
    )
    assert disabled_source.status_code == 200

    summary_response = client.get("/api/sources/summary", params={"stale_after_hours": 24})
    assert summary_response.status_code == 200
    summary = summary_response.json()
    assert summary["total_count"] == 4
    assert summary["enabled_count"] == 3
    assert summary["disabled_count"] == 1
    assert summary["scheduled_count"] == 1
    assert summary["unscheduled_count"] == 3
    assert summary["failing_count"] == 1
    assert summary["stale_count"] == 2
    assert any(bucket["key"] == "local_file" for bucket in summary["source_kind_counts"])
    assert any(bucket["key"] == "failed" for bucket in summary["latest_status_counts"])
    assert any(bucket["key"] == "never_run" for bucket in summary["latest_status_counts"])

    report_index_response = client.get(
        "/api/sources/report-index",
        params={"stale_after_hours": 24, "limit": 10, "stale_source_limit": 10},
    )
    assert report_index_response.status_code == 200
    report = report_index_response.json()
    assert report["sync_task_count"] == 1
    assert report["sync_run_count"] == 1
    assert report["sync_failure_count"] == 0
    assert report["inventory_summary"]["total_count"] == 4
    assert any(row["source"]["source_id"] == failing_source_id for row in report["failing_sources"])
    assert any(row["source"]["source_id"] == failing_source_id for row in report["stale_sources"])
    assert any(row["source"]["source_id"] == unscheduled_source_id for row in report["unscheduled_sources"])
    healthy_status = next(
        row for row in report["recent_runs"] if row["source_id"] == healthy_source_id
    )
    assert healthy_status["status"] == "completed"

    export_response = client.get(
        "/api/sources/export/summary",
        params={"stale_after_hours": 24, "source_limit": 10, "report_limit": 10, "stale_source_limit": 10},
    )
    assert export_response.status_code == 200
    export_payload = export_response.json()
    assert export_payload["filters_json"]["source_limit"] == 10
    assert export_payload["report_index"]["inventory_summary"]["total_count"] == 4
    assert len(export_payload["sources"]) == 4
    assert any(row["source_id"] == healthy_source_id for row in export_payload["sources"])

    healthy_ops = client.get(f"/api/sources/{healthy_source_id}/ops")
    assert healthy_ops.status_code == 200
    assert healthy_ops.json()["storage_objects"]


def test_sse_runtime_source_ingests_stream_batch(client: TestClient) -> None:
    events = [
        {
            "id": "evt-1",
            "event": "observation",
            "data": {
                "title": "SSE alpha",
                "url": "https://stream.example.com/alpha",
                "lat": 44.98,
                "lon": -93.26,
            },
        },
        {
            "id": "evt-2",
            "event": "observation",
            "data": {
                "title": "SSE bravo",
                "url": "https://stream.example.com/bravo",
                "lat": 44.99,
                "lon": -93.27,
            },
        },
    ]
    with sse_server(events) as (target_uri, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "sse-runtime-source",
                "source_kind": "sse_stream",
                "layer_key": "traffic-stream",
                "target_uri": target_uri,
                "metadata_json": {
                    "request_timeout_seconds": 2,
                    "stream_idle_timeout_seconds": 0.25,
                    "stream_max_records": 10,
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        runtime_response = client.post(f"/api/sources/{source_id}/runtime")
        assert runtime_response.status_code == 200
        assert runtime_response.json()["records_imported"] == 2

        runs = client.get("/api/sources/runs").json()
        assert runs[0]["source_id"] == source_id
        assert runs[0]["status"] == "completed"
        assert runs[0]["records_imported"] == 2
        assert runs[0]["adapter_kind"] == "sse_stream"
        assert runs[0]["output_json"]["transport_kind"] == "sse_stream"
        assert runs[0]["last_event_id"] == "evt-2"

        checkpoints = client.get("/api/sources/checkpoints").json()
        checkpoint = next(row for row in checkpoints if row["source_id"] == source_id)
        assert checkpoint["last_event_id"] == "evt-2"
        assert checkpoint["fetch_mode"] == "stream"
        assert state["last_event_id"] is None


def test_websocket_runtime_source_ingests_stream_batch(client: TestClient) -> None:
    messages = [
        {
            "event_id": "ws-1",
            "title": "WS alpha",
            "url": "https://ws.example.com/alpha",
            "lat": 44.95,
            "lon": -93.1,
        },
        {
            "event_id": "ws-2",
            "title": "WS bravo",
            "url": "https://ws.example.com/bravo",
            "lat": 44.96,
            "lon": -93.11,
        },
    ]
    with websocket_server(messages) as (target_uri, state):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "websocket-runtime-source",
                "source_kind": "websocket_stream",
                "layer_key": "camera-stream",
                "target_uri": target_uri,
                "metadata_json": {
                    "request_timeout_seconds": 2,
                    "stream_idle_timeout_seconds": 0.5,
                    "stream_max_records": 10,
                },
            },
        )
        assert source_response.status_code == 200
        source_id = source_response.json()["source_id"]

        runtime_response = client.post(f"/api/sources/{source_id}/runtime")
        assert runtime_response.status_code == 200
        assert runtime_response.json()["records_imported"] == 2

        runs = client.get("/api/sources/runs").json()
        assert runs[0]["source_id"] == source_id
        assert runs[0]["status"] == "completed"
        assert runs[0]["records_imported"] == 2
        assert runs[0]["adapter_kind"] == "websocket_stream"
        assert runs[0]["output_json"]["transport_kind"] == "websocket_stream"
        assert runs[0]["last_event_id"] == "ws-2"
        assert state["connections"] == 1


def test_runtime_cycle_continues_past_failed_stream_source(client: TestClient) -> None:
    with sse_server(
        [
            {
                "id": "evt-1",
                "event": "observation",
                "data": {
                    "title": "Still alive",
                    "url": "https://stream.example.com/live",
                    "lat": 44.97,
                    "lon": -93.2,
                },
            }
        ]
    ) as (target_uri, _):
        good_source = client.post(
            "/api/sources",
            json={
                "name": "runtime-good-source",
                "source_kind": "sse_stream",
                "layer_key": "ops-stream",
                "target_uri": target_uri,
                "metadata_json": {
                    "request_timeout_seconds": 2,
                    "stream_idle_timeout_seconds": 0.25,
                    "stream_max_records": 5,
                },
            },
        )
        assert good_source.status_code == 200

        bad_source = client.post(
            "/api/sources",
            json={
                "name": "runtime-bad-source",
                "source_kind": "websocket_stream",
                "layer_key": "ops-stream",
                "target_uri": "ws://127.0.0.1:9/broken",
                "metadata_json": {
                    "request_timeout_seconds": 0.2,
                    "stream_idle_timeout_seconds": 0.1,
                    "stream_max_records": 5,
                },
            },
        )
        assert bad_source.status_code == 200

        session = get_session_factory()()
        try:
            cycle = run_source_runtime_cycle(session, actor="test_runtime_cycle")
        finally:
            session.close()
        assert cycle["source_count"] == 2
        assert cycle["records_imported"] == 1

        runs = client.get("/api/sources/runs").json()
        latest_statuses = {row["source_id"]: row["status"] for row in runs[:2]}
        assert "completed" in latest_statuses.values()
        assert "failed" in latest_statuses.values()


def test_source_runtime_worker_publishes_worker_status(client: TestClient) -> None:
    with sse_server(
        [
            {
                "id": "evt-runtime-worker-1",
                "event": "observation",
                "data": {
                    "title": "Runtime worker source",
                    "url": "https://runtime-worker.example.com/1",
                    "lat": 44.97,
                    "lon": -93.26,
                },
            }
        ]
    ) as (target_uri, _):
        source_response = client.post(
            "/api/sources",
            json={
                "name": "runtime-worker-source",
                "source_kind": "sse_stream",
                "layer_key": "runtime-worker-layer",
                "target_uri": target_uri,
                "metadata_json": {
                    "request_timeout_seconds": 2,
                    "stream_idle_timeout_seconds": 0.25,
                    "stream_max_records": 5,
                },
            },
        )
        assert source_response.status_code == 200

        result = run_source_runtime_worker(
            get_session_factory(),
            poll_seconds=0,
            actor="test_source_runtime_worker",
            once=True,
            sleep_fn=lambda _: None,
        )
        assert result.iterations == 1
        assert result.records_imported == 1

        worker_summary_response = client.get("/api/operations/workers/summary")
        assert worker_summary_response.status_code == 200
        worker_summary = worker_summary_response.json()
        assert worker_summary["total_count"] >= 1
        assert any(bucket["key"] == "source_runtime_worker" for bucket in worker_summary["worker_type_counts"])
        assert any(row["worker_type"] == "source_runtime_worker" for row in worker_summary["workers"])


def test_operations_runtime_cycle_route_returns_structured_502_for_stream_runtime_failure(client: TestClient) -> None:
    source_response = client.post(
        "/api/sources",
        json={
            "name": "operations-runtime-failing-stream-source",
            "source_kind": "websocket_stream",
            "layer_key": "operations-runtime-layer",
            "target_uri": "ws://127.0.0.1:1/operations-runtime",
            "metadata_json": {
                "request_timeout_seconds": 1,
                "stream_idle_timeout_seconds": 0.1,
                "stream_max_records": 5,
            },
        },
    )
    assert source_response.status_code == 200
    source_id = source_response.json()["source_id"]

    cycle_response = client.post(
        "/api/operations/runtime/cycle",
        params={
            "include_stream_runtime": True,
            "include_enabled_schedules": False,
            "source_id": source_id,
        },
    )
    assert cycle_response.status_code == 502
    payload = cycle_response.json()
    assert payload["detail"]["source_id"] == source_id
    assert payload["detail"]["source_kind"] == "websocket_stream"
    assert payload["detail"]["error_type"] in {
        "RuntimeError",
        "ConnectionRefusedError",
        "TimeoutError",
        "OSError",
    }
    assert payload["detail"]["message"]

    runs_response = client.get("/api/sources/runs")
    assert runs_response.status_code == 200
    latest_run = next(row for row in runs_response.json() if row["source_id"] == source_id)
    assert latest_run["status"] == "failed"
    assert latest_run["source_run_id"] == payload["detail"]["source_run_id"]


def test_platform_runtime_cycle_materializes_and_verifies_camera_streams(client: TestClient) -> None:
    with camera_endpoint_server() as base_url:
        messages = [
            {
                "camera_id": "runtime-cam-1",
                "camera_name": "Runtime Camera 1",
                "title": "Runtime Camera 1",
                "text": "Runtime camera stream observation",
                "image_url": f"{base_url}/camera.jpg",
                "stream_url": f"{base_url}/camera.m3u8",
                "page_url": f"{base_url}/camera-page",
                "url": f"{base_url}/camera-page",
                "provider": "runtime-test",
                "status": "active",
                "lat": 44.95,
                "lon": -93.27,
                "observed_at": "2026-07-08T02:30:00Z",
            }
        ]
        with websocket_server(messages) as (target_uri, _state):
            source_response = client.post(
                "/api/sources",
                json={
                    "name": "runtime-camera-stream",
                    "source_kind": "websocket_stream",
                    "layer_key": "runtime-camera-layer",
                    "target_uri": target_uri,
                    "metadata_json": {
                        "request_timeout_seconds": 5,
                        "stream_idle_timeout_seconds": 0.1,
                        "stream_max_records": 5,
                        "send_messages": [{"op": "subscribe", "topic": "cameras"}],
                    },
                },
            )
            assert source_response.status_code == 200
            source_id = source_response.json()["source_id"]

            runtime_response = client.post(f"/api/sources/{source_id}/runtime")
            assert runtime_response.status_code == 200
            runtime_payload = runtime_response.json()
            assert runtime_payload["source_count"] == 1
            assert len(runtime_payload["source_run_ids"]) == 1

            refresh_response = client.post(
                "/api/scheduler/tasks",
                json={
                    "name": "runtime-camera-refresh",
                    "task_type": "camera_inventory_refresh",
                    "interval_seconds": 300,
                    "layer_key": "runtime-camera-layer",
                    "payload_json": {"limit": 25},
                },
            )
            assert refresh_response.status_code == 200

            verify_response = client.post(
                "/api/scheduler/tasks",
                json={
                    "name": "runtime-camera-verify",
                    "task_type": "camera_source_verification",
                    "interval_seconds": 300,
                    "layer_key": "runtime-camera-layer",
                    "payload_json": {"limit": 25, "timeout_seconds": 5.0},
                },
            )
            assert verify_response.status_code == 200

            session = get_session_factory()()
            try:
                result = run_platform_runtime_cycle(
                    session,
                    include_stream_runtime=False,
                    include_enabled_schedules=True,
                    task_types=["camera_inventory_refresh", "camera_source_verification"],
                    actor="test_platform_runtime_camera_cycle",
                )
            finally:
                session.close()

            assert result["source_runtime"]["source_count"] == 0
            assert result["schedules"]["completed_count"] == 2
            assert set(result["schedules"]["task_types"]) == {
                "camera_inventory_refresh",
                "camera_source_verification",
            }

            cameras_response = client.get("/api/cameras", params={"layer_key": "runtime-camera-layer"})
            assert cameras_response.status_code == 200
            cameras = cameras_response.json()
            assert len(cameras) == 1
            assert cameras[0]["external_id"] == "runtime-cam-1"

            camera_sources_response = client.get(
                "/api/camera-sources",
                params={"layer_key": "runtime-camera-layer", "limit": 10},
            )
            assert camera_sources_response.status_code == 200
            camera_sources = camera_sources_response.json()
            assert len(camera_sources) == 3
            assert any(row["endpoint_kind"] == "image" and row["verification_state"] == "reachable" for row in camera_sources)
            assert any(row["endpoint_kind"] == "stream" and row["verification_state"] == "reachable" for row in camera_sources)
            assert any(row["endpoint_kind"] == "page" and row["verification_state"] == "reachable" for row in camera_sources)
