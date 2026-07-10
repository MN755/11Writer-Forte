from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any
from urllib.parse import quote

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response, StreamingResponse

app = FastAPI(title="11Writer Forte Local Upstreams")

CHANNEL_COORDINATES: dict[str, tuple[float, float]] = {
    "harbor": (44.9778, -93.2650),
    "cameras": (44.9537, -93.0900),
    "rail": (44.9867, -93.2581),
}

CAMERA_FIXTURES: list[dict[str, object]] = [
    {
        "slug": "i94-snelling-east",
        "camera_id": "local-cam-i94-snelling-east",
        "camera_name": "I-94 EB @ Snelling",
        "road_name": "I-94",
        "status": "active",
        "latitude": 44.9537,
        "longitude": -93.0900,
    },
    {
        "slug": "i35w-lake-north",
        "camera_id": "local-cam-i35w-lake-north",
        "camera_name": "I-35W NB @ Lake St",
        "road_name": "I-35W",
        "status": "active",
        "latitude": 44.9487,
        "longitude": -93.2694,
    },
    {
        "slug": "us10-th39-west",
        "camera_id": "local-cam-us10-th39-west",
        "camera_name": "US-10 WB @ TH-39",
        "road_name": "US-10",
        "status": "offline",
        "latitude": 45.1731,
        "longitude": -93.3034,
    },
]

DISCOVERY_PAGES: dict[str, dict[str, Any]] = {
    "/sites/briefs/harbor-departure": {
        "title": "Harbor Departure Confirmed",
        "description": "Port-side observers confirm a cargo vessel departed the harbor just after dawn.",
        "latitude": 44.9778,
        "longitude": -93.265,
        "observed_at": "2026-07-07T05:45:00Z",
        "query_terms": ["harbor", "departure", "cargo vessel", "port"],
        "paragraphs": [
            "Harbor watchers documented a cargo vessel clearing the southern berth shortly after dawn.",
            "Spotters matched the movement to tug activity and outbound channel traffic, making this a useful local OSINT starter page.",
        ],
        "links": [
            "/sites/reference/local-osint-overview",
            "/sites/briefs/harbor-manifest",
        ],
    },
    "/sites/briefs/harbor-manifest": {
        "title": "Harbor Manifest Notes",
        "description": "A follow-up brief summarizing the outbound vessel manifest and berth activity.",
        "latitude": 44.9784,
        "longitude": -93.2641,
        "observed_at": "2026-07-07T05:52:00Z",
        "query_terms": ["harbor", "manifest", "vessel", "port"],
        "paragraphs": [
            "The outbound manifest references container cargo and a routine tug escort.",
            "Additional context ties the departure window to nearby yard movement and outbound channel traffic.",
        ],
        "links": [
            "/sites/reference/local-osint-overview",
        ],
    },
    "/sites/briefs/rail-delay": {
        "title": "Rail Delay Bulletin",
        "description": "Dispatch reports a late-evening rail slowdown after maintenance crews occupied a junction.",
        "latitude": 44.9867,
        "longitude": -93.2581,
        "observed_at": "2026-07-07T22:10:00Z",
        "query_terms": ["rail", "delay", "dispatch", "junction"],
        "paragraphs": [
            "Regional dispatch traffic references a multi-train slowdown near the yard lead.",
            "The bulletin cross-references maintenance occupancy and cascading schedule drift.",
        ],
        "links": [
            "/sites/reference/local-osint-overview",
            "/sites/briefs/camera-outage",
        ],
    },
    "/sites/briefs/camera-outage": {
        "title": "Camera Outage Notice",
        "description": "A roadside traffic camera dropped offline during weather-related power instability.",
        "latitude": 44.9537,
        "longitude": -93.09,
        "observed_at": "2026-07-07T19:22:00Z",
        "query_terms": ["camera", "outage", "traffic", "weather"],
        "paragraphs": [
            "Operators flagged an intermittent roadside camera outage during a power event.",
            "The notice is useful as a test artifact for camera-source discovery and downstream alerting.",
        ],
        "links": [
            "/sites/reference/local-osint-overview",
        ],
    },
    "/sites/reference/local-osint-overview": {
        "title": "Local OSINT Overview",
        "description": "Reference material describing how multiple public signals can corroborate a local event.",
        "query_terms": ["osint", "reference", "overview", "corroborate"],
        "paragraphs": [
            "This reference page explains how transport, camera, and public reporting layers can reinforce one another.",
            "It exists mainly so the crawler has a stable pivot page instead of wandering into the void like a tiny confused spider.",
        ],
        "links": [
            "/sites/briefs/harbor-departure",
            "/sites/briefs/rail-delay",
            "/sites/briefs/camera-outage",
        ],
    },
}


def upstream_now() -> datetime:
    return datetime.now(timezone.utc)


def resolve_http_base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def resolve_websocket_http_base_url(websocket: WebSocket) -> str:
    host = websocket.headers.get("host") or "127.0.0.1:8010"
    scheme = "https" if websocket.url.scheme == "wss" else "http"
    return f"{scheme}://{host}"


def camera_fixture_for_sequence(sequence: int) -> dict[str, object]:
    index = max(0, sequence - 1) % len(CAMERA_FIXTURES)
    return CAMERA_FIXTURES[index]


def build_camera_observation(
    *,
    sequence: int,
    transport: str,
    base_url: str,
    observed_at: datetime,
) -> dict[str, Any]:
    fixture = camera_fixture_for_sequence(sequence)
    latitude = float(fixture["latitude"])
    longitude = float(fixture["longitude"])
    camera_slug = str(fixture["slug"])
    camera_name = str(fixture["camera_name"])
    status = str(fixture["status"])
    page_url = f"{base_url}/camera-sites/{camera_slug}"
    image_url = f"{base_url}/camera-assets/{camera_slug}.jpg"
    stream_url = f"{base_url}/camera-streams/{camera_slug}.m3u8"
    return {
        "event_id": f"cameras-{transport}-{sequence}",
        "camera_id": str(fixture["camera_id"]),
        "camera_name": camera_name,
        "title": camera_name,
        "text": f"{camera_name} status {status} from local upstream simulation",
        "url": page_url,
        "page_url": page_url,
        "image_url": image_url,
        "stream_url": stream_url,
        "source_url": f"{base_url}/feeds/{transport}/cameras",
        "lat": latitude,
        "lon": longitude,
        "provider": "11writer-local-camera-grid",
        "road_name": str(fixture["road_name"]),
        "status": status,
        "channel": "cameras",
        "transport": transport,
        "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
    }


def build_mock_observation(channel: str, sequence: int, *, transport: str, base_url: str) -> dict[str, Any]:
    latitude, longitude = CHANNEL_COORDINATES.get(channel, (44.9500, -93.1000))
    observed_at = upstream_now() + timedelta(seconds=sequence)
    if channel == "cameras":
        return build_camera_observation(
            sequence=sequence,
            transport=transport,
            base_url=base_url,
            observed_at=observed_at,
        )
    return {
        "event_id": f"{channel}-{transport}-{sequence}",
        "title": f"{channel.title()} {transport.upper()} observation {sequence}",
        "text": f"{channel} simulated {transport} event {sequence}",
        "url": f"https://local-upstreams.invalid/{channel}/{transport}/{sequence}",
        "source_url": f"{base_url}/feeds/{transport}/{channel}",
        "lat": round(latitude + (sequence * 0.001), 6),
        "lon": round(longitude - (sequence * 0.001), 6),
        "provider": "11writer-local-upstreams",
        "channel": channel,
        "transport": transport,
        "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
    }


def build_stream_batch(channel: str, *, count: int, transport: str, base_url: str) -> list[dict[str, Any]]:
    bounded_count = max(1, min(count, 500))
    return [
        build_mock_observation(channel, sequence=index + 1, transport=transport, base_url=base_url)
        for index in range(bounded_count)
    ]


def build_html_document(
    *,
    title: str,
    description: str,
    paragraphs: list[str],
    links: list[tuple[str, str]],
    latitude: float | None = None,
    longitude: float | None = None,
    observed_at: str | None = None,
) -> str:
    paragraph_markup = "\n".join(f"<p>{escape(paragraph)}</p>" for paragraph in paragraphs)
    link_markup = "\n".join(
        f'<li><a href="{escape(href, quote=True)}">{escape(label)}</a></li>' for href, label in links
    )
    meta_lines = []
    if latitude is not None:
        meta_lines.append(f'<meta name="latitude" content="{latitude}" />')
    if longitude is not None:
        meta_lines.append(f'<meta name="longitude" content="{longitude}" />')
    if latitude is not None and longitude is not None:
        meta_lines.append(f'<meta name="geo.position" content="{latitude};{longitude}" />')
    if observed_at is not None:
        meta_lines.append(f'<meta property="article:published_time" content="{escape(observed_at, quote=True)}" />')
    meta_markup = "\n    ".join(meta_lines)
    time_markup = (
        f'<time datetime="{escape(observed_at, quote=True)}">{escape(observed_at)}</time>'
        if observed_at is not None
        else ""
    )
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>{escape(title)}</title>
    <meta name="description" content="{escape(description, quote=True)}" />
    {meta_markup}
  </head>
  <body>
    <main>
      <h1>{escape(title)}</h1>
      <p>{escape(description)}</p>
      {time_markup}
      {paragraph_markup}
      <nav>
        <ul>
          {link_markup}
        </ul>
      </nav>
    </main>
  </body>
</html>"""


def discovery_page_links(page: dict[str, Any]) -> list[tuple[str, str]]:
    links = []
    for path in page.get("links", []):
        target = DISCOVERY_PAGES.get(path)
        label = str(target.get("title")) if isinstance(target, dict) else path
        links.append((path, label))
    return links


def query_matches_page(query: str, page: dict[str, Any]) -> bool:
    normalized_query = query.strip().lower()
    if not normalized_query:
        return False
    haystacks = [
        str(page.get("title", "")).lower(),
        str(page.get("description", "")).lower(),
        " ".join(str(term).lower() for term in page.get("query_terms", [])),
    ]
    return any(normalized_query in haystack for haystack in haystacks) or any(
        term in normalized_query for term in page.get("query_terms", [])
    )


@app.get("/health")
def health() -> dict[str, object]:
    return {"status": "ok", "service": "local-upstreams"}


@app.get("/feeds/snapshot/{channel}")
def snapshot(channel: str, request: Request, count: int = 5) -> dict[str, object]:
    base_url = resolve_http_base_url(request)
    return {
        "channel": channel,
        "count": max(1, min(count, 500)),
        "items": build_stream_batch(channel, count=count, transport="snapshot", base_url=base_url),
    }


@app.get("/feeds/sse/{channel}")
async def sse_feed(request: Request, channel: str, count: int = 10, interval_ms: int = 100) -> StreamingResponse:
    payloads = build_stream_batch(channel, count=count, transport="sse", base_url=resolve_http_base_url(request))
    sleep_seconds = max(0.0, interval_ms / 1000.0)

    async def event_stream():
        for item in payloads:
            message = (
                f"id: {item['event_id']}\n"
                "event: observation\n"
                f"data: {json.dumps(item, sort_keys=True)}\n\n"
            )
            yield message.encode("utf-8")
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/search/html", response_class=HTMLResponse)
def search_html(
    q: str = "",
    limit: int = 5,
    redirect: bool = True,
    page: int = 1,
    page_size: int | None = None,
) -> HTMLResponse:
    bounded_limit = max(1, min(limit, 20))
    bounded_page = max(1, page)
    matching_paths = [
        path for path, page in DISCOVERY_PAGES.items() if query_matches_page(q, page)
    ]
    if not matching_paths:
        matching_paths = list(DISCOVERY_PAGES.keys())
    effective_page_size = max(1, min(page_size if page_size is not None else bounded_limit, bounded_limit))
    start_index = (bounded_page - 1) * effective_page_size
    end_index = start_index + effective_page_size
    result_links: list[tuple[str, str]] = []
    for path in matching_paths[start_index:end_index]:
        page = DISCOVERY_PAGES[path]
        href = f"/search/redirect?target={quote(path, safe='/%')}" if redirect else path
        result_links.append((href, str(page["title"])))
    result_links.append(("/search/about", "About This Local Search Harness"))
    if end_index < min(len(matching_paths), bounded_limit):
        result_links.append((f"/search/html?q={quote(q)}&page={bounded_page + 1}&page_size={effective_page_size}", "Next Results"))
    html = build_html_document(
        title=f"Local Search Results: {q or 'all'} (page {bounded_page})",
        description="Local HTML search page used for backend-only web discovery tests.",
        paragraphs=[
            f"Search query: {q or 'all'}",
            f"Page {bounded_page} of local search results with page_size={effective_page_size}.",
            "These results intentionally live on the same host so Forte can test local-first search discovery without third-party infrastructure.",
        ],
        links=result_links,
    )
    return HTMLResponse(content=html)


@app.get("/search/about", response_class=HTMLResponse)
def search_about() -> HTMLResponse:
    html = build_html_document(
        title="Local Search Harness",
        description="Reference page for the local HTML search harness.",
        paragraphs=[
            "This page exists to make sure include/exclude URL filters can ignore non-result provider links.",
        ],
        links=[("/sites/reference/local-osint-overview", "Local OSINT Overview")],
    )
    return HTMLResponse(content=html)


@app.get("/search/redirect")
def search_redirect(target: str) -> RedirectResponse:
    normalized_target = target if target.startswith("/") else f"/{target.lstrip('/')}"
    return RedirectResponse(url=normalized_target, status_code=307)


@app.get("/crawl/root", response_class=HTMLResponse)
def crawl_root() -> HTMLResponse:
    html = build_html_document(
        title="Local Crawl Root",
        description="Seed page for local crawl tests.",
        paragraphs=[
            "This root page links into a small site graph so Forte can crawl multiple HTML pages locally.",
        ],
        links=[
            ("/sites/reference/local-osint-overview", "Local OSINT Overview"),
            ("/sites/briefs/harbor-departure", "Harbor Departure Confirmed"),
            ("/sites/briefs/rail-delay", "Rail Delay Bulletin"),
        ],
    )
    return HTMLResponse(content=html)


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt(request: Request) -> PlainTextResponse:
    base_url = resolve_http_base_url(request)
    return PlainTextResponse(
        f"User-agent: *\nAllow: /\nSitemap: {base_url}/sitemap.xml\n"
    )


@app.get("/sitemap.xml", response_class=PlainTextResponse)
def sitemap_index(request: Request) -> PlainTextResponse:
    base_url = resolve_http_base_url(request)
    return PlainTextResponse(
        (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            f"<sitemap><loc>{base_url}/sitemaps/discovery.xml</loc></sitemap>"
            "</sitemapindex>"
        ),
        media_type="application/xml",
    )


@app.get("/sitemaps/discovery.xml", response_class=PlainTextResponse)
def discovery_sitemap(request: Request) -> PlainTextResponse:
    base_url = resolve_http_base_url(request)
    page_entries = "".join(
        f"<url><loc>{base_url}{path}</loc></url>"
        for path in sorted(DISCOVERY_PAGES.keys())
    )
    return PlainTextResponse(
        (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            f"{page_entries}</urlset>"
        ),
        media_type="application/xml",
    )


@app.get("/camera-assets/{camera_slug}.jpg")
def camera_snapshot(camera_slug: str) -> Response:
    payload = f"local-camera:{camera_slug}".encode("utf-8")
    return Response(content=payload, media_type="image/jpeg")


@app.get("/camera-streams/{camera_slug}.m3u8", response_class=PlainTextResponse)
def camera_stream_manifest(camera_slug: str, request: Request) -> PlainTextResponse:
    base_url = resolve_http_base_url(request)
    return PlainTextResponse(
        (
            "#EXTM3U\n"
            "#EXT-X-VERSION:3\n"
            "#EXT-X-TARGETDURATION:6\n"
            "#EXTINF:6.0,\n"
            f"{base_url}/camera-assets/{camera_slug}.jpg\n"
            "#EXT-X-ENDLIST\n"
        ),
        media_type="application/vnd.apple.mpegurl",
    )


@app.get("/camera-sites/{camera_slug}", response_class=HTMLResponse)
def camera_site(camera_slug: str) -> HTMLResponse:
    fixture = next((item for item in CAMERA_FIXTURES if item["slug"] == camera_slug), None)
    if fixture is None:
        html = build_html_document(
            title="Missing Camera Site",
            description="Requested local camera page does not exist.",
            paragraphs=[f"No local camera page is registered for {camera_slug}."],
            links=[("/crawl/root", "Local Crawl Root")],
        )
        return HTMLResponse(content=html, status_code=404)
    html = build_html_document(
        title=str(fixture["camera_name"]),
        description=f"Local traffic camera page for {fixture['camera_name']}.",
        paragraphs=[
            f"Road: {fixture['road_name']}",
            f"Status: {fixture['status']}",
            "This page exists so the backend camera verifier has a real local HTML target instead of pure wishful thinking.",
        ],
        links=[("/sites/briefs/camera-outage", "Camera Outage Notice")],
        latitude=float(fixture["latitude"]),
        longitude=float(fixture["longitude"]),
        observed_at=upstream_now().isoformat().replace("+00:00", "Z"),
    )
    return HTMLResponse(content=html)


@app.get("/sites/{section}/{slug}", response_class=HTMLResponse)
def discovery_page(section: str, slug: str) -> HTMLResponse:
    path = f"/sites/{section}/{slug}"
    page = DISCOVERY_PAGES.get(path)
    if page is None:
        html = build_html_document(
            title="Missing Discovery Page",
            description="Requested mock discovery page does not exist.",
            paragraphs=[f"No page is registered for {path}."],
            links=[("/crawl/root", "Local Crawl Root")],
        )
        return HTMLResponse(content=html, status_code=404)
    html = build_html_document(
        title=str(page["title"]),
        description=str(page["description"]),
        paragraphs=[str(item) for item in page.get("paragraphs", [])],
        links=discovery_page_links(page),
        latitude=float(page["latitude"]) if page.get("latitude") is not None else None,
        longitude=float(page["longitude"]) if page.get("longitude") is not None else None,
        observed_at=str(page["observed_at"]) if page.get("observed_at") is not None else None,
    )
    return HTMLResponse(content=html)


@app.websocket("/feeds/ws/{channel}")
async def websocket_feed(websocket: WebSocket, channel: str, count: int = 10, interval_ms: int = 100) -> None:
    await websocket.accept()
    sleep_seconds = max(0.0, interval_ms / 1000.0)
    base_url = resolve_websocket_http_base_url(websocket)
    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_text(), timeout=0.05)
            except asyncio.TimeoutError:
                break
            if not message:
                break
        for item in build_stream_batch(channel, count=count, transport="websocket", base_url=base_url):
            await websocket.send_text(json.dumps(item, sort_keys=True))
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)
    finally:
        await websocket.close()
