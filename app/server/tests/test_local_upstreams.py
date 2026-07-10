from __future__ import annotations

import json

from fastapi.testclient import TestClient

from src.local_upstreams import app


def test_local_camera_snapshot_payloads_are_materializable() -> None:
    client = TestClient(app)

    response = client.get("/feeds/snapshot/cameras", params={"count": 2})

    assert response.status_code == 200
    payload = response.json()
    assert payload["channel"] == "cameras"
    assert payload["count"] == 2
    items = payload["items"]
    assert len(items) == 2
    assert all(item["camera_id"].startswith("local-cam-") for item in items)
    assert all(item["camera_name"] for item in items)
    assert all(item["page_url"].startswith("http://testserver/camera-sites/") for item in items)
    assert all(item["image_url"].startswith("http://testserver/camera-assets/") for item in items)
    assert all(item["stream_url"].startswith("http://testserver/camera-streams/") for item in items)
    assert all(item["provider"] == "11writer-local-camera-grid" for item in items)


def test_local_camera_websocket_payloads_use_request_host() -> None:
    client = TestClient(app)

    with client.websocket_connect("/feeds/ws/cameras?count=1") as websocket:
        websocket.send_text('{"op":"subscribe","topic":"cameras"}')
        item = json.loads(websocket.receive_text())

    assert item["camera_id"].startswith("local-cam-")
    assert item["page_url"].startswith("http://testserver/camera-sites/")
    assert item["image_url"].startswith("http://testserver/camera-assets/")
    assert item["stream_url"].startswith("http://testserver/camera-streams/")
    assert item["source_url"].startswith("http://testserver/feeds/websocket/cameras")


def test_local_upstreams_publish_dynamic_robots_and_camera_targets() -> None:
    client = TestClient(app)

    robots_response = client.get("/robots.txt")
    sitemap_response = client.get("/sitemap.xml")
    image_response = client.get("/camera-assets/i94-snelling-east.jpg")
    stream_response = client.get("/camera-streams/i94-snelling-east.m3u8")
    page_response = client.get("/camera-sites/i94-snelling-east")

    assert robots_response.status_code == 200
    assert "http://testserver/sitemap.xml" in robots_response.text
    assert sitemap_response.status_code == 200
    assert "http://testserver/sitemaps/discovery.xml" in sitemap_response.text

    assert image_response.status_code == 200
    assert image_response.headers["content-type"].startswith("image/jpeg")
    assert stream_response.status_code == 200
    assert stream_response.headers["content-type"].startswith("application/vnd.apple.mpegurl")
    assert "http://testserver/camera-assets/i94-snelling-east.jpg" in stream_response.text
    assert page_response.status_code == 200
    assert "I-94 EB @ Snelling" in page_response.text
