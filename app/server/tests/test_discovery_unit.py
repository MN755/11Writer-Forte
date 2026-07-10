from __future__ import annotations

import pytest

from src.services.discovery_analysis import (
    DocumentAnalysis,
    LinkSignal,
    analyze_document,
    canonical_url_hash,
    canonicalize_url,
    compute_candidate_score,
    normalize_path_pattern,
    recommend_source_kind,
    score_geo_relevance,
)


def test_canonical_url_normalization_and_dedupe() -> None:
    noisy = "HTTPS://ExAmPle.com:443/a/../reports//daily/?z=2&utm_source=mail&a=1#part"
    clean = "https://example.com/reports/daily?a=1&z=2"

    assert canonicalize_url(noisy) == clean
    assert canonical_url_hash(noisy) == canonical_url_hash(clean)
    assert canonicalize_url("../feed.xml?b=2&a=1", "https://EXAMPLE.com/area/page") == (
        "https://example.com/feed.xml?a=1&b=2"
    )
    assert canonicalize_url("https://münchen.example/%7euser") == (
        "https://xn--mnchen-3ya.example/~user"
    )


def test_xml_discovery_rejects_dtd_and_entity_declarations() -> None:
    payload = b'<!DOCTYPE data [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><data>&xxe;</data>'
    with pytest.raises(ValueError, match="DTD and entity"):
        analyze_document(
            "https://example.gov/data.xml",
            payload,
            content_type="application/xml",
        )


def test_canonical_url_validation_and_path_patterns() -> None:
    with pytest.raises(ValueError):
        canonicalize_url("mailto:ops@example.com")
    with pytest.raises(ValueError):
        canonicalize_url("/relative")

    assert normalize_path_pattern(
        "/incidents/123/550e8400-e29b-41d4-a716-446655440000/2026-07-09.json"
    ) == "/incidents/{id}/{uuid}/{date}.json"
    assert normalize_path_pattern("https://example.test/files/abcdef0123456789.csv?q=1") == (
        "/files/{hash}.csv"
    )


@pytest.mark.parametrize(
    ("url", "payload", "content_type", "expected_type", "expected_format", "expected_kind"),
    [
        (
            "https://agency.gov/feed",
            "<rss version='2.0'><channel><title>Alerts</title></channel></rss>",
            "application/xml",
            "rss_feed",
            "rss",
            "rss",
        ),
        (
            "https://agency.gov/atom",
            "<feed xmlns='http://www.w3.org/2005/Atom'><title>Updates</title></feed>",
            "application/atom+xml",
            "atom_feed",
            "atom",
            "rss",
        ),
        (
            "https://agency.gov/sitemap.xml",
            "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'><url><loc>https://agency.gov/a</loc></url></urlset>",
            "application/xml",
            "sitemap",
            "sitemap",
            "http_xml",
        ),
        (
            "https://agency.gov/data.geojson",
            {"type": "FeatureCollection", "features": []},
            "application/json",
            "geojson_endpoint",
            "geojson",
            "http_json",
        ),
        (
            "https://agency.gov/openapi.json",
            {"openapi": "3.1.0", "info": {"title": "Road API"}, "paths": {}},
            "application/json",
            "api_docs",
            "openapi",
            "http_json",
        ),
        (
            "https://agency.gov/events.jsonl",
            '{"id":1}\n{"id":2}\n',
            "application/x-ndjson",
            "jsonl_endpoint",
            "jsonl",
            "http_jsonl",
        ),
        (
            "https://agency.gov/points.csv",
            "name,latitude,longitude\nA,44.98,-93.27\n",
            "text/csv",
            "csv_endpoint",
            "csv",
            "http_text",
        ),
        (
            "https://agency.gov/area.kml",
            "<kml xmlns='http://www.opengis.net/kml/2.2'><Placemark><name>Port</name><Point><coordinates>-93.27,44.98</coordinates></Point></Placemark></kml>",
            "application/vnd.google-earth.kml+xml",
            "kml_endpoint",
            "kml",
            "http_xml",
        ),
        (
            "https://agency.gov/bulletin.pdf",
            b"%PDF-1.7\n1 0 obj<</Title (Port Bulletin)>>endobj",
            "application/pdf",
            "government_notice",
            "pdf",
            "web_discovery",
        ),
    ],
)
def test_format_and_candidate_type_inference(
    url: str,
    payload: bytes | str | dict[str, object],
    content_type: str,
    expected_type: str,
    expected_format: str,
    expected_kind: str,
) -> None:
    analysis = analyze_document(url, payload, content_type)

    assert analysis.document_type == expected_type
    assert analysis.format_hints["detected_format"] == expected_format
    assert recommend_source_kind(analysis) == expected_kind
    assert len(analysis.schema_fingerprint) == 64


def test_html_link_title_geo_route_and_temporal_extraction() -> None:
    html = """
    <html><head>
      <title>Metro I-94 Traffic Cameras</title>
      <meta property="geo.position" content="44.981;-93.271">
      <meta property="article:modified_time" content="2026-07-09T12:30:00Z">
      <link rel="alternate" type="application/rss+xml" href="/alerts.xml?utm_medium=web">
      <script type="application/ld+json">
        {"@type":"Dataset","name":"Metro cameras","spatialCoverage":{"name":"Minneapolis"},
         "distribution":{"contentUrl":"https://data.example.gov/cameras.geojson"}}
      </script>
    </head><body>
      <a href="../snapshot.jpg?fbclid=garbage">Latest snapshot</a>
      Route I-94 is updated every minute.
    </body></html>
    """

    analysis = analyze_document("https://data.example.gov/cameras/index.html", html, "text/html")

    assert analysis.title == "Metro I-94 Traffic Cameras"
    assert analysis.document_type == "camera_page"
    assert analysis.geo_hints["points"][0]["lat"] == 44.981
    assert "I-94" in analysis.geo_hints["routes"]
    assert analysis.geo_hints["footprint_kind"] == "route"
    assert analysis.temporal_hints["modified_at"] == "2026-07-09T12:30:00Z"
    assert analysis.temporal_hints["live"] is True
    assert "Minneapolis" in analysis.geo_hints["places"]
    canonical_links = {link.canonical_url: link for link in analysis.links}
    assert "https://data.example.gov/alerts.xml" in canonical_links
    assert "https://data.example.gov/snapshot.jpg" in canonical_links
    assert "https://data.example.gov/cameras.geojson" in canonical_links
    assert canonical_links["https://data.example.gov/snapshot.jpg"].anchor_text == "Latest snapshot"
    assert canonical_links["https://data.example.gov/alerts.xml"].format_hint == "rss"


def test_json_geo_url_and_schema_fingerprint_is_value_stable() -> None:
    first = {
        "name": "Sensor A",
        "location": {"latitude": 44.98, "longitude": -93.27, "city": "Minneapolis"},
        "updated_at": "2026-07-09T10:00:00Z",
        "data_url": "https://example.gov/data/1?utm_campaign=x",
    }
    second = {
        "name": "Sensor B",
        "location": {"latitude": 45.01, "longitude": -93.2, "city": "Saint Paul"},
        "updated_at": "2026-07-09T11:00:00Z",
        "data_url": "https://example.gov/data/2",
    }

    one = analyze_document("https://example.gov/sensor.json", first, "application/json")
    two = analyze_document("https://example.gov/sensor.json", second, "application/json")

    assert one.schema_fingerprint == two.schema_fingerprint
    assert one.content_hash != two.content_hash
    assert one.geo_hints["points"][0]["lon"] == -93.27
    assert "Minneapolis" in one.geo_hints["places"]
    assert one.links[0].canonical_url == "https://example.gov/data/1"


def test_geo_relevance_intersection_and_name_overlap() -> None:
    hints = {
        "points": [{"lat": 44.98, "lon": -93.27}],
        "places": ["Minneapolis"],
        "jurisdictions": ["Minnesota"],
        "routes": ["I-94"],
        "footprint_kind": "route",
    }

    assert score_geo_relevance(hints, {"bbox": [-94.0, 44.5, -93.0, 45.5]}) == 100
    assert score_geo_relevance(
        hints,
        {"polygon": [[-94, 44], [-92, 44], [-92, 46], [-94, 46], [-94, 44]]},
    ) == 100
    assert score_geo_relevance(
        hints,
        {
            "type": "Polygon",
            "coordinates": [[[-94, 44], [-92, 44], [-92, 46], [-94, 46], [-94, 44]]],
        },
    ) == 100
    assert score_geo_relevance(hints, {"routes": ["Interstate I-94"]}) >= 78
    assert score_geo_relevance(hints, {"places": ["Duluth"], "bbox": [-93, 46, -92, 47]}) < 20


def test_score_breakdown_and_all_outcome_buckets() -> None:
    analysis = analyze_document(
        "https://alerts.dot.example.gov/roads.geojson",
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-93.27, 44.98]},
                    "properties": {"updated_at": "2026-07-09T12:00:00Z"},
                }
            ],
        },
        "application/geo+json",
        {"ETag": '"abc"', "Last-Modified": "Thu, 09 Jul 2026 12:00:00 GMT"},
    )
    promoted = compute_candidate_score(
        analysis,
        target_geo={"bbox": [-94, 44, -93, 46]},
        query_terms="road alerts",
        health={"reachable": True, "changed": True},
        novelty=100,
        component_overrides={"relevance": 95, "temporal": 95},
    )

    assert promoted["bucket"] == "promote_now"
    assert 0 <= promoted["total_score"] <= 100
    assert set(promoted["components"]) == {
        "relevance",
        "geo",
        "temporal",
        "structural",
        "freshness",
        "stability",
        "trust",
        "operational_cost",
        "novelty",
        "redundancy_penalty",
    }
    assert promoted["evidence"]["recommended_source_kind"] == "http_json"
    assert len(promoted["reasons"]) >= 10

    keep = compute_candidate_score(
        analysis,
        component_overrides={
            "relevance": 60,
            "geo": 60,
            "temporal": 60,
            "structural": 60,
            "freshness": 60,
            "stability": 60,
            "trust": 60,
            "novelty": 60,
            "operational_cost": 20,
        },
    )
    revisit = compute_candidate_score(
        analysis,
        component_overrides={
            "relevance": 42,
            "geo": 42,
            "temporal": 42,
            "structural": 42,
            "freshness": 42,
            "stability": 42,
            "trust": 42,
            "novelty": 42,
            "operational_cost": 20,
        },
    )
    ignored = compute_candidate_score(
        analysis,
        component_overrides={key: 10 for key in promoted["components"]},
    )
    quarantined = compute_candidate_score(analysis, quarantine=True)

    assert keep["bucket"] == "keep_candidate"
    assert revisit["bucket"] == "revisit_later"
    assert ignored["bucket"] == "ignore"
    assert quarantined["bucket"] == "quarantine"


def test_private_target_is_quarantined_and_social_is_reference_only() -> None:
    private = analyze_document("http://127.0.0.1/feed.json", {"items": []}, "application/json")
    social = analyze_document(
        "https://x.com/agency/status/123",
        "<html><head><title>Agency update</title></head><body>Reference only</body></html>",
        "text/html",
    )

    assert compute_candidate_score(private)["bucket"] == "quarantine"
    assert social.document_type == "social_reference"
    assert recommend_source_kind(social) is None
    assert compute_candidate_score(social)["bucket"] == "ignore"


def test_camera_image_stream_websocket_sse_and_webhook_mappings() -> None:
    image = analyze_document(
        "https://roads.example.gov/camera/snapshot.jpg",
        b"\xff\xd8\xff\xe0",
        "image/jpeg",
    )
    stream = analyze_document(
        "https://roads.example.gov/camera/live.m3u8",
        "#EXTM3U\nhttps://roads.example.gov/camera/part.ts",
        "application/vnd.apple.mpegurl",
    )

    assert image.document_type == "camera_image"
    assert recommend_source_kind(image) == "camera_image"
    assert stream.document_type == "camera_stream"
    assert recommend_source_kind(stream) == "camera_stream"
    assert recommend_source_kind(
        {
            "canonical_url": "wss://stream.example.gov/events",
            "document_type": "stream_endpoint",
            "format_hints": {"detected_format": "stream"},
        }
    ) == "websocket_stream"
    assert recommend_source_kind(
        {
            "canonical_url": "https://stream.example.gov/events",
            "media_type": "text/event-stream",
            "document_type": "stream_endpoint",
            "format_hints": {"detected_format": "stream"},
        }
    ) == "sse_stream"
    assert recommend_source_kind(
        {
            "canonical_url": "https://receiver.example.gov/hook",
            "document_type": "api_docs",
            "format_hints": {"detected_format": "html"},
            "operational_hints": {"webhook": True},
        }
    ) == "webhook_ingest"


def test_dataclass_serialization_contract() -> None:
    link = LinkSignal(
        url="https://example.gov/data",
        canonical_url="https://example.gov/data",
        source="test",
    )
    analysis = analyze_document("https://example.gov/index.txt", "https://example.gov/data")

    assert link.as_dict()["source"] == "test"
    assert isinstance(analysis, DocumentAnalysis)
    assert analysis.candidate_type == analysis.document_type
    assert analysis.outbound_links == analysis.links
    assert analysis.text_excerpt == analysis.text
    assert analysis.format_hint == "text"
    assert analysis.footprint_kind == "unknown"
    assert analysis.as_dict()["links"][0]["canonical_url"] == "https://example.gov/data"
