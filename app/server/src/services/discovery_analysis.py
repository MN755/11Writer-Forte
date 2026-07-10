"""Pure document analysis and scoring primitives for source discovery.

This module deliberately has no database, HTTP-client, or framework dependency.  It
turns an already-fetched, bounded payload into JSON-serialisable evidence that the
discovery service can persist.  Fetch policy (robots, pacing, retries and SSRF
protection) belongs to the orchestration layer; this module still identifies private
targets and other operational risks so they can affect promotion decisions.

Public API:

* :class:`LinkSignal` and :class:`DocumentAnalysis`
* :func:`canonicalize_url`, :func:`canonical_url_hash`,
  :func:`normalize_path_pattern`
* :func:`analyze_document`
* :func:`score_geo_relevance`, :func:`compute_candidate_score`
* :func:`recommend_source_kind`
"""

from __future__ import annotations

import csv
import hashlib
import html
import ipaddress
import json
import posixpath
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from io import StringIO
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import (
    parse_qsl,
    quote,
    urlencode,
    urljoin,
    urlsplit,
    urlunsplit,
)
from xml.etree import ElementTree


MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
MAX_EXTRACTED_TEXT = 256 * 1024
MAX_LINKS = 500
MAX_JSON_NODES = 10_000
MAX_GEO_POINTS = 200

_TRACKING_PARAMETERS = {
    "dclid",
    "fbclid",
    "gbraid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "msclkid",
    "mkt_tok",
    "vero_conv",
    "vero_id",
    "wbraid",
    "_hsenc",
    "_hsmi",
}
_UNRESERVED = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)
_URL_RE = re.compile(r"(?i)\b(?:https?|wss?)://[^\s<>\"'{}|\\^`]+")
_PERCENT_ESCAPE_RE = re.compile(r"%([0-9a-fA-F]{2})")
_ISO_DATE_RE = re.compile(
    r"\b(20\d{2}-[01]\d-[0-3]\d(?:[T ][0-2]\d:[0-5]\d(?::[0-5]\d)?(?:\.\d+)?(?:Z|[+-][0-2]\d:?\d{2})?)?)\b"
)
_UUID_RE = re.compile(
    r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_DATE_SEGMENT_RE = re.compile(r"^(?:19|20)\d{2}[-_/](?:0?[1-9]|1[0-2])[-_/](?:0?[1-9]|[12]\d|3[01])$")
_COMPACT_DATE_RE = re.compile(r"^(?:19|20)\d{6}$")
_HEX_RE = re.compile(r"(?i)^[0-9a-f]{16,}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{24,}$")
_SOCIAL_DOMAINS = {
    "bsky.app",
    "discord.com",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "mastodon.social",
    "reddit.com",
    "t.me",
    "telegram.me",
    "threads.net",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}
_CAMERA_TERMS = {
    "camera",
    "cctv",
    "mjpeg",
    "snapshot",
    "traffic cam",
    "trafficcam",
    "webcam",
}
_ROUTE_RE = re.compile(
    r"(?i)\b(?:I(?:nterstate)?[- ]?\d{1,3}|US[- ]?\d{1,3}|State Route[- ]?\d{1,4}|"
    r"Highway[- ]?\d{1,4}|Route[- ]?\d{1,4}|[A-Z][A-Za-z0-9 .'-]{1,40} (?:Line|Corridor))\b"
)


@dataclass(frozen=True)
class LinkSignal:
    """One resolved outbound-link observation and its extraction provenance.

    ``url`` is the resolved discovered URL while ``canonical_url`` is suitable for
    deduplication.  ``source`` names the extractor (for example ``html_anchor`` or
    ``json_url``), and ``relation`` preserves useful HTML/XML relationship data.
    """

    url: str
    canonical_url: str
    source: str
    relation: str = "outbound"
    anchor_text: str | None = None
    format_hint: str | None = None
    same_domain: bool = False
    nofollow: bool = False

    def as_dict(self) -> dict[str, Any]:
        """Return a persistence-friendly representation."""

        return asdict(self)


@dataclass(frozen=True)
class DocumentAnalysis:
    """Deterministic evidence extracted from one bounded document payload.

    Hint dictionaries intentionally contain plain JSON values.  The schema
    fingerprint describes structure rather than volatile field values so repeated
    observations can detect endpoint schema changes without treating every update as
    a schema migration.
    """

    url: str
    canonical_url: str
    media_type: str
    document_type: str
    title: str | None
    text: str
    links: tuple[LinkSignal, ...]
    geo_hints: dict[str, Any]
    temporal_hints: dict[str, Any]
    format_hints: dict[str, Any]
    structural_hints: dict[str, Any]
    trust_hints: dict[str, Any]
    operational_hints: dict[str, Any]
    schema_fingerprint: str
    content_hash: str
    truncated: bool = False
    warnings: tuple[str, ...] = field(default_factory=tuple)

    @property
    def candidate_type(self) -> str:
        """Alias used by persistence and presentation layers."""

        return self.document_type

    @property
    def outbound_links(self) -> tuple[LinkSignal, ...]:
        """Explicit alias that makes graph-building call sites self-documenting."""

        return self.links

    @property
    def text_excerpt(self) -> str:
        """Compatibility alias for managed web-source materialisation."""

        return self.text

    @property
    def format_hint(self) -> str:
        """Return the primary detected format as a compact inventory value."""

        return str(self.format_hints.get("detected_format") or "unknown")

    @property
    def footprint_kind(self) -> str:
        """Return the estimated geographic footprint class."""

        return str(self.geo_hints.get("footprint_kind") or "unknown")

    def as_dict(self) -> dict[str, Any]:
        """Return a fully JSON-serialisable representation."""

        result = asdict(self)
        result["links"] = [link.as_dict() for link in self.links]
        result["warnings"] = list(self.warnings)
        result["candidate_type"] = self.document_type
        result["text_excerpt"] = self.text
        result["format_hint"] = self.format_hint
        result["footprint_kind"] = self.footprint_kind
        return result


def _safe_xml_fromstring(value: str) -> ElementTree.Element:
    if re.search(r"(?is)<!\s*(?:DOCTYPE|ENTITY)\b", value):
        # Keep this distinct from malformed XML.  Callers may inventory malformed
        # public documents, but a DTD/entity declaration is an explicit safety
        # rejection and must not fall through into text-link extraction.
        raise ValueError("XML DTD and entity declarations are not allowed")
    return ElementTree.fromstring(value)


def canonicalize_url(url: str, base_url: str | None = None) -> str:
    """Resolve and canonicalise an HTTP(S)/WS(S) URL for stable deduplication.

    The canonical form removes fragments and known tracking parameters, sorts the
    remaining query pairs, folds scheme/host case, removes default ports, resolves dot
    segments, collapses repeated slashes and normalises percent escapes.  A bare host
    is treated as HTTPS.  Credentials are intentionally dropped because they should
    never distinguish public-source candidates.

    ``ValueError`` is raised for an empty URL, missing host, or unsupported scheme.
    """

    if not isinstance(url, str):
        raise TypeError("url must be a string")
    raw = html.unescape(url).strip().replace("\x00", "")
    raw = re.sub(r"[\x01-\x1f\x7f]", "", raw)
    if not raw:
        raise ValueError("url is empty")

    if base_url:
        raw = urljoin(base_url, raw)
    elif raw.startswith("//"):
        raw = "https:" + raw
    elif "://" not in raw:
        explicit_scheme = re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", raw)
        if explicit_scheme:
            raise ValueError(f"unsupported URL scheme: {raw.split(':', 1)[0]}")
        if raw.startswith(('/', './', '../', '?', '#')):
            raise ValueError("relative URL requires base_url")
        raw = "https://" + raw

    parts = urlsplit(raw)
    scheme = parts.scheme.casefold()
    if scheme not in {"http", "https", "ws", "wss"}:
        raise ValueError(f"unsupported URL scheme: {parts.scheme or '<missing>'}")
    hostname = parts.hostname
    if not hostname:
        raise ValueError("URL must include a host")

    hostname = hostname.rstrip(".").casefold()
    try:
        host_ascii = hostname.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ValueError("invalid internationalized host") from exc
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError("invalid URL port") from exc

    if ":" in host_ascii and not host_ascii.startswith("["):
        netloc = f"[{host_ascii}]"
    else:
        netloc = host_ascii
    default_port = (scheme in {"http", "ws"} and port == 80) or (
        scheme in {"https", "wss"} and port == 443
    )
    if port is not None and not default_port:
        netloc += f":{port}"

    path = _normalise_url_path(parts.path)
    query_pairs = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True, errors="replace"):
        lowered = key.casefold()
        if lowered.startswith("utm_") or lowered in _TRACKING_PARAMETERS:
            continue
        query_pairs.append((key, value))
    query_pairs.sort(key=lambda pair: (pair[0].casefold(), pair[0], pair[1]))
    query = urlencode(query_pairs, doseq=True, quote_via=quote, safe="~")
    return urlunsplit((scheme, netloc, path, query, ""))


def canonical_url_hash(url: str, base_url: str | None = None) -> str:
    """Return a lowercase SHA-256 hex digest of :func:`canonicalize_url`."""

    canonical = canonicalize_url(url, base_url=base_url)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_path_pattern(url_or_path: str) -> str:
    """Generalise volatile path identifiers while retaining useful route shape.

    Numeric IDs, UUIDs, ISO dates, long hashes and opaque tokens are replaced with
    named markers.  File extensions are preserved (``/report/42.pdf`` becomes
    ``/report/{id}.pdf``), which keeps this useful for redundancy and cadence analysis.
    """

    if not isinstance(url_or_path, str):
        raise TypeError("url_or_path must be a string")
    raw = url_or_path.strip()
    if not raw:
        return "/"
    if "://" in raw or raw.startswith("//"):
        parts = urlsplit(raw if not raw.startswith("//") else "https:" + raw)
        path = parts.path
    else:
        path = raw.split("?", 1)[0].split("#", 1)[0]
    path = _normalise_url_path(path)
    if path == "/":
        return path

    result: list[str] = []
    for segment in path.strip("/").split("/"):
        stem, suffix = _split_extension(segment)
        result.append(_normalise_path_segment(stem) + suffix.casefold())
    return "/" + "/".join(result)


def analyze_document(
    url: str,
    payload: bytes | str | Mapping[str, Any] | Sequence[Any],
    content_type: str | None = None,
    headers: Mapping[str, str] | None = None,
) -> DocumentAnalysis:
    """Classify and extract bounded source-discovery signals from ``payload``.

    ``payload`` may be bytes, text, or an already-decoded JSON-compatible object.
    At most :data:`MAX_DOCUMENT_BYTES` are parsed and all link/JSON/geo walks have
    explicit caps.  The returned evidence is deterministic for the same inputs.
    """

    canonical = canonicalize_url(url)
    header_map = {str(k).casefold(): str(v).strip() for k, v in (headers or {}).items()}
    declared_type = content_type or header_map.get("content-type") or ""
    media_type = declared_type.split(";", 1)[0].strip().casefold()

    original_bytes, structured_payload = _payload_bytes(payload)
    content_hash = hashlib.sha256(original_bytes).hexdigest()
    truncated = len(original_bytes) > MAX_DOCUMENT_BYTES
    bounded = original_bytes[:MAX_DOCUMENT_BYTES]
    warnings: list[str] = []
    if truncated:
        warnings.append("payload_truncated")

    charset = _content_charset(declared_type)
    text = _decode_payload(bounded, charset)
    extension = _url_extension(canonical)
    detected = _detect_format(
        canonical,
        bounded,
        text,
        media_type,
        extension,
        structured_payload if not truncated else None,
    )

    title: str | None = None
    extracted_text = ""
    raw_links: list[tuple[str, str, str, str | None, bool, str | None]] = []
    geo_hints = _empty_geo_hints()
    temporal_hints = _base_temporal_hints(header_map)
    format_hints: dict[str, Any] = {
        "declared_content_type": media_type or None,
        "detected_format": detected,
        "extension": extension or None,
        "charset": charset,
        "structured": detected
        in {"json", "jsonl", "geojson", "openapi", "csv", "xml", "rss", "atom", "kml", "sitemap"},
    }
    structural_hints: dict[str, Any] = {
        "machine_readable": bool(format_hints["structured"]),
        "link_count": 0,
    }
    operational_hints: dict[str, Any] = {
        "byte_size": len(original_bytes),
        "parsed_byte_size": len(bounded),
        "truncated": truncated,
        "private_network": _is_private_target(canonical),
        "secure_transport": urlsplit(canonical).scheme in {"https", "wss"},
        "streaming": detected == "stream",
        "camera": False,
        "requires_javascript": False,
        "likely_auth_required": False,
        "etag": header_map.get("etag"),
        "cache_control": header_map.get("cache-control"),
        "content_length": _safe_int(header_map.get("content-length")),
    }
    schema_shape: Any = {"format": detected}
    document_type = _base_document_type(detected, canonical)

    if detected == "html":
        parser = _DiscoveryHTMLParser()
        try:
            parser.feed(text)
            parser.close()
        except Exception:
            warnings.append("malformed_html")
        effective_base = urljoin(canonical, parser.base_href) if parser.base_href else canonical
        raw_links.extend(parser.raw_links)
        title = _clean_text(parser.title) or _first_meta(
            parser.meta, "og:title", "twitter:title", "citation_title"
        )
        extracted_text = _clean_text(" ".join(parser.text_parts))[:MAX_EXTRACTED_TEXT]
        for script in parser.json_ld_scripts:
            try:
                json_ld = json.loads(script)
            except (ValueError, TypeError):
                continue
            _extract_json_signals(json_ld, raw_links, geo_hints, temporal_hints)
        raw_links.extend(_text_url_links(extracted_text, "html_text_url"))
        raw_links.extend(_text_url_links(" ".join(parser.scripts), "inline_script_url"))
        _extract_html_geo(parser, text, geo_hints)
        _extract_html_temporal(parser, text, temporal_hints)
        structural_hints.update(
            {
                "tag_count": sum(parser.tags.values()),
                "unique_tags": len(parser.tags),
                "has_schema_org": bool(parser.json_ld_scripts),
                "has_article": bool(parser.tags.get("article")),
                "has_table": bool(parser.tags.get("table")),
                "has_feed_link": any(
                    relation == "alternate" and (hint in {"rss", "atom", "xml"})
                    for _, _, relation, _, _, hint in parser.raw_links
                ),
            }
        )
        scripts_text = " ".join(parser.scripts).casefold()
        operational_hints["requires_javascript"] = _requires_javascript(parser, extracted_text)
        operational_hints["likely_auth_required"] = _likely_auth(extracted_text, canonical)
        operational_hints["camera"] = _has_camera_signal(
            canonical, title or "", extracted_text, scripts_text
        )
        schema_shape = {
            "format": "html",
            "tags": sorted(parser.tags),
            "meta": sorted(parser.meta),
            "json_ld_types": sorted(_json_ld_types(parser.json_ld_scripts)),
            "table_headers": sorted(parser.table_headers),
        }
        canonical_for_links = effective_base
        document_type = _classify_html(
            canonical, title, extracted_text, parser, operational_hints["camera"]
        )
    elif detected in {"json", "geojson", "openapi", "jsonl"}:
        data: Any
        if detected == "jsonl":
            records = _parse_json_lines(text)
            data = records
            format_hints["record_count"] = len(records)
        elif structured_payload is not None and not truncated:
            data = structured_payload
        else:
            try:
                data = json.loads(text)
            except (ValueError, TypeError):
                data = None
                warnings.append("malformed_json")
        if data is not None:
            _extract_json_signals(data, raw_links, geo_hints, temporal_hints)
            title = _json_title(data)
            schema_shape = _json_schema_shape(data)
            structural_hints.update(_json_structure_hints(data))
            if isinstance(data, Mapping):
                format_hints["top_level_keys"] = sorted(str(key) for key in data)[:100]
            if detected == "openapi":
                format_hints["openapi_version"] = _mapping_value(data, "openapi", "swagger")
                structural_hints["api_path_count"] = _mapping_length(data, "paths")
        extracted_text = _json_search_text(data)[:MAX_EXTRACTED_TEXT] if data is not None else ""
        operational_hints["likely_auth_required"] = _json_auth_required(data)
        operational_hints["camera"] = _has_camera_signal(
            canonical, title or "", extracted_text
        )
        canonical_for_links = canonical
        if _archive_signal(canonical, title or "", extracted_text):
            document_type = "archive_dataset"
    elif detected in {"xml", "rss", "atom", "kml", "sitemap"}:
        root: ElementTree.Element | None = None
        try:
            root = _safe_xml_fromstring(text)
        except ElementTree.ParseError:
            warnings.append("malformed_xml")
        if root is not None:
            raw_links.extend(_extract_xml_links(root))
            title = _xml_title(root)
            _extract_xml_geo(root, geo_hints)
            _extract_xml_temporal(root, temporal_hints)
            schema_shape = _xml_schema_shape(root)
            structural_hints.update(
                {
                    "root_tag": _local_name(root.tag),
                    "element_count": sum(1 for _ in root.iter()),
                }
            )
            format_hints["root_tag"] = _local_name(root.tag)
        extracted_text = _clean_text(" ".join(root.itertext()))[:MAX_EXTRACTED_TEXT] if root is not None else ""
        operational_hints["camera"] = _has_camera_signal(
            canonical, title or "", extracted_text
        )
        canonical_for_links = canonical
        if _archive_signal(canonical, title or "", extracted_text):
            document_type = "archive_dataset"
    elif detected == "csv":
        rows, delimiter = _parse_csv(text)
        headers_row = rows[0] if rows else []
        format_hints.update(
            {
                "delimiter": delimiter,
                "columns": headers_row[:200],
                "record_count": max(0, len(rows) - 1),
            }
        )
        structural_hints.update(
            {
                "column_count": len(headers_row),
                "record_count_sampled": max(0, len(rows) - 1),
            }
        )
        _extract_csv_signals(rows, headers_row, raw_links, geo_hints, temporal_hints)
        title = _title_from_url(canonical)
        extracted_text = _clean_text(" ".join(" ".join(row) for row in rows[:50]))[:MAX_EXTRACTED_TEXT]
        schema_shape = {"format": "csv", "columns": [_normalise_field_name(x) for x in headers_row]}
        canonical_for_links = canonical
        if _archive_signal(canonical, title or "", extracted_text):
            document_type = "archive_dataset"
    elif detected == "pdf":
        latin_text = bounded.decode("latin-1", errors="ignore")
        raw_links.extend((match, "pdf_url", "reference", None, False, None) for match in _URL_RE.findall(latin_text))
        title_match = re.search(r"/Title\s*\((.{1,500}?)\)", latin_text, re.DOTALL)
        title = _clean_text(title_match.group(1)) if title_match else _title_from_url(canonical)
        extracted_text = ""
        schema_shape = {"format": "pdf"}
        structural_hints["machine_readable"] = False
        canonical_for_links = canonical
        if _archive_signal(canonical, title or "", ""):
            document_type = "archive_document"
        elif _notice_signal(canonical, title or ""):
            document_type = "government_notice"
    elif detected == "image":
        title = _title_from_url(canonical)
        operational_hints["camera"] = _has_camera_signal(canonical, title or "")
        document_type = "camera_image" if operational_hints["camera"] else "image_endpoint"
        extracted_text = ""
        schema_shape = {"format": "image", "media_type": media_type or extension}
        canonical_for_links = canonical
    elif detected == "stream":
        title = _title_from_url(canonical)
        operational_hints["camera"] = _has_camera_signal(canonical, title or "", text[:5000])
        document_type = "camera_stream" if operational_hints["camera"] else "stream_endpoint"
        extracted_text = text[:MAX_EXTRACTED_TEXT]
        schema_shape = {"format": "stream", "media_type": media_type or extension}
        canonical_for_links = canonical
        raw_links.extend(_text_url_links(extracted_text, "stream_manifest"))
    else:
        extracted_text = _clean_text(text)[:MAX_EXTRACTED_TEXT]
        title = _title_from_url(canonical)
        raw_links.extend(_text_url_links(text, "text_url"))
        _extract_text_geo(text, geo_hints)
        _extract_text_temporal(text, temporal_hints)
        schema_shape = {"format": "text", "line_shapes": _text_line_shapes(text)}
        structural_hints["machine_readable"] = False
        operational_hints["camera"] = _has_camera_signal(
            canonical, title or "", extracted_text
        )
        canonical_for_links = canonical
        if _archive_signal(canonical, title or "", extracted_text):
            document_type = "archive_document"

    if detected not in {"html", "stream", "text"}:
        raw_links.extend(_text_url_links(text, "embedded_url"))
    links = _normalise_links(raw_links, canonical_for_links, canonical)
    if len(raw_links) > MAX_LINKS:
        warnings.append("link_limit_reached")
    structural_hints["link_count"] = len(links)

    _finalise_geo_hints(geo_hints, extracted_text, canonical)
    _finalise_temporal_hints(temporal_hints, extracted_text, detected)
    if geo_hints["footprint_kind"] == "route" and document_type == "html_page":
        document_type = "repeating_status_page"

    host = (urlsplit(canonical).hostname or "").casefold()
    trust_hints = {
        "domain": host,
        "secure_transport": operational_hints["secure_transport"],
        "official_domain": _is_official_domain(host),
        "education_domain": host.endswith(".edu"),
        "social_reference": _is_social_domain(host),
        "publisher": _publisher_hint(canonical, title, extracted_text),
        "integrity_headers": sorted(
            key for key in ("content-security-policy", "digest", "etag", "last-modified") if header_map.get(key)
        ),
    }
    if trust_hints["social_reference"]:
        document_type = "social_reference"
    operational_hints["path_pattern"] = normalize_path_pattern(canonical)
    operational_hints["format_hint"] = detected
    schema_fingerprint = _stable_hash(schema_shape)
    format_hints["schema_fingerprint"] = schema_fingerprint

    return DocumentAnalysis(
        url=url,
        canonical_url=canonical,
        media_type=media_type or _media_type_for_format(detected),
        document_type=document_type,
        title=title,
        text=extracted_text,
        links=tuple(links),
        geo_hints=geo_hints,
        temporal_hints=temporal_hints,
        format_hints=format_hints,
        structural_hints=structural_hints,
        trust_hints=trust_hints,
        operational_hints=operational_hints,
        schema_fingerprint=schema_fingerprint,
        content_hash=content_hash,
        truncated=truncated,
        warnings=tuple(dict.fromkeys(warnings)),
    )


def score_geo_relevance(
    geo_hints: Mapping[str, Any] | None,
    target_geo: Mapping[str, Any] | None,
) -> float:
    """Score geographic overlap from 0 to 100.

    Candidate points are intersected with target bounding boxes and polygons.  The
    function also compares normalised place, jurisdiction, and route labels.  It is
    intentionally deterministic and gazetteer-free: upstream enrichment can add
    normalised labels or coordinates without changing this scoring contract.
    """

    hints = geo_hints or {}
    points = _coerce_points(hints.get("points"))
    candidate_bbox = _coerce_bbox(hints.get("bbox"))
    places = _normalised_label_set(hints.get("places"))
    jurisdictions = _normalised_label_set(hints.get("jurisdictions"))
    routes = _normalised_label_set(hints.get("routes"))
    footprint = str(hints.get("footprint_kind") or "unknown").casefold()

    if not target_geo:
        if points:
            return 65.0
        if routes or jurisdictions:
            return 55.0
        if places:
            return 45.0
        if footprint in {"nationwide", "global"}:
            return 30.0
        return 20.0

    target_bbox = _coerce_bbox(target_geo.get("bbox")) or _coerce_bbox(target_geo)
    polygon_value: Any = target_geo.get("polygon") or target_geo.get("geometry")
    if polygon_value is None and str(target_geo.get("type") or "").casefold() == "polygon":
        polygon_value = target_geo
    polygon = _coerce_polygon(polygon_value)
    target_points = _coerce_points(target_geo.get("points"))
    if not target_points and target_geo.get("point") is not None:
        target_points = _coerce_points([target_geo.get("point")])
    target_places = _normalised_label_set(
        target_geo.get("places") or target_geo.get("place_names") or target_geo.get("place")
    )
    target_jurisdictions = _normalised_label_set(
        target_geo.get("jurisdictions") or target_geo.get("jurisdiction")
    )
    target_routes = _normalised_label_set(target_geo.get("routes") or target_geo.get("route"))
    target_name = target_geo.get("name")
    if isinstance(target_name, str) and target_name.strip():
        target_places.add(_normalise_label(target_name))

    scores: list[float] = []
    for lon, lat in points:
        if target_bbox and _point_in_bbox((lon, lat), target_bbox):
            scores.append(100.0)
        if polygon and _point_in_polygon((lon, lat), polygon):
            scores.append(100.0)
    if candidate_bbox and target_bbox and _bbox_intersects(candidate_bbox, target_bbox):
        scores.append(95.0)
    if candidate_bbox:
        for point in target_points:
            if _point_in_bbox(point, candidate_bbox):
                scores.append(95.0)
    if polygon:
        for point in points:
            if _point_in_polygon(point, polygon):
                scores.append(100.0)

    scores.extend(_label_overlap_scores(routes, target_routes, exact=96.0, partial=78.0))
    scores.extend(
        _label_overlap_scores(jurisdictions, target_jurisdictions, exact=92.0, partial=76.0)
    )
    scores.extend(_label_overlap_scores(places, target_places, exact=90.0, partial=72.0))
    if scores:
        overlap_types = sum(
            bool(value)
            for value in (
                routes & target_routes,
                jurisdictions & target_jurisdictions,
                places & target_places,
            )
        )
        return round(min(100.0, max(scores) + max(0, overlap_types - 1) * 3.0), 2)
    if footprint == "nationwide" and (target_places or target_jurisdictions or target_bbox or polygon):
        return 45.0
    if footprint == "global":
        return 30.0
    if points or candidate_bbox or places or jurisdictions or routes:
        return 8.0
    return 0.0


def compute_candidate_score(
    analysis: DocumentAnalysis | Mapping[str, Any],
    *,
    target_geo: Mapping[str, Any] | None = None,
    query_terms: Sequence[str] | str | None = None,
    health: Mapping[str, Any] | None = None,
    novelty: float | None = None,
    redundancy_penalty: float | None = None,
    trust: float | None = None,
    component_overrides: Mapping[str, float] | None = None,
    quarantine: bool = False,
) -> dict[str, Any]:
    """Compute an explainable, rule-based candidate score.

    Component values and the final ``total_score`` use a 0..100 scale.  Positive
    components are weighted to one; operational cost and redundancy are explicit
    deductions.  The result is JSON-ready and contains ``components``, ``weights``,
    ``penalties``, ``reasons`` and compact ``evidence`` in addition to the outcome
    ``bucket``.
    """

    view = _analysis_view(analysis)
    health_map = dict(health or {})
    components: dict[str, float] = {
        "relevance": _score_relevance(view, query_terms),
        "geo": score_geo_relevance(view["geo_hints"], target_geo),
        "temporal": _score_temporal(view),
        "structural": _score_structural(view),
        "freshness": _score_freshness(view, health_map),
        "stability": _score_stability(view, health_map),
        "trust": _score_trust(view) if trust is None else _clamp(trust),
        "operational_cost": _score_operational_cost(view, health_map),
        "novelty": 70.0 if novelty is None else _clamp(novelty),
        "redundancy_penalty": 0.0
        if redundancy_penalty is None
        else _clamp(redundancy_penalty),
    }
    for key, value in (component_overrides or {}).items():
        if key in components:
            components[key] = _clamp(value)
    components = {key: round(value, 2) for key, value in components.items()}

    weights = {
        "relevance": 0.18,
        "geo": 0.18,
        "temporal": 0.10,
        "structural": 0.12,
        "freshness": 0.10,
        "stability": 0.10,
        "trust": 0.12,
        "novelty": 0.10,
    }
    positive = sum(components[key] * weight for key, weight in weights.items())
    penalties = {
        "operational_cost": round(components["operational_cost"] * 0.10, 2),
        "redundancy_penalty": round(components["redundancy_penalty"] * 0.15, 2),
    }
    total = round(_clamp(positive - sum(penalties.values())), 2)

    kind = recommend_source_kind(analysis)
    private_target = bool(view["operational_hints"].get("private_network"))
    hard_health_failure = bool(
        health_map.get("quarantine")
        or health_map.get("unsafe")
        or health_map.get("tls_invalid")
        or health_map.get("content_mismatch") == "dangerous"
    )
    if quarantine or private_target or hard_health_failure:
        bucket = "quarantine"
    elif view["document_type"] == "social_reference" or kind is None:
        bucket = "ignore"
    elif total >= 72.0:
        bucket = "promote_now"
    elif total >= 52.0:
        bucket = "keep_candidate"
    elif total >= 32.0:
        bucket = "revisit_later"
    else:
        bucket = "ignore"

    reasons = _score_reasons(components, penalties, bucket, view, target_geo)
    evidence = {
        "document_type": view["document_type"],
        "detected_format": view["format_hints"].get("detected_format"),
        "recommended_source_kind": kind,
        "footprint_kind": view["geo_hints"].get("footprint_kind", "unknown"),
        "official_domain": bool(view["trust_hints"].get("official_domain")),
        "secure_transport": bool(view["operational_hints"].get("secure_transport")),
        "private_network": private_target,
        "schema_fingerprint": view.get("schema_fingerprint"),
    }
    return {
        "total_score": total,
        "bucket": bucket,
        "components": components,
        "weights": weights,
        "penalties": penalties,
        "reasons": reasons,
        "evidence": evidence,
    }


def recommend_source_kind(analysis: DocumentAnalysis | Mapping[str, Any]) -> str | None:
    """Map analysis evidence to a managed source kind.

    Social/profile pages are reference-only and always return ``None``.  Explicit
    camera endpoints use ``camera_image``/``camera_stream``; a camera *page* remains a
    bounded ``web_discovery`` source until its endpoint is verified.
    """

    view = _analysis_view(analysis)
    document_type = view["document_type"]
    if document_type == "social_reference" or view["trust_hints"].get("social_reference"):
        return None

    canonical = str(view.get("canonical_url") or "")
    scheme = urlsplit(canonical).scheme.casefold()
    detected = str(view["format_hints"].get("detected_format") or "").casefold()
    media_type = str(view.get("media_type") or "").casefold()
    operational = view["operational_hints"]
    if scheme in {"ws", "wss"}:
        return "websocket_stream"
    if media_type == "text/event-stream" or operational.get("sse"):
        return "sse_stream"
    if operational.get("webhook"):
        return "webhook_ingest"
    if document_type == "camera_image":
        return "camera_image"
    if document_type == "camera_stream":
        return "camera_stream"
    if detected in {"rss", "atom"}:
        return "rss"
    if detected == "jsonl":
        return "http_jsonl"
    if detected in {"json", "geojson", "openapi"}:
        return "http_json"
    if detected in {"xml", "kml", "sitemap"}:
        return "http_xml"
    if detected in {"csv", "text"}:
        return "http_text"
    if detected == "pdf":
        return "web_discovery"
    if document_type == "search_results":
        return "web_search"
    if detected == "html":
        if document_type in {"api_docs", "camera_page", "open_data_portal"}:
            return "web_discovery"
        return "web_crawl"
    return "web_discovery"


class _DiscoveryHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: Counter[str] = Counter()
        self.meta: dict[str, list[str]] = {}
        self.raw_links: list[tuple[str, str, str, str | None, bool, str | None]] = []
        self.text_parts: list[str] = []
        self.scripts: list[str] = []
        self.json_ld_scripts: list[str] = []
        self.table_headers: set[str] = set()
        self.base_href: str | None = None
        self._title_parts: list[str] = []
        self._in_title = False
        self._skip_text = 0
        self._script_type = ""
        self._script_parts: list[str] = []
        self._anchor_index: int | None = None
        self._anchor_parts: list[str] = []
        self._in_th = False

    @property
    def title(self) -> str:
        return " ".join(self._title_parts)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.casefold()
        self.tags[tag] += 1
        attr = {key.casefold(): (value or "") for key, value in attrs}
        if tag == "title":
            self._in_title = True
        if tag in {"style", "noscript"}:
            self._skip_text += 1
        if tag == "script":
            self._skip_text += 1
            self._script_type = attr.get("type", "").casefold()
            self._script_parts = []
        if tag == "th":
            self._in_th = True
        if tag == "base" and attr.get("href") and not self.base_href:
            self.base_href = attr["href"]
        if tag == "meta":
            name = (attr.get("name") or attr.get("property") or attr.get("http-equiv") or "").casefold()
            content = attr.get("content", "").strip()
            if name and content:
                self.meta.setdefault(name, []).append(content)
            if name == "refresh" and content:
                match = re.search(r"(?i)\burl\s*=\s*['\"]?(.+?)['\"]?\s*$", content)
                if match:
                    self.raw_links.append((match.group(1), "html_meta", "refresh", None, False, None))
            elif content and (name.endswith(":url") or name.endswith("_url")):
                self.raw_links.append((content, "html_meta", name or "reference", None, False, None))
        if tag == "a" and attr.get("href"):
            rel_tokens = set(attr.get("rel", "").casefold().split())
            relation = "nofollow" if "nofollow" in rel_tokens else "outbound"
            self.raw_links.append(
                (attr["href"], "html_anchor", relation, None, "nofollow" in rel_tokens, None)
            )
            self._anchor_index = len(self.raw_links) - 1
            self._anchor_parts = []
        elif tag == "link" and attr.get("href"):
            relation = (attr.get("rel") or "reference").casefold().split()[0]
            hint = _format_hint_from_mime_or_url(attr.get("type"), attr["href"])
            self.raw_links.append((attr["href"], "html_link", relation, None, False, hint))
        elif tag in {"img", "iframe", "video", "audio", "source", "script"} and attr.get("src"):
            hint = _format_hint_from_mime_or_url(attr.get("type"), attr["src"])
            self.raw_links.append((attr["src"], f"html_{tag}", "embed", None, False, hint))
        elif tag == "form" and attr.get("action"):
            self.raw_links.append((attr["action"], "html_form", "action", None, False, None))
        for key in ("data-url", "data-src", "data-stream", "data-endpoint", "data-api"):
            if attr.get(key):
                self.raw_links.append((attr[key], "html_data", key[5:], None, False, None))

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if tag == "title":
            self._in_title = False
        if tag in {"style", "noscript"} and self._skip_text:
            self._skip_text -= 1
        if tag == "script":
            script = "".join(self._script_parts)[:MAX_EXTRACTED_TEXT]
            self.scripts.append(script)
            if "ld+json" in self._script_type:
                self.json_ld_scripts.append(script)
            self._script_parts = []
            self._script_type = ""
            if self._skip_text:
                self._skip_text -= 1
        if tag == "a" and self._anchor_index is not None:
            current = self.raw_links[self._anchor_index]
            self.raw_links[self._anchor_index] = (
                current[0],
                current[1],
                current[2],
                _clean_text(" ".join(self._anchor_parts))[:500] or None,
                current[4],
                current[5],
            )
            self._anchor_index = None
            self._anchor_parts = []
        if tag == "th":
            self._in_th = False

    def handle_data(self, data: str) -> None:
        if self._script_type or (self.tags.get("script") and self._skip_text):
            if self._script_parts is not None:
                self._script_parts.append(data)
        if self._in_title:
            self._title_parts.append(data)
        if self._anchor_index is not None:
            self._anchor_parts.append(data)
        if self._in_th:
            cleaned = _clean_text(data)
            if cleaned:
                self.table_headers.add(cleaned[:200])
        if not self._skip_text:
            self.text_parts.append(data)


def _normalise_url_path(path: str) -> str:
    path = path or "/"
    path = _normalise_percent_escapes(path)
    path = re.sub(r"/{2,}", "/", path)
    normalised = posixpath.normpath(path)
    if not normalised.startswith("/"):
        normalised = "/" + normalised
    if normalised != "/":
        normalised = normalised.rstrip("/")
    # Percent escapes were validated/upper-cased above; keep their ``%`` intact.
    return quote(normalised, safe="/%:@!$&'()*+,;=-._~")


def _normalise_percent_escapes(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        byte = int(match.group(1), 16)
        character = chr(byte)
        return character if character in _UNRESERVED else f"%{byte:02X}"

    return _PERCENT_ESCAPE_RE.sub(replace, value)


def _split_extension(segment: str) -> tuple[str, str]:
    match = re.match(r"^(.*?)(\.[A-Za-z0-9]{1,10})$", segment)
    return (match.group(1), match.group(2)) if match else (segment, "")


def _normalise_path_segment(segment: str) -> str:
    if _UUID_RE.fullmatch(segment):
        return "{uuid}"
    if _DATE_SEGMENT_RE.fullmatch(segment) or _COMPACT_DATE_RE.fullmatch(segment):
        return "{date}"
    if segment.isdigit():
        return "{id}"
    if _HEX_RE.fullmatch(segment):
        return "{hash}"
    if _TOKEN_RE.fullmatch(segment) and any(ch.isdigit() for ch in segment):
        return "{token}"
    return segment


def _payload_bytes(
    payload: bytes | str | Mapping[str, Any] | Sequence[Any],
) -> tuple[bytes, Any | None]:
    if isinstance(payload, bytes):
        return payload, None
    if isinstance(payload, str):
        return payload.encode("utf-8"), None
    if isinstance(payload, Mapping) or (
        isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray))
    ):
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
        return encoded, payload
    raise TypeError("payload must be bytes, str, mapping, or sequence")


def _content_charset(content_type: str) -> str:
    match = re.search(r"(?i)\bcharset\s*=\s*['\"]?([\w.-]+)", content_type)
    return match.group(1).casefold() if match else "utf-8"


def _decode_payload(payload: bytes, charset: str) -> str:
    if payload.startswith(b"\xef\xbb\xbf"):
        charset = "utf-8-sig"
    elif payload.startswith((b"\xff\xfe", b"\xfe\xff")):
        charset = "utf-16"
    try:
        return payload.decode(charset, errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


def _detect_format(
    url: str,
    payload: bytes,
    text: str,
    media_type: str,
    extension: str,
    structured_payload: Any,
) -> str:
    scheme = urlsplit(url).scheme.casefold()
    if scheme in {"ws", "wss"} or media_type in {
        "application/vnd.apple.mpegurl",
        "application/x-mpegurl",
        "application/dash+xml",
        "multipart/x-mixed-replace",
        "text/event-stream",
        "video/mp2t",
    } or extension in {"m3u8", "mpd", "mjpg", "mjpeg"}:
        return "stream"
    if payload.startswith(b"%PDF-") or media_type == "application/pdf" or extension == "pdf":
        return "pdf"
    if media_type.startswith("image/") or extension in {
        "avif",
        "bmp",
        "gif",
        "jpeg",
        "jpg",
        "png",
        "svg",
        "webp",
    } or payload.startswith((b"\x89PNG", b"GIF8", b"\xff\xd8\xff")):
        return "image"

    stripped = text.lstrip("\ufeff\x00 \t\r\n")
    if structured_payload is not None:
        return _classify_json_value(structured_payload)
    json_declared = (
        media_type.endswith("+json")
        or media_type in {"application/json", "application/geo+json", "application/x-ndjson"}
        or extension in {"json", "geojson", "jsonl", "ndjson"}
    )
    if json_declared or stripped.startswith(("{", "[")):
        try:
            return _classify_json_value(json.loads(stripped))
        except (ValueError, TypeError):
            records = _parse_json_lines(stripped)
            if records and len(records) >= 2:
                return "jsonl"
            if extension in {"jsonl", "ndjson"} or media_type == "application/x-ndjson":
                return "jsonl"

    html_declared = media_type in {"text/html", "application/xhtml+xml"} or extension in {
        "htm",
        "html",
    }
    if html_declared or re.match(r"(?is)^\s*(?:<!doctype\s+html|<html\b)", stripped):
        return "html"

    xml_declared = (
        media_type.endswith("+xml")
        or media_type in {"application/xml", "text/xml"}
        or extension in {"atom", "kml", "rss", "xml"}
    )
    if xml_declared or stripped.startswith("<"):
        try:
            root = _safe_xml_fromstring(stripped)
        except ElementTree.ParseError:
            root = None
        if root is not None:
            root_name = _local_name(root.tag).casefold()
            namespace = root.tag.casefold()
            if root_name in {"urlset", "sitemapindex"}:
                return "sitemap"
            if root_name == "rss" or root_name == "rdf" or root.find("channel") is not None:
                return "rss"
            if root_name == "feed" and "atom" in namespace:
                return "atom"
            if root_name == "kml" or "opengis.net/kml" in namespace:
                return "kml"
            return "xml"
        if xml_declared:
            return "xml"

    if media_type in {"text/csv", "application/csv", "application/vnd.ms-excel"} or extension == "csv":
        return "csv"
    if _looks_like_csv(stripped):
        return "csv"
    return "text"


def _classify_json_value(data: Any) -> str:
    if isinstance(data, Mapping):
        lowered = {str(key).casefold() for key in data}
        if "openapi" in lowered or "swagger" in lowered:
            return "openapi"
        geo_type = str(data.get("type") or "").casefold()
        if geo_type in {
            "feature",
            "featurecollection",
            "geometrycollection",
            "point",
            "multipoint",
            "linestring",
            "multilinestring",
            "polygon",
            "multipolygon",
        } and ("coordinates" in data or "features" in data or "geometry" in data):
            return "geojson"
    return "json"


def _base_document_type(detected: str, url: str) -> str:
    mapping = {
        "atom": "atom_feed",
        "csv": "csv_endpoint",
        "geojson": "geojson_endpoint",
        "html": "html_page",
        "image": "image_endpoint",
        "json": "json_endpoint",
        "jsonl": "jsonl_endpoint",
        "kml": "kml_endpoint",
        "openapi": "api_docs",
        "pdf": "pdf_document",
        "rss": "rss_feed",
        "sitemap": "sitemap",
        "stream": "stream_endpoint",
        "text": "text_document",
        "xml": "xml_endpoint",
    }
    if "search" in (urlsplit(url).path + "?" + urlsplit(url).query).casefold() and detected == "html":
        return "search_results"
    return mapping[detected]


def _classify_html(
    url: str,
    title: str | None,
    text: str,
    parser: _DiscoveryHTMLParser,
    camera: bool,
) -> str:
    host = (urlsplit(url).hostname or "").casefold()
    if _is_social_domain(host):
        return "social_reference"
    haystack = " ".join((url, title or "", text[:50_000])).casefold()
    if camera:
        return "camera_page"
    if any(term in haystack for term in ("swagger ui", "openapi", "api reference", "redoc")):
        return "api_docs"
    if _archive_signal(url, title or "", text):
        return "archive_dataset"
    if any(term in haystack for term in ("open data", "data catalog", "download dataset", "ckan", "socrata", "arcgis hub")):
        return "open_data_portal"
    if _notice_signal(url, title or "", text) or (
        _is_official_domain(host)
        and any(term in haystack for term in ("bulletin", "notice", "advisory", "press release"))
    ):
        return "government_notice"
    if any(term in haystack for term in ("current status", "incident status", "service alert", "results updated", "road conditions")):
        return "repeating_status_page"
    og_type = " ".join(parser.meta.get("og:type", [])).casefold()
    if parser.tags.get("article") or "article" in og_type or "newsarticle" in haystack:
        return "article_page"
    if "search" in (urlsplit(url).path + "?" + urlsplit(url).query).casefold():
        return "search_results"
    return "html_page"


def _normalise_links(
    raw_links: Iterable[tuple[str, str, str, str | None, bool, str | None]],
    base_url: str,
    document_url: str,
) -> list[LinkSignal]:
    result: list[LinkSignal] = []
    seen: set[str] = set()
    document_host = (urlsplit(document_url).hostname or "").casefold()
    for raw_url, source, relation, anchor_text, nofollow, hint in raw_links:
        if len(result) >= MAX_LINKS:
            break
        candidate = html.unescape(str(raw_url)).strip().strip("\"'")
        if not candidate or candidate.startswith(("#", "data:", "javascript:", "mailto:", "tel:", "file:")):
            continue
        try:
            canonical = canonicalize_url(candidate, base_url=base_url)
        except (TypeError, ValueError):
            continue
        if canonical == document_url or canonical in seen:
            continue
        seen.add(canonical)
        resolved = urljoin(base_url, candidate)
        try:
            resolved = urlunsplit((*urlsplit(resolved)[:4], ""))
        except ValueError:
            resolved = canonical
        candidate_host = (urlsplit(canonical).hostname or "").casefold()
        result.append(
            LinkSignal(
                url=resolved,
                canonical_url=canonical,
                source=source,
                relation=relation or "outbound",
                anchor_text=anchor_text,
                format_hint=hint or _format_hint_from_mime_or_url(None, canonical),
                same_domain=candidate_host == document_host,
                nofollow=nofollow,
            )
        )
    return result


def _extract_json_signals(
    data: Any,
    links: list[tuple[str, str, str, str | None, bool, str | None]],
    geo: dict[str, Any],
    temporal: dict[str, Any],
) -> None:
    stack: list[tuple[Any, str | None]] = [(data, None)]
    visited = 0
    while stack and visited < MAX_JSON_NODES:
        value, parent_key = stack.pop()
        visited += 1
        if isinstance(value, Mapping):
            lowered = {str(key).casefold(): item for key, item in value.items()}
            normalised_parent = _normalise_field_name(parent_key or "")
            if normalised_parent in {
                "address",
                "area",
                "areaserved",
                "geo",
                "location",
                "spatial",
                "spatialcoverage",
            }:
                for name_key in ("name", "placename", "addresslocality"):
                    name_value = lowered.get(name_key)
                    if isinstance(name_value, str):
                        _append_unique(geo["places"], _clean_text(name_value))
                for region_key in ("addressregion", "region", "jurisdiction", "country"):
                    region_value = lowered.get(region_key)
                    if isinstance(region_value, str):
                        _append_unique(geo["jurisdictions"], _clean_text(region_value))
            lat = _first_numeric(lowered, "latitude", "lat")
            lon = _first_numeric(lowered, "longitude", "lon", "lng", "long")
            if lat is None and "y" in lowered and ("x" in lowered or "longitude" in lowered):
                lat = _as_float(lowered.get("y"))
            if lon is None and "x" in lowered and ("y" in lowered or "latitude" in lowered):
                lon = _as_float(lowered.get("x"))
            _add_point(geo, lat, lon, "json")
            geo_type = str(lowered.get("type") or "").casefold()
            if "coordinates" in lowered and geo_type:
                _extract_geojson_coordinates(lowered["coordinates"], geo_type, geo)
            for key, item in value.items():
                key_text = str(key).casefold()
                if isinstance(item, str):
                    if key_text in {
                        "url",
                        "uri",
                        "href",
                        "link",
                        "downloadurl",
                        "download_url",
                        "endpoint",
                        "serviceurl",
                        "service_url",
                    } or _looks_like_url(item):
                        links.append((item, "json_url", key_text or "reference", None, False, None))
                    _add_named_geo_hint(geo, key_text, item)
                    _add_temporal_value(temporal, key_text, item)
                stack.append((item, key_text))
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            for item in reversed(value[:1000] if isinstance(value, list) else list(value)[:1000]):
                stack.append((item, parent_key))
        elif isinstance(value, str):
            if _looks_like_url(value):
                links.append((value, "json_url", parent_key or "reference", None, False, None))
            if parent_key:
                _add_named_geo_hint(geo, parent_key, value)
                _add_temporal_value(temporal, parent_key, value)


def _extract_geojson_coordinates(coordinates: Any, geo_type: str, geo: dict[str, Any]) -> None:
    route = "line" in geo_type

    def walk(value: Any) -> None:
        if len(geo["points"]) >= MAX_GEO_POINTS:
            return
        if (
            isinstance(value, Sequence)
            and not isinstance(value, (str, bytes, bytearray))
            and len(value) >= 2
            and _as_float(value[0]) is not None
            and _as_float(value[1]) is not None
        ):
            _add_point(geo, _as_float(value[1]), _as_float(value[0]), "geojson")
            return
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            for item in value:
                walk(item)

    walk(coordinates)
    if route and "GeoJSON route" not in geo["routes"]:
        geo["routes"].append("GeoJSON route")


def _extract_xml_links(root: ElementTree.Element) -> list[tuple[str, str, str, str | None, bool, str | None]]:
    links: list[tuple[str, str, str, str | None, bool, str | None]] = []
    for element in root.iter():
        name = _local_name(element.tag).casefold()
        href = element.attrib.get("href") or element.attrib.get("url")
        if href:
            links.append((href, "xml_attribute", element.attrib.get("rel", name), None, False, None))
        value = (element.text or "").strip()
        if value and (name in {"loc", "link", "url", "uri", "guid"} or _looks_like_url(value)):
            links.append((value, "xml_loc", name, None, False, None))
    return links


def _extract_xml_geo(root: ElementTree.Element, geo: dict[str, Any]) -> None:
    for element in root.iter():
        name = _local_name(element.tag).casefold()
        value = (element.text or "").strip()
        if name == "coordinates" and value:
            for coordinate in re.split(r"\s+", value):
                parts = coordinate.split(",")
                if len(parts) >= 2:
                    _add_point(geo, _as_float(parts[1]), _as_float(parts[0]), "kml")
        if name in {"placename", "locality", "city", "location"} and value:
            _append_unique(geo["places"], value)
        if name in {"jurisdiction", "region", "state", "province", "country"} and value:
            _append_unique(geo["jurisdictions"], value)
        if name in {"route", "corridor", "line"} and value:
            _append_unique(geo["routes"], value)
        children = {_local_name(child.tag).casefold(): (child.text or "").strip() for child in element}
        lat = _first_numeric(children, "latitude", "lat")
        lon = _first_numeric(children, "longitude", "lon", "lng", "long")
        _add_point(geo, lat, lon, "xml")


def _extract_xml_temporal(root: ElementTree.Element, temporal: dict[str, Any]) -> None:
    for element in root.iter():
        name = _local_name(element.tag).casefold()
        value = (element.text or "").strip()
        if value:
            _add_temporal_value(temporal, name, value)


def _extract_html_geo(parser: _DiscoveryHTMLParser, raw_text: str, geo: dict[str, Any]) -> None:
    position = _first_meta(parser.meta, "geo.position", "icbm")
    if position:
        values = re.findall(r"[-+]?\d+(?:\.\d+)?", position)
        if len(values) >= 2:
            _add_point(geo, _as_float(values[0]), _as_float(values[1]), "html_meta")
    lat = _as_float(_first_meta(parser.meta, "place:location:latitude", "geo.latitude"))
    lon = _as_float(_first_meta(parser.meta, "place:location:longitude", "geo.longitude"))
    _add_point(geo, lat, lon, "html_meta")
    for key in ("geo.placename", "og:locality", "article:location"):
        for value in parser.meta.get(key, []):
            _append_unique(geo["places"], value)
    for key in ("geo.region", "og:region", "article:section"):
        for value in parser.meta.get(key, []):
            _append_unique(geo["jurisdictions"], value)
    _extract_text_geo(" ".join(parser.scripts) + " " + raw_text[:MAX_EXTRACTED_TEXT], geo)


def _extract_text_geo(text: str, geo: dict[str, Any]) -> None:
    sample = text[:MAX_EXTRACTED_TEXT]
    patterns = (
        re.compile(
            r"(?i)\b(?:lat|latitude)\s*[:=]\s*([-+]?\d{1,2}(?:\.\d+)?)\D{0,40}?"
            r"(?:lon|lng|long|longitude)\s*[:=]\s*([-+]?\d{1,3}(?:\.\d+)?)"
        ),
        re.compile(
            r"(?i)\b(?:lon|lng|long|longitude)\s*[:=]\s*([-+]?\d{1,3}(?:\.\d+)?)\D{0,40}?"
            r"(?:lat|latitude)\s*[:=]\s*([-+]?\d{1,2}(?:\.\d+)?)"
        ),
    )
    for index, pattern in enumerate(patterns):
        for match in pattern.finditer(sample):
            if index == 0:
                _add_point(geo, _as_float(match.group(1)), _as_float(match.group(2)), "text")
            else:
                _add_point(geo, _as_float(match.group(2)), _as_float(match.group(1)), "text")
    for route in _ROUTE_RE.findall(sample):
        _append_unique(geo["routes"], _clean_text(route))


def _extract_html_temporal(
    parser: _DiscoveryHTMLParser, text: str, temporal: dict[str, Any]
) -> None:
    for key, values in parser.meta.items():
        if any(token in key for token in ("date", "time", "publish", "modified", "updated")):
            for value in values:
                _add_temporal_value(temporal, key, value)
    _extract_text_temporal(text, temporal)


def _extract_text_temporal(text: str, temporal: dict[str, Any]) -> None:
    for value in _ISO_DATE_RE.findall(text[:MAX_EXTRACTED_TEXT])[:20]:
        _add_temporal_value(temporal, "observed_date", value)


def _extract_csv_signals(
    rows: list[list[str]],
    headers: list[str],
    links: list[tuple[str, str, str, str | None, bool, str | None]],
    geo: dict[str, Any],
    temporal: dict[str, Any],
) -> None:
    normalised = [_normalise_field_name(value) for value in headers]
    lat_index = _first_index(normalised, "latitude", "lat")
    lon_index = _first_index(normalised, "longitude", "lon", "lng", "long")
    url_indexes = [index for index, name in enumerate(normalised) if name in {"url", "uri", "link", "endpoint", "downloadurl"}]
    for row in rows[1:201]:
        if lat_index is not None and lon_index is not None:
            if lat_index < len(row) and lon_index < len(row):
                _add_point(geo, _as_float(row[lat_index]), _as_float(row[lon_index]), "csv")
        for index in url_indexes:
            if index < len(row) and row[index]:
                links.append((row[index], "csv_url", headers[index], None, False, None))
        for index, name in enumerate(normalised):
            if index >= len(row):
                continue
            value = row[index]
            _add_named_geo_hint(geo, name, value)
            _add_temporal_value(temporal, name, value)


def _empty_geo_hints() -> dict[str, Any]:
    return {
        "points": [],
        "bbox": None,
        "places": [],
        "jurisdictions": [],
        "routes": [],
        "footprint_kind": "unknown",
    }


def _add_point(
    geo: dict[str, Any], lat: float | None, lon: float | None, source: str
) -> None:
    if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return
    point = {"lat": round(lat, 7), "lon": round(lon, 7), "source": source}
    key = (point["lat"], point["lon"])
    existing = {(item["lat"], item["lon"]) for item in geo["points"]}
    if key not in existing and len(geo["points"]) < MAX_GEO_POINTS:
        geo["points"].append(point)


def _add_named_geo_hint(geo: dict[str, Any], key: str, value: str) -> None:
    cleaned = _clean_text(value)
    if not cleaned or len(cleaned) > 300:
        return
    key = _normalise_field_name(key)
    if key in {
        "addresslocality",
        "areaserved",
        "city",
        "locality",
        "location",
        "place",
        "placename",
        "port",
        "spatialcoverage",
        "town",
    }:
        _append_unique(geo["places"], cleaned)
    elif key in {
        "addresscountry",
        "addressregion",
        "country",
        "county",
        "district",
        "jurisdiction",
        "province",
        "region",
        "state",
    }:
        _append_unique(geo["jurisdictions"], cleaned)
    elif key in {"route", "routeid", "routename", "corridor", "line", "highway", "road"}:
        _append_unique(geo["routes"], cleaned)


def _finalise_geo_hints(geo: dict[str, Any], text: str, url: str) -> None:
    points = geo["points"]
    if points:
        lons = [point["lon"] for point in points]
        lats = [point["lat"] for point in points]
        geo["bbox"] = [min(lons), min(lats), max(lons), max(lats)]
    haystack = (url + " " + text[:20_000]).casefold()
    if geo["routes"]:
        footprint = "route"
    elif len(points) == 1:
        footprint = "point"
    elif len(points) > 1:
        footprint = "local_area"
    elif geo["jurisdictions"]:
        footprint = "jurisdiction"
    elif any(term in haystack for term in ("nationwide", "national coverage", "across the country")):
        footprint = "nationwide"
    elif any(term in haystack for term in ("worldwide", "global coverage", "international")):
        footprint = "global"
    elif geo["places"]:
        footprint = "local_area"
    else:
        footprint = "unknown"
    geo["footprint_kind"] = footprint
    geo["coordinate_count"] = len(points)


def _base_temporal_hints(headers: Mapping[str, str]) -> dict[str, Any]:
    hints: dict[str, Any] = {
        "published_at": None,
        "modified_at": None,
        "updated_at": None,
        "observed_dates": [],
        "cadence_hint": "unknown",
        "live": False,
        "historical": False,
    }
    if headers.get("last-modified"):
        hints["modified_at"] = _normalise_datetime(headers["last-modified"])
    if headers.get("date"):
        hints["response_date"] = _normalise_datetime(headers["date"])
    return hints


def _add_temporal_value(temporal: dict[str, Any], key: str, value: str) -> None:
    key = key.casefold()
    if not any(token in key for token in ("date", "time", "publish", "modified", "updated", "created", "issued", "valid")):
        return
    normalised = _normalise_datetime(value)
    if not normalised:
        match = _ISO_DATE_RE.search(value)
        normalised = _normalise_datetime(match.group(1)) if match else None
    if not normalised:
        return
    if "publish" in key or "issued" in key or "created" in key:
        temporal["published_at"] = temporal.get("published_at") or normalised
    elif "modif" in key:
        temporal["modified_at"] = temporal.get("modified_at") or normalised
    elif "updat" in key:
        temporal["updated_at"] = temporal.get("updated_at") or normalised
    else:
        _append_unique(temporal["observed_dates"], normalised)


def _finalise_temporal_hints(temporal: dict[str, Any], text: str, detected: str) -> None:
    haystack = text[:50_000].casefold()
    if detected == "stream" or any(term in haystack for term in ("live feed", "real-time", "realtime", "updated every minute")):
        temporal["cadence_hint"] = "live"
        temporal["live"] = True
    elif any(term in haystack for term in ("hourly", "every hour")):
        temporal["cadence_hint"] = "hourly"
    elif any(term in haystack for term in ("daily", "every day")):
        temporal["cadence_hint"] = "daily"
    elif any(term in haystack for term in ("weekly", "every week")):
        temporal["cadence_hint"] = "weekly"
    elif any(term in haystack for term in ("monthly", "every month")):
        temporal["cadence_hint"] = "monthly"
    elif any(term in haystack for term in ("archive", "historical", "annual dataset")):
        temporal["cadence_hint"] = "archive"
        temporal["historical"] = True
    elif any(temporal.get(key) for key in ("updated_at", "modified_at")):
        temporal["cadence_hint"] = "changing"


def _analysis_view(analysis: DocumentAnalysis | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(analysis, DocumentAnalysis):
        return analysis.as_dict()
    if not isinstance(analysis, Mapping):
        raise TypeError("analysis must be DocumentAnalysis or mapping")
    view = dict(analysis)
    view.setdefault("document_type", view.get("candidate_type", "unknown"))
    for key in (
        "geo_hints",
        "temporal_hints",
        "format_hints",
        "structural_hints",
        "trust_hints",
        "operational_hints",
    ):
        value = view.get(key)
        view[key] = dict(value) if isinstance(value, Mapping) else {}
    view.setdefault("title", None)
    view.setdefault("text", "")
    view.setdefault("canonical_url", view.get("url", ""))
    view.setdefault("media_type", "")
    return view


def _score_relevance(view: Mapping[str, Any], query_terms: Sequence[str] | str | None) -> float:
    document_type = str(view["document_type"])
    base = {
        "api_docs": 75,
        "archive_dataset": 65,
        "article_page": 65,
        "camera_image": 75,
        "camera_page": 65,
        "camera_stream": 78,
        "geojson_endpoint": 80,
        "government_notice": 78,
        "open_data_portal": 75,
        "repeating_status_page": 78,
        "rss_feed": 75,
        "atom_feed": 75,
        "social_reference": 25,
    }.get(document_type, 55)
    if not query_terms:
        return float(base)
    if isinstance(query_terms, str):
        terms = [term.casefold() for term in re.findall(r"[\w'-]+", query_terms) if len(term) > 1]
    else:
        terms = [str(term).strip().casefold() for term in query_terms if str(term).strip()]
    if not terms:
        return float(base)
    title = str(view.get("title") or "").casefold()
    url = str(view.get("canonical_url") or "").casefold()
    text = str(view.get("text") or "")[:100_000].casefold()
    matched = sum(1 for term in terms if term in title or term in url or term in text)
    ratio = matched / len(terms)
    placement_bonus = sum(1 for term in terms if term in title or term in url) / len(terms) * 15
    return _clamp(15 + ratio * 70 + placement_bonus)


def _score_temporal(view: Mapping[str, Any]) -> float:
    hints = view["temporal_hints"]
    if hints.get("live") or hints.get("cadence_hint") == "live":
        return 95.0
    cadence = hints.get("cadence_hint")
    if cadence in {"hourly", "daily", "changing"}:
        return 82.0
    if cadence in {"weekly", "monthly"}:
        return 68.0
    if hints.get("updated_at") or hints.get("modified_at"):
        return 70.0
    if hints.get("published_at") or hints.get("observed_dates"):
        return 58.0
    if hints.get("historical") or cadence == "archive":
        return 50.0
    return 35.0


def _score_structural(view: Mapping[str, Any]) -> float:
    detected = str(view["format_hints"].get("detected_format") or "")
    scores = {
        "openapi": 98,
        "geojson": 95,
        "json": 88,
        "jsonl": 88,
        "rss": 90,
        "atom": 90,
        "csv": 82,
        "kml": 86,
        "sitemap": 80,
        "xml": 78,
        "stream": 70,
        "html": 48,
        "pdf": 38,
        "image": 40,
        "text": 30,
    }
    score = float(scores.get(detected, 35))
    structural = view["structural_hints"]
    if structural.get("has_schema_org"):
        score += 10
    if structural.get("has_table"):
        score += 5
    return _clamp(score)


def _score_freshness(view: Mapping[str, Any], health: Mapping[str, Any]) -> float:
    temporal = view["temporal_hints"]
    if health.get("stale") is True:
        return 15.0
    if health.get("changed") is True:
        return 90.0
    if temporal.get("live"):
        return 90.0
    if temporal.get("updated_at") or temporal.get("modified_at"):
        return 78.0
    if temporal.get("published_at"):
        return 66.0
    if temporal.get("historical"):
        return 42.0
    return 48.0


def _score_stability(view: Mapping[str, Any], health: Mapping[str, Any]) -> float:
    operational = view["operational_hints"]
    score = 58.0
    if operational.get("secure_transport"):
        score += 10
    if operational.get("etag") or view["temporal_hints"].get("modified_at"):
        score += 12
    if health.get("reachable") is True:
        score += 10
    failures = _safe_int(health.get("consecutive_failures")) or 0
    score -= min(50, failures * 12)
    status = _safe_int(health.get("status_code"))
    if status and status >= 400:
        score -= 30
    if operational.get("private_network"):
        score = 0
    return _clamp(score)


def _score_trust(view: Mapping[str, Any]) -> float:
    trust = view["trust_hints"]
    operational = view["operational_hints"]
    if trust.get("official_domain"):
        return 92.0
    if trust.get("education_domain"):
        return 80.0
    if trust.get("social_reference"):
        return 35.0
    score = 48.0
    if operational.get("secure_transport"):
        score += 14
    if trust.get("publisher"):
        score += 8
    if trust.get("integrity_headers"):
        score += 6
    return _clamp(score)


def _score_operational_cost(view: Mapping[str, Any], health: Mapping[str, Any]) -> float:
    operational = view["operational_hints"]
    detected = str(view["format_hints"].get("detected_format") or "")
    score = {
        "json": 18,
        "jsonl": 20,
        "geojson": 20,
        "openapi": 15,
        "rss": 18,
        "atom": 18,
        "csv": 20,
        "xml": 22,
        "kml": 24,
        "sitemap": 22,
        "html": 42,
        "pdf": 50,
        "image": 28,
        "stream": 58,
        "text": 32,
    }.get(detected, 45)
    if operational.get("requires_javascript"):
        score += 25
    if operational.get("likely_auth_required"):
        score += 25
    if operational.get("truncated"):
        score += 15
    if operational.get("private_network"):
        return 100.0
    failures = _safe_int(health.get("consecutive_failures")) or 0
    score += min(30, failures * 8)
    return _clamp(score)


def _score_reasons(
    components: Mapping[str, float],
    penalties: Mapping[str, float],
    bucket: str,
    view: Mapping[str, Any],
    target_geo: Mapping[str, Any] | None,
) -> list[str]:
    reasons = [
        f"relevance={components['relevance']:.1f}: query/type match",
        f"geo={components['geo']:.1f}: "
        + ("target geography overlap" if target_geo else "intrinsic geographic specificity"),
        f"temporal={components['temporal']:.1f}: cadence and timestamp evidence",
        f"structural={components['structural']:.1f}: machine-readable/schema evidence",
        f"freshness={components['freshness']:.1f}: update and health evidence",
        f"stability={components['stability']:.1f}: transport/cache/health evidence",
        f"trust={components['trust']:.1f}: domain and integrity evidence",
        f"novelty={components['novelty']:.1f}: inventory novelty",
        f"operational_cost=-{penalties['operational_cost']:.1f}: fetch/parse fragility",
        f"redundancy=-{penalties['redundancy_penalty']:.1f}: overlap with inventory",
    ]
    if view["operational_hints"].get("private_network"):
        reasons.append("quarantine: target resolves syntactically to a private/local address")
    if view["document_type"] == "social_reference":
        reasons.append("ignore: social/profile pages are reference-only")
    reasons.append(f"outcome={bucket}: deterministic threshold and safety policy")
    return reasons


def _json_schema_shape(value: Any, depth: int = 0) -> Any:
    if depth >= 8:
        return "..."
    if isinstance(value, Mapping):
        return {
            str(key): _json_schema_shape(item, depth + 1)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))[:200]
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        shapes = []
        seen = set()
        for item in list(value)[:50]:
            shape = _json_schema_shape(item, depth + 1)
            marker = json.dumps(shape, sort_keys=True, separators=(",", ":"))
            if marker not in seen:
                seen.add(marker)
                shapes.append(shape)
        return {"array": sorted(shapes, key=lambda item: json.dumps(item, sort_keys=True))[:20]}
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    return "string"


def _xml_schema_shape(root: ElementTree.Element, depth: int = 0) -> Any:
    if depth >= 6:
        return _local_name(root.tag)
    children: dict[str, Any] = {}
    for child in list(root)[:500]:
        name = _local_name(child.tag)
        if name not in children:
            children[name] = _xml_schema_shape(child, depth + 1)
    return {
        "tag": _local_name(root.tag),
        "attributes": sorted(_local_name(key) for key in root.attrib),
        "children": {key: children[key] for key in sorted(children)},
    }


def _json_structure_hints(data: Any) -> dict[str, Any]:
    if isinstance(data, Mapping):
        return {"top_level": "object", "field_count": len(data), "record_count_sampled": 1}
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        fields: set[str] = set()
        for item in list(data)[:100]:
            if isinstance(item, Mapping):
                fields.update(str(key) for key in item)
        return {
            "top_level": "array",
            "field_count": len(fields),
            "record_count_sampled": min(len(data), 100),
        }
    return {"top_level": type(data).__name__, "field_count": 0, "record_count_sampled": 1}


def _parse_json_lines(text: str) -> list[Any]:
    records: list[Any] = []
    nonempty = [line.strip() for line in text.splitlines() if line.strip()][:1000]
    for line in nonempty:
        try:
            records.append(json.loads(line))
        except (ValueError, TypeError):
            return []
    return records


def _parse_csv(text: str) -> tuple[list[list[str]], str]:
    sample = text[:64_000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ","
    reader = csv.reader(StringIO(text), delimiter=delimiter)
    rows: list[list[str]] = []
    try:
        for row in reader:
            rows.append([cell.strip() for cell in row[:500]])
            if len(rows) >= 201:
                break
    except csv.Error:
        pass
    return rows, delimiter


def _looks_like_csv(text: str) -> bool:
    lines = [line for line in text.splitlines()[:10] if line.strip()]
    if len(lines) < 2:
        return False
    for delimiter in (",", "\t", ";", "|"):
        counts = [line.count(delimiter) for line in lines]
        if counts[0] >= 1 and len(set(counts)) == 1:
            return True
    return False


def _text_url_links(
    text: str, source: str
) -> list[tuple[str, str, str, str | None, bool, str | None]]:
    return [
        (match.rstrip(".,;:!?)]"), source, "reference", None, False, None)
        for match in _URL_RE.findall(text[:MAX_EXTRACTED_TEXT])[:MAX_LINKS]
    ]


def _json_title(data: Any) -> str | None:
    if isinstance(data, Mapping):
        for key in ("title", "name", "description", "dataset_name"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return _clean_text(value)[:500]
        info = data.get("info")
        if isinstance(info, Mapping) and isinstance(info.get("title"), str):
            return _clean_text(info["title"])[:500]
    return None


def _json_search_text(data: Any) -> str:
    values: list[str] = []
    stack = [data]
    visited = 0
    while stack and visited < 5000 and sum(len(item) for item in values) < MAX_EXTRACTED_TEXT:
        value = stack.pop()
        visited += 1
        if isinstance(value, Mapping):
            stack.extend(list(value.values())[:500])
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            stack.extend(list(value)[:500])
        elif isinstance(value, str) and len(value) <= 5000:
            values.append(value)
    return _clean_text(" ".join(values))


def _xml_title(root: ElementTree.Element) -> str | None:
    for element in root.iter():
        if _local_name(element.tag).casefold() in {"title", "name"} and (element.text or "").strip():
            return _clean_text(element.text or "")[:500]
    return None


def _local_name(tag: str) -> str:
    """Strip an ElementTree namespace or XML prefix from a name."""

    return str(tag).rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def _xml_schema_shape_text(root: ElementTree.Element) -> str:
    return json.dumps(_xml_schema_shape(root), sort_keys=True, separators=(",", ":"))


def _json_ld_types(scripts: Iterable[str]) -> set[str]:
    result: set[str] = set()
    for script in scripts:
        try:
            data = json.loads(script)
        except (TypeError, ValueError):
            continue
        stack = [data]
        while stack:
            item = stack.pop()
            if isinstance(item, Mapping):
                value = item.get("@type")
                if isinstance(value, str):
                    result.add(value)
                elif isinstance(value, list):
                    result.update(str(entry) for entry in value)
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
    return result


def _publisher_hint(url: str, title: str | None, text: str) -> str | None:
    host = (urlsplit(url).hostname or "").removeprefix("www.")
    if _is_official_domain(host):
        return host
    match = re.search(r"(?i)\b(?:published by|publisher|agency)\s*[:\-]\s*([^|\n]{2,100})", text)
    if match:
        return _clean_text(match.group(1))
    if title and host:
        return host
    return None


def _format_hint_from_mime_or_url(media_type: str | None, url: str) -> str | None:
    media = (media_type or "").casefold()
    extension = _url_extension(url)
    if "rss" in media or extension == "rss":
        return "rss"
    if "atom" in media or extension == "atom":
        return "atom"
    if "geo+json" in media or extension == "geojson":
        return "geojson"
    if "json" in media or extension in {"json", "jsonl", "ndjson"}:
        return "jsonl" if extension in {"jsonl", "ndjson"} else "json"
    if "xml" in media or extension in {"xml", "kml"}:
        return "kml" if extension == "kml" else "xml"
    if "csv" in media or extension == "csv":
        return "csv"
    if "pdf" in media or extension == "pdf":
        return "pdf"
    if media.startswith("image/") or extension in {"jpg", "jpeg", "png", "gif", "webp"}:
        return "image"
    if extension in {"m3u8", "mpd", "mjpeg", "mjpg"}:
        return "stream"
    if extension in {"htm", "html"}:
        return "html"
    return None


def _url_extension(url: str) -> str:
    path = urlsplit(url).path
    final = path.rsplit("/", 1)[-1]
    if "." not in final:
        return ""
    return final.rsplit(".", 1)[-1].casefold()[:12]


def _media_type_for_format(detected: str) -> str:
    return {
        "atom": "application/atom+xml",
        "csv": "text/csv",
        "geojson": "application/geo+json",
        "html": "text/html",
        "image": "image/*",
        "json": "application/json",
        "jsonl": "application/x-ndjson",
        "kml": "application/vnd.google-earth.kml+xml",
        "openapi": "application/json",
        "pdf": "application/pdf",
        "rss": "application/rss+xml",
        "sitemap": "application/xml",
        "stream": "application/octet-stream",
        "text": "text/plain",
        "xml": "application/xml",
    }[detected]


def _looks_like_url(value: str) -> bool:
    value = value.strip()
    return bool(re.match(r"(?i)^(?:https?|wss?)://", value))


def _first_meta(meta: Mapping[str, list[str]], *keys: str) -> str | None:
    for key in keys:
        values = meta.get(key.casefold())
        if values:
            return values[0]
    return None


def _mapping_value(data: Any, *keys: str) -> Any:
    if not isinstance(data, Mapping):
        return None
    lowered = {str(key).casefold(): value for key, value in data.items()}
    for key in keys:
        if key.casefold() in lowered:
            return lowered[key.casefold()]
    return None


def _mapping_length(data: Any, key: str) -> int:
    value = _mapping_value(data, key)
    return len(value) if isinstance(value, (Mapping, Sequence)) and not isinstance(value, str) else 0


def _json_auth_required(data: Any) -> bool:
    if not isinstance(data, Mapping):
        return False
    components = data.get("components")
    security = data.get("security")
    return bool(security or (isinstance(components, Mapping) and components.get("securitySchemes")))


def _requires_javascript(parser: _DiscoveryHTMLParser, text: str) -> bool:
    script_count = parser.tags.get("script", 0)
    text_length = len(text.strip())
    return bool(script_count >= 8 and text_length < 500) or any(
        marker in " ".join(parser.text_parts).casefold()
        for marker in ("enable javascript", "javascript is required")
    )


def _likely_auth(text: str, url: str) -> bool:
    haystack = (url + " " + text[:20_000]).casefold()
    return any(term in haystack for term in ("sign in to continue", "login required", "authentication required", "oauth authorize"))


def _has_camera_signal(*values: str) -> bool:
    haystack = " ".join(values).casefold()
    return any(term in haystack for term in _CAMERA_TERMS)


def _archive_signal(*values: str) -> bool:
    haystack = " ".join(values).casefold()
    return any(term in haystack for term in ("/archive", "historical data", "data archive", "back catalog", "backfile"))


def _notice_signal(*values: str) -> bool:
    haystack = " ".join(values).casefold()
    return any(term in haystack for term in ("government notice", "official notice", "public notice", "bulletin", "advisory"))


def _is_social_domain(host: str) -> bool:
    host = host.casefold().removeprefix("www.")
    return any(host == domain or host.endswith("." + domain) for domain in _SOCIAL_DOMAINS)


def _is_official_domain(host: str) -> bool:
    host = host.casefold().rstrip(".")
    labels = host.split(".")
    if host.endswith((".gov", ".mil", ".gc.ca", ".gouv.fr")):
        return True
    # Many country-code namespaces reserve gov/go/gob below the ccTLD.  Limit this
    # inference to the penultimate label so a hostname such as gov.example.com does
    # not receive an official-source trust bonus merely for containing the token.
    return len(labels) >= 3 and len(labels[-1]) == 2 and labels[-2] in {"go", "gob", "gov"}


def _is_private_target(url: str) -> bool:
    host = (urlsplit(url).hostname or "").casefold().rstrip(".")
    if host in {"localhost", "localhost.localdomain"} or host.endswith((".local", ".internal", ".localhost")):
        return True
    try:
        address = ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        return False
    return not address.is_global


def _normalise_datetime(value: str) -> str | None:
    raw = str(value).strip()
    if not raw:
        return None
    parsed: datetime | None = None
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        pass
    if parsed is None:
        iso_value = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(iso_value)
        except ValueError:
            try:
                parsed = datetime.strptime(raw[:10], "%Y-%m-%d")
            except ValueError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _coerce_points(value: Any) -> list[tuple[float, float]]:
    result: list[tuple[float, float]] = []
    if isinstance(value, Mapping):
        value = [value]
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return result
    for item in value:
        lon: float | None = None
        lat: float | None = None
        if isinstance(item, Mapping):
            lat = _as_float(item.get("lat", item.get("latitude")))
            lon = _as_float(item.get("lon", item.get("lng", item.get("longitude"))))
            coordinates = item.get("coordinates")
            if (lat is None or lon is None) and isinstance(coordinates, Sequence) and len(coordinates) >= 2:
                lon, lat = _as_float(coordinates[0]), _as_float(coordinates[1])
        elif isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)) and len(item) >= 2:
            lon, lat = _as_float(item[0]), _as_float(item[1])
        if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
            result.append((lon, lat))
    return result


def _coerce_bbox(value: Any) -> tuple[float, float, float, float] | None:
    if isinstance(value, Mapping):
        values = (
            value.get("min_lon", value.get("west")),
            value.get("min_lat", value.get("south")),
            value.get("max_lon", value.get("east")),
            value.get("max_lat", value.get("north")),
        )
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and len(value) >= 4:
        values = value[:4]
    else:
        return None
    numbers = tuple(_as_float(item) for item in values)
    if any(item is None for item in numbers):
        return None
    min_lon, min_lat, max_lon, max_lat = numbers
    if min_lon > max_lon or min_lat > max_lat:
        return None
    return min_lon, min_lat, max_lon, max_lat  # type: ignore[return-value]


def _coerce_polygon(value: Any) -> list[tuple[float, float]]:
    if isinstance(value, Mapping):
        if str(value.get("type", "")).casefold() == "polygon":
            value = value.get("coordinates")
        else:
            return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    candidate: Any = value
    while (
        isinstance(candidate, Sequence)
        and candidate
        and not isinstance(candidate, (str, bytes, bytearray))
        and isinstance(candidate[0], Sequence)
        and not isinstance(candidate[0], (str, bytes, bytearray))
        and candidate[0]
        and isinstance(candidate[0][0], Sequence)
    ):
        candidate = candidate[0]
    return _coerce_points(candidate)


def _point_in_bbox(point: tuple[float, float], bbox: tuple[float, float, float, float]) -> bool:
    lon, lat = point
    return bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]


def _bbox_intersects(
    first: tuple[float, float, float, float], second: tuple[float, float, float, float]
) -> bool:
    return not (
        first[2] < second[0]
        or first[0] > second[2]
        or first[3] < second[1]
        or first[1] > second[3]
    )


def _point_in_polygon(point: tuple[float, float], polygon: Sequence[tuple[float, float]]) -> bool:
    if len(polygon) < 3:
        return False
    x, y = point
    inside = False
    previous = polygon[-1]
    for current in polygon:
        x1, y1 = previous
        x2, y2 = current
        if (y1 > y) != (y2 > y):
            crossing = (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-12) + x1
            if x <= crossing:
                inside = not inside
        previous = current
    return inside


def _normalised_label_set(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, Sequence):
        values = value
    else:
        values = [value]
    return {_normalise_label(str(item)) for item in values if _normalise_label(str(item))}


def _normalise_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _label_overlap_scores(
    candidates: set[str], targets: set[str], *, exact: float, partial: float
) -> list[float]:
    result: list[float] = []
    for candidate in candidates:
        for target in targets:
            if candidate == target:
                result.append(exact)
            elif len(candidate) >= 4 and len(target) >= 4 and (
                candidate in target or target in candidate
            ):
                result.append(partial)
    return result


def _title_from_url(url: str) -> str | None:
    final = urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]
    if not final:
        return None
    stem = final.rsplit(".", 1)[0]
    title = _clean_text(re.sub(r"[-_]+", " ", stem))
    return title[:500] or None


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(str(value))).strip()


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in {float("inf"), float("-inf")} else None


def _first_numeric(mapping: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in mapping:
            value = _as_float(mapping[key])
            if value is not None:
                return value
    return None


def _append_unique(items: list[Any], value: Any) -> None:
    if value not in items and len(items) < 200:
        items.append(value)


def _normalise_field_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).casefold())


def _first_index(values: Sequence[str], *targets: str) -> int | None:
    for target in targets:
        try:
            return values.index(target)
        except ValueError:
            continue
    return None


def _text_line_shapes(text: str) -> list[str]:
    shapes: set[str] = set()
    for line in text.splitlines()[:100]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        cleaned = _URL_RE.sub("{url}", cleaned)
        cleaned = _ISO_DATE_RE.sub("{date}", cleaned)
        cleaned = re.sub(r"\b\d+(?:\.\d+)?\b", "{number}", cleaned)
        shapes.add(cleaned[:200])
    return sorted(shapes)[:50]


def _clamp(value: float | int) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("score components must be numeric") from exc
    return max(0.0, min(100.0, number))


__all__ = [
    "DocumentAnalysis",
    "LinkSignal",
    "analyze_document",
    "canonical_url_hash",
    "canonicalize_url",
    "compute_candidate_score",
    "normalize_path_pattern",
    "recommend_source_kind",
    "score_geo_relevance",
]
