from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect as websocket_connect

from src.models import SourceDefinitionORM
from src.services.ingestion_service import (
    IngestionEnvelope,
    infer_source_format,
    normalize_payload,
    parse_json_envelopes,
    parse_sqlite_envelopes,
    parse_text_envelopes,
)


@dataclass(frozen=True)
class AdapterContext:
    cursor_text: str | None = None
    last_event_id: str | None = None
    last_offset: int | None = None
    checkpoint_json: dict[str, Any] = field(default_factory=dict)
    actor: str = "source_runner"


@dataclass(frozen=True)
class DeadLetterCandidate:
    stage: str
    failure_reason: str
    raw_payload_text: str | None = None
    payload_json: dict[str, Any] = field(default_factory=dict)
    record_key: str | None = None
    record_hash: str | None = None
    cursor_text: str | None = None
    checkpoint_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AdapterExecutionResult:
    adapter_kind: str
    fetch_mode: str
    envelopes: list[IngestionEnvelope]
    dead_letters: list[DeadLetterCandidate]
    records_seen: int
    cursor_text: str | None = None
    last_event_id: str | None = None
    last_offset: int | None = None
    checkpoint_json: dict[str, Any] = field(default_factory=dict)
    metadata_json: dict[str, Any] = field(default_factory=dict)
    materialized_payload: bytes | None = None
    materialized_suffix: str = ".json"
    materialized_content_type: str | None = None


class SourceAdapter(Protocol):
    source_kind: str
    fetch_mode: str

    def run(self, source: SourceDefinitionORM, context: AdapterContext) -> AdapterExecutionResult:
        ...

    def replay_dead_letter(
        self,
        source: SourceDefinitionORM,
        candidate: DeadLetterCandidate,
    ) -> AdapterExecutionResult:
        ...


class FileSourceAdapter:
    fetch_mode = "pull"

    def __init__(self, source_kind: str) -> None:
        self.source_kind = source_kind

    def run(self, source: SourceDefinitionORM, context: AdapterContext) -> AdapterExecutionResult:
        path = Path(source.target_uri).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"Input path does not exist: {path}")

        if self.source_kind == "sqlite_file":
            envelopes = parse_sqlite_envelopes(path, source_type=source.source_kind)
            source_format = "sqlite"
        else:
            source_format = infer_source_format(path)
            if source_format == "json":
                envelopes = parse_json_envelopes(path, source_type=source.source_kind)
            elif source_format == "sqlite":
                envelopes = parse_sqlite_envelopes(path, source_type=source.source_kind)
            else:
                envelopes = parse_text_envelopes(path, source_type=source.source_kind)
        payload = path.read_bytes()
        return AdapterExecutionResult(
            adapter_kind=self.source_kind,
            fetch_mode=self.fetch_mode,
            envelopes=envelopes,
            dead_letters=[],
            records_seen=len(envelopes),
            checkpoint_json={
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "path": str(path),
                "source_format": source_format,
            },
            metadata_json={
                "materialization_kind": "local_file",
                "resolved_path": str(path),
                "byte_count": len(payload),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
            },
            materialized_payload=payload,
            materialized_suffix=path.suffix or ".dat",
            materialized_content_type=content_type_for_suffix(path.suffix.lower()),
        )

    def replay_dead_letter(
        self,
        source: SourceDefinitionORM,
        candidate: DeadLetterCandidate,
    ) -> AdapterExecutionResult:
        if candidate.payload_json:
            envelope = normalize_payload(candidate.payload_json, "json", source_type=source.source_kind)
            return completed_replay_result(self.source_kind, self.fetch_mode, [envelope])
        if candidate.raw_payload_text:
            envelope = normalize_payload({"text": candidate.raw_payload_text}, "txt", source_type=source.source_kind)
            return completed_replay_result(self.source_kind, self.fetch_mode, [envelope])
        raise ValueError("Dead-letter record is missing replayable payload data.")


class HttpSnapshotAdapter:
    def __init__(self, source_kind: str, *, record_format: str, accept_header: str, suffix: str) -> None:
        self.source_kind = source_kind
        self.fetch_mode = "pull"
        self.record_format = record_format
        self.accept_header = accept_header
        self.suffix = suffix

    def run(self, source: SourceDefinitionORM, context: AdapterContext) -> AdapterExecutionResult:
        payload, metadata = fetch_http_payload(source, accept_header=self.accept_header, context=context)
        text_payload = payload.decode("utf-8")
        dead_letters: list[DeadLetterCandidate] = []
        materialized_payload = payload
        materialized_suffix = self.suffix
        materialized_content_type = metadata.get("content_type")
        if self.source_kind == "http_json":
            envelopes = normalize_http_json_payload(text_payload, source)
        elif self.source_kind == "http_jsonl":
            envelopes, dead_letters = normalize_http_jsonl_payload(text_payload, source, context)
        elif self.source_kind == "http_text":
            envelopes = [
                normalize_payload({"text": line.strip(), "url": line.strip()}, "txt", source_type=source.source_kind)
                for line in text_payload.splitlines()
                if line.strip()
            ]
        else:
            envelopes, dead_letters = normalize_http_xml_payload(text_payload, source, context)
            materialized_payload = json.dumps(
                [envelope.content_json for envelope in envelopes],
                default=str,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            materialized_suffix = ".json"
            materialized_content_type = "application/json"
        records_seen = len(envelopes) + len(dead_letters)
        return AdapterExecutionResult(
            adapter_kind=self.source_kind,
            fetch_mode=self.fetch_mode,
            envelopes=envelopes,
            dead_letters=dead_letters,
            records_seen=records_seen,
            checkpoint_json={
                **context.checkpoint_json,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "source_kind": self.source_kind,
            },
            metadata_json={
                **metadata,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "cached_record_count": len(envelopes),
                "materialized_content_type": materialized_content_type,
            },
            materialized_payload=materialized_payload,
            materialized_suffix=materialized_suffix,
            materialized_content_type=materialized_content_type,
        )

    def replay_dead_letter(
        self,
        source: SourceDefinitionORM,
        candidate: DeadLetterCandidate,
    ) -> AdapterExecutionResult:
        return replay_generic_payload_dead_letter(source, candidate, source.source_kind, self.fetch_mode)


class RssSourceAdapter:
    source_kind = "rss"
    fetch_mode = "pull"

    def run(self, source: SourceDefinitionORM, context: AdapterContext) -> AdapterExecutionResult:
        payload, metadata = fetch_http_payload(
            source,
            accept_header="application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
            context=context,
        )
        text_payload = payload.decode("utf-8")
        envelopes, dead_letters = normalize_rss_payload(text_payload, source, context)
        return AdapterExecutionResult(
            adapter_kind=self.source_kind,
            fetch_mode=self.fetch_mode,
            envelopes=envelopes,
            dead_letters=dead_letters,
            records_seen=len(envelopes) + len(dead_letters),
            checkpoint_json={
                **context.checkpoint_json,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "entry_count": len(envelopes),
            },
            metadata_json={
                **metadata,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "entry_count": len(envelopes),
                "cached_record_count": len(envelopes),
                "materialized_content_type": metadata.get("content_type"),
            },
            materialized_payload=payload,
            materialized_suffix=".xml",
            materialized_content_type=metadata.get("content_type"),
        )

    def replay_dead_letter(
        self,
        source: SourceDefinitionORM,
        candidate: DeadLetterCandidate,
    ) -> AdapterExecutionResult:
        return replay_generic_payload_dead_letter(source, candidate, self.source_kind, self.fetch_mode)


class PassiveWebhookAdapter:
    source_kind = "webhook_ingest"
    fetch_mode = "push"

    def run(self, source: SourceDefinitionORM, context: AdapterContext) -> AdapterExecutionResult:
        return AdapterExecutionResult(
            adapter_kind=self.source_kind,
            fetch_mode=self.fetch_mode,
            envelopes=[],
            dead_letters=[],
            records_seen=0,
            checkpoint_json=context.checkpoint_json,
            metadata_json={"push_only": True},
        )

    def ingest(
        self,
        source: SourceDefinitionORM,
        *,
        payload: bytes,
        content_type: str | None,
        context: AdapterContext,
    ) -> AdapterExecutionResult:
        text_payload = payload.decode("utf-8")
        envelopes, dead_letters = normalize_runtime_payload(
            text_payload,
            source,
            context,
            default_format=str((source.metadata_json or {}).get("event_format") or "json"),
        )
        return AdapterExecutionResult(
            adapter_kind=self.source_kind,
            fetch_mode=self.fetch_mode,
            envelopes=envelopes,
            dead_letters=dead_letters,
            records_seen=len(envelopes) + len(dead_letters),
            cursor_text=str((context.last_offset or 0) + len(envelopes) + len(dead_letters)),
            last_offset=(context.last_offset or 0) + len(envelopes) + len(dead_letters),
            checkpoint_json={
                **context.checkpoint_json,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "content_type": content_type,
                "last_offset": (context.last_offset or 0) + len(envelopes) + len(dead_letters),
            },
            metadata_json={
                "byte_count": len(payload),
                "content_type": content_type,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
            },
            materialized_payload=payload,
            materialized_suffix=guess_payload_suffix(content_type, default_suffix=".json"),
            materialized_content_type=content_type,
        )

    def replay_dead_letter(
        self,
        source: SourceDefinitionORM,
        candidate: DeadLetterCandidate,
    ) -> AdapterExecutionResult:
        return replay_generic_payload_dead_letter(source, candidate, self.source_kind, self.fetch_mode)


class SseStreamAdapter:
    source_kind = "sse_stream"
    fetch_mode = "stream"

    def run(self, source: SourceDefinitionORM, context: AdapterContext) -> AdapterExecutionResult:
        payload, metadata = fetch_http_payload(
            source,
            accept_header="text/event-stream",
            context=context,
            extra_headers={"Last-Event-ID": context.last_event_id} if context.last_event_id else None,
        )
        text_payload = payload.decode("utf-8")
        event_format = str((source.metadata_json or {}).get("event_format") or "json")
        envelopes, dead_letters, last_event_id = normalize_sse_payload(
            text_payload,
            source,
            context,
            event_format=event_format,
        )
        return AdapterExecutionResult(
            adapter_kind=self.source_kind,
            fetch_mode=self.fetch_mode,
            envelopes=envelopes,
            dead_letters=dead_letters,
            records_seen=len(envelopes) + len(dead_letters),
            last_event_id=last_event_id or context.last_event_id,
            last_offset=(context.last_offset or 0) + len(envelopes) + len(dead_letters),
            checkpoint_json={
                **context.checkpoint_json,
                "last_event_id": last_event_id or context.last_event_id,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
            },
            metadata_json={
                **metadata,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "event_format": event_format,
            },
            materialized_payload=payload,
            materialized_suffix=".sse",
            materialized_content_type=metadata.get("content_type"),
        )

    def replay_dead_letter(
        self,
        source: SourceDefinitionORM,
        candidate: DeadLetterCandidate,
    ) -> AdapterExecutionResult:
        return replay_generic_payload_dead_letter(source, candidate, self.source_kind, self.fetch_mode)


class WebsocketStreamAdapter:
    source_kind = "websocket_stream"
    fetch_mode = "stream"

    def run(self, source: SourceDefinitionORM, context: AdapterContext) -> AdapterExecutionResult:
        metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
        max_messages = max(1, int(metadata.get("max_messages_per_run", 20)))
        open_timeout = float(metadata.get("open_timeout_seconds", 10.0))
        close_timeout = float(metadata.get("close_timeout_seconds", 5.0))
        additional_headers = resolve_websocket_headers(source)
        collected_messages: list[str] = []
        with websocket_connect(
            source.target_uri,
            additional_headers=additional_headers,
            open_timeout=open_timeout,
            close_timeout=close_timeout,
        ) as websocket:
            send_initial_websocket_messages(websocket, metadata, context)
            for _ in range(max_messages):
                try:
                    message = websocket.recv()
                except ConnectionClosed:
                    break
                if isinstance(message, bytes):
                    collected_messages.append(message.decode("utf-8"))
                else:
                    collected_messages.append(message)

        joined_payload = "\n".join(collected_messages)
        event_format = str(metadata.get("event_format") or "json")
        envelopes: list[IngestionEnvelope] = []
        dead_letters: list[DeadLetterCandidate] = []
        for index, message in enumerate(collected_messages, start=1):
            candidate_envelopes, candidate_dead_letters = normalize_runtime_payload(
                message,
                source,
                context,
                default_format=event_format,
                record_key=f"message:{index}",
            )
            envelopes.extend(candidate_envelopes)
            dead_letters.extend(candidate_dead_letters)
        return AdapterExecutionResult(
            adapter_kind=self.source_kind,
            fetch_mode=self.fetch_mode,
            envelopes=envelopes,
            dead_letters=dead_letters,
            records_seen=len(envelopes) + len(dead_letters),
            cursor_text=str((context.last_offset or 0) + len(collected_messages)),
            last_offset=(context.last_offset or 0) + len(collected_messages),
            checkpoint_json={
                **context.checkpoint_json,
                "message_count": len(collected_messages),
                "last_offset": (context.last_offset or 0) + len(collected_messages),
            },
            metadata_json={
                "message_count": len(collected_messages),
                "event_format": event_format,
            },
            materialized_payload=joined_payload.encode("utf-8"),
            materialized_suffix=".jsonl" if event_format == "json" else ".txt",
            materialized_content_type="application/jsonl" if event_format == "json" else "text/plain",
        )

    def replay_dead_letter(
        self,
        source: SourceDefinitionORM,
        candidate: DeadLetterCandidate,
    ) -> AdapterExecutionResult:
        return replay_generic_payload_dead_letter(source, candidate, self.source_kind, self.fetch_mode)


ADAPTER_REGISTRY: dict[str, SourceAdapter] = {
    "local_file": FileSourceAdapter("local_file"),
    "sqlite_file": FileSourceAdapter("sqlite_file"),
    "http_json": HttpSnapshotAdapter(
        "http_json",
        record_format="json",
        accept_header="application/json, */*",
        suffix=".json",
    ),
    "http_jsonl": HttpSnapshotAdapter(
        "http_jsonl",
        record_format="jsonl",
        accept_header="application/x-ndjson, application/jsonl, text/plain, */*",
        suffix=".jsonl",
    ),
    "http_text": HttpSnapshotAdapter(
        "http_text",
        record_format="txt",
        accept_header="text/plain, */*",
        suffix=".txt",
    ),
    "http_xml": HttpSnapshotAdapter(
        "http_xml",
        record_format="xml",
        accept_header="application/xml, text/xml, */*",
        suffix=".xml",
    ),
    "rss": RssSourceAdapter(),
    "webhook_ingest": PassiveWebhookAdapter(),
    "sse_stream": SseStreamAdapter(),
    "websocket_stream": WebsocketStreamAdapter(),
}


def list_supported_source_kinds() -> list[str]:
    return sorted(ADAPTER_REGISTRY)


def get_adapter_for_source(source_kind: str) -> SourceAdapter:
    adapter = ADAPTER_REGISTRY.get(source_kind)
    if adapter is None:
        raise ValueError(f"Unsupported source kind: {source_kind}")
    return adapter


def run_source_adapter(source: SourceDefinitionORM, context: AdapterContext) -> AdapterExecutionResult:
    adapter = get_adapter_for_source(source.source_kind)
    return adapter.run(source, context)


def ingest_webhook_source(
    source: SourceDefinitionORM,
    *,
    payload: bytes,
    content_type: str | None,
    context: AdapterContext,
) -> AdapterExecutionResult:
    adapter = get_adapter_for_source(source.source_kind)
    if not isinstance(adapter, PassiveWebhookAdapter):
        raise ValueError(f"Source {source.source_id} is not configured as webhook_ingest.")
    return adapter.ingest(source, payload=payload, content_type=content_type, context=context)


def replay_source_dead_letter(
    source: SourceDefinitionORM,
    candidate: DeadLetterCandidate,
) -> AdapterExecutionResult:
    adapter = get_adapter_for_source(source.source_kind)
    return adapter.replay_dead_letter(source, candidate)


def fetch_http_payload(
    source: SourceDefinitionORM,
    *,
    accept_header: str,
    context: AdapterContext,
    extra_headers: dict[str, str] | None = None,
) -> tuple[bytes, dict[str, Any]]:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    timeout_seconds = float(metadata.get("request_timeout_seconds", 30.0))
    retry_attempts = max(1, int(metadata.get("retry_attempts", 3)))
    retry_backoff_seconds = max(0.0, float(metadata.get("retry_backoff_seconds", 0.0)))
    headers = build_source_request_headers(source, accept_header=accept_header)
    if extra_headers:
        headers.update({key: value for key, value in extra_headers.items() if value})

    last_error: Exception | None = None
    for attempt in range(1, retry_attempts + 1):
        request = Request(source.target_uri, headers=headers)
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read()
                content_type = response.headers.get("Content-Type")
                status_code = getattr(response, "status", None) or getattr(response, "code", None) or 200
                return payload, {
                    "attempt_count": attempt,
                    "http_status": int(status_code),
                    "content_type": content_type,
                    "byte_count": len(payload),
                    "request_timeout_seconds": timeout_seconds,
                    "retry_attempts": retry_attempts,
                    "host": urlparse(source.target_uri).netloc,
                    "headers": headers,
                }
        except Exception as exc:
            last_error = exc
            if attempt >= retry_attempts:
                break
            if retry_backoff_seconds > 0:
                import time

                time.sleep(retry_backoff_seconds * attempt)
    assert last_error is not None
    raise RuntimeError(
        f"HTTP source fetch failed after {retry_attempts} attempts: {last_error}"
    ) from last_error


def build_source_request_headers(source: SourceDefinitionORM, *, accept_header: str) -> dict[str, str]:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    user_headers = metadata.get("headers", {})
    headers = {
        "User-Agent": str(metadata.get("user_agent", "11Writer-Forte/0.1 (+headless-source-fetch)")),
        "Accept": accept_header,
    }
    if isinstance(user_headers, dict):
        headers.update({str(key): str(value) for key, value in user_headers.items()})
    username = metadata.get("basic_auth_username")
    password_env = metadata.get("basic_auth_password_env")
    if username is not None or password_env is not None:
        if not isinstance(username, str) or not username:
            raise RuntimeError("HTTP source basic auth username is missing.")
        if not isinstance(password_env, str) or not password_env:
            raise RuntimeError("HTTP source basic auth password env var is missing.")
        password = os.getenv(password_env)
        if password is None:
            raise RuntimeError(f"HTTP source basic auth password env var '{password_env}' is not set.")
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        headers.setdefault("Authorization", f"Basic {token}")
    return headers


def resolve_websocket_headers(source: SourceDefinitionORM) -> list[tuple[str, str]]:
    metadata = source.metadata_json if isinstance(source.metadata_json, dict) else {}
    headers = build_source_request_headers(source, accept_header="application/json, text/plain, */*")
    return [(str(key), str(value)) for key, value in headers.items()]


def send_initial_websocket_messages(  # type: ignore[no-untyped-def]
    websocket,
    metadata: dict[str, Any],
    context: AdapterContext,
) -> None:
    send_text = metadata.get("send_text")
    if isinstance(send_text, str):
        websocket.send(format_runtime_template(send_text, context))
    send_json = metadata.get("send_json")
    if isinstance(send_json, dict):
        websocket.send(
            json.dumps(
                render_runtime_template(send_json, context),
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    send_messages = metadata.get("send_messages")
    if isinstance(send_messages, list):
        for item in send_messages:
            if isinstance(item, dict):
                websocket.send(
                    json.dumps(
                        render_runtime_template(item, context),
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                )
            elif isinstance(item, str):
                websocket.send(format_runtime_template(item, context))


def render_runtime_template(value: Any, context: AdapterContext) -> Any:
    if isinstance(value, str):
        return format_runtime_template(value, context)
    if isinstance(value, dict):
        return {str(key): render_runtime_template(item, context) for key, item in value.items()}
    if isinstance(value, list):
        return [render_runtime_template(item, context) for item in value]
    return value


def format_runtime_template(template: str, context: AdapterContext) -> str:
    replacements: dict[str, str] = {
        "cursor_text": context.cursor_text or "",
        "last_event_id": context.last_event_id or "",
        "last_offset": "" if context.last_offset is None else str(context.last_offset),
        "checkpoint_json": json.dumps(context.checkpoint_json, default=str, separators=(",", ":")),
    }
    for key, value in context.checkpoint_json.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            replacements[f"checkpoint_{key}"] = "" if value is None else str(value)
    def replace(match: re.Match[str]) -> str:
        return replacements.get(match.group(1), "")

    return re.sub(r"\{([A-Za-z0-9_]+)\}", replace, template)


def normalize_http_json_payload(text_payload: str, source: SourceDefinitionORM) -> list[IngestionEnvelope]:
    payload = json.loads(text_payload)
    if isinstance(payload, dict):
        items = payload.get("items") if isinstance(payload.get("items"), list) else [payload]
    elif isinstance(payload, list):
        items = payload
    else:
        items = [{"value": payload}]
    return [normalize_payload(item, "json", source_type=source.source_kind) for item in items if isinstance(item, dict)]


def normalize_http_jsonl_payload(
    text_payload: str,
    source: SourceDefinitionORM,
    context: AdapterContext,
) -> tuple[list[IngestionEnvelope], list[DeadLetterCandidate]]:
    envelopes: list[IngestionEnvelope] = []
    dead_letters: list[DeadLetterCandidate] = []
    for line_number, line in enumerate(text_payload.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
            if not isinstance(payload, dict):
                payload = {"value": payload}
            envelopes.append(normalize_payload(payload, "jsonl", source_type=source.source_kind))
        except json.JSONDecodeError as exc:
            dead_letters.append(
                DeadLetterCandidate(
                    stage="parse",
                    failure_reason=f"Invalid JSONL at line {line_number}: {exc.msg}",
                    raw_payload_text=line,
                    record_key=f"line:{line_number}",
                    record_hash=hashlib.sha256(line.encode("utf-8")).hexdigest(),
                    cursor_text=str(line_number),
                    checkpoint_json=context.checkpoint_json,
                )
            )
    return envelopes, dead_letters


def normalize_http_xml_payload(
    text_payload: str,
    source: SourceDefinitionORM,
    context: AdapterContext,
) -> tuple[list[IngestionEnvelope], list[DeadLetterCandidate]]:
    try:
        records = parse_http_xml_payload(text_payload.encode("utf-8"), source.target_uri)
    except ElementTree.ParseError as exc:
        return [], [
            DeadLetterCandidate(
                stage="parse",
                failure_reason=f"Invalid XML payload: {exc}",
                raw_payload_text=text_payload,
                record_hash=hashlib.sha256(text_payload.encode("utf-8")).hexdigest(),
                checkpoint_json=context.checkpoint_json,
            )
        ]
    return [normalize_payload(record, "xml", source_type=source.source_kind) for record in records], []


def normalize_rss_payload(
    text_payload: str,
    source: SourceDefinitionORM,
    context: AdapterContext,
) -> tuple[list[IngestionEnvelope], list[DeadLetterCandidate]]:
    try:
        root = ElementTree.fromstring(text_payload)
    except ElementTree.ParseError as exc:
        return [], [
            DeadLetterCandidate(
                stage="parse",
                failure_reason=f"Invalid RSS/Atom payload: {exc}",
                raw_payload_text=text_payload,
                record_hash=hashlib.sha256(text_payload.encode("utf-8")).hexdigest(),
                checkpoint_json=context.checkpoint_json,
            )
        ]

    items = root.findall(".//item")
    entries = root.findall(".//{*}entry")
    envelopes: list[IngestionEnvelope] = []
    for element in items + entries:
        payload = {
            "title": xml_find_text(element, "title"),
            "text": xml_find_text(element, "description")
            or xml_find_text(element, "summary")
            or xml_find_text(element, "content")
            or xml_find_text(element, "title"),
            "url": xml_find_link(element),
            "published_at": xml_find_text(element, "pubDate")
            or xml_find_text(element, "updated")
            or xml_find_text(element, "published"),
            "guid": xml_find_text(element, "guid") or xml_find_text(element, "id"),
            "latitude": xml_find_text(element, "lat"),
            "longitude": xml_find_text(element, "long"),
        }
        envelopes.append(normalize_payload(payload, "rss", source_type=source.source_kind))
    return envelopes, []


def normalize_sse_payload(
    text_payload: str,
    source: SourceDefinitionORM,
    context: AdapterContext,
    *,
    event_format: str,
) -> tuple[list[IngestionEnvelope], list[DeadLetterCandidate], str | None]:
    envelopes: list[IngestionEnvelope] = []
    dead_letters: list[DeadLetterCandidate] = []
    last_event_id: str | None = context.last_event_id
    current_id: str | None = None
    current_data: list[str] = []

    def flush_event() -> None:
        nonlocal last_event_id, current_id, current_data
        if not current_data:
            current_id = None
            return
        data = "\n".join(current_data)
        candidate_envelopes, candidate_dead_letters = normalize_runtime_payload(
            data,
            source,
            context,
            default_format=event_format,
            record_key=current_id,
        )
        envelopes.extend(candidate_envelopes)
        dead_letters.extend(candidate_dead_letters)
        if current_id:
            last_event_id = current_id
        current_id = None
        current_data = []

    for raw_line in text_payload.splitlines():
        line = raw_line.rstrip("\r")
        if not line:
            flush_event()
            continue
        if line.startswith(":"):
            continue
        field_name, _, field_value = line.partition(":")
        value = field_value.lstrip(" ")
        if field_name == "id":
            current_id = value
        elif field_name == "data":
            current_data.append(value)
    flush_event()
    return envelopes, dead_letters, last_event_id


def normalize_runtime_payload(
    text_payload: str,
    source: SourceDefinitionORM,
    context: AdapterContext,
    *,
    default_format: str,
    record_key: str | None = None,
) -> tuple[list[IngestionEnvelope], list[DeadLetterCandidate]]:
    normalized_format = default_format.strip().lower()
    if normalized_format in {"json", "jsonl"}:
        try:
            payload = json.loads(text_payload)
        except json.JSONDecodeError as exc:
            return [], [
                DeadLetterCandidate(
                    stage="parse",
                    failure_reason=f"Invalid JSON payload: {exc.msg}",
                    raw_payload_text=text_payload,
                    record_key=record_key,
                    record_hash=hashlib.sha256(text_payload.encode("utf-8")).hexdigest(),
                    checkpoint_json=context.checkpoint_json,
                )
            ]
        if isinstance(payload, list):
            envelopes = [
                normalize_payload(item if isinstance(item, dict) else {"value": item}, normalized_format, source_type=source.source_kind)
                for item in payload
            ]
        else:
            if not isinstance(payload, dict):
                payload = {"value": payload}
            envelopes = [normalize_payload(payload, normalized_format, source_type=source.source_kind)]
        return envelopes, []

    envelope = normalize_payload(
        {"text": text_payload, "record_key": record_key},
        "txt",
        source_type=source.source_kind,
    )
    return [envelope], []


def replay_generic_payload_dead_letter(
    source: SourceDefinitionORM,
    candidate: DeadLetterCandidate,
    adapter_kind: str,
    fetch_mode: str,
) -> AdapterExecutionResult:
    if candidate.payload_json:
        payload = candidate.payload_json
        envelope = normalize_payload(
            payload if isinstance(payload, dict) else {"value": payload},
            "json",
            source_type=source.source_kind,
        )
        return completed_replay_result(adapter_kind, fetch_mode, [envelope])
    if candidate.raw_payload_text:
        envelopes, dead_letters = normalize_runtime_payload(
            candidate.raw_payload_text,
            source,
            AdapterContext(checkpoint_json=candidate.checkpoint_json),
            default_format=str((source.metadata_json or {}).get("event_format") or "json"),
            record_key=candidate.record_key,
        )
        if dead_letters:
            raise ValueError(dead_letters[0].failure_reason)
        return completed_replay_result(adapter_kind, fetch_mode, envelopes)
    raise ValueError("Dead-letter record is missing replayable payload data.")


def completed_replay_result(
    adapter_kind: str,
    fetch_mode: str,
    envelopes: list[IngestionEnvelope],
) -> AdapterExecutionResult:
    return AdapterExecutionResult(
        adapter_kind=adapter_kind,
        fetch_mode=fetch_mode,
        envelopes=envelopes,
        dead_letters=[],
        records_seen=len(envelopes),
        checkpoint_json={},
        metadata_json={"replayed": True},
    )


def content_type_for_suffix(suffix: str) -> str:
    return {
        ".json": "application/json",
        ".jsonl": "application/jsonl",
        ".txt": "text/plain",
        ".xml": "application/xml",
        ".rss": "application/rss+xml",
        ".db": "application/vnd.sqlite3",
        ".sqlite": "application/vnd.sqlite3",
        ".sqlite3": "application/vnd.sqlite3",
    }.get(suffix, "application/octet-stream")


def guess_payload_suffix(content_type: str | None, *, default_suffix: str) -> str:
    normalized = (content_type or "").lower()
    if "json" in normalized:
        return ".json"
    if "xml" in normalized:
        return ".xml"
    if "text" in normalized:
        return ".txt"
    return default_suffix


def strip_xml_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    if ":" in tag:
        return tag.rsplit(":", 1)[-1]
    return tag


def parse_http_xml_payload(payload: bytes, source_uri: str) -> list[dict[str, Any]]:
    root = ElementTree.fromstring(payload)
    root_tag = strip_xml_namespace(root.tag)
    records: list[dict[str, Any]] = []
    for child in root:
        if not isinstance(child.tag, str):
            continue
        child_payload = xml_element_to_data(child)
        if not isinstance(child_payload, dict):
            child_payload = {"value": child_payload}
        record_type = strip_xml_namespace(child.tag)
        headline = extract_xml_headline(child_payload)
        route_designator = first_nested_value(child_payload, "route-designator")
        event_id = first_nested_value(child_payload, "event-id")
        status = first_nested_value(child_payload, "status")
        observed_at = first_feu_timestamp(child_payload)
        latitude = normalize_coordinate(first_nested_value(child_payload, "latitude"))
        longitude = normalize_coordinate(first_nested_value(child_payload, "longitude"))

        record: dict[str, Any] = {
            "source_url": source_uri,
            "feed_type": root_tag,
            "record_type": record_type,
            "title": headline or event_id or record_type,
            "text": " | ".join(
                part
                for part in (
                    event_id,
                    headline,
                    route_designator,
                    f"status={status}" if status else None,
                )
                if part
            ),
            "event_id": event_id,
            "status": status,
            "route_designator": route_designator,
            "observed_at": observed_at,
            "raw_xml": child_payload,
        }
        if latitude is not None and longitude is not None:
            record["latitude"] = latitude
            record["longitude"] = longitude
        records.append(record)
    return records


def xml_element_to_data(element: ElementTree.Element) -> Any:
    children = list(element)
    text = (element.text or "").strip()
    if not children and not element.attrib:
        return text

    node: dict[str, Any] = {}
    for key, value in element.attrib.items():
        node[f"@{strip_xml_namespace(key)}"] = value

    grouped: dict[str, list[Any]] = {}
    for child in children:
        grouped.setdefault(strip_xml_namespace(child.tag), []).append(xml_element_to_data(child))

    for key, values in grouped.items():
        node[key] = values[0] if len(values) == 1 else values

    if text:
        node["text"] = text
    return node


def first_nested_value(payload: Any, key: str) -> str | None:
    if isinstance(payload, dict):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
        for child_value in payload.values():
            nested = first_nested_value(child_value, key)
            if nested:
                return nested
    elif isinstance(payload, list):
        for item in payload:
            nested = first_nested_value(item, key)
            if nested:
                return nested
    return None


def collect_scalar_strings(payload: Any, limit: int = 6) -> list[str]:
    values: list[str] = []
    if isinstance(payload, dict):
        for item in payload.values():
            if len(values) >= limit:
                break
            values.extend(collect_scalar_strings(item, limit=limit - len(values)))
    elif isinstance(payload, list):
        for item in payload:
            if len(values) >= limit:
                break
            values.extend(collect_scalar_strings(item, limit=limit - len(values)))
    elif isinstance(payload, str):
        stripped = payload.strip()
        if stripped:
            values.append(stripped)
    elif isinstance(payload, (int, float)):
        values.append(str(payload))
    return values[:limit]


def extract_xml_headline(payload: dict[str, Any]) -> str | None:
    headline_payload = payload.get("headline")
    if headline_payload is None:
        return None
    parts = [part for part in collect_scalar_strings(headline_payload, limit=4) if not part.isdigit()]
    if not parts:
        return None
    return " | ".join(parts)


def first_feu_timestamp(payload: dict[str, Any]) -> str | None:
    candidates = [
        payload.get("message-header"),
        payload.get("times"),
        payload.get("detail"),
    ]
    for candidate in candidates:
        timestamp = find_timestamp_in_payload(candidate)
        if timestamp:
            return timestamp
    return None


def find_timestamp_in_payload(payload: Any) -> str | None:
    if isinstance(payload, dict):
        date_value = payload.get("date")
        time_value = payload.get("time")
        offset_value = payload.get("utc-offset")
        if isinstance(date_value, str) and isinstance(time_value, str):
            return format_feu_timestamp(date_value, time_value, offset_value if isinstance(offset_value, str) else None)
        for value in payload.values():
            timestamp = find_timestamp_in_payload(value)
            if timestamp:
                return timestamp
    elif isinstance(payload, list):
        for item in payload:
            timestamp = find_timestamp_in_payload(item)
            if timestamp:
                return timestamp
    return None


def format_feu_timestamp(date_value: str, time_value: str, offset_value: str | None) -> str:
    cleaned_date = date_value.strip()
    cleaned_time = time_value.strip()
    if len(cleaned_date) != 8 or len(cleaned_time) not in {4, 6}:
        return f"{cleaned_date}T{cleaned_time}"
    normalized_time = cleaned_time if len(cleaned_time) == 6 else f"{cleaned_time}00"
    timestamp = (
        f"{cleaned_date[0:4]}-{cleaned_date[4:6]}-{cleaned_date[6:8]}"
        f"T{normalized_time[0:2]}:{normalized_time[2:4]}:{normalized_time[4:6]}"
    )
    if not offset_value:
        return timestamp
    cleaned_offset = offset_value.strip()
    if len(cleaned_offset) == 5:
        return f"{timestamp}{cleaned_offset[0:3]}:{cleaned_offset[3:5]}"
    return f"{timestamp}{cleaned_offset}"


def normalize_coordinate(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    if abs(parsed) > 1000:
        return parsed / 1_000_000.0
    return parsed


def xml_find_text(element: ElementTree.Element, tag_name: str) -> str | None:
    for child in element.iter():
        if not isinstance(child.tag, str):
            continue
        if strip_xml_namespace(child.tag) == tag_name:
            text = (child.text or "").strip()
            if text:
                return text
    return None


def xml_find_link(element: ElementTree.Element) -> str | None:
    for child in element.iter():
        if not isinstance(child.tag, str):
            continue
        if strip_xml_namespace(child.tag) != "link":
            continue
        href = child.attrib.get("href")
        if href:
            return href
        text = (child.text or "").strip()
        if text:
            return text
    return None
