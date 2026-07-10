from __future__ import annotations

import io
import socket
import ssl
from email.message import Message
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from src.services.discovery_fetch import (
    FetchPolicy,
    PinnedHTTPSConnection,
    ResponseTooLargeError,
    SafeRedirectHandler,
    UnsafeTargetError,
    ValidatedAddressPool,
    fetch_url,
    read_bounded,
    sanitize_response_headers,
    validate_fetch_url,
)


def public_resolver(
    hostname: str,
    port: object,
    *,
    type: object,
) -> list[tuple[object, object, object, object, tuple[object, ...]]]:
    assert port is None
    assert type == socket.SOCK_STREAM
    return [
        (
            socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP,
            hostname,
            ("93.184.216.34", 0),
        )
    ]


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/data",
        "http://10.0.0.1/data",
        "http://172.16.0.1/data",
        "http://192.168.1.10/data",
        "http://169.254.10.20/data",
        "http://[::1]/data",
        "http://[fe80::1]/data",
    ],
)
def test_validate_fetch_url_blocks_non_public_literals(url: str) -> None:
    with pytest.raises(UnsafeTargetError, match="non-public address"):
        validate_fetch_url(url)


def test_validate_fetch_url_blocks_hostname_resolving_private() -> None:
    def private_resolver(
        hostname: str,
        port: object,
        *,
        type: object,
    ) -> list[tuple[object, object, object, object, tuple[object, ...]]]:
        assert hostname == "public-looking.example"
        assert port is None
        assert type == socket.SOCK_STREAM
        return [
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "",
                ("10.20.30.40", 0),
            )
        ]

    with pytest.raises(UnsafeTargetError, match="10.20.30.40"):
        validate_fetch_url(
            "https://public-looking.example/data",
            resolver=private_resolver,
        )


def test_redirect_handler_revalidates_private_location() -> None:
    handler = SafeRedirectHandler(FetchPolicy())
    request = Request("https://public.example/start")

    with pytest.raises(UnsafeTargetError, match="127.0.0.1"):
        handler.redirect_request(
            request,
            None,
            302,
            "Found",
            {},
            "http://127.0.0.1/private",
        )


def test_cross_origin_redirect_strips_sensitive_headers() -> None:
    handler = SafeRedirectHandler(FetchPolicy(), resolver=public_resolver)
    request = Request(
        "https://trusted.example/start",
        headers={
            "Authorization": "Bearer secret",
            "Cookie": "session=secret",
            "X-Api-Key": "secret",
            "X-Auth-Token": "secret",
            "Accept": "application/json",
        },
    )

    redirected = handler.redirect_request(
        request,
        None,
        302,
        "Found",
        {},
        "https://attacker.example/capture",
    )

    assert redirected is not None
    redirected_headers = {key.lower(): value for key, value in redirected.header_items()}
    assert redirected_headers == {"accept": "application/json"}


def test_same_origin_redirect_preserves_authorization() -> None:
    handler = SafeRedirectHandler(FetchPolicy(), resolver=public_resolver)
    request = Request(
        "https://trusted.example/start",
        headers={"Authorization": "Bearer expected", "Accept": "application/json"},
    )

    redirected = handler.redirect_request(
        request,
        None,
        302,
        "Found",
        {},
        "https://trusted.example/next",
    )

    assert redirected is not None
    assert redirected.get_header("Authorization") == "Bearer expected"


def test_validated_address_pool_pins_first_dns_answer_for_connection() -> None:
    resolver_calls = 0
    connected_addresses: list[str] = []

    def rebinding_resolver(
        hostname: str,
        port: object,
        *,
        type: object,
    ) -> list[tuple[object, object, object, object, tuple[object, ...]]]:
        nonlocal resolver_calls
        resolver_calls += 1
        address = "93.184.216.34" if resolver_calls == 1 else "127.0.0.1"
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (address, 0))]

    def capture_connection(records, port, timeout, source_address):  # type: ignore[no-untyped-def]
        connected_addresses.extend(record.ip for record in records)
        return object()

    pool = ValidatedAddressPool(resolver=rebinding_resolver, connector=capture_connection)
    pool.validate_url("https://rebind.example/data")
    pool.create_connection(("rebind.example", 443), timeout=1.0)

    assert resolver_calls == 1
    assert connected_addresses == ["93.184.216.34"]


def test_pinned_https_connection_verifies_original_hostname() -> None:
    server_hostnames: list[str | None] = []

    class DummySocket:
        def setsockopt(self, *args: object) -> None:
            return None

    class DummyContext:
        verify_mode = ssl.CERT_REQUIRED
        check_hostname = True

        def wrap_socket(self, sock: object, *, server_hostname: str | None) -> object:
            server_hostnames.append(server_hostname)
            return sock

    pool = ValidatedAddressPool(
        resolver=public_resolver,
        connector=lambda records, port, timeout, source_address: DummySocket(),
    )
    pool.validate_url("https://trusted.example/data")
    connection = PinnedHTTPSConnection(
        "trusted.example",
        timeout=1.0,
        context=DummyContext(),  # type: ignore[arg-type]
        address_pool=pool,
    )

    connection.connect()

    assert server_hostnames == ["trusted.example"]


class StreamingResponse:
    def __init__(self, payload: bytes, declared_length: int | None = None) -> None:
        self._stream = io.BytesIO(payload)
        self.headers = Message()
        if declared_length is not None:
            self.headers["Content-Length"] = str(declared_length)

    def read(self, amount: int = -1) -> bytes:
        return self._stream.read(amount)


class SuccessfulResponse(StreamingResponse):
    status = 200

    def __init__(self, url: str, payload: bytes) -> None:
        super().__init__(payload)
        self.url = url
        self.headers["Content-Type"] = "text/plain"

    def geturl(self) -> str:
        return self.url

    def __enter__(self) -> "SuccessfulResponse":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        return None


@pytest.mark.parametrize(("retry_after", "expected_delay"), [("7", 7.0), ("600", 60.0)])
def test_fetch_url_honors_bounded_retry_after_without_real_sleep(
    retry_after: str,
    expected_delay: float,
) -> None:
    class RetryThenSuccessOpener:
        def __init__(self) -> None:
            self.calls = 0

        def open(self, request: Request, timeout: float) -> SuccessfulResponse:
            self.calls += 1
            if self.calls == 1:
                headers = Message()
                headers["Retry-After"] = retry_after
                raise HTTPError(request.full_url, 429, "Too Many Requests", headers, None)
            return SuccessfulResponse(request.full_url, b"ok")

    sleeps: list[float] = []
    opener = RetryThenSuccessOpener()

    result = fetch_url(
        "https://public.example/data",
        policy=FetchPolicy(retry_attempts=2, retry_backoff_seconds=1),
        resolver=public_resolver,
        opener=opener,  # type: ignore[arg-type]
        sleep_fn=lambda delay: sleeps.append(delay),
    )

    assert result.payload == b"ok"
    assert result.attempt_count == 2
    assert sleeps == [expected_delay]


def test_read_bounded_rejects_streaming_overflow_without_content_length() -> None:
    response = StreamingResponse(b"x" * 1025)

    with pytest.raises(ResponseTooLargeError, match="configured limit of 1024 bytes"):
        read_bounded(response, 1024)


def test_read_bounded_rejects_declared_overflow_before_read() -> None:
    response = StreamingResponse(b"small", declared_length=5000)

    with pytest.raises(ResponseTooLargeError, match="Content-Length 5000"):
        read_bounded(response, 1024)
    assert response._stream.tell() == 0


def test_sanitize_response_headers_keeps_allowlist_and_drops_secrets() -> None:
    sanitized = sanitize_response_headers(
        {
            "Content-Type": "application/json",
            "ETag": '"abc"',
            "Last-Modified": "Thu, 09 Jul 2026 12:00:00 GMT",
            "Authorization": "Bearer absolutely-not-persisted",
            "Set-Cookie": "session=also-not-persisted",
            "X-Api-Key": "nope",
        }
    )

    assert sanitized == {
        "content-type": "application/json",
        "etag": '"abc"',
        "last-modified": "Thu, 09 Jul 2026 12:00:00 GMT",
    }
    assert "absolutely-not-persisted" not in repr(sanitized)
