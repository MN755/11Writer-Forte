from __future__ import annotations

import ipaddress
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import Message
from email.utils import parsedate_to_datetime
from functools import partial
from http.client import HTTPConnection, HTTPSConnection
from typing import Callable, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import (
    HTTPHandler,
    HTTPRedirectHandler,
    HTTPSHandler,
    OpenerDirector,
    ProxyHandler,
    Request,
    build_opener,
)


class DiscoveryFetchError(RuntimeError):
    """Base error for bounded discovery fetches."""


class UnsafeTargetError(DiscoveryFetchError):
    """Raised when a target violates the public-network policy."""


class ResponseTooLargeError(DiscoveryFetchError):
    """Raised when a response exceeds the configured byte budget."""


class HTTPStatusFetchError(DiscoveryFetchError):
    """Raised for a terminal non-success HTTP response."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = int(status_code)


class FetchResponse(Protocol):
    headers: Message

    def read(self, amount: int = -1) -> bytes: ...

    def geturl(self) -> str: ...

    def __enter__(self) -> "FetchResponse": ...

    def __exit__(self, exc_type, exc, traceback) -> object: ...


Resolver = Callable[..., list[tuple[object, object, object, object, tuple[object, ...]]]]
SocketConnector = Callable[
    [tuple["ResolvedAddress", ...], int, object, tuple[object, ...] | None],
    object,
]


@dataclass(frozen=True)
class ResolvedAddress:
    family: int
    socktype: int
    proto: int
    sockaddr: tuple[object, ...]

    @property
    def ip(self) -> str:
        return str(self.sockaddr[0])


@dataclass(frozen=True)
class FetchPolicy:
    timeout_seconds: float = 15.0
    retry_attempts: int = 2
    retry_backoff_seconds: float = 1.0
    max_response_bytes: int = 5 * 1024 * 1024
    allow_private_networks: bool = False
    user_agent: str = "11Writer-Forte/0.1 (+bounded-source-discovery)"

    def normalized(self) -> "FetchPolicy":
        return FetchPolicy(
            timeout_seconds=max(0.1, min(float(self.timeout_seconds), 120.0)),
            retry_attempts=max(1, min(int(self.retry_attempts), 5)),
            retry_backoff_seconds=max(0.0, min(float(self.retry_backoff_seconds), 60.0)),
            max_response_bytes=max(1024, min(int(self.max_response_bytes), 100 * 1024 * 1024)),
            allow_private_networks=bool(self.allow_private_networks),
            user_agent=str(self.user_agent or "11Writer-Forte/0.1 (+bounded-source-discovery)"),
        )


@dataclass(frozen=True)
class FetchResult:
    requested_url: str
    final_url: str
    status_code: int
    headers: dict[str, str]
    payload: bytes
    elapsed_ms: float
    attempt_count: int

    @property
    def content_type(self) -> str | None:
        value = self.headers.get("content-type")
        return value.split(";", 1)[0].strip().lower() if value else None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_hostname(hostname: str) -> str:
    candidate = hostname.strip().rstrip(".").lower()
    try:
        return candidate.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise UnsafeTargetError("Target hostname is not valid IDNA.") from exc


def is_non_public_ip(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value.split("%", 1)[0])
    except ValueError:
        return True
    return bool(
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    )


def resolve_host_records(
    hostname: str,
    resolver: Resolver = socket.getaddrinfo,
) -> tuple[ResolvedAddress, ...]:
    try:
        raw_records = resolver(hostname, None, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise DiscoveryFetchError(f"DNS resolution failed for {hostname}: {exc}") from exc

    records: list[ResolvedAddress] = []
    seen: set[tuple[int, int, int, tuple[object, ...]]] = set()
    for family, socktype, proto, _canonical_name, sockaddr in raw_records:
        if not sockaddr:
            continue
        record = ResolvedAddress(
            family=int(family),
            socktype=int(socktype or socket.SOCK_STREAM),
            proto=int(proto or 0),
            sockaddr=tuple(sockaddr),
        )
        marker = (record.family, record.socktype, record.proto, record.sockaddr)
        if marker not in seen:
            records.append(record)
            seen.add(marker)
    if not records:
        raise DiscoveryFetchError(f"DNS resolution returned no addresses for {hostname}.")
    return tuple(records)


def resolve_host_addresses(hostname: str, resolver: Resolver = socket.getaddrinfo) -> set[str]:
    return {record.ip for record in resolve_host_records(hostname, resolver=resolver)}


def connect_to_resolved_addresses(
    records: tuple[ResolvedAddress, ...],
    port: int,
    timeout: object,
    source_address: tuple[object, ...] | None,
) -> socket.socket:
    """Open a socket to one of the exact addresses already approved by policy."""

    last_error: OSError | None = None
    for record in records:
        sock: socket.socket | None = None
        try:
            sock = socket.socket(record.family, record.socktype, record.proto)
            if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
                sock.settimeout(timeout)  # type: ignore[arg-type]
            if source_address:
                sock.bind(source_address)  # type: ignore[arg-type]
            sockaddr = list(record.sockaddr)
            if len(sockaddr) < 2:
                raise OSError(f"Resolved address for {record.ip} has no port slot.")
            sockaddr[1] = port
            sock.connect(tuple(sockaddr))  # type: ignore[arg-type]
            return sock
        except OSError as exc:
            last_error = exc
            if sock is not None:
                sock.close()
    if last_error is not None:
        raise last_error
    raise OSError("No validated address was available for the connection.")


class ValidatedAddressPool:
    """Per-fetch DNS snapshot used for both policy validation and socket creation."""

    def __init__(
        self,
        *,
        resolver: Resolver = socket.getaddrinfo,
        allow_private_networks: bool = False,
        connector: SocketConnector = connect_to_resolved_addresses,
    ) -> None:
        self.resolver = resolver
        self.allow_private_networks = allow_private_networks
        self.connector = connector
        self._records: dict[str, tuple[ResolvedAddress, ...]] = {}

    def resolve_and_validate(self, hostname: str) -> tuple[ResolvedAddress, ...]:
        normalized = normalize_hostname(hostname)
        existing = self._records.get(normalized)
        if existing is not None:
            return existing
        records = resolve_host_records(normalized, resolver=self.resolver)
        blocked = sorted(record.ip for record in records if is_non_public_ip(record.ip))
        if blocked and not self.allow_private_networks:
            raise UnsafeTargetError(
                "Target resolves to a non-public address and is blocked: "
                f"{normalized} ({', '.join(blocked)})"
            )
        self._records[normalized] = records
        return records

    def validate_url(self, url: str) -> str:
        return validate_fetch_url(
            url,
            allow_private_networks=self.allow_private_networks,
            resolver=self.resolver,
            address_pool=self,
        )

    def create_connection(
        self,
        address: tuple[str, int],
        timeout: object = socket._GLOBAL_DEFAULT_TIMEOUT,
        source_address: tuple[object, ...] | None = None,
    ) -> object:
        hostname, port = address
        records = self.resolve_and_validate(hostname)
        return self.connector(records, int(port), timeout, source_address)


def validate_fetch_url(
    url: str,
    *,
    allow_private_networks: bool = False,
    resolver: Resolver = socket.getaddrinfo,
    address_pool: ValidatedAddressPool | None = None,
) -> str:
    parsed = urlsplit(url.strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        raise UnsafeTargetError("Discovery fetches support only http and https URLs.")
    if not parsed.hostname:
        raise UnsafeTargetError("Discovery target is missing a hostname.")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeTargetError("Discovery target URLs may not contain credentials.")
    try:
        port = parsed.port
    except ValueError as exc:
        raise UnsafeTargetError("Discovery target contains an invalid port.") from exc
    if port is not None and not (1 <= port <= 65535):
        raise UnsafeTargetError("Discovery target contains an invalid port.")

    hostname = normalize_hostname(parsed.hostname)
    if hostname in {"localhost", "localhost.localdomain"} or hostname.endswith((".localhost", ".local")):
        if not allow_private_networks:
            raise UnsafeTargetError(f"Private or local target is blocked: {hostname}")

    if address_pool is not None:
        if address_pool.allow_private_networks != bool(allow_private_networks):
            raise ValueError("Address-pool private-network policy does not match fetch policy.")
        address_pool.resolve_and_validate(hostname)
        return url
    if allow_private_networks:
        return url

    try:
        literal = ipaddress.ip_address(hostname.split("%", 1)[0])
    except ValueError:
        addresses = resolve_host_addresses(hostname, resolver=resolver)
    else:
        addresses = {str(literal)}
    blocked = sorted(address for address in addresses if is_non_public_ip(address))
    if blocked:
        raise UnsafeTargetError(
            f"Target resolves to a non-public address and is blocked: {hostname} ({', '.join(blocked)})"
        )
    return url


class PinnedHTTPConnection(HTTPConnection):
    def __init__(
        self,
        host: str,
        port: int | None = None,
        *,
        address_pool: ValidatedAddressPool,
        **kwargs: object,
    ) -> None:
        super().__init__(host, port=port, **kwargs)  # type: ignore[arg-type]
        self._create_connection = address_pool.create_connection


class PinnedHTTPSConnection(HTTPSConnection):
    def __init__(
        self,
        host: str,
        port: int | None = None,
        *,
        address_pool: ValidatedAddressPool,
        **kwargs: object,
    ) -> None:
        # HTTPSConnection retains ``host`` for SNI and certificate verification while
        # the injected socket creator connects to the vetted numeric address.
        super().__init__(host, port=port, **kwargs)  # type: ignore[arg-type]
        self._create_connection = address_pool.create_connection


class PinnedHTTPHandler(HTTPHandler):
    def __init__(self, address_pool: ValidatedAddressPool) -> None:
        super().__init__()
        self.address_pool = address_pool

    def http_open(self, request: Request):  # type: ignore[no-untyped-def]
        connection = partial(PinnedHTTPConnection, address_pool=self.address_pool)
        return self.do_open(connection, request)


class PinnedHTTPSHandler(HTTPSHandler):
    def __init__(self, address_pool: ValidatedAddressPool) -> None:
        super().__init__()
        self.address_pool = address_pool

    def https_open(self, request: Request):  # type: ignore[no-untyped-def]
        connection = partial(PinnedHTTPSConnection, address_pool=self.address_pool)
        return self.do_open(
            connection,
            request,
            context=self._context,
            check_hostname=self._check_hostname,
        )


def url_origin(url: str) -> tuple[str, str, int | None]:
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()
    hostname = normalize_hostname(parsed.hostname or "")
    port = parsed.port
    if port is None:
        port = 80 if scheme == "http" else 443 if scheme == "https" else None
    return scheme, hostname, port


def is_sensitive_redirect_header(name: str) -> bool:
    lowered = name.lower()
    if lowered in {"authorization", "proxy-authorization", "cookie", "cookie2", "referer"}:
        return True
    return any(marker in lowered for marker in ("token", "secret", "api-key", "apikey"))


class SafeRedirectHandler(HTTPRedirectHandler):
    def __init__(
        self,
        policy: FetchPolicy,
        resolver: Resolver = socket.getaddrinfo,
        *,
        address_pool: ValidatedAddressPool | None = None,
    ) -> None:
        super().__init__()
        self.policy = policy.normalized()
        self.resolver = resolver
        self.address_pool = address_pool or ValidatedAddressPool(
            resolver=resolver,
            allow_private_networks=self.policy.allow_private_networks,
        )

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        resolved = urljoin(req.full_url, newurl)
        self.address_pool.validate_url(resolved)
        redirected = super().redirect_request(req, fp, code, msg, headers, resolved)
        if redirected is None:
            return None

        cross_origin = url_origin(req.full_url) != url_origin(resolved)
        for header_name in list(redirected.headers):
            if header_name.lower() == "host" or (
                cross_origin and is_sensitive_redirect_header(header_name)
            ):
                redirected.remove_header(header_name)
        return redirected


def build_safe_opener(
    policy: FetchPolicy,
    *,
    resolver: Resolver = socket.getaddrinfo,
    address_pool: ValidatedAddressPool | None = None,
) -> OpenerDirector:
    normalized_policy = policy.normalized()
    pool = address_pool or ValidatedAddressPool(
        resolver=resolver,
        allow_private_networks=normalized_policy.allow_private_networks,
    )
    return build_opener(
        ProxyHandler({}),
        SafeRedirectHandler(normalized_policy, resolver=resolver, address_pool=pool),
        PinnedHTTPHandler(pool),
        PinnedHTTPSHandler(pool),
    )


def sanitize_response_headers(headers: Mapping[str, str] | Message) -> dict[str, str]:
    allowed = {
        "accept-ranges",
        "age",
        "cache-control",
        "content-encoding",
        "content-language",
        "content-length",
        "content-location",
        "content-type",
        "date",
        "etag",
        "expires",
        "last-modified",
        "link",
        "location",
        "retry-after",
        "server",
        "vary",
    }
    return {
        str(key).lower(): str(value)[:2000]
        for key, value in headers.items()
        if str(key).lower() in allowed
    }


def should_retry(status_code: int) -> bool:
    return status_code in {408, 425, 429, 500, 502, 503, 504}


def parse_retry_after_seconds(
    headers: Mapping[str, str] | Message | None,
    *,
    now: datetime | None = None,
) -> float | None:
    if headers is None:
        return None
    raw_value = headers.get("Retry-After") or headers.get("retry-after")
    if not raw_value:
        return None
    value = str(raw_value).strip()
    if value.isdigit():
        return float(value)
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    reference = now or utcnow()
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at.astimezone(timezone.utc) - reference).total_seconds())


def read_bounded(response: FetchResponse, max_response_bytes: int) -> bytes:
    raw_length = response.headers.get("Content-Length")
    if raw_length:
        try:
            declared_length = int(raw_length)
        except ValueError:
            declared_length = None
        if declared_length is not None and declared_length > max_response_bytes:
            raise ResponseTooLargeError(
                f"Response Content-Length {declared_length} exceeds limit {max_response_bytes}."
            )

    chunks: list[bytes] = []
    received = 0
    while True:
        chunk = response.read(min(64 * 1024, max_response_bytes - received + 1))
        if not chunk:
            break
        received += len(chunk)
        if received > max_response_bytes:
            raise ResponseTooLargeError(
                f"Response exceeded configured limit of {max_response_bytes} bytes."
            )
        chunks.append(chunk)
    return b"".join(chunks)


def fetch_url(
    url: str,
    *,
    policy: FetchPolicy | None = None,
    request_headers: Mapping[str, str] | None = None,
    resolver: Resolver = socket.getaddrinfo,
    opener: OpenerDirector | None = None,
    sleep_fn: Callable[[float], object] = time.sleep,
    monotonic_fn: Callable[[], float] = time.monotonic,
) -> FetchResult:
    normalized_policy = (policy or FetchPolicy()).normalized()
    address_pool = ValidatedAddressPool(
        resolver=resolver,
        allow_private_networks=normalized_policy.allow_private_networks,
    )
    address_pool.validate_url(url)
    active_opener = opener or build_safe_opener(
        normalized_policy,
        resolver=resolver,
        address_pool=address_pool,
    )
    headers = {
        "User-Agent": normalized_policy.user_agent,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        **{str(key): str(value) for key, value in (request_headers or {}).items()},
    }
    last_error: Exception | None = None

    for attempt in range(1, normalized_policy.retry_attempts + 1):
        started = monotonic_fn()
        request = Request(url, headers=headers, method="GET")
        retry_after_delay: float | None = None
        try:
            with active_opener.open(request, timeout=normalized_policy.timeout_seconds) as response:
                final_url = response.geturl() or url
                address_pool.validate_url(final_url)
                payload = read_bounded(response, normalized_policy.max_response_bytes)
                status_code = int(
                    getattr(response, "status", None)
                    or getattr(response, "code", None)
                    or 200
                )
                return FetchResult(
                    requested_url=url,
                    final_url=final_url,
                    status_code=status_code,
                    headers=sanitize_response_headers(response.headers),
                    payload=payload,
                    elapsed_ms=round(max(0.0, monotonic_fn() - started) * 1000.0, 3),
                    attempt_count=attempt,
                )
        except HTTPError as exc:
            last_error = exc
            if not should_retry(exc.code) or attempt >= normalized_policy.retry_attempts:
                break
            retry_after_delay = parse_retry_after_seconds(exc.headers)
        except (URLError, TimeoutError, OSError, DiscoveryFetchError) as exc:
            last_error = exc
            if isinstance(exc, (UnsafeTargetError, ResponseTooLargeError)):
                break
            if attempt >= normalized_policy.retry_attempts:
                break

        delay = normalized_policy.retry_backoff_seconds * attempt
        if retry_after_delay is not None:
            # Avoid one synchronous fetch sleeping indefinitely; longer retry windows
            # are handed back to the frontier after this bounded courtesy delay.
            delay = max(delay, min(retry_after_delay, 60.0))
        if delay > 0:
            sleep_fn(delay)

    assert last_error is not None
    if isinstance(last_error, DiscoveryFetchError):
        raise last_error
    if isinstance(last_error, HTTPError):
        raise HTTPStatusFetchError(
            last_error.code,
            f"Discovery fetch returned HTTP {last_error.code} after "
            f"{attempt} attempts: {last_error.reason}",
        ) from last_error
    raise DiscoveryFetchError(
        f"Discovery fetch failed after {normalized_policy.retry_attempts} attempts: {last_error}"
    ) from last_error


def apply_domain_pacing(
    last_request_at: datetime | None,
    min_delay_seconds: float,
    *,
    now_fn: Callable[[], datetime] = utcnow,
    sleep_fn: Callable[[float], object] = time.sleep,
) -> float:
    if last_request_at is None or min_delay_seconds <= 0:
        return 0.0
    normalized_last = (
        last_request_at
        if last_request_at.tzinfo is not None
        else last_request_at.replace(tzinfo=timezone.utc)
    )
    elapsed = max(0.0, (now_fn() - normalized_last).total_seconds())
    delay = max(0.0, float(min_delay_seconds) - elapsed)
    if delay > 0:
        sleep_fn(delay)
    return delay
