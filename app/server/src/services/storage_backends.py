from __future__ import annotations

import hashlib
import hmac
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

from src.config import Settings, get_settings


@dataclass(frozen=True)
class StorageTransferResult:
    uri: str
    content_hash: str
    byte_size: int
    verified: bool
    backend: str
    metadata: dict[str, str]


def backend_now() -> datetime:
    return datetime.now(timezone.utc)


def hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def local_path_to_uri(path: Path) -> str:
    return path.resolve().as_uri()


def local_path_from_uri(uri: str) -> Path:
    if uri.startswith("file://"):
        parsed = urlparse(uri)
        raw_path = unquote(parsed.path or "")
        if parsed.netloc:
            raw_path = f"//{parsed.netloc}{raw_path}"
        if len(raw_path) >= 3 and raw_path[0] == "/" and raw_path[2] == ":":
            raw_path = raw_path[1:]
        return Path(raw_path)
    return Path(uri)


class StorageBackend(ABC):
    backend_name: str

    @abstractmethod
    def copy_from_local(
        self,
        source_path: Path,
        destination_uri: str,
        *,
        media_type: str | None = None,
        content_hash: str | None = None,
    ) -> StorageTransferResult:
        raise NotImplementedError

    @abstractmethod
    def verify(
        self,
        uri: str,
        *,
        expected_hash: str | None = None,
        expected_size: int | None = None,
    ) -> StorageTransferResult:
        raise NotImplementedError

    @abstractmethod
    def download_to_local(
        self,
        uri: str,
        destination_path: Path,
        *,
        expected_hash: str | None = None,
    ) -> StorageTransferResult:
        raise NotImplementedError

    @abstractmethod
    def exists(self, uri: str) -> bool:
        raise NotImplementedError


class LocalFilesystemBackend(StorageBackend):
    backend_name = "local"

    def copy_from_local(
        self,
        source_path: Path,
        destination_uri: str,
        *,
        media_type: str | None = None,
        content_hash: str | None = None,
    ) -> StorageTransferResult:
        del media_type
        destination_path = local_path_from_uri(destination_uri)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)
        return self.verify(
            destination_uri,
            expected_hash=content_hash or hash_file(source_path),
            expected_size=source_path.stat().st_size,
        )

    def verify(
        self,
        uri: str,
        *,
        expected_hash: str | None = None,
        expected_size: int | None = None,
    ) -> StorageTransferResult:
        path = local_path_from_uri(uri)
        if not path.exists() or not path.is_file():
            raise ValueError(f"Local storage artifact '{path}' does not exist.")
        byte_size = path.stat().st_size
        content_hash = hash_file(path)
        if expected_hash is not None and content_hash != expected_hash:
            raise ValueError(
                f"Checksum mismatch for local artifact '{path}': expected {expected_hash}, got {content_hash}."
            )
        if expected_size is not None and byte_size != expected_size:
            raise ValueError(
                f"Byte-size mismatch for local artifact '{path}': expected {expected_size}, got {byte_size}."
            )
        return StorageTransferResult(
            uri=local_path_to_uri(path),
            content_hash=content_hash,
            byte_size=byte_size,
            verified=True,
            backend=self.backend_name,
            metadata={},
        )

    def download_to_local(
        self,
        uri: str,
        destination_path: Path,
        *,
        expected_hash: str | None = None,
    ) -> StorageTransferResult:
        source_path = local_path_from_uri(uri)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)
        return self.verify(
            local_path_to_uri(destination_path),
            expected_hash=expected_hash,
            expected_size=source_path.stat().st_size,
        )

    def exists(self, uri: str) -> bool:
        path = local_path_from_uri(uri)
        return path.exists() and path.is_file()


class S3CompatibleBackend(StorageBackend):
    backend_name = "r2"

    def __init__(
        self,
        *,
        endpoint: str,
        bucket: str,
        access_key_id: str,
        secret_access_key: str,
        region: str,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.bucket = bucket
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region = region

    def build_object_uri(self, object_key: str) -> str:
        return f"s3://{self.bucket}/{object_key.lstrip('/')}"

    def copy_from_local(
        self,
        source_path: Path,
        destination_uri: str,
        *,
        media_type: str | None = None,
        content_hash: str | None = None,
    ) -> StorageTransferResult:
        payload = source_path.read_bytes()
        resolved_hash = content_hash or hash_bytes(payload)
        headers = {
            "Content-Type": media_type or "application/octet-stream",
            "x-amz-meta-sha256": resolved_hash,
            "x-amz-meta-byte-size": str(len(payload)),
        }
        self._request("PUT", destination_uri, data=payload, extra_headers=headers)
        return self.verify(destination_uri, expected_hash=resolved_hash, expected_size=len(payload))

    def verify(
        self,
        uri: str,
        *,
        expected_hash: str | None = None,
        expected_size: int | None = None,
    ) -> StorageTransferResult:
        response = self._request("HEAD", uri)
        metadata_hash = response.headers.get("x-amz-meta-sha256")
        byte_size = int(response.headers.get("Content-Length", "0") or 0)
        if expected_hash is not None and metadata_hash != expected_hash:
            raise ValueError(
                f"Checksum metadata mismatch for archived artifact '{uri}': expected {expected_hash}, got {metadata_hash}."
            )
        if expected_size is not None and byte_size != expected_size:
            raise ValueError(
                f"Byte-size mismatch for archived artifact '{uri}': expected {expected_size}, got {byte_size}."
            )
        return StorageTransferResult(
            uri=uri,
            content_hash=metadata_hash or "",
            byte_size=byte_size,
            verified=bool(metadata_hash),
            backend=self.backend_name,
            metadata={"etag": response.headers.get("ETag", "")},
        )

    def download_to_local(
        self,
        uri: str,
        destination_path: Path,
        *,
        expected_hash: str | None = None,
    ) -> StorageTransferResult:
        response = self._request("GET", uri)
        payload = response.read()
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        destination_path.write_bytes(payload)
        result = StorageTransferResult(
            uri=local_path_to_uri(destination_path),
            content_hash=hash_bytes(payload),
            byte_size=len(payload),
            verified=True,
            backend="local",
            metadata={},
        )
        if expected_hash is not None and result.content_hash != expected_hash:
            raise ValueError(
                f"Checksum mismatch while rehydrating '{uri}': expected {expected_hash}, got {result.content_hash}."
            )
        return result

    def exists(self, uri: str) -> bool:
        try:
            self._request("HEAD", uri)
        except Exception:
            return False
        return True

    def _request(
        self,
        method: str,
        uri: str,
        *,
        data: bytes | None = None,
        extra_headers: dict[str, str] | None = None,
    ):
        bucket, object_key = parse_s3_uri(uri)
        parsed_endpoint = urlparse(self.endpoint)
        host = parsed_endpoint.netloc
        encoded_key = quote(object_key, safe="/-_.~")
        canonical_uri = f"/{bucket}/{encoded_key}"
        url = f"{self.endpoint}{canonical_uri}"
        request_time = backend_now()
        amz_date = request_time.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = request_time.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(data or b"").hexdigest()
        headers = {"Host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amz_date}
        if extra_headers:
            headers.update(extra_headers)
        signed_headers = ";".join(key.lower() for key in sorted(headers))
        canonical_headers = "".join(f"{key.lower()}:{str(headers[key]).strip()}\n" for key in sorted(headers))
        canonical_request = (
            f"{method}\n"
            f"{canonical_uri}\n"
            "\n"
            f"{canonical_headers}\n"
            f"{signed_headers}\n"
            f"{payload_hash}"
        )
        credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = (
            "AWS4-HMAC-SHA256\n"
            f"{amz_date}\n"
            f"{credential_scope}\n"
            f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
        )
        signing_key = _get_signature_key(self.secret_access_key, date_stamp, self.region, "s3")
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        headers["Authorization"] = (
            "AWS4-HMAC-SHA256 "
            f"Credential={self.access_key_id}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
        request = Request(url, data=data, method=method)
        for key, value in headers.items():
            request.add_header(key, value)
        return urlopen(request, timeout=get_settings().clickhouse_timeout_seconds)


def _get_signature_key(secret_key: str, date_stamp: str, region_name: str, service_name: str) -> bytes:
    key_date = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    key_region = _sign(key_date, region_name)
    key_service = _sign(key_region, service_name)
    return _sign(key_service, "aws4_request")


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Unsupported S3 URI '{uri}'.")
    return parsed.netloc, parsed.path.lstrip("/")


def build_archive_backend(settings: Settings | None = None) -> StorageBackend:
    resolved_settings = settings or get_settings()
    if resolved_settings.storage_archive_backend == "r2":
        if not resolved_settings.storage_r2_configured:
            raise ValueError("Storage archive backend is set to R2, but R2 settings are incomplete.")
        return S3CompatibleBackend(
            endpoint=resolved_settings.storage_s3_endpoint_effective or "",
            bucket=resolved_settings.storage_s3_bucket_effective or "",
            access_key_id=resolved_settings.storage_s3_access_key_id_effective or "",
            secret_access_key=resolved_settings.storage_s3_secret_access_key_effective or "",
            region=resolved_settings.storage_s3_region_effective,
        )
    return LocalFilesystemBackend()


def build_local_backend() -> LocalFilesystemBackend:
    return LocalFilesystemBackend()
