"""Deterministic, local-only media validation and content-addressed intake.

This module intentionally does not fetch URLs, invoke codecs, or invoke models.  It is
the small, safe boundary that workers can use *after* a downloader has supplied bytes.
Its results are immutable/auditable enough to be persisted by the storage ledger.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import struct
import zlib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit


MediaKind = Literal["image", "video", "audio"]


@dataclass(frozen=True)
class MediaLimits:
    """Conservative resource limits, all enforced before persistence."""

    max_bytes: int = 80 * 1024 * 1024
    max_image_width: int = 12_000
    max_image_height: int = 12_000
    max_image_pixels: int = 48_000_000
    max_duration_seconds: float = 4 * 60 * 60
    max_decoded_image_bytes: int = 256 * 1024 * 1024
    near_duplicate_hamming_distance: int = 8


@dataclass(frozen=True)
class WebImageReference:
    image_url: str
    source_page_url: str | None
    normalized_image_url: str
    normalized_source_page_url: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MediaIntakeResult:
    status: Literal["accepted", "quarantined"]
    reason_codes: tuple[str, ...]
    media_kind: MediaKind | None
    media_type: str | None
    byte_size: int
    sha256: str
    blake2b_256: str
    perceptual_hash: str | None
    duplicate_of: str | None
    near_duplicate_of: str | None
    stored_path: Path | None
    quarantine_path: Path | None
    metadata: dict[str, Any]
    transform_chain: tuple[dict[str, Any], ...]
    source_uri: str | None = None
    source_page_uri: str | None = None

    @property
    def accepted(self) -> bool:
        return self.status == "accepted"

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["stored_path"] = str(self.stored_path) if self.stored_path else None
        value["quarantine_path"] = str(self.quarantine_path) if self.quarantine_path else None
        return value


class MediaIntakeService:
    """A filesystem-backed, deterministic intake service.

    ``root`` is owned by this service.  Accepted originals are stored under
    ``hot/sha256/<first-two>/<next-two>/<digest>`` and rejected payloads under
    ``quarantine/``.  The JSON index only contains derived identifiers, never bytes.
    """

    _INDEX_VERSION = 1

    def __init__(self, root: Path | str, *, limits: MediaLimits | None = None) -> None:
        self.root = Path(root).expanduser().resolve()
        self.limits = limits or MediaLimits()
        self.hot_root = self.root / "hot" / "sha256"
        self.quarantine_root = self.root / "quarantine"
        self.index_path = self.root / "media-index.json"
        self.hot_root.mkdir(parents=True, exist_ok=True)
        self.quarantine_root.mkdir(parents=True, exist_ok=True)

    def ingest_bytes(
        self,
        payload: bytes,
        *,
        claimed_mime: str | None = None,
        source_uri: str | None = None,
        source_page_uri: str | None = None,
    ) -> MediaIntakeResult:
        """Validate and ingest already-downloaded bytes without external side effects."""
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")
        sha256 = hashlib.sha256(payload).hexdigest()
        blake2b_256 = hashlib.blake2b(payload, digest_size=32).hexdigest()
        normalized_claim = _normalise_mime(claimed_mime)
        base_chain = _base_chain(sha256, len(payload), normalized_claim)

        if len(payload) > self.limits.max_bytes:
            return self._quarantine(
                payload, sha256, blake2b_256, ("byte_limit_exceeded",), base_chain,
                source_uri=source_uri, source_page_uri=source_page_uri,
            )

        detected = _detect_media(payload)
        if detected is None:
            return self._quarantine(
                payload, sha256, blake2b_256, ("unrecognized_signature",), base_chain,
                source_uri=source_uri, source_page_uri=source_page_uri,
            )
        kind, mime, parser = detected
        if normalized_claim and normalized_claim not in _MIME_ALIASES[mime]:
            return self._quarantine(
                payload, sha256, blake2b_256, ("mime_signature_mismatch",), base_chain,
                source_uri=source_uri, source_page_uri=source_page_uri,
                detected_kind=kind, detected_mime=mime,
            )

        try:
            metadata = parser(payload, self.limits)
            _enforce_metadata_limits(kind, metadata, self.limits)
        except MediaValidationError as error:
            return self._quarantine(
                payload, sha256, blake2b_256, (error.code,), base_chain,
                source_uri=source_uri, source_page_uri=source_page_uri,
                detected_kind=kind, detected_mime=mime,
            )

        perceptual_hash = _perceptual_hash(payload, mime, metadata, self.limits)
        chain = base_chain + (
            {"step": "signature_validation", "status": "passed", "detected_mime": mime},
            {"step": "metadata_extraction", "status": "passed", "metadata": metadata},
            {
                "step": "perceptual_fingerprint",
                "status": "available" if perceptual_hash else "fallback_unavailable",
                "algorithm": "png-ahash-v1" if mime == "image/png" and perceptual_hash else None,
            },
        )
        index = self._load_index()
        records: dict[str, Any] = index["records"]
        existing = records.get(sha256)
        if existing:
            return MediaIntakeResult(
                status="accepted", reason_codes=("exact_duplicate",), media_kind=kind,
                media_type=mime, byte_size=len(payload), sha256=sha256,
                blake2b_256=blake2b_256, perceptual_hash=perceptual_hash,
                duplicate_of=sha256, near_duplicate_of=None,
                stored_path=Path(existing["stored_path"]), quarantine_path=None,
                metadata=metadata, transform_chain=chain + ({"step": "dedupe", "result": "exact"},),
                source_uri=source_uri, source_page_uri=source_page_uri,
            )
        near_duplicate = self._find_near_duplicate(perceptual_hash, records)
        stored_path = self._object_path(sha256)
        _write_once(stored_path, payload)
        records[sha256] = {
            "stored_path": str(stored_path), "media_kind": kind, "media_type": mime,
            "perceptual_hash": perceptual_hash,
        }
        self._write_index(index)
        dedupe_result = "near" if near_duplicate else "unique"
        return MediaIntakeResult(
            status="accepted", reason_codes=(("near_duplicate",) if near_duplicate else ()),
            media_kind=kind, media_type=mime, byte_size=len(payload), sha256=sha256,
            blake2b_256=blake2b_256, perceptual_hash=perceptual_hash,
            duplicate_of=None, near_duplicate_of=near_duplicate, stored_path=stored_path,
            quarantine_path=None, metadata=metadata,
            transform_chain=chain + ({"step": "dedupe", "result": dedupe_result},),
            source_uri=source_uri, source_page_uri=source_page_uri,
        )

    def validate_web_image_reference(
        self, image_url: str, *, source_page_url: str | None = None
    ) -> WebImageReference:
        """Validate/canonicalise public references without downloading either URL."""
        normalized_image = _normalise_public_http_url(image_url)
        normalized_page = _normalise_public_http_url(source_page_url) if source_page_url else None
        return WebImageReference(
            image_url=image_url, source_page_url=source_page_url,
            normalized_image_url=normalized_image, normalized_source_page_url=normalized_page,
        )

    def ingest_web_image_reference(
        self, image_url: str, *, source_page_url: str | None = None
    ) -> WebImageReference:
        """Alias kept explicit for pipeline call sites: references are not fetched here."""
        return self.validate_web_image_reference(image_url, source_page_url=source_page_url)

    def _quarantine(
        self, payload: bytes, sha256: str, blake2b_256: str, reason_codes: tuple[str, ...],
        chain: tuple[dict[str, Any], ...], *, source_uri: str | None, source_page_uri: str | None,
        detected_kind: MediaKind | None = None, detected_mime: str | None = None,
    ) -> MediaIntakeResult:
        payload_path = self.quarantine_root / f"{sha256}.bin"
        _write_once(payload_path, payload)
        receipt_path = self.quarantine_root / f"{sha256}.json"
        receipt = {
            "sha256": sha256, "blake2b_256": blake2b_256, "byte_size": len(payload),
            "reason_codes": list(reason_codes), "source_uri": source_uri,
            "source_page_uri": source_page_uri, "detected_mime": detected_mime,
        }
        _write_json_once(receipt_path, receipt)
        return MediaIntakeResult(
            status="quarantined", reason_codes=reason_codes, media_kind=detected_kind,
            media_type=detected_mime, byte_size=len(payload), sha256=sha256,
            blake2b_256=blake2b_256, perceptual_hash=None, duplicate_of=None,
            near_duplicate_of=None, stored_path=None, quarantine_path=payload_path,
            metadata={}, transform_chain=chain + ({"step": "quarantine", "reason_codes": list(reason_codes)},),
            source_uri=source_uri, source_page_uri=source_page_uri,
        )

    def _object_path(self, sha256: str) -> Path:
        return self.hot_root / sha256[:2] / sha256[2:4] / sha256

    def _load_index(self) -> dict[str, Any]:
        if not self.index_path.exists():
            return {"version": self._INDEX_VERSION, "records": {}}
        try:
            value = json.loads(self.index_path.read_text(encoding="utf-8"))
            if value.get("version") != self._INDEX_VERSION or not isinstance(value.get("records"), dict):
                raise ValueError("invalid index")
            return value
        except (OSError, ValueError, json.JSONDecodeError):
            # A corrupt index must never make untrusted media trusted.  Keep bytes safe and
            # rebuild the index as new intake occurs; exact content-addressed paths still hold.
            return {"version": self._INDEX_VERSION, "records": {}}

    def _write_index(self, index: dict[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = self.index_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(index, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        os.replace(temporary, self.index_path)

    def _find_near_duplicate(self, perceptual_hash: str | None, records: dict[str, Any]) -> str | None:
        if perceptual_hash is None:
            return None
        candidates = []
        for sha256, record in records.items():
            existing = record.get("perceptual_hash")
            if isinstance(existing, str) and len(existing) == len(perceptual_hash):
                distance = _hamming_distance(existing, perceptual_hash)
                if distance <= self.limits.near_duplicate_hamming_distance:
                    candidates.append((distance, sha256))
        return min(candidates)[1] if candidates else None


class MediaValidationError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


_MIME_ALIASES: dict[str, set[str]] = {
    "image/jpeg": {"image/jpeg", "image/jpg", "image/pjpeg"},
    "image/png": {"image/png"}, "image/gif": {"image/gif"}, "image/webp": {"image/webp"},
    "video/mp4": {"video/mp4", "video/quicktime"}, "audio/wav": {"audio/wav", "audio/x-wav", "audio/wave"},
    "audio/mpeg": {"audio/mpeg", "audio/mp3"}, "audio/flac": {"audio/flac", "audio/x-flac"},
}


def _normalise_mime(value: str | None) -> str | None:
    if not value:
        return None
    return value.split(";", 1)[0].strip().lower() or None


def _base_chain(sha256: str, byte_size: int, claimed_mime: str | None) -> tuple[dict[str, Any], ...]:
    return ({"step": "received", "sha256": sha256, "byte_size": byte_size}, {"step": "claimed_mime", "value": claimed_mime})


def _detect_media(payload: bytes) -> tuple[MediaKind, str, Any] | None:
    if payload.startswith(b"\xff\xd8\xff"):
        return "image", "image/jpeg", _parse_jpeg
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image", "image/png", _parse_png
    if payload.startswith((b"GIF87a", b"GIF89a")):
        return "image", "image/gif", _parse_gif
    if len(payload) >= 12 and payload[:4] == b"RIFF" and payload[8:12] == b"WEBP":
        return "image", "image/webp", _parse_webp
    if len(payload) >= 12 and payload[4:8] == b"ftyp":
        return "video", "video/mp4", _parse_mp4
    if len(payload) >= 12 and payload[:4] == b"RIFF" and payload[8:12] == b"WAVE":
        return "audio", "audio/wav", _parse_wav
    if payload.startswith(b"fLaC"):
        return "audio", "audio/flac", _parse_flac
    if payload.startswith(b"ID3") or _looks_like_mp3_frame(payload):
        return "audio", "audio/mpeg", _parse_mp3
    return None


def _parse_png(payload: bytes, limits: MediaLimits) -> dict[str, Any]:
    if len(payload) < 33 or payload[12:16] != b"IHDR" or struct.unpack(">I", payload[8:12])[0] != 13:
        raise MediaValidationError("malformed_png")
    width, height, depth, color_type, compression, filtering, interlace = struct.unpack(">IIBBBBB", payload[16:29])
    if (
        not width
        or not height
        or depth not in {1, 2, 4, 8, 16}
        or color_type not in {0, 2, 3, 4, 6}
        or compression != 0
        or filtering != 0
        or interlace not in {0, 1}
    ):
        raise MediaValidationError("malformed_png")
    position, saw_idat, saw_iend = 8, False, False
    while position + 12 <= len(payload):
        length = struct.unpack(">I", payload[position:position + 4])[0]
        end = position + 12 + length
        if end > len(payload):
            raise MediaValidationError("malformed_png")
        chunk_type = payload[position + 4:position + 8]
        chunk_data = payload[position + 8:position + 8 + length]
        chunk_crc = struct.unpack(">I", payload[position + 8 + length:end])[0]
        if zlib.crc32(chunk_type + chunk_data) & 0xFFFFFFFF != chunk_crc:
            raise MediaValidationError("malformed_png")
        if chunk_type == b"IDAT":
            saw_idat = True
        if chunk_type == b"IEND":
            if length != 0 or not saw_idat or end != len(payload):
                raise MediaValidationError("malformed_png")
            saw_iend = True
            break
        position = end
    if not saw_iend:
        raise MediaValidationError("malformed_png")
    return {"width": width, "height": height, "bit_depth": depth, "color_type": color_type, "interlace": interlace}


def _parse_gif(payload: bytes, limits: MediaLimits) -> dict[str, Any]:
    if len(payload) < 10:
        raise MediaValidationError("malformed_gif")
    width, height = struct.unpack("<HH", payload[6:10])
    if not width or not height or payload[-1:] != b";":
        raise MediaValidationError("malformed_gif")
    return {"width": width, "height": height}


def _parse_jpeg(payload: bytes, limits: MediaLimits) -> dict[str, Any]:
    if not payload.endswith(b"\xff\xd9"):
        raise MediaValidationError("malformed_jpeg")
    position = 2
    while position + 4 <= len(payload):
        if payload[position] != 0xFF:
            raise MediaValidationError("malformed_jpeg")
        while position < len(payload) and payload[position] == 0xFF:
            position += 1
        if position >= len(payload):
            break
        marker = payload[position]
        position += 1
        if marker in {0xD8, 0xD9} or 0xD0 <= marker <= 0xD7:
            continue
        if position + 2 > len(payload):
            raise MediaValidationError("malformed_jpeg")
        length = struct.unpack(">H", payload[position:position + 2])[0]
        if length < 2 or position + length > len(payload):
            raise MediaValidationError("malformed_jpeg")
        if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
            if length < 8:
                raise MediaValidationError("malformed_jpeg")
            height, width = struct.unpack(">HH", payload[position + 3:position + 7])
            if not width or not height:
                raise MediaValidationError("malformed_jpeg")
            return {"width": width, "height": height, "precision": payload[position + 2]}
        position += length
    raise MediaValidationError("jpeg_dimensions_missing")


def _parse_webp(payload: bytes, limits: MediaLimits) -> dict[str, Any]:
    if len(payload) < 30:
        raise MediaValidationError("malformed_webp")
    chunk = payload[12:16]
    if chunk == b"VP8X" and len(payload) >= 30:
        width = int.from_bytes(payload[24:27], "little") + 1
        height = int.from_bytes(payload[27:30], "little") + 1
    elif chunk == b"VP8L" and len(payload) >= 25 and payload[20] == 0x2F:
        bits = int.from_bytes(payload[21:25], "little")
        width, height = (bits & 0x3FFF) + 1, ((bits >> 14) & 0x3FFF) + 1
    elif chunk == b"VP8 " and len(payload) >= 30 and payload[23:26] == b"\x9d\x01\x2a":
        width = struct.unpack("<H", payload[26:28])[0] & 0x3FFF
        height = struct.unpack("<H", payload[28:30])[0] & 0x3FFF
    else:
        raise MediaValidationError("malformed_webp")
    return {"width": width, "height": height}


def _parse_wav(payload: bytes, limits: MediaLimits) -> dict[str, Any]:
    position, channels, sample_rate, bits_per_sample, data_bytes = 12, None, None, None, None
    while position + 8 <= len(payload):
        chunk_id, chunk_size = payload[position:position + 4], struct.unpack("<I", payload[position + 4:position + 8])[0]
        end = position + 8 + chunk_size
        if end > len(payload):
            raise MediaValidationError("malformed_wav")
        if chunk_id == b"fmt " and chunk_size >= 16:
            _, channels, sample_rate, _, _, bits_per_sample = struct.unpack("<HHIIHH", payload[position + 8:position + 24])
        elif chunk_id == b"data":
            data_bytes = chunk_size
        position = end + (chunk_size % 2)
    if not channels or not sample_rate or not bits_per_sample or data_bytes is None:
        raise MediaValidationError("wav_metadata_missing")
    bytes_per_second = channels * sample_rate * bits_per_sample / 8
    if bytes_per_second <= 0:
        raise MediaValidationError("malformed_wav")
    return {"duration_seconds": round(data_bytes / bytes_per_second, 6), "sample_rate": sample_rate, "channels": channels, "bits_per_sample": bits_per_sample}


def _parse_flac(payload: bytes, limits: MediaLimits) -> dict[str, Any]:
    if len(payload) < 42 or payload[4] & 0x7F != 0 or int.from_bytes(payload[5:8], "big") != 34:
        raise MediaValidationError("malformed_flac")
    value = int.from_bytes(payload[18:26], "big")
    sample_rate, channels, total_samples = value >> 44, ((value >> 41) & 7) + 1, value & ((1 << 36) - 1)
    if not sample_rate or not total_samples:
        raise MediaValidationError("flac_duration_missing")
    return {"duration_seconds": round(total_samples / sample_rate, 6), "sample_rate": sample_rate, "channels": channels}


def _looks_like_mp3_frame(payload: bytes) -> bool:
    return len(payload) >= 4 and payload[0] == 0xFF and (payload[1] & 0xE0) == 0xE0 and ((payload[1] >> 1) & 3) != 0


def _parse_mp3(payload: bytes, limits: MediaLimits) -> dict[str, Any]:
    offset = 10 + _synchsafe(payload[6:10]) if payload.startswith(b"ID3") and len(payload) >= 10 else 0
    if offset + 4 > len(payload) or not _looks_like_mp3_frame(payload[offset:]):
        raise MediaValidationError("malformed_mp3")
    header = int.from_bytes(payload[offset:offset + 4], "big")
    version_bits, layer_bits, bitrate_index, sample_index = (header >> 19) & 3, (header >> 17) & 3, (header >> 12) & 15, (header >> 10) & 3
    if layer_bits != 1 or bitrate_index in {0, 15} or sample_index == 3:
        raise MediaValidationError("mp3_metadata_missing")
    # Layer III bit-rate tables (kbps), enough for a bounded duration estimate.
    table = [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0] if version_bits == 3 else [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0]
    bitrate = table[bitrate_index] * 1000
    if bitrate == 0:
        raise MediaValidationError("mp3_metadata_missing")
    return {"duration_seconds": round((len(payload) - offset) * 8 / bitrate, 6), "bitrate": bitrate, "duration_estimate": True}


def _synchsafe(value: bytes) -> int:
    return sum((byte & 0x7F) << (7 * (3 - index)) for index, byte in enumerate(value))


def _parse_mp4(payload: bytes, limits: MediaLimits) -> dict[str, Any]:
    duration = _mp4_mvhd_duration(payload, 0, len(payload))
    if duration is None:
        raise MediaValidationError("mp4_duration_missing")
    return {"duration_seconds": round(duration, 6)}


def _mp4_mvhd_duration(payload: bytes, start: int, end: int) -> float | None:
    position = start
    while position + 8 <= end:
        size, atom = struct.unpack(">I4s", payload[position:position + 8])
        header = 8
        if size == 1:
            if position + 16 > end:
                raise MediaValidationError("malformed_mp4")
            size, header = struct.unpack(">Q", payload[position + 8:position + 16])[0], 16
        if size == 0:
            size = end - position
        if size < header or position + size > end:
            raise MediaValidationError("malformed_mp4")
        content_start, content_end = position + header, position + size
        if atom == b"mvhd":
            if content_start + 20 > content_end:
                raise MediaValidationError("malformed_mp4")
            version = payload[content_start]
            if version == 0:
                timescale, duration = struct.unpack(">II", payload[content_start + 12:content_start + 20])
            elif version == 1 and content_start + 32 <= content_end:
                timescale = struct.unpack(">I", payload[content_start + 20:content_start + 24])[0]
                duration = struct.unpack(">Q", payload[content_start + 24:content_start + 32])[0]
            else:
                raise MediaValidationError("malformed_mp4")
            if not timescale or duration == 0xFFFFFFFFFFFFFFFF:
                raise MediaValidationError("mp4_duration_missing")
            return duration / timescale
        if atom in {b"moov", b"trak", b"mdia", b"udta"}:
            found = _mp4_mvhd_duration(payload, content_start, content_end)
            if found is not None:
                return found
        position += size
    return None


def _enforce_metadata_limits(kind: MediaKind, metadata: dict[str, Any], limits: MediaLimits) -> None:
    if kind == "image":
        width, height = int(metadata.get("width", 0)), int(metadata.get("height", 0))
        if width > limits.max_image_width or height > limits.max_image_height or width * height > limits.max_image_pixels:
            raise MediaValidationError("image_dimension_limit_exceeded")
    else:
        duration = metadata.get("duration_seconds")
        if not isinstance(duration, (int, float)) or duration < 0:
            raise MediaValidationError("duration_missing")
        if duration > limits.max_duration_seconds:
            raise MediaValidationError("duration_limit_exceeded")


def _perceptual_hash(payload: bytes, mime: str, metadata: dict[str, Any], limits: MediaLimits) -> str | None:
    if mime != "image/png" or metadata.get("interlace") != 0:
        return None
    try:
        pixels = _decode_png_luma(payload, metadata, limits)
    except (MediaValidationError, zlib.error, ValueError):
        return None
    width, height = int(metadata["width"]), int(metadata["height"])
    samples = []
    for y in range(8):
        row = min(height - 1, (y * height) // 8)
        for x in range(8):
            column = min(width - 1, (x * width) // 8)
            samples.append(pixels[row * width + column])
    average = sum(samples) / len(samples)
    return "".join("1" if value >= average else "0" for value in samples)


def _decode_png_luma(payload: bytes, metadata: dict[str, Any], limits: MediaLimits) -> list[int]:
    width, height, depth, color_type = (int(metadata["width"]), int(metadata["height"]), int(metadata["bit_depth"]), int(metadata["color_type"]))
    channels = {0: 1, 2: 3, 4: 2, 6: 4}.get(color_type)
    if channels is None or depth != 8:
        raise MediaValidationError("png_fingerprint_unsupported")
    stride = width * channels
    expected = height * (stride + 1)
    if expected > limits.max_decoded_image_bytes:
        raise MediaValidationError("decoded_image_limit_exceeded")
    position, idat = 8, []
    while position + 12 <= len(payload):
        length = struct.unpack(">I", payload[position:position + 4])[0]
        end = position + 12 + length
        if end > len(payload):
            raise MediaValidationError("malformed_png")
        chunk_type = payload[position + 4:position + 8]
        if chunk_type == b"IDAT":
            idat.append(payload[position + 8:position + 8 + length])
        if chunk_type == b"IEND":
            break
        position = end
    decoded = zlib.decompress(b"".join(idat))
    if len(decoded) != expected:
        raise MediaValidationError("malformed_png")
    previous, luma, position = bytearray(stride), [], 0
    for _ in range(height):
        filter_type, raw = decoded[position], bytearray(decoded[position + 1:position + 1 + stride])
        position += stride + 1
        for index in range(stride):
            left = raw[index - channels] if index >= channels else 0
            up = previous[index]
            upper_left = previous[index - channels] if index >= channels else 0
            if filter_type == 1:
                raw[index] = (raw[index] + left) & 255
            elif filter_type == 2:
                raw[index] = (raw[index] + up) & 255
            elif filter_type == 3:
                raw[index] = (raw[index] + ((left + up) // 2)) & 255
            elif filter_type == 4:
                raw[index] = (raw[index] + _paeth(left, up, upper_left)) & 255
            elif filter_type != 0:
                raise MediaValidationError("malformed_png")
        for column in range(width):
            pixel = raw[column * channels:column * channels + channels]
            if color_type in {0, 4}:
                luma.append(pixel[0])
            else:
                luma.append((299 * pixel[0] + 587 * pixel[1] + 114 * pixel[2]) // 1000)
        previous = raw
    return luma


def _paeth(left: int, up: int, upper_left: int) -> int:
    prediction = left + up - upper_left
    distances = abs(prediction - left), abs(prediction - up), abs(prediction - upper_left)
    return left if distances[0] <= distances[1] and distances[0] <= distances[2] else up if distances[1] <= distances[2] else upper_left


def _hamming_distance(first: str, second: str) -> int:
    return sum(a != b for a, b in zip(first, second))


def _write_once(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
    except FileExistsError:
        # Content-addressed destination may only be reused when it proves identical.
        if hashlib.sha256(path.read_bytes()).hexdigest() != hashlib.sha256(payload).hexdigest():
            raise RuntimeError(f"content-addressed path collision at {path}")


def _write_json_once(path: Path, value: dict[str, Any]) -> None:
    try:
        with path.open("x", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True, separators=(",", ":"))
    except FileExistsError:
        return


def _normalise_public_http_url(value: str | None) -> str:
    if not isinstance(value, str) or not value or len(value) > 4096 or re.search(r"[\x00-\x20]", value):
        raise ValueError("URL must be a non-empty public http(s) URL")
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("URL must be a public http(s) URL without credentials")
    try:
        address = ipaddress.ip_address(parsed.hostname)
        if not address.is_global:
            raise ValueError("URL host must not be a private or reserved IP address")
    except ValueError as error:
        # A hostname is intentionally not DNS-resolved here; fetching policy owns that later.
        if str(error).startswith("URL host"):
            raise
    hostname = parsed.hostname.lower().rstrip(".")
    port = parsed.port
    netloc = hostname if port is None or (parsed.scheme == "http" and port == 80) or (parsed.scheme == "https" and port == 443) else f"{hostname}:{port}"
    return urlunsplit((parsed.scheme.lower(), netloc, parsed.path or "/", parsed.query, ""))
