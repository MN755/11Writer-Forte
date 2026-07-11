from __future__ import annotations

import struct
import zlib
from pathlib import Path

import pytest

from src.services.media_intake_service import MediaIntakeService, MediaLimits


def _png(width: int, height: int, pixels: bytes) -> bytes:
    assert len(pixels) == width * height
    raw = b"".join(b"\x00" + pixels[row * width:(row + 1) * width] for row in range(height))
    def chunk(kind: bytes, value: bytes) -> bytes:
        return struct.pack(">I", len(value)) + kind + value + struct.pack(">I", zlib.crc32(kind + value) & 0xFFFFFFFF)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")


def _wav(seconds: int = 1) -> bytes:
    data = b"\x00\x00" * (8_000 * seconds)
    fmt = struct.pack("<HHIIHH", 1, 1, 8_000, 16_000, 2, 16)
    body = b"fmt " + struct.pack("<I", len(fmt)) + fmt + b"data" + struct.pack("<I", len(data)) + data
    return b"RIFF" + struct.pack("<I", len(body) + 4) + b"WAVE" + body


def test_malformed_spoofed_and_oversized_inputs_quarantine(tmp_path: Path) -> None:
    service = MediaIntakeService(tmp_path / "media", limits=MediaLimits(max_bytes=100))
    malformed = service.ingest_bytes(b"not actually an image", claimed_mime="image/png")
    assert malformed.status == "quarantined"
    assert malformed.reason_codes == ("unrecognized_signature",)
    spoofed = service.ingest_bytes(_png(2, 2, b"\x00" * 4), claimed_mime="audio/mpeg")
    assert spoofed.status == "quarantined"
    assert spoofed.reason_codes == ("mime_signature_mismatch",)
    oversize = service.ingest_bytes(b"x" * 101)
    assert oversize.reason_codes == ("byte_limit_exceeded",)
    assert malformed.quarantine_path and malformed.quarantine_path.exists()
    truncated_png = service.ingest_bytes(_png(2, 2, b"\x00" * 4)[:-1])
    assert truncated_png.reason_codes == ("malformed_png",)


def test_dimension_and_duration_safeguards_quarantine(tmp_path: Path) -> None:
    service = MediaIntakeService(tmp_path / "media", limits=MediaLimits(max_image_pixels=16, max_duration_seconds=1.5))
    giant = service.ingest_bytes(_png(5, 5, b"\x00" * 25))
    assert giant.reason_codes == ("image_dimension_limit_exceeded",)
    long_audio = service.ingest_bytes(_wav(2))
    assert long_audio.reason_codes == ("duration_limit_exceeded",)


def test_exact_duplicate_reuses_content_addressed_blob_and_keeps_audit_chain(tmp_path: Path) -> None:
    service = MediaIntakeService(tmp_path / "media")
    image = _png(8, 8, bytes(range(64)))
    first = service.ingest_bytes(image, source_uri="https://camera.example/a.jpg")
    second = service.ingest_bytes(image, source_uri="https://camera.example/b.jpg")
    assert first.accepted and second.accepted
    assert first.stored_path == second.stored_path
    assert second.duplicate_of == first.sha256
    assert second.transform_chain[-1] == {"step": "dedupe", "result": "exact"}
    assert len(list((tmp_path / "media" / "hot").rglob("*"))) > 0


def test_changed_png_gets_feature_record_and_near_duplicate_flag(tmp_path: Path) -> None:
    service = MediaIntakeService(tmp_path / "media", limits=MediaLimits(near_duplicate_hamming_distance=8))
    original = _png(8, 8, bytes([0] * 32 + [255] * 32))
    # One pixel changes bytes/hash but not the deterministic 8x8 visual fingerprint.
    changed_pixels = bytearray([0] * 32 + [255] * 32)
    changed_pixels[0] = 1
    changed = _png(8, 8, bytes(changed_pixels))
    first = service.ingest_bytes(original)
    second = service.ingest_bytes(changed)
    assert first.sha256 != second.sha256
    assert second.near_duplicate_of == first.sha256
    assert second.perceptual_hash is not None
    assert any(step["step"] == "metadata_extraction" for step in second.transform_chain)
    assert second.transform_chain[-1] == {"step": "dedupe", "result": "near"}


def test_web_image_refs_are_local_only_and_reject_private_targets(tmp_path: Path) -> None:
    service = MediaIntakeService(tmp_path / "media")
    reference = service.ingest_web_image_reference(
        "HTTPS://Images.Example.test:443/site.jpg#ignored", source_page_url="https://example.test/page"
    )
    assert reference.normalized_image_url == "https://images.example.test/site.jpg"
    assert reference.normalized_source_page_url == "https://example.test/page"
    with pytest.raises(ValueError):
        service.validate_web_image_reference("http://127.0.0.1/photo.jpg")
