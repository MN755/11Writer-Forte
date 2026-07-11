"""Approved local codecs and speech-to-text runtime; no runtime downloads allowed."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
import wave
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


class LocalMediaRuntimeError(RuntimeError):
    pass


@dataclass(frozen=True)
class LocalModelApproval:
    model_id: str
    version: str
    upstream_origin: str
    license_id: str
    model_tree_sha256: str
    package_lock_sha256: str
    sbom_sha256: str
    cve_review_ref: str
    hardware_requirement: str
    test_fixture_ref: str
    model_path: str
    runtime: str = "faster-whisper"
    network_disabled: bool = True


@dataclass(frozen=True)
class TranscriptResult:
    text: str
    language: str
    duration_seconds: float
    execution_device: str
    cpu_fallback: bool
    segment_count: int
    model_tree_sha256: str


@dataclass(frozen=True)
class TesseractApproval:
    engine_version: str
    executable_path: str
    executable_sha256: str
    tessdata_path: str
    tessdata_tree_sha256: str
    package_lock_sha256: str
    sbom_sha256: str
    cve_review_ref: str
    upstream_origin: str = "https://github.com/tesseract-ocr/tesseract"
    license_id: str = "Apache-2.0"
    network_disabled: bool = True


@dataclass(frozen=True)
class TesseractOcrResult:
    text: str
    language: str
    page_segmentation_mode: int
    execution_device: str
    engine_version: str
    executable_sha256: str
    tessdata_tree_sha256: str


def build_model_approval(
    model_path: str | Path,
    *,
    model_id: str = "Systran/faster-whisper-tiny.en",
    version: str = "tiny.en-ctranslate2",
    upstream_origin: str = "https://huggingface.co/Systran/faster-whisper-tiny.en",
    license_id: str = "MIT",
    package_lock_path: str | Path,
    sbom_path: str | Path,
    cve_review_ref: str,
    hardware_requirement: str = "Intel i9 13th-gen CPU; NVIDIA RTX 4070 Laptop optional",
    test_fixture_ref: str = "var/models/offline-silence.wav",
) -> LocalModelApproval:
    """Produce the required approval evidence from already-provisioned local files."""
    root = Path(model_path).resolve()
    if not (root / "model.bin").is_file() or not (root / "config.json").is_file():
        raise LocalMediaRuntimeError("Model directory is missing faster-whisper model.bin or config.json.")
    package_lock = Path(package_lock_path).resolve()
    sbom = Path(sbom_path).resolve()
    if not package_lock.is_file() or not sbom.is_file():
        raise LocalMediaRuntimeError("Package lock and SBOM must exist before approval.")
    if not cve_review_ref.strip():
        raise LocalMediaRuntimeError("A completed CVE review reference is required before approval.")
    return LocalModelApproval(
        model_id=model_id,
        version=version,
        upstream_origin=upstream_origin,
        license_id=license_id,
        model_tree_sha256=hash_tree(root),
        package_lock_sha256=hash_file(package_lock),
        sbom_sha256=hash_file(sbom),
        cve_review_ref=cve_review_ref,
        hardware_requirement=hardware_requirement,
        test_fixture_ref=test_fixture_ref,
        model_path=str(root),
    )


def write_model_approval(approval: LocalModelApproval, destination: str | Path) -> Path:
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(asdict(approval), indent=2, sort_keys=True), encoding="utf-8")
    return target


def build_tesseract_approval(
    executable_path: str | Path,
    tessdata_path: str | Path,
    *,
    package_lock_path: str | Path,
    sbom_path: str | Path,
    cve_review_ref: str,
) -> TesseractApproval:
    executable, tessdata = Path(executable_path).resolve(), Path(tessdata_path).resolve()
    package_lock, sbom = Path(package_lock_path).resolve(), Path(sbom_path).resolve()
    if not executable.is_file() or not (tessdata / "eng.traineddata").is_file():
        raise LocalMediaRuntimeError("Tesseract executable or Apache-licensed eng.traineddata is unavailable.")
    if not package_lock.is_file() or not sbom.is_file() or not cve_review_ref.strip():
        raise LocalMediaRuntimeError("Tesseract approval requires a lockfile, SBOM, and CVE review reference.")
    version = _run_command(executable, "--version").splitlines()[0].strip()
    return TesseractApproval(
        engine_version=version,
        executable_path=str(executable),
        executable_sha256=hash_file(executable),
        tessdata_path=str(tessdata),
        tessdata_tree_sha256=hash_tree(tessdata),
        package_lock_sha256=hash_file(package_lock),
        sbom_sha256=hash_file(sbom),
        cve_review_ref=cve_review_ref,
    )


def write_tesseract_approval(approval: TesseractApproval, destination: str | Path) -> Path:
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(asdict(approval), indent=2, sort_keys=True), encoding="utf-8")
    return target


def tesseract_ocr_offline(
    image_path: str | Path,
    *,
    approval_path: str | Path,
    language: str = "eng",
    page_segmentation_mode: int = 3,
) -> TesseractOcrResult:
    """Run a checksum-verified Tesseract LSTM OCR invocation with no network capability."""
    if not language.replace("+", "").replace("_", "").isalnum() or not 0 <= page_segmentation_mode <= 13:
        raise LocalMediaRuntimeError("Invalid Tesseract language or page segmentation mode.")
    try:
        raw = json.loads(Path(approval_path).read_text(encoding="utf-8"))
        approval = TesseractApproval(**raw)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LocalMediaRuntimeError(f"Invalid Tesseract approval record: {exc}") from exc
    executable, tessdata, image = (
        Path(approval.executable_path).resolve(),
        Path(approval.tessdata_path).resolve(),
        Path(image_path).resolve(),
    )
    if not approval.network_disabled or hash_file(executable) != approval.executable_sha256:
        raise LocalMediaRuntimeError("Tesseract executable is not approved or has changed.")
    if hash_tree(tessdata) != approval.tessdata_tree_sha256 or not image.is_file():
        raise LocalMediaRuntimeError("Tesseract language data changed or image input is missing.")
    text = _run_command(
        executable,
        str(image),
        "stdout",
        "--tessdata-dir",
        str(tessdata),
        "--oem",
        "1",
        "--psm",
        str(page_segmentation_mode),
        "-l",
        language,
    ).strip()
    return TesseractOcrResult(
        text=text,
        language=language,
        page_segmentation_mode=page_segmentation_mode,
        execution_device="cpu",
        engine_version=approval.engine_version,
        executable_sha256=approval.executable_sha256,
        tessdata_tree_sha256=approval.tessdata_tree_sha256,
    )


def transcribe_offline(
    audio_path: str | Path,
    *,
    approval_path: str | Path,
    prefer_gpu: bool = True,
) -> TranscriptResult:
    """Transcribe a local file with a verified model; network access is disabled first."""
    approval = load_model_approval(approval_path)
    model_path = Path(approval.model_path).resolve()
    if hash_tree(model_path) != approval.model_tree_sha256:
        raise LocalMediaRuntimeError("Local model checksum differs from its approval record.")
    audio = Path(audio_path).resolve()
    if not audio.is_file():
        raise LocalMediaRuntimeError(f"Audio input does not exist: {audio}")
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    try:
        from faster_whisper import WhisperModel
    except ImportError as exc:
        raise LocalMediaRuntimeError("faster-whisper is not installed in this runtime.") from exc

    def execute(device: str, compute_type: str) -> tuple[list[Any], Any, float]:
        model = WhisperModel(str(model_path), device=device, compute_type=compute_type, local_files_only=True)
        started = time.perf_counter()
        segments, info = model.transcribe(str(audio), beam_size=1, vad_filter=False)
        return list(segments), info, time.perf_counter() - started

    device, cpu_fallback = "cpu", False
    if prefer_gpu:
        try:
            rows, info, elapsed = execute("cuda", "float16")
            device = "cuda"
        except Exception:
            rows, info, elapsed = execute("cpu", "int8")
            cpu_fallback = True
    else:
        rows, info, elapsed = execute("cpu", "int8")
    return TranscriptResult(
        text=" ".join(row.text.strip() for row in rows if row.text.strip()),
        language=str(info.language or "unknown"),
        duration_seconds=round(elapsed, 6),
        execution_device=device,
        cpu_fallback=cpu_fallback,
        segment_count=len(rows),
        model_tree_sha256=approval.model_tree_sha256,
    )


def generate_image_derivatives(source: str | Path, output_dir: str | Path) -> dict[str, str]:
    """Create bounded JPEG thumbnail/proxy derivatives from an already accepted image."""
    try:
        from PIL import Image, ImageOps
    except ImportError as exc:
        raise LocalMediaRuntimeError("Pillow is required for image derivatives.") from exc
    source_path, root = Path(source).resolve(), Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    with Image.open(source_path) as image:
        normalized = ImageOps.exif_transpose(image).convert("RGB")
        thumbnail = normalized.copy()
        thumbnail.thumbnail((640, 640))
        proxy = normalized.copy()
        proxy.thumbnail((1920, 1080))
        thumbnail_path, proxy_path = root / "thumbnail.jpg", root / "proxy.jpg"
        thumbnail.save(thumbnail_path, "JPEG", quality=85, optimize=True)
        proxy.save(proxy_path, "JPEG", quality=88, optimize=True)
    return {"thumbnail": str(thumbnail_path), "proxy": str(proxy_path)}


def generate_video_derivatives(source: str | Path, output_dir: str | Path) -> dict[str, str]:
    """Use the explicitly provisioned local FFmpeg binary for keyframe/proxy/waveform."""
    ffmpeg = resolve_ffmpeg()
    source_path, root = Path(source).resolve(), Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    proxy, keyframe, wav, waveform = root / "proxy.mp4", root / "keyframe.jpg", root / "audio.wav", root / "waveform.json"
    _run_ffmpeg(ffmpeg, "-i", str(source_path), "-vf", "scale='min(1280,iw)':-2", "-an", "-movflags", "+faststart", str(proxy))
    _run_ffmpeg(ffmpeg, "-ss", "0", "-i", str(source_path), "-frames:v", "1", str(keyframe))
    _run_ffmpeg(ffmpeg, "-i", str(source_path), "-vn", "-ac", "1", "-ar", "8000", str(wav))
    waveform.write_text(json.dumps(_waveform(wav), separators=(",", ":")), encoding="utf-8")
    return {"proxy": str(proxy), "keyframe": str(keyframe), "waveform": str(waveform)}


def resolve_ffmpeg() -> str:
    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        executable = shutil.which("ffmpeg")
        if executable:
            return executable
    raise LocalMediaRuntimeError("No approved FFmpeg binary is installed.")


def load_model_approval(path: str | Path) -> LocalModelApproval:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        approval = LocalModelApproval(**raw)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LocalMediaRuntimeError(f"Invalid model approval record: {exc}") from exc
    if not approval.network_disabled:
        raise LocalMediaRuntimeError("Network-enabled models are forbidden.")
    return approval


def hash_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def hash_tree(root: str | Path) -> str:
    base, digest = Path(root).resolve(), hashlib.sha256()
    for path in sorted(item for item in base.rglob("*") if item.is_file() and ".cache" not in item.parts):
        digest.update(path.relative_to(base).as_posix().encode("utf-8"))
        digest.update(hash_file(path).encode("ascii"))
    return digest.hexdigest()


def _run_ffmpeg(ffmpeg: str, *arguments: str) -> None:
    completed = subprocess.run([ffmpeg, "-nostdin", "-v", "error", "-y", *arguments], capture_output=True, text=True, check=False)
    if completed.returncode:
        raise LocalMediaRuntimeError(f"FFmpeg failed: {completed.stderr.strip()}")


def _run_command(executable: Path, *arguments: str) -> str:
    completed = subprocess.run(
        [str(executable), *arguments], capture_output=True, text=True, check=False, shell=False
    )
    if completed.returncode:
        raise LocalMediaRuntimeError(completed.stderr.strip() or "Local OCR command failed.")
    return completed.stdout


def _waveform(wav_path: Path, buckets: int = 200) -> dict[str, Any]:
    with wave.open(str(wav_path), "rb") as stream:
        frames, width = stream.getnframes(), stream.getsampwidth()
        if width != 2:
            raise LocalMediaRuntimeError("Expected FFmpeg to produce signed 16-bit PCM.")
        raw = stream.readframes(frames)
        samples = [int.from_bytes(raw[index:index + 2], "little", signed=True) for index in range(0, len(raw), 2)]
        step = max(1, len(samples) // buckets)
        points = [round(max(abs(sample) for sample in samples[index:index + step]) / 32768, 6) for index in range(0, len(samples), step)]
        return {"sample_rate": stream.getframerate(), "duration_seconds": round(frames / stream.getframerate(), 6), "peaks": points[:buckets]}
