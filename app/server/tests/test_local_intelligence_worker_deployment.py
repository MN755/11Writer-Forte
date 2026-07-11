from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ROOT / "docker-compose.yml"
WORKER = ROOT / "app" / "server" / "local_intelligence_worker"


def _service_block() -> str:
    text = COMPOSE.read_text(encoding="utf-8")
    start = text.index("  local-intelligence-worker:\n")
    end = text.index("\n  clickhouse:\n", start)
    return text[start:end]


def test_local_intelligence_worker_is_sealed_from_network_and_host_writes() -> None:
    service = _service_block()
    for required in (
        'network_mode: "none"',
        "read_only: true",
        'user: "65532:65532"',
        "- ALL",
        "- no-new-privileges:true",
        "target: /models",
        "target: /data",
        "read_only: true",
        "/output:rw,noexec,nosuid,nodev",
    ):
        assert required in service
    assert "ports:" not in service


def test_worker_image_has_a_narrow_checksum_pinned_entrypoint_contract() -> None:
    dockerfile = (WORKER / "Dockerfile").read_text(encoding="utf-8")
    entrypoint = (WORKER / "entrypoint.py").read_text(encoding="utf-8")
    assert "USER 65532:65532" in dockerfile
    assert 'ENTRYPOINT ["python", "/opt/11writer-local-intelligence/entrypoint.py"]' in dockerfile
    for required in (
        "DATA_ROOT = Path(\"/data\")",
        "MODEL_ROOT = Path(\"/models\")",
        "ELEVENWRITER_LOCAL_INTELLIGENCE_MODEL_RUNNER_SHA256",
        "ELEVENWRITER_APPROVED_MODEL_SHA256",
        "_verify_artifact",
        "_require_under",
        "shell=False",
    ):
        assert required in entrypoint
