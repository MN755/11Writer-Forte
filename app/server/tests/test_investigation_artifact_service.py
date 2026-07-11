import hashlib

import blake3

from src.services.investigation_artifact_service import capture_public_evidence_artifact


def test_public_evidence_artifact_is_local_content_addressed_and_dual_hashed(tmp_path) -> None:
    content = b"public source capture"
    artifact = capture_public_evidence_artifact(
        content, artifact_root=tmp_path, data_role="evidence"
    )

    assert artifact.object_path.read_bytes() == content
    assert artifact.blake3_digest == blake3.blake3(content).hexdigest()
    assert artifact.sha256_digest == hashlib.sha256(content).hexdigest()
    assert (
        capture_public_evidence_artifact(content, artifact_root=tmp_path, data_role="evidence")
        == artifact
    )
