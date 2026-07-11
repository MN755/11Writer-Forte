"""Local, content-addressed public-evidence artifacts.

Database rows retain only the resulting references and provenance; evidence bytes live
in this local artifact store so they can be reproduced and retained independently.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import blake3


VALID_DATA_ROLES = frozenset({"raw", "normalized", "derived", "evidence", "cache"})


@dataclass(frozen=True)
class ContentAddressedArtifact:
    data_role: str
    blake3_digest: str
    sha256_digest: str
    object_path: Path
    byte_size: int

    @property
    def object_key(self) -> str:
        return f"blake3/{self.blake3_digest[:2]}/{self.blake3_digest}"


def capture_public_evidence_artifact(
    content: bytes,
    *,
    artifact_root: Path,
    data_role: str,
) -> ContentAddressedArtifact:
    """Store bytes once, keyed by BLAKE3 and cross-checked with SHA-256."""
    if data_role not in VALID_DATA_ROLES:
        raise ValueError(f"Unsupported investigation data role: {data_role!r}.")
    if not isinstance(content, bytes):
        raise TypeError("Evidence artifact content must be bytes.")
    blake3_digest = blake3.blake3(content).hexdigest()
    sha256_digest = hashlib.sha256(content).hexdigest()
    object_path = artifact_root / "blake3" / blake3_digest[:2] / blake3_digest
    object_path.parent.mkdir(parents=True, exist_ok=True)
    if object_path.exists():
        if object_path.read_bytes() != content:
            raise RuntimeError("BLAKE3 object path collision with different content.")
    else:
        object_path.write_bytes(content)
    return ContentAddressedArtifact(
        data_role=data_role,
        blake3_digest=blake3_digest,
        sha256_digest=sha256_digest,
        object_path=object_path,
        byte_size=len(content),
    )
