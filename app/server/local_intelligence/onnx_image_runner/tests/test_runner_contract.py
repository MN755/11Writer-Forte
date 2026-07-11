from __future__ import annotations

import io
import json
from pathlib import Path
import runpy
import sys

import pytest


RUNNER = Path(__file__).parents[1] / "onnx_image_runner.py"
MODULE = runpy.run_path(str(RUNNER), run_name="onnx_image_runner_contract")


def test_command_request_rejects_file_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ELEVENWRITER_LOCAL_INTELLIGENCE_OFFLINE", "1")
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({"schema_version": 1, "artifact_path": "C:/oops"})))
    with pytest.raises(MODULE["RunnerError"], match="artifact-ID-only"):
        MODULE["_read_request"]()


def test_command_request_accepts_artifact_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ELEVENWRITER_LOCAL_INTELLIGENCE_OFFLINE", "1")
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1, "artifact_id": "a", "input_sha256": "a" * 64,
                    "byte_size": 1, "task": "image_embedding", "model_id": "m", "model_version": "1",
                }
            )
        ),
    )
    assert MODULE["_read_request"]()["artifact_id"] == "a"
