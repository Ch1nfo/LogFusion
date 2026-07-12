from __future__ import annotations

import json

from logfusion.cli import main
from tests.test_features import _event


def test_orchestrate_run_cli(tmp_path, capsys):
    source = tmp_path / "normalized.jsonl"
    source.write_text(json.dumps(_event("one", "2026-07-01T10:00:00Z")) + "\n", encoding="utf-8")
    assert main([
        "orchestrate", "run", "--input", str(source),
        "--feature-state", str(tmp_path / "features.db"),
        "--baseline-state", str(tmp_path / "baseline.db"),
        "--detection-state", str(tmp_path / "detection.db"),
        "--incident-state", str(tmp_path / "incidents.db"),
        "--risk-state", str(tmp_path / "risk.db"),
        "--checkpoint", str(tmp_path / "orchestrator.checkpoint.json"),
        "--no-downstream",
    ]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["counts"]["processed"] == 1
    assert report["downstream"] is None
