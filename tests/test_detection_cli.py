from __future__ import annotations

import json

from logfusion.baseline import BaselineEngine
from logfusion.cli import main
from logfusion.features import build_features_from_jsonl


def _event() -> dict:
    return {
        "event": {"id": "one", "time": "2026-01-01T12:00:00Z", "action": "repo_read", "outcome": "success"},
        "user": {"name": "wangkun78"},
        "raw": {"source_type": "gitlab", "source_id": "gitlab-main"},
        "source": {"ip": "10.0.0.1"},
        "resource": {"type": "git_repo", "name": "core"},
        "network": {"bytes": 1},
    }


def test_detect_run_and_query_cli(tmp_path, capsys):
    normalized = tmp_path / "normalized.jsonl"
    feature_state, baseline_state, detection_state = tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db"
    normalized.write_text(json.dumps(_event()) + "\n", encoding="utf-8")
    build_features_from_jsonl(normalized, feature_state)
    with BaselineEngine(feature_state, baseline_state) as baseline:
        baseline.update(as_of="2026-01-01T12:00:00Z")

    assert main([
        "detect", "run", "--feature-state", str(feature_state), "--baseline-state", str(baseline_state),
        "--state", str(detection_state),
    ]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["scanned_windows"] > 0

    assert main([
        "detect", "query", "--state", str(detection_state), "--user", "wangkun78",
        "--from", "2026-01-01T00:00:00Z", "--to", "2026-01-02T00:00:00Z",
    ]) == 0
    assert json.loads(capsys.readouterr().out)["user_name"] == "wangkun78"

