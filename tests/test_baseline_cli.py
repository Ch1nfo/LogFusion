from __future__ import annotations

import json

from logfusion.cli import main
from logfusion.features import build_features_from_jsonl


def _event(event_id: str, timestamp: str) -> dict:
    return {
        "event": {"id": event_id, "time": timestamp, "action": "repo_read", "outcome": "success"},
        "user": {"name": "wangkun78"},
        "raw": {"source_type": "gitlab", "source_id": "gitlab-main"},
        "source": {"ip": "10.0.0.1"},
        "resource": {"type": "git_repo", "name": "core"},
        "network": {"bytes": 1},
    }


def test_baseline_build_and_query_cli(tmp_path, capsys):
    normalized = tmp_path / "normalized.jsonl"
    normalized.write_text(json.dumps(_event("one", "2026-01-01T12:00:00Z")) + "\n", encoding="utf-8")
    feature_state = tmp_path / "features.db"
    baseline_state = tmp_path / "baseline.db"
    build_features_from_jsonl(normalized, feature_state)

    assert main([
        "baseline", "build", "--feature-state", str(feature_state), "--state", str(baseline_state),
        "--as-of", "2026-01-02T00:00:00Z",
    ]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["recomputed_users"] == 1

    assert main(["baseline", "query", "--state", str(baseline_state), "--user", "wangkun78", "--window-size", "3600"]) == 0
    query = json.loads(capsys.readouterr().out)
    assert query["metrics"]
    assert any(row["value_key"] == "git_repo:core" for row in query["values"])
