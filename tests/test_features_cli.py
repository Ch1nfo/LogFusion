from __future__ import annotations

import json

import pytest

from logfusion.cli import main


def _event(event_id: str, timestamp: str, action: str = "user_login") -> dict:
    return {
        "event": {"id": event_id, "time": timestamp, "action": action, "outcome": "success"},
        "user": {"name": "wangkun78"},
        "raw": {"source_type": "sso", "source_id": "sso-main"},
        "source": {"ip": "10.0.0.1"},
        "resource": {"type": "application", "name": "portal"},
        "network": {"bytes": 5},
    }


def test_features_build_and_query_cli(tmp_path, capsys):
    input_path = tmp_path / "normalized.jsonl"
    input_path.write_text(
        "\n".join(json.dumps(event) for event in [
            _event("one", "2026-07-01T09:00:00Z"),
            _event("two", "2026-07-01T09:05:00Z", "repo_read"),
        ]) + "\n",
        encoding="utf-8",
    )
    state = tmp_path / "features.db"

    assert main(["features", "build", "--input", str(input_path), "--state", str(state)]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["processed"] == 2
    assert summary["sessions_closed"] == 1

    assert main([
        "features", "query", "--state", str(state), "--user", "wangkun78",
        "--from", "2026-07-01T09:00:00Z", "--to", "2026-07-01T10:00:00Z", "--window-size", "3600",
    ]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["windows"][0]["event_count"] == 2
    assert report["sessions"][0]["event_count"] == 2
    assert report["sessions"][0]["first_actions_json"] == [
        [1782896400000, "one", "user_login"],
        [1782896700000, "two", "repo_read"],
    ]


def test_features_build_rejects_reusing_finalized_state(tmp_path, capsys):
    input_path = tmp_path / "normalized.jsonl"
    input_path.write_text(json.dumps(_event("one", "2026-07-01T09:00:00Z")) + "\n", encoding="utf-8")
    state = tmp_path / "features.db"
    assert main(["features", "build", "--input", str(input_path), "--state", str(state)]) == 0
    capsys.readouterr()

    assert main(["features", "build", "--input", str(input_path), "--state", str(state)]) == 1
    assert "rebuild" in capsys.readouterr().out
