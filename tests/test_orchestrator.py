from __future__ import annotations

import json

import pytest

from logfusion.orchestrator import OrchestratorConfig, OrchestratorError, run_continuous_pipeline
from tests.test_features import _event


def _append(path, *events: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event))
            handle.write("\n")


def _paths(tmp_path):
    return {
        "feature": tmp_path / "features.db",
        "baseline": tmp_path / "baseline.db",
        "detection": tmp_path / "detection.db",
        "incident": tmp_path / "incidents.db",
        "risk": tmp_path / "risk.db",
        "checkpoint": tmp_path / "orchestrator.checkpoint.json",
    }


def test_continuous_pipeline_processes_append_only_input_and_runs_full_chain(tmp_path):
    source = tmp_path / "normalized.jsonl"
    paths = _paths(tmp_path)
    _append(
        source,
        _event("one", "2026-07-01T10:00:00Z"),
        _event("two", "2026-07-01T11:00:00Z", action="repo_read"),
    )
    first = run_continuous_pipeline(
        source, paths["feature"], paths["baseline"], paths["detection"], paths["incident"], paths["risk"],
        checkpoint_path=paths["checkpoint"],
        config=OrchestratorConfig(watermark_lag_seconds=0),
    )

    assert first["counts"] == {"read": 2, "processed": 2, "duplicates": 0, "rejected": 0}
    assert first["watermark"] == "2026-07-01T11:00:00.000Z"
    assert first["downstream"] is not None
    checkpoint = json.loads(paths["checkpoint"].read_text())
    assert checkpoint["committed_offset"] == source.stat().st_size

    _append(source, _event("two", "2026-07-01T11:00:00Z"), _event("three", "2026-07-01T12:00:00Z"))
    second = run_continuous_pipeline(
        source, paths["feature"], paths["baseline"], paths["detection"], paths["incident"], paths["risk"],
        checkpoint_path=paths["checkpoint"], resume=True, config=OrchestratorConfig(watermark_lag_seconds=0),
    )
    assert second["counts"] == {"read": 2, "processed": 1, "duplicates": 1, "rejected": 0}
    assert second["feature_revision"] > first["feature_revision"]


def test_orchestrator_rejects_changed_committed_prefix_and_incomplete_line_is_not_committed(tmp_path):
    source = tmp_path / "normalized.jsonl"
    paths = _paths(tmp_path)
    _append(source, _event("one", "2026-07-01T10:00:00Z"))
    with source.open("ab") as handle:
        handle.write(b'{"event":')
    report = run_continuous_pipeline(
        source, paths["feature"], paths["baseline"], paths["detection"], paths["incident"], paths["risk"],
        checkpoint_path=paths["checkpoint"], run_downstream=False,
    )
    assert report["counts"]["read"] == 1
    assert report["committed_offset"] < source.stat().st_size

    source.write_text(source.read_text().replace("one", "changed", 1), encoding="utf-8")
    with pytest.raises(OrchestratorError, match="before checkpoint changed"):
        run_continuous_pipeline(
            source, paths["feature"], paths["baseline"], paths["detection"], paths["incident"], paths["risk"],
            checkpoint_path=paths["checkpoint"], resume=True,
        )
