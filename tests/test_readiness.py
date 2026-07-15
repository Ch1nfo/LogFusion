from __future__ import annotations

import pytest

from logfusion.baseline import BaselineEngine
from logfusion.features import FeatureEngine
from logfusion.readiness import ReadinessEngine, ReadinessError
from tests.test_detection import _seed_ready
from tests.test_evaluation import _seed_model_history


def test_readiness_reports_history_and_window_gaps_without_labels(tmp_path):
    feature_db, baseline_db = tmp_path / "features.db", tmp_path / "baseline.db"
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    with ReadinessEngine(feature_db, baseline_db) as engine:
        report = engine.report()
    assert report["history"]["calendar_span_days"] == 21
    assert report["history"]["missing_days"] == 9
    assert report["windows"]["3600"]["model"]["missing_samples"] == 79
    assert "labels" not in report
    assert report["detectors"]["hbos"]["training_status"] == "not_ready"
    assert any(action["kind"] == "collect_history" for action in report["next_actions"])


def test_readiness_uses_training_data_as_unsupervised_evaluation_gate(tmp_path):
    feature_db, baseline_db, user, day_start = _seed_model_history(tmp_path)
    with ReadinessEngine(feature_db, baseline_db) as engine:
        report = engine.report()
    assert report["history"]["ready"] is True
    assert report["windows"]["3600"]["model"]["ready"] is True
    assert report["windows"]["86400"]["model"]["ready"] is True
    assert report["peer_groups"][0]["windows"]["3600"]["model"]["ready"] is True
    assert report["detectors"]["hbos"] == {
        "training_status": "ready", "training_ready": True, "evaluation_ready": True, "evaluation_mode": "unsupervised",
    }
    assert not any(action["kind"].startswith("label_") for action in report["next_actions"])
    assert report["next_actions"][-1]["kind"] == "run_benchmark"


def test_readiness_reports_peer_group_gap_but_global_model_ready(tmp_path):
    feature_db, baseline_db, user, day_start = _seed_model_history(tmp_path, peer_members=4)
    with ReadinessEngine(feature_db, baseline_db) as engine:
        report = engine.report()
    group = report["peer_groups"][0]
    assert group["configured_member_count"] == 4
    assert group["windows"]["3600"]["model"]["missing_members"] == 1
    assert group["windows"]["3600"]["model"]["ready"] is False
    assert report["detectors"]["isolation_forest"]["training_status"] == "ready"
    assert any(action["kind"] == "expand_peer_groups" for action in report["next_actions"])


def test_readiness_rejects_empty_feature_state_and_non_utc_boundary(tmp_path):
    feature_db, baseline_db = tmp_path / "features.db", tmp_path / "baseline.db"
    with FeatureEngine(feature_db, mode="replay"):
        pass
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update()
    with ReadinessEngine(feature_db, baseline_db) as engine:
        with pytest.raises(ReadinessError, match="no user activity"):
            engine.report()

    populated_feature, populated_baseline = tmp_path / "populated.db", tmp_path / "populated-baseline.db"
    as_of = _seed_ready(populated_feature)
    with BaselineEngine(populated_feature, populated_baseline) as baseline:
        baseline.update(as_of)
    with ReadinessEngine(populated_feature, populated_baseline) as engine:
        with pytest.raises(ReadinessError, match="UTC natural-day"):
            engine.report("2026-01-20T01:00:00Z")
