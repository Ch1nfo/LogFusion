from __future__ import annotations

import sqlite3

import pytest

from logfusion.baseline import BaselineEngine
from logfusion.features import FeatureEngine
from logfusion.readiness import ReadinessEngine, ReadinessError, ReadinessTargets
from tests.test_detection import _seed_ready
from tests.test_evaluation import _case_state, _seed_model_history


def test_readiness_reports_history_window_and_label_gaps(tmp_path):
    feature_db, baseline_db, case_db = tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "cases.db"
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    _case_state(case_db)
    with ReadinessEngine(feature_db, baseline_db, case_db) as engine:
        report = engine.report()
    assert report["history"]["calendar_span_days"] == 21
    assert report["history"]["missing_days"] == 9
    assert report["windows"]["3600"]["model"]["missing_samples"] == 79
    assert report["labels"]["confirmed_threat_user_days"]["current"] == 1
    assert report["labels"]["benign_user_days"]["missing"] == 100
    assert report["detectors"]["hbos"]["training_status"] == "not_ready"
    assert any(action["kind"] == "collect_history" for action in report["next_actions"])


def test_readiness_separates_training_readiness_from_label_readiness(tmp_path):
    feature_db, baseline_db, user, day_start = _seed_model_history(tmp_path)
    case_db = tmp_path / "cases.db"
    _case_state(case_db, user=user, first_seen=day_start, last_seen=day_start + 86_399_999)
    with ReadinessEngine(feature_db, baseline_db, case_db) as engine:
        report = engine.report()
    assert report["history"]["ready"] is True
    assert report["windows"]["3600"]["model"]["ready"] is True
    assert report["windows"]["86400"]["model"]["ready"] is True
    assert report["peer_groups"][0]["windows"]["3600"]["model"]["ready"] is True
    assert report["detectors"]["hbos"] == {
        "training_status": "ready", "training_ready": True, "evaluation_ready": False, "label_ready": False,
    }
    assert any(action["kind"] == "label_benign" for action in report["next_actions"])


def test_readiness_marks_complete_collection_and_case_priority(tmp_path):
    feature_db, baseline_db, user, day_start = _seed_model_history(tmp_path)
    case_db = tmp_path / "cases.db"
    _case_state(case_db, user=user, first_seen=day_start, last_seen=day_start + 86_399_999)
    connection = sqlite3.connect(case_db)
    connection.execute(
        "INSERT INTO cases(case_id,user_name,first_seen,last_seen,disposition,updated_at) VALUES ('benign-other','benign-user',?,?, 'benign',0)",
        (day_start, day_start + 86_399_999),
    )
    connection.execute(
        "INSERT INTO cases(case_id,user_name,first_seen,last_seen,disposition,updated_at) VALUES ('benign-same',?,?,?, 'benign',0)",
        (user, day_start, day_start + 86_399_999),
    )
    connection.commit()
    connection.close()
    targets = ReadinessTargets(history_days=30, confirmed_threat_user_days=1, benign_user_days=1)
    with ReadinessEngine(feature_db, baseline_db, case_db, targets=targets) as engine:
        report = engine.report()
    assert report["labels"]["labeled_user_days"] == 2
    assert report["labels"]["confirmed_threat_user_days"]["current"] == 1
    assert report["labels"]["benign_user_days"]["current"] == 1
    assert all(item["evaluation_ready"] for item in report["detectors"].values())
    assert report["next_actions"][-1]["kind"] == "run_benchmark"


def test_readiness_reports_peer_group_gap_but_global_model_ready(tmp_path):
    feature_db, baseline_db, user, day_start = _seed_model_history(tmp_path, peer_members=4)
    case_db = tmp_path / "cases.db"
    _case_state(case_db, user=user, first_seen=day_start, last_seen=day_start + 86_399_999)
    with ReadinessEngine(feature_db, baseline_db, case_db) as engine:
        report = engine.report()
    group = report["peer_groups"][0]
    assert group["configured_member_count"] == 4
    assert group["windows"]["3600"]["model"]["missing_members"] == 1
    assert group["windows"]["3600"]["model"]["ready"] is False
    assert report["detectors"]["isolation_forest"]["training_status"] == "ready"
    assert any(action["kind"] == "expand_peer_groups" for action in report["next_actions"])


def test_readiness_rejects_empty_feature_state_and_non_utc_boundary(tmp_path):
    feature_db, baseline_db, case_db = tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "cases.db"
    with FeatureEngine(feature_db, mode="replay"):
        pass
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update()
    _case_state(case_db)
    with ReadinessEngine(feature_db, baseline_db, case_db) as engine:
        with pytest.raises(ReadinessError, match="no user activity"):
            engine.report()

    populated_feature, populated_baseline = tmp_path / "populated.db", tmp_path / "populated-baseline.db"
    as_of = _seed_ready(populated_feature)
    with BaselineEngine(populated_feature, populated_baseline) as baseline:
        baseline.update(as_of)
    with ReadinessEngine(populated_feature, populated_baseline, case_db) as engine:
        with pytest.raises(ReadinessError, match="UTC natural-day"):
            engine.report("2026-01-20T01:00:00Z")
