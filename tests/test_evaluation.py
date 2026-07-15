from __future__ import annotations

import json
import random
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from logfusion.baseline import BaselineEngine
from logfusion.evaluation import EvaluationEngine, EvaluationError
from logfusion.features import FeatureEngine
from logfusion.model_detection import ModelDetectionConfig
from tests.test_detection import _event
from tests.test_detection import _seed_ready


def test_evaluation_builds_point_in_time_snapshots_and_is_idempotent(tmp_path):
    feature_db, baseline_db, evaluation_db = (tmp_path / name for name in ("features.db", "baseline.db", "evaluation.db"))
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    with EvaluationEngine(feature_db, baseline_db, evaluation_db) as engine:
        report = engine.run("policy-v2", "2026-01-21T00:00:00Z", "2026-01-22T00:00:00Z")
        repeated = engine.run("policy-v2", "2026-01-21T00:00:00Z", "2026-01-22T00:00:00Z")
        assert report["experiment_id"] == repeated["experiment_id"]
        assert report["metrics"]["candidate_count"] > 0
        comparison = engine.compare(report["experiment_id"], report["experiment_id"])
        assert comparison["delta"]["daily_prediction_cv"] == 0
        assert comparison["overlap"]["jaccard"] == 1.0
    connection = sqlite3.connect(evaluation_db)
    assert connection.execute("SELECT COUNT(*) FROM daily_baseline_snapshots").fetchone()[0] > 0
    assert connection.execute("SELECT COUNT(*) FROM candidate_predictions").fetchone()[0] > 0
    connection.close()


def test_evaluation_requires_utc_day_boundaries_and_persists_unsupervised_metrics(tmp_path):
    feature_db, baseline_db, evaluation_db = (tmp_path / name for name in ("features.db", "baseline.db", "evaluation.db"))
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    with EvaluationEngine(feature_db, baseline_db, evaluation_db) as engine:
        with pytest.raises(EvaluationError, match="UTC natural-day"):
            engine.run("bad", "2026-01-21T01:00:00Z", "2026-01-22T00:00:00Z")
        report = engine.run("unknown", "2026-01-21T00:00:00Z", "2026-01-22T00:00:00Z")
    assert report["metrics"]["predicted_user_days"] == 1
    assert report["metrics"]["unique_predicted_users"] == 1
    assert report["metrics"]["daily_prediction_cv"] == 0
    connection = sqlite3.connect(evaluation_db)
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "user_day_labels" not in tables
    assert "precision" not in {row[1] for row in connection.execute("PRAGMA table_info(experiment_metrics)")}
    connection.close()


def _seed_model_history(tmp_path, *, peer_members: int = 5):
    feature_db, baseline_db = tmp_path / "features.db", tmp_path / "baseline.db"
    start = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    users = [f"user-{index}" for index in range(5)]
    with FeatureEngine(feature_db, mode="replay") as features:
        for day in range(35):
            for user_index, user in enumerate(users):
                generator = random.Random(day * 10 + user_index)
                count = max(10, int(30 + generator.gauss(0, 4)))
                distinct = max(1, int(5 + generator.gauss(0, 1)))
                for item in range(count):
                    timestamp = (start + timedelta(days=day)).isoformat().replace("+00:00", "Z")
                    features.process_event(_event(
                        f"normal-{day}-{user_index}-{item}", timestamp, user=user,
                        ip=f"10.0.{user_index}.{item % distinct}", resource=f"repo-{item % distinct}",
                        count_bytes=int(100 + generator.random() * 30),
                    ))
        outlier_time = (start + timedelta(days=35)).isoformat().replace("+00:00", "Z")
        for item in range(300):
            features.process_event(_event(f"outlier-{item}", outlier_time, user=users[0], ip=f"10.99.0.{item}", resource=f"secret-{item}", count_bytes=100_000))
        features.flush_sessions()
    mapping = tmp_path / "peer-groups.csv"
    mapping.write_text("user_name,group_id\n" + "".join(f"{user},engineering\n" for user in users[:peer_members]), encoding="utf-8")
    with BaselineEngine(feature_db, baseline_db, peer_groups_path=mapping) as baseline:
        baseline.update(outlier_time)
    day_start = int((start + timedelta(days=35)).replace(hour=0).timestamp() * 1000)
    return feature_db, baseline_db, users[0], day_start


@pytest.mark.parametrize("detector_id", ["hbos", "isolation_forest"])
def test_model_evaluation_uses_prior_history_and_persists_evidence(tmp_path, detector_id):
    feature_db, baseline_db, user, day_start = _seed_model_history(tmp_path)
    evaluation_db = tmp_path / "evaluation.db"
    with EvaluationEngine(feature_db, baseline_db, evaluation_db, detector_id=detector_id) as engine:
        report = engine.run(detector_id, day_start, day_start + 86_400_000)
    assert report["detector_id"] == detector_id
    assert report["metrics"]["predicted_user_days"] == 1
    connection = sqlite3.connect(evaluation_db)
    connection.row_factory = sqlite3.Row
    snapshots = connection.execute("SELECT * FROM daily_model_snapshots ORDER BY window_size").fetchall()
    candidate = connection.execute("SELECT * FROM candidate_predictions ORDER BY score DESC LIMIT 1").fetchone()
    assert {row["window_size"] for row in snapshots} == {3600, 86400}
    assert all(row["history_end"] == day_start and row["sample_count"] == 150 for row in snapshots)
    assert all(row["scope"] == "peer_group" and row["member_count"] == 5 for row in snapshots)
    assert candidate["raw_score"] is not None and candidate["score_percentile"] == 1.0
    assert json.loads(candidate["explanation_json"])["training"]["history_end"].startswith("2026-02-05")
    connection.close()


def test_model_falls_back_to_global_and_records_not_ready(tmp_path):
    feature_db, baseline_db, user, day_start = _seed_model_history(tmp_path, peer_members=4)
    evaluation_db = tmp_path / "evaluation.db"
    with EvaluationEngine(feature_db, baseline_db, evaluation_db, detector_id="hbos") as engine:
        engine.run("global", day_start, day_start + 86_400_000)
    connection = sqlite3.connect(evaluation_db)
    assert {row[0] for row in connection.execute("SELECT DISTINCT scope FROM daily_model_snapshots")} == {"global"}
    connection.close()

    small_feature, small_baseline, small_eval = (tmp_path / name for name in ("small-features.db", "small-baseline.db", "small-eval.db"))
    as_of = _seed_ready(small_feature, include_outlier=True)
    with BaselineEngine(small_feature, small_baseline) as baseline:
        baseline.update(as_of)
    with EvaluationEngine(small_feature, small_baseline, small_eval, detector_id="hbos") as engine:
        report = engine.run("not-ready", "2026-01-21T00:00:00Z", "2026-01-22T00:00:00Z")
    assert report["metrics"]["candidate_count"] == 0
    connection = sqlite3.connect(small_eval)
    assert {row[0] for row in connection.execute("SELECT DISTINCT status FROM daily_model_snapshots")} == {"model_not_ready"}
    connection.close()


def test_model_config_changes_experiment_id_and_v1_db_is_rejected(tmp_path):
    feature_db, baseline_db = tmp_path / "features.db", tmp_path / "baseline.db"
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    with EvaluationEngine(feature_db, baseline_db, tmp_path / "left.db", detector_id="hbos", model_config=ModelDetectionConfig(threshold_quantile=0.99)) as engine:
        left = engine.run("same", "2026-01-21T00:00:00Z", "2026-01-22T00:00:00Z")
    with EvaluationEngine(feature_db, baseline_db, tmp_path / "right.db", detector_id="hbos") as engine:
        right = engine.run("same", "2026-01-21T00:00:00Z", "2026-01-22T00:00:00Z")
    assert left["experiment_id"] != right["experiment_id"]

    legacy = tmp_path / "legacy.db"
    connection = sqlite3.connect(legacy)
    connection.execute("CREATE TABLE evaluation_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
    connection.execute("INSERT INTO evaluation_meta VALUES ('evaluation_schema_version','1')")
    connection.commit()
    connection.close()
    with pytest.raises(EvaluationError, match="rebuild Evaluation DB"):
        EvaluationEngine(feature_db, baseline_db, legacy)
