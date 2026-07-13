from __future__ import annotations

import sqlite3

import pytest

from logfusion.baseline import BaselineEngine
from logfusion.evaluation import EvaluationEngine, EvaluationError
from tests.test_detection import _seed_ready


def _case_state(path, *, disposition: str = "confirmed_threat") -> None:
    connection = sqlite3.connect(path)
    connection.executescript("""
    CREATE TABLE case_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE cases (case_id TEXT PRIMARY KEY, alert_key TEXT, current_alert_id TEXT, user_name TEXT NOT NULL, incident_id TEXT,
      first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, risk_score INTEGER, severity TEXT, status TEXT, owner TEXT,
      tags_json TEXT, suppression_until INTEGER, requires_review INTEGER, source_correlation_revision INTEGER, disposition TEXT NOT NULL,
      created_at INTEGER, updated_at INTEGER NOT NULL);
    """)
    connection.execute("INSERT INTO case_meta(key,value) VALUES ('case_schema_version','1')")
    connection.execute("INSERT INTO cases(case_id,user_name,first_seen,last_seen,disposition,updated_at) VALUES ('case-1','wangkun78',?,?,?,0)", (1768953600000, 1769039999999, disposition))
    connection.commit()
    connection.close()


def test_evaluation_builds_point_in_time_snapshots_and_is_idempotent(tmp_path):
    feature_db, baseline_db, case_db, evaluation_db = (tmp_path / name for name in ("features.db", "baseline.db", "cases.db", "evaluation.db"))
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    _case_state(case_db)

    with EvaluationEngine(feature_db, baseline_db, case_db, evaluation_db) as engine:
        report = engine.run("policy-v2", "2026-01-21T00:00:00Z", "2026-01-22T00:00:00Z")
        repeated = engine.run("policy-v2", "2026-01-21T00:00:00Z", "2026-01-22T00:00:00Z")
        assert report["experiment_id"] == repeated["experiment_id"]
        assert report["metrics"]["true_positive"] == 1
        assert engine.compare(report["experiment_id"], report["experiment_id"])["delta"]["f1"] == 0
    connection = sqlite3.connect(evaluation_db)
    assert connection.execute("SELECT COUNT(*) FROM daily_baseline_snapshots").fetchone()[0] > 0
    assert connection.execute("SELECT COUNT(*) FROM candidate_predictions").fetchone()[0] > 0
    connection.close()


def test_evaluation_requires_utc_day_boundaries_and_ignores_unknown_labels(tmp_path):
    feature_db, baseline_db, case_db, evaluation_db = (tmp_path / name for name in ("features.db", "baseline.db", "cases.db", "evaluation.db"))
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    _case_state(case_db, disposition="unknown")
    with EvaluationEngine(feature_db, baseline_db, case_db, evaluation_db) as engine:
        with pytest.raises(EvaluationError, match="UTC natural-day"):
            engine.run("bad", "2026-01-21T01:00:00Z", "2026-01-22T00:00:00Z")
        report = engine.run("unknown", "2026-01-21T00:00:00Z", "2026-01-22T00:00:00Z")
    assert report["metrics"]["unlabeled_predictions"] == 1
    assert report["metrics"]["false_positive"] == 0
