from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from logfusion.baseline import BaselineEngine
from logfusion.correlation import CorrelationEngine, CorrelationError
from logfusion.detection import DetectionEngine
from logfusion.features import FeatureEngine


def _event(event_id: str, timestamp: str, *, user: str = "wangkun78", source_type: str = "gitlab", ip: str = "10.0.0.1", action: str = "repo_read", resource: str = "core", bytes_: int = 1) -> dict:
    return {
        "event": {"id": event_id, "time": timestamp, "action": action, "outcome": "success"},
        "user": {"name": user},
        "raw": {"source_type": source_type, "source_id": f"{source_type}-main"},
        "source": {"ip": ip},
        "resource": {"type": "git_repo", "name": resource},
        "network": {"bytes": bytes_},
    }


def _seed_states(feature_db, baseline_db, detection_db) -> None:
    start = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    with FeatureEngine(feature_db, mode="replay") as features:
        for day in range(20):
            timestamp = (start + timedelta(days=day)).isoformat().replace("+00:00", "Z")
            features.process_event(_event(f"normal-{day}", timestamp))
        outlier_time = (start + timedelta(days=20)).isoformat().replace("+00:00", "Z")
        for index in range(20):
            features.process_event(_event(
                f"outlier-{index}", outlier_time, source_type="svn" if index % 2 else "gitlab",
                ip="10.99.0.1", resource="secret", bytes_=1_000,
            ))
        features.flush_sessions()
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of=outlier_time)
    with DetectionEngine(feature_db, baseline_db, detection_db) as detection:
        detection.run()


def test_correlates_same_user_session_candidates_into_explainable_incident(tmp_path):
    feature_db, baseline_db, detection_db, incident_db = (
        tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db", tmp_path / "incidents.db",
    )
    _seed_states(feature_db, baseline_db, detection_db)
    engine = CorrelationEngine(feature_db, detection_db, incident_db)
    report = engine.run()
    incidents = engine.query("wangkun78", "2026-01-01T00:00:00Z", "2026-02-28T00:00:00Z")

    assert report["incident_count"] > 0
    assert any(row["candidate_count"] >= 2 for row in incidents)
    incident = next(row for row in incidents if row["source_types"] == ["gitlab", "svn"])
    assert incident["aggregation_score"] >= max(item["score"] for item in incident["timeline"])
    assert incident["incident_type"] == "cross_system_behavior_anomaly"
    assert incident["evidence"]["candidate_ids"]


def test_second_run_is_noop_and_detection_must_be_current(tmp_path):
    feature_db, baseline_db, detection_db, incident_db = (
        tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db", tmp_path / "incidents.db",
    )
    _seed_states(feature_db, baseline_db, detection_db)
    engine = CorrelationEngine(feature_db, detection_db, incident_db)
    engine.run()
    assert engine.run()["incident_count"] == 0

    with FeatureEngine(feature_db, mode="realtime") as features:
        features.process_event(_event("new-live", "2026-01-21T13:00:00Z", bytes_=5_000))
    with pytest.raises(CorrelationError, match="detect run"):
        engine.run()


def test_new_detection_revision_supersedes_previous_incident_version(tmp_path):
    feature_db, baseline_db, detection_db, incident_db = (
        tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db", tmp_path / "incidents.db",
    )
    _seed_states(feature_db, baseline_db, detection_db)
    engine = CorrelationEngine(feature_db, detection_db, incident_db)
    first = engine.run()

    with FeatureEngine(feature_db, mode="realtime") as features:
        features.process_event(_event("changed", "2026-01-21T12:00:00Z", bytes_=8_000))
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of="2026-01-21T12:00:00Z")
    with DetectionEngine(feature_db, baseline_db, detection_db) as detection:
        detection.run()
    second = engine.run()

    assert first["incident_count"] > 0
    assert second["superseded_count"] >= 1
    assert engine.query("wangkun78", "2026-01-01T00:00:00Z", "2026-02-28T00:00:00Z")
