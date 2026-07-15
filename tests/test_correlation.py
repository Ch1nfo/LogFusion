from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from logfusion.baseline import BaselineEngine
from logfusion.correlation import CorrelationEngine, CorrelationError
from logfusion.detection import DetectionEngine
from logfusion.features import FeatureEngine
from logfusion.fusion import FusionEngine
from logfusion.rules import save_rules


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
    assert incident["detector_family_count"] == 1
    assert incident["detector_families"] == ["statistical"]


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


def test_rule_refresh_without_new_feature_revision_rebuilds_family_diversity(tmp_path):
    feature_db, baseline_db, detection_db, incident_db, risk_db, rules = (
        tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db",
        tmp_path / "incidents.db", tmp_path / "risk.db", tmp_path / "rules.json",
    )
    _seed_states(feature_db, baseline_db, detection_db)
    rule = {
        "rule_id": "hourly-burst", "version": 1, "name": "Hourly burst", "description": "", "status": "shadow",
        "scope": "window", "source_types": ["gitlab"], "window_size": 3600, "score": 90, "match": "all",
        "conditions": [{"field": "event_count", "operator": "gte", "value": 20}], "tags": [],
        "tests": [
            {"name": "positive", "input": {"event_count": 20, "source_types": ["gitlab"]}, "expected_match": True},
            {"name": "negative", "input": {"event_count": 1, "source_types": ["gitlab"]}, "expected_match": False},
        ],
    }
    save_rules({"schema_version": 1, "rules": [rule]}, rules)
    with DetectionEngine(feature_db, baseline_db, detection_db, rules_path=rules) as detection:
        shadow_report = detection.run(refresh_rules=True)
    with CorrelationEngine(feature_db, detection_db, incident_db) as correlation:
        correlation.run()
        assert all(item["detector_family_count"] == 1 for item in correlation.query("wangkun78", "2026-01-01", "2026-03-01"))

    save_rules({"schema_version": 1, "rules": [{**rule, "status": "active"}]}, rules)
    with DetectionEngine(feature_db, baseline_db, detection_db, rules_path=rules) as detection:
        active_report = detection.run(refresh_rules=True)
    assert active_report["processed_feature_revision"] == shadow_report["processed_feature_revision"]
    assert active_report["processed_detection_revision"] > shadow_report["processed_detection_revision"]
    with CorrelationEngine(feature_db, detection_db, incident_db) as correlation:
        report = correlation.run()
        incidents = correlation.query("wangkun78", "2026-01-01", "2026-03-01")
    assert report["incident_count"] > 0
    mixed = next(item for item in incidents if item["detector_family_count"] == 2)
    assert mixed["detector_families"] == ["rule", "statistical"]

    with FusionEngine(detection_db, incident_db, risk_db) as fusion:
        fusion.run()
        assessments = fusion.query("wangkun78", "2026-01-01", "2026-03-01")["assessments"]
    assessment = next(item for item in assessments if item["incident_id"] == mixed["incident_id"])
    assert assessment["risk_breakdown"]["detector_family_diversity_bonus"] == 3
