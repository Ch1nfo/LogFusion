from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sqlite3

import pytest

from logfusion.baseline import BaselineEngine
from logfusion.correlation import CorrelationEngine
from logfusion.detection import DetectionEngine, DetectionError
from logfusion.features import FeatureEngine
from logfusion.rules import save_rules


def _event(event_id: str, timestamp: str, outcome: str = "success") -> dict:
    return {
        "event": {"id": event_id, "time": timestamp, "category": "authentication", "type": "login", "action": "user_login", "outcome": outcome},
        "user": {"name": "alice"}, "source": {"ip": "10.1.2.3"}, "destination": {}, "host": {},
        "resource": {"type": "application", "name": "portal"}, "http": {}, "parser": {"id": "sso_json", "version": "1"},
        "raw": {"source_id": "sso-main", "source_type": "sso", "record_id": f"raw-{event_id}", "checksum": "sha256:" + "a" * 64, "storage_ref": f"kafka://sso/0/{event_id}"},
    }


def _active_rule(rule_id="failed-login", version=1, status="active"):
    return {
        "rule_id": rule_id, "version": version, "name": "Failed login", "description": "", "status": status,
        "scope": "event", "source_types": ["sso"], "score": 90, "match": "all",
        "conditions": [{"field": "event.outcome", "operator": "eq", "value": "failure"}], "tags": ["auth"],
        "tests": [
            {"name": "positive", "input": {"event.outcome": "failure", "raw.source_type": "sso"}, "expected_match": True},
            {"name": "negative", "input": {"event.outcome": "success", "raw.source_type": "sso"}, "expected_match": False},
        ],
    }


def _window_rule():
    return {
        "rule_id": "failure-burst", "version": 1, "name": "Failure burst", "description": "", "status": "active",
        "scope": "window", "source_types": ["sso"], "window_size": 60, "score": 85, "match": "all",
        "conditions": [{"field": "failure_count", "operator": "gte", "value": 20}], "tags": ["auth"],
        "tests": [
            {"name": "positive", "input": {"failure_count": 20, "source_types": ["sso"]}, "expected_match": True},
            {"name": "negative", "input": {"failure_count": 2, "source_types": ["sso"]}, "expected_match": False},
        ],
    }


def _states(tmp_path):
    feature, baseline = tmp_path / "features.db", tmp_path / "baseline.db"
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with FeatureEngine(feature, mode="replay") as engine:
        for day in range(20):
            engine.process_event(_event(f"normal-{day}", (start + timedelta(days=day)).isoformat().replace("+00:00", "Z")))
        engine.process_event(_event("failed", (start + timedelta(days=20)).isoformat().replace("+00:00", "Z"), "failure"))
        engine.flush_sessions()
    with BaselineEngine(feature, baseline) as engine:
        engine.update(as_of=(start + timedelta(days=20)).isoformat().replace("+00:00", "Z"))
    return feature, baseline


def test_event_rule_writes_unified_candidate_and_metadata_only_evidence(tmp_path):
    feature, baseline = _states(tmp_path)
    rules, detection = tmp_path / "rules.json", tmp_path / "detection.db"
    save_rules({"schema_version": 1, "rules": [_active_rule()]}, rules)
    with DetectionEngine(feature, baseline, detection, rules_path=rules) as engine:
        report = engine.run()
        rows = [row for row in engine.query("alice", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z") if row["detector_family"] == "rule"]
    assert report["rules"]["candidate_count"] == 1
    assert rows[0]["detector_id"] == "failed-login" and rows[0]["detector_mode"] == "active"
    assert rows[0]["evidence"][0]["storage_ref"].startswith("kafka://")
    assert "raw_text" not in rows[0]["evidence"][0]


def test_shadow_rule_is_queryable_but_not_correlated(tmp_path):
    feature, baseline = _states(tmp_path)
    rules, detection, incidents = tmp_path / "rules.json", tmp_path / "detection.db", tmp_path / "incidents.db"
    save_rules({"schema_version": 1, "rules": [{**_active_rule(status="shadow"), "tests": []}]}, rules)
    with DetectionEngine(feature, baseline, detection, rules_path=rules) as engine:
        engine.run()
        assert any(row["detector_mode"] == "shadow" for row in engine.query("alice", "2026-01-01", "2026-02-01"))
    with CorrelationEngine(feature, detection, incidents) as engine:
        engine.run()
        correlated = engine.query("alice", "2026-01-01", "2026-02-01")
        assert all(item["detector_family"] != "rule" for incident in correlated for item in incident["timeline"])


def test_rule_change_requires_explicit_refresh_and_preserves_statistical_candidates(tmp_path):
    feature, baseline = _states(tmp_path)
    rules, detection = tmp_path / "rules.json", tmp_path / "detection.db"
    save_rules({"schema_version": 1, "rules": [_active_rule()]}, rules)
    with DetectionEngine(feature, baseline, detection, rules_path=rules) as engine:
        engine.run()
    changed = {**_active_rule(rule_id="failed-login-v2"), "score": 95}
    save_rules({"schema_version": 1, "rules": [changed]}, rules)
    with DetectionEngine(feature, baseline, detection, rules_path=rules) as engine:
        with pytest.raises(DetectionError, match="refresh-rules"):
            engine.run()
        report = engine.run(refresh_rules=True)
        rows = engine.query("alice", "2026-01-01", "2026-02-01")
    assert report["rules"]["candidate_count"] == 1
    assert any(row["detector_id"] == "failed-login-v2" for row in rows)


def test_window_rule_caps_representative_evidence_and_reports_total(tmp_path):
    feature, baseline = tmp_path / "features.db", tmp_path / "baseline.db"
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with FeatureEngine(feature, mode="replay") as engine:
        for day in range(20):
            engine.process_event(_event(f"normal-{day}", (start + timedelta(days=day)).isoformat().replace("+00:00", "Z")))
        burst = start + timedelta(days=20)
        for index in range(25):
            engine.process_event(_event(f"failure-{index}", (burst + timedelta(seconds=index)).isoformat().replace("+00:00", "Z"), "failure"))
        engine.flush_sessions()
    with BaselineEngine(feature, baseline) as engine:
        engine.update(as_of=(burst + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"))
    rules, detection = tmp_path / "rules.json", tmp_path / "detection.db"
    save_rules({"schema_version": 1, "rules": [_window_rule()]}, rules)
    with DetectionEngine(feature, baseline, detection, rules_path=rules) as engine:
        engine.run()
        candidate = next(row for row in engine.query("alice", burst, burst + timedelta(minutes=1)) if row["detector_id"] == "failure-burst")
    assert len(candidate["evidence"]) == 20
    assert candidate["evidence_query"]["event_count"] == 25
    assert candidate["evidence_query"]["evidence_truncated"] is True


def test_detection_v2_state_is_explicitly_rejected(tmp_path):
    feature, baseline = _states(tmp_path)
    detection = tmp_path / "detection.db"
    with sqlite3.connect(detection) as connection:
        connection.execute("CREATE TABLE detection_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        connection.execute("INSERT INTO detection_meta VALUES ('detection_schema_version','2')")
    with pytest.raises(DetectionError, match="rebuild Detection, Incident, Risk"):
        DetectionEngine(feature, baseline, detection)
