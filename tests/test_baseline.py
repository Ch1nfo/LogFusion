from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from logfusion.baseline import BaselineConfig, BaselineEngine, BaselineError
from logfusion.features import FeatureEngine


def _event(event_id: str, timestamp: str, *, user: str = "wangkun78", ip: str = "10.0.0.1", action: str = "repo_read", resource_type: str = "git_repo", resource: str = "core", bytes_: int = 0) -> dict:
    return {
        "event": {"id": event_id, "time": timestamp, "action": action, "outcome": "success"},
        "user": {"name": user},
        "raw": {"source_type": "gitlab", "source_id": "gitlab-main"},
        "source": {"ip": ip},
        "resource": {"type": resource_type, "name": resource},
        "network": {"bytes": bytes_},
    }


def _seed_feature_state(path, days: int = 14) -> None:
    with FeatureEngine(path, mode="replay") as engine:
        start = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
        for day in range(days):
            timestamp = (start + timedelta(days=day)).isoformat().replace("+00:00", "Z")
            engine.process_event(_event(f"event-{day}", timestamp, bytes_=day + 1))
            engine.process_event(_event(f"second-{day}", timestamp, action="repo_write", resource="ops"))
        engine.flush_sessions()


def test_builds_personal_global_metrics_value_frequency_and_ready_state(tmp_path):
    feature_db = tmp_path / "features.db"
    baseline_db = tmp_path / "baseline.db"
    _seed_feature_state(feature_db, days=20)

    engine = BaselineEngine(feature_db, baseline_db)
    report = engine.update()

    assert report["processed_revision"] > 0
    user = engine.query_user("wangkun78", 86400)
    daily_event = next(row for row in user["metrics"] if row["metric"] == "event_count")
    assert daily_event["status"] == "ready"
    assert daily_event["sample_count"] >= 14
    assert daily_event["mean"] == 2.0
    assert any(row["value_key"] == "git_repo:core" for row in user["values"])
    assert any(row["metric"] == "event_count" for row in user["global_metrics"])
    assert any(row["value_key"] == "git_repo:core" for row in user["global_values"])


def test_personal_baseline_fills_zero_windows_but_global_uses_existing_windows(tmp_path):
    feature_db = tmp_path / "features.db"
    baseline_db = tmp_path / "baseline.db"
    with FeatureEngine(feature_db, mode="replay") as features:
        features.process_event(_event("only", "2026-01-01T12:00:00Z"))
        features.process_event(_event("later", "2026-01-02T12:00:00Z"))
        features.flush_sessions()

    engine = BaselineEngine(feature_db, baseline_db)
    engine.update(as_of="2026-01-03T00:00:00Z")
    user_metric = next(row for row in engine.query_user("wangkun78", 3600)["metrics"] if row["metric"] == "event_count")
    global_metric = next(row for row in engine.query_user("wangkun78", 3600)["global_metrics"] if row["metric"] == "event_count")
    assert user_metric["sample_count"] > 1
    assert user_metric["mean"] < 1.0
    assert global_metric["sample_count"] == 2
    assert global_metric["mean"] == 1.0


def test_ready_requires_both_fourteen_active_days_and_twenty_samples(tmp_path):
    feature_db = tmp_path / "features.db"
    baseline_db = tmp_path / "baseline.db"
    _seed_feature_state(feature_db, days=14)
    engine = BaselineEngine(feature_db, baseline_db)
    engine.update(as_of="2026-01-14T13:00:00Z")
    daily = next(row for row in engine.query_user("wangkun78", 86400)["metrics"] if row["metric"] == "event_count")
    hourly = next(row for row in engine.query_user("wangkun78", 3600)["metrics"] if row["metric"] == "event_count")
    assert daily["active_day_count"] == 14
    assert daily["sample_count"] == 14
    assert daily["status"] == "warming_up"
    assert hourly["sample_count"] >= 20
    assert hourly["status"] == "ready"


def test_incremental_revision_recomputes_changed_user_and_rejects_feature_mismatch(tmp_path):
    feature_db = tmp_path / "features.db"
    baseline_db = tmp_path / "baseline.db"
    _seed_feature_state(feature_db)
    engine = BaselineEngine(feature_db, baseline_db)
    first = engine.update()

    with FeatureEngine(feature_db, mode="realtime") as features:
        features.process_event(_event("live", "2026-01-14T13:00:00Z", bytes_=100))
    second = engine.update(as_of="2026-01-14T13:00:00Z")
    assert second["processed_revision"] > first["processed_revision"]
    assert second["recomputed_users"] == 1

    with pytest.raises(BaselineError, match="baseline configuration"):
        BaselineEngine(feature_db, baseline_db, BaselineConfig(history_days=7))


def test_as_of_advance_expires_history_and_resources_do_not_mix(tmp_path):
    feature_db = tmp_path / "features.db"
    baseline_db = tmp_path / "baseline.db"
    with FeatureEngine(feature_db, mode="replay") as features:
        features.process_event(_event("old", "2026-01-01T12:00:00Z", resource_type="git_repo", resource="common"))
        features.process_event(_event("new", "2026-02-01T12:00:00Z", resource_type="svn_path", resource="common"))
        features.flush_sessions()
    engine = BaselineEngine(feature_db, baseline_db)
    engine.update(as_of="2026-02-02T00:00:00Z")

    values = engine.query_user("wangkun78", 3600)["values"]
    assert [row["value_key"] for row in values if row["value_type"] == "resource"] == ["svn_path:common"]
