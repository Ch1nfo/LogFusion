from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from logfusion.baseline import BaselineEngine
from logfusion.detection import DetectionEngine, DetectionError
from logfusion.features import FeatureEngine


def _event(event_id: str, timestamp: str, *, user: str = "wangkun78", ip: str = "10.0.0.1", action: str = "repo_read", resource: str = "core", count_bytes: int = 0) -> dict:
    return {
        "event": {"id": event_id, "time": timestamp, "action": action, "outcome": "success"},
        "user": {"name": user},
        "raw": {"source_type": "gitlab", "source_id": "gitlab-main"},
        "source": {"ip": ip},
        "resource": {"type": "git_repo", "name": resource},
        "network": {"bytes": count_bytes},
    }


def _seed_ready(feature_db, *, include_outlier: bool = False) -> str:
    start = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    with FeatureEngine(feature_db, mode="replay") as features:
        for day in range(20):
            timestamp = (start + timedelta(days=day)).isoformat().replace("+00:00", "Z")
            features.process_event(_event(f"normal-{day}", timestamp, count_bytes=1))
        if include_outlier:
            timestamp = (start + timedelta(days=20)).isoformat().replace("+00:00", "Z")
            for index in range(20):
                features.process_event(_event(f"outlier-{index}", timestamp, ip="10.99.0.1", resource="secret", count_bytes=1_000))
        features.flush_sessions()
    return (start + timedelta(days=20 if include_outlier else 19)).isoformat().replace("+00:00", "Z")


def _build_baseline(feature_db, baseline_db, as_of: str) -> None:
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of=as_of)


def test_numeric_personal_candidates_include_p95_p99_explanation_and_dedupe(tmp_path):
    feature_db, baseline_db, detection_db = tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db"
    as_of = _seed_ready(feature_db, include_outlier=True)
    _build_baseline(feature_db, baseline_db, as_of)

    engine = DetectionEngine(feature_db, baseline_db, detection_db)
    report = engine.run()
    candidates = engine.query("wangkun78", "2026-01-01T00:00:00Z", "2026-02-28T00:00:00Z")
    numeric = [row for row in candidates if row["detector_id"] == "numeric_p99" and row["metric"] == "event_count"]
    assert report["candidate_count"] > 0
    assert numeric and numeric[0]["severity"] == "high"
    assert numeric[0]["explanation"]["reference"]["p99"] is not None
    assert engine.run()["candidate_count"] == 0


def test_new_and_rare_values_use_composite_resource_key(tmp_path):
    feature_db, baseline_db, detection_db = tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db"
    as_of = _seed_ready(feature_db, include_outlier=True)
    _build_baseline(feature_db, baseline_db, as_of)

    engine = DetectionEngine(feature_db, baseline_db, detection_db)
    engine.run()
    candidates = engine.query("wangkun78", "2026-01-01T00:00:00Z", "2026-02-28T00:00:00Z")
    values = [row for row in candidates if row["value_type"] == "resource"]
    assert any(row["value_key"] == "git_repo:secret" and row["detector_id"] == "new_value_30d" for row in values)
    assert all(row["value_key"] != "secret" for row in values)


def test_detection_refuses_stale_baseline_and_supersedes_changed_window_candidates(tmp_path):
    feature_db, baseline_db, detection_db = tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db"
    as_of = _seed_ready(feature_db, include_outlier=True)
    _build_baseline(feature_db, baseline_db, as_of)
    engine = DetectionEngine(feature_db, baseline_db, detection_db)
    engine.run()

    with FeatureEngine(feature_db, mode="realtime") as features:
        features.process_event(_event("changed", "2026-01-21T12:00:00Z", ip="10.99.0.1", resource="secret"))
    with pytest.raises(DetectionError, match="baseline build"):
        engine.run()

    _build_baseline(feature_db, baseline_db, "2026-01-21T12:00:00Z")
    report = engine.run()
    assert report["superseded_count"] >= 1


def test_warming_user_uses_ready_global_reference_or_skips_when_global_warming(tmp_path):
    feature_db, baseline_db, detection_db = tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db"
    as_of = _seed_ready(feature_db, include_outlier=True)
    with FeatureEngine(feature_db, mode="realtime") as features:
        features.process_event(_event("new-user", "2026-01-21T12:00:00Z", user="newcomer", count_bytes=5_000))
    _build_baseline(feature_db, baseline_db, "2026-01-21T12:00:00Z")
    engine = DetectionEngine(feature_db, baseline_db, detection_db)
    engine.run()
    newcomer = engine.query("newcomer", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")
    assert any(row["baseline_scope"] == "global" for row in newcomer)

