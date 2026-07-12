from __future__ import annotations

import pytest

from logfusion.correlation import CorrelationEngine
from logfusion.baseline import BaselineEngine
from logfusion.detection import DetectionEngine
from logfusion.features import FeatureEngine
from logfusion.fusion import FusionEngine, FusionError
from tests.test_correlation import _event, _seed_states


def _seed_incidents(tmp_path):
    feature_db, baseline_db, detection_db, incident_db, risk_db = (
        tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db", tmp_path / "incidents.db", tmp_path / "risk.db",
    )
    _seed_states(feature_db, baseline_db, detection_db)
    with CorrelationEngine(feature_db, detection_db, incident_db) as correlation:
        correlation.run()
    return feature_db, baseline_db, detection_db, incident_db, risk_db


def test_fuses_incidents_into_explainable_risk_and_alerts(tmp_path):
    feature_db, baseline_db, detection_db, incident_db, risk_db = _seed_incidents(tmp_path)
    engine = FusionEngine(detection_db, incident_db, risk_db)
    report = engine.run()
    result = engine.query("wangkun78", "2026-01-01T00:00:00Z", "2026-02-28T00:00:00Z")

    assert report["assessment_count"] > 0
    assessment = max(result["assessments"], key=lambda row: row["risk_score"])
    assert assessment["risk_score"] >= assessment["base_score"]
    assert assessment["risk_breakdown"]["cross_system_bonus"] == 10
    assert result["alerts"]


def test_fusion_is_noop_until_correlation_changes_and_rejects_stale_correlation(tmp_path):
    feature_db, baseline_db, detection_db, incident_db, risk_db = _seed_incidents(tmp_path)
    engine = FusionEngine(detection_db, incident_db, risk_db)
    engine.run()
    assert engine.run()["assessment_count"] == 0

    with FeatureEngine(feature_db, mode="realtime") as features:
        features.process_event(_event("later", "2026-01-21T13:00:00Z", bytes_=9_000))
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of="2026-01-21T13:00:00Z")
    with DetectionEngine(feature_db, baseline_db, detection_db) as detection:
        detection.run()
    with pytest.raises(FusionError, match="correlate run"):
        engine.run()
