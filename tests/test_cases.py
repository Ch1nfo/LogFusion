from __future__ import annotations

from logfusion.baseline import BaselineEngine
from logfusion.cases import CaseEngine, CaseError, get_case, list_cases
from logfusion.correlation import CorrelationEngine
from logfusion.detection import DetectionEngine
from logfusion.features import FeatureEngine
from logfusion.fusion import FusionEngine
from tests.test_correlation import _event
from tests.test_fusion import _seed_incidents


def test_case_sync_preserves_analyst_state_across_fusion_alert_versions(tmp_path):
    feature_db, baseline_db, detection_db, incident_db, risk_db = _seed_incidents(tmp_path)
    with FusionEngine(detection_db, incident_db, risk_db) as fusion:
        fusion.run()
    case_db = tmp_path / "cases.db"
    with CaseEngine(risk_db, case_db) as cases:
        report = cases.sync()
        assert report["created_count"] > 0
        case_id = cases.list()[0]["case_id"]
        cases.assign(case_id, "alice", "analyst-1", "take ownership")
        cases.set_tags(case_id, ["ueba", "repo"], "analyst-1")
        cases.comment(case_id, "analyst-1", "reviewed evidence")
        resolved = cases.transition(case_id, "false_positive", "analyst-1", "approved automation")
        assert resolved["status"] == "false_positive"
        assert resolved["owner"] == "alice"
        assert resolved["tags"] == ["repo", "ueba"]

    with FeatureEngine(feature_db, mode="realtime") as features:
        features.process_event(_event("new-risk", "2026-01-21T13:00:00Z", bytes_=9_000))
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of="2026-01-21T13:00:00Z")
    with DetectionEngine(feature_db, baseline_db, detection_db) as detection:
        detection.run()
    with CorrelationEngine(feature_db, detection_db, incident_db) as correlation:
        correlation.run()
    with FusionEngine(detection_db, incident_db, risk_db) as fusion:
        fusion.run()

    with CaseEngine(risk_db, case_db) as cases:
        report = cases.sync()
        refreshed = cases.get(case_id)
    assert report["refreshed_count"] >= 1
    assert refreshed["status"] == "false_positive"
    assert refreshed["requires_review"] is True
    assert any(event["event_type"] == "risk_alert_refreshed" for event in refreshed["events"])
    assert get_case(case_db, case_id)["owner"] == "alice"
    assert list_cases(case_db, status="false_positive")


def test_case_suppression_requires_future_expiry_and_is_audited(tmp_path):
    _, _, detection_db, incident_db, risk_db = _seed_incidents(tmp_path)
    with FusionEngine(detection_db, incident_db, risk_db) as fusion:
        fusion.run()
    case_db = tmp_path / "cases.db"
    with CaseEngine(risk_db, case_db) as cases:
        cases.sync()
        case_id = cases.list()[0]["case_id"]
        try:
            cases.transition(case_id, "suppressed", "analyst", suppression_until="2020-01-01T00:00:00Z")
        except CaseError as error:
            assert "future" in str(error)
        else:  # pragma: no cover
            raise AssertionError("past suppression should fail")
        suppressed = cases.transition(case_id, "suppressed", "analyst", suppression_until="2030-01-01T00:00:00Z")
    assert suppressed["status"] == "suppressed"
    assert suppressed["suppression_until"] == "2030-01-01T00:00:00.000Z"


def test_case_suppression_refreshes_fusion_without_deleting_assessment(tmp_path):
    _, _, detection_db, incident_db, risk_db = _seed_incidents(tmp_path)
    with FusionEngine(detection_db, incident_db, risk_db) as fusion:
        fusion.run()
    case_db = tmp_path / "cases.db"
    with CaseEngine(risk_db, case_db) as cases:
        cases.sync()
        case_id = cases.list()[0]["case_id"]
        alert_key = cases.get(case_id)["alert_key"]
        cases.transition(case_id, "suppressed", "analyst", suppression_until="2030-01-01T00:00:00Z")
    with FusionEngine(detection_db, incident_db, risk_db, case_state=case_db) as fusion:
        report = fusion.run(refresh_policy=True)
        result = fusion.query("wangkun78", "2026-01-01T00:00:00Z", "2026-02-28T00:00:00Z")
    assert report["suppressed_count"] >= 1
    assert all(alert["alert_key"] != alert_key for alert in result["alerts"])
    assert any(row["policy_action"] == "suppressed_by_case_policy" for row in result["assessments"])
