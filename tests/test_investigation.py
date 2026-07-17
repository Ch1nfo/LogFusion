from __future__ import annotations

import json
import sqlite3

from logfusion.cases import CaseEngine
from logfusion.fusion import FusionEngine
from logfusion.investigation import get_case_investigation
from tests.test_fusion import _seed_incidents


def _seed_case(tmp_path):
    _, _, detection_db, incident_db, risk_db = _seed_incidents(tmp_path)
    with FusionEngine(detection_db, incident_db, risk_db) as fusion:
        fusion.run()
    case_db = tmp_path / "cases.db"
    with CaseEngine(risk_db, case_db) as cases:
        cases.sync()
        case_id = cases.list()[0]["case_id"]
    return case_db, risk_db, incident_db, detection_db, case_id


def test_investigation_joins_exact_chain_and_keeps_referenced_superseded_candidate(tmp_path):
    case_db, risk_db, incident_db, detection_db, case_id = _seed_case(tmp_path)
    case_connection = sqlite3.connect(case_db)
    incident_id = case_connection.execute("SELECT incident_id FROM cases WHERE case_id=?", (case_id,)).fetchone()[0]
    case_connection.close()
    incident_connection = sqlite3.connect(incident_db)
    candidate_id = incident_connection.execute(
        "SELECT candidate_id FROM incident_candidates WHERE incident_id=? LIMIT 1", (incident_id,)
    ).fetchone()[0]
    incident_connection.close()
    connection = sqlite3.connect(detection_db)
    connection.execute("UPDATE anomaly_candidates SET status='superseded',explanation_json=? WHERE candidate_id=?", (
        json.dumps({"safe": "kept", "raw_text": "must-not-leak"}), candidate_id,
    ))
    connection.commit()
    connection.close()

    result = get_case_investigation(case_db, risk_db, incident_db, detection_db, case_id)
    assert result["alert"]["alert_id"] == result["case"]["current_alert_id"]
    assert result["assessment"]["assessment_id"] == result["alert"]["assessment_id"]
    assert result["incident"]["incident_id"] == result["case"]["incident_id"]
    referenced = next(item for item in result["candidates"] if item["candidate_id"] == candidate_id)
    assert referenced["status"] == "superseded"
    assert referenced["explanation"] == {"safe": "kept"}
    assert "raw_text" not in json.dumps(result)
    assert result["evidence_status"] == {"risk": "available", "incident": "available", "detection": "available"}


def test_investigation_degrades_by_section_when_auxiliary_state_is_missing(tmp_path):
    case_db, risk_db, incident_db, detection_db, case_id = _seed_case(tmp_path)
    result = get_case_investigation(case_db, tmp_path / "missing-risk.db", incident_db, detection_db, case_id)
    assert result["case"]["case_id"] == case_id
    assert result["alert"] is None
    assert result["evidence_status"]["risk"] == "missing"
    assert result["evidence_status"]["incident"] == "available"
    assert result["candidates"]
