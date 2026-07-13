from __future__ import annotations

import json

from logfusion.cli import main
from logfusion.fusion import FusionEngine
from tests.test_fusion import _seed_incidents


def test_cases_cli_sync_list_and_transition(tmp_path, capsys):
    _, _, detection_db, incident_db, risk_db = _seed_incidents(tmp_path)
    with FusionEngine(detection_db, incident_db, risk_db) as fusion:
        fusion.run()
    case_db = tmp_path / "cases.db"
    assert main(["cases", "sync", "--risk-state", str(risk_db), "--state", str(case_db)]) == 0
    assert json.loads(capsys.readouterr().out)["created_count"] > 0

    assert main(["cases", "list", "--state", str(case_db)]) == 0
    case_id = json.loads(capsys.readouterr().out)[0]["case_id"]
    assert main([
        "cases", "transition", "--risk-state", str(risk_db), "--state", str(case_db),
        "--case-id", case_id, "--status", "acknowledged", "--actor", "analyst",
    ]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "acknowledged"


def test_cases_disposition_cli(tmp_path, capsys):
    _, _, detection_db, incident_db, risk_db = _seed_incidents(tmp_path)
    with FusionEngine(detection_db, incident_db, risk_db) as fusion:
        fusion.run()
    case_db = tmp_path / "cases.db"
    assert main(["cases", "sync", "--risk-state", str(risk_db), "--state", str(case_db)]) == 0
    assert json.loads(capsys.readouterr().out)["created_count"] > 0
    assert main(["cases", "list", "--state", str(case_db)]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert main(["cases", "disposition", "--risk-state", str(risk_db), "--state", str(case_db), "--case-id", rows[0]["case_id"], "--value", "benign", "--actor", "analyst"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["disposition"] == "benign"
    assert result["status"] == "new"
