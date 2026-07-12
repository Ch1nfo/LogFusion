from __future__ import annotations

import json

from logfusion.cli import main
from tests.test_fusion import _seed_incidents


def test_fuse_run_and_query_cli(tmp_path, capsys):
    _, _, detection_db, incident_db, risk_db = _seed_incidents(tmp_path)
    assert main([
        "fuse", "run", "--detection-state", str(detection_db), "--incident-state", str(incident_db), "--state", str(risk_db),
    ]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["assessment_count"] > 0

    assert main([
        "fuse", "query", "--state", str(risk_db), "--user", "wangkun78",
        "--from", "2026-01-01T00:00:00Z", "--to", "2026-02-28T00:00:00Z",
    ]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["alerts"]

