from __future__ import annotations

import json

from logfusion.cli import main
from tests.test_correlation import _seed_states


def test_correlate_run_and_query_cli(tmp_path, capsys):
    feature_db, baseline_db, detection_db, incident_db = (
        tmp_path / "features.db", tmp_path / "baseline.db", tmp_path / "detection.db", tmp_path / "incidents.db",
    )
    _seed_states(feature_db, baseline_db, detection_db)

    assert main([
        "correlate", "run", "--feature-state", str(feature_db), "--detection-state", str(detection_db),
        "--state", str(incident_db),
    ]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["incident_count"] > 0

    assert main([
        "correlate", "query", "--state", str(incident_db), "--user", "wangkun78",
        "--from", "2026-01-01T00:00:00Z", "--to", "2026-02-28T00:00:00Z",
    ]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["incidents"]

