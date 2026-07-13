from __future__ import annotations

import json

from logfusion.baseline import BaselineEngine
from logfusion.cli import main
from tests.test_detection import _seed_ready
from tests.test_evaluation import _case_state


def test_evaluate_run_query_and_compare_cli(tmp_path, capsys):
    feature_db, baseline_db, case_db, evaluation_db = (tmp_path / name for name in ("features.db", "baseline.db", "cases.db", "evaluation.db"))
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    _case_state(case_db)
    args = ["evaluate", "run", "--feature-state", str(feature_db), "--baseline-state", str(baseline_db), "--case-state", str(case_db), "--state", str(evaluation_db), "--name", "cli", "--from", "2026-01-21T00:00:00Z", "--to", "2026-01-22T00:00:00Z"]
    assert main(args) == 0
    report = json.loads(capsys.readouterr().out)
    experiment_id = report["experiment_id"]
    assert main(["evaluate", "query", "--state", str(evaluation_db), "--experiment-id", experiment_id]) == 0
    assert json.loads(capsys.readouterr().out)["experiment_id"] == experiment_id
    assert main(["evaluate", "compare", "--state", str(evaluation_db), "--experiment-id", experiment_id, "--experiment-id", experiment_id]) == 0
    assert json.loads(capsys.readouterr().out)["delta"]["f1"] == 0
