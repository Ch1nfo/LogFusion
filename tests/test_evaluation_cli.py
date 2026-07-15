from __future__ import annotations

import json

from logfusion.baseline import BaselineEngine
from logfusion.cli import main
from logfusion.config import load_model_detection_config
from tests.test_detection import _seed_ready


def test_evaluate_run_query_and_compare_cli(tmp_path, capsys):
    feature_db, baseline_db, evaluation_db = (tmp_path / name for name in ("features.db", "baseline.db", "evaluation.db"))
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    args = ["evaluate", "run", "--feature-state", str(feature_db), "--baseline-state", str(baseline_db), "--state", str(evaluation_db), "--name", "cli", "--from", "2026-01-21T00:00:00Z", "--to", "2026-01-22T00:00:00Z"]
    assert main(args) == 0
    report = json.loads(capsys.readouterr().out)
    experiment_id = report["experiment_id"]
    assert main(["evaluate", "query", "--state", str(evaluation_db), "--experiment-id", experiment_id]) == 0
    assert json.loads(capsys.readouterr().out)["experiment_id"] == experiment_id
    assert main(["evaluate", "compare", "--state", str(evaluation_db), "--experiment-id", experiment_id, "--experiment-id", experiment_id]) == 0
    compared = json.loads(capsys.readouterr().out)
    assert compared["delta"]["daily_prediction_cv"] == 0
    assert compared["overlap"]["jaccard"] == 1.0


def test_evaluate_model_cli_loads_config_and_writes_report(tmp_path, capsys):
    feature_db, baseline_db, evaluation_db = (tmp_path / name for name in ("features.db", "baseline.db", "evaluation.db"))
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    config = tmp_path / "model.yaml"
    config.write_text(
        "model_detection:\n"
        "  threshold_quantile: 0.99\n"
        "  min_training_samples: 10\n"
        "  hbos_min_bins: 4\n",
        encoding="utf-8",
    )
    assert load_model_detection_config(config)["threshold_quantile"] == 0.99
    output = tmp_path / "report.json"
    assert main([
        "evaluate", "run", "--feature-state", str(feature_db), "--baseline-state", str(baseline_db),
        "--state", str(evaluation_db), "--detector", "hbos",
        "--model-config", str(config), "--name", "hbos-cli", "--from", "2026-01-21T00:00:00Z",
        "--to", "2026-01-22T00:00:00Z", "--report-output", str(output),
    ]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["detector_id"] == "hbos"
    assert json.loads(output.read_text(encoding="utf-8"))["experiment_id"] == report["experiment_id"]


def test_evaluate_cli_rejects_invalid_model_configuration(tmp_path, capsys):
    config = tmp_path / "invalid.yaml"
    config.write_text("model_detection:\n  threshold_quantile: 2\n", encoding="utf-8")
    assert main([
        "evaluate", "run", "--feature-state", "missing-feature", "--baseline-state", "missing-baseline",
        "--state", str(tmp_path / "evaluation.db"), "--detector", "hbos",
        "--model-config", str(config), "--name", "invalid", "--from", "2026-01-01T00:00:00Z", "--to", "2026-01-02T00:00:00Z",
    ]) == 1
    assert "threshold_quantile" in capsys.readouterr().out


def test_evaluate_readiness_cli_writes_report(tmp_path, capsys):
    feature_db, baseline_db = tmp_path / "features.db", tmp_path / "baseline.db"
    as_of = _seed_ready(feature_db, include_outlier=True)
    with BaselineEngine(feature_db, baseline_db) as baseline:
        baseline.update(as_of)
    output = tmp_path / "readiness.json"
    assert main([
        "evaluate", "readiness", "--feature-state", str(feature_db), "--baseline-state", str(baseline_db),
        "--report-output", str(output),
    ]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["history"]["missing_days"] == 9
    assert json.loads(output.read_text(encoding="utf-8"))["as_of"] == report["as_of"]
