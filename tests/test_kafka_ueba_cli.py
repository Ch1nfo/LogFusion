from __future__ import annotations

from logfusion.cli import main


def test_consume_kafka_ueba_cli_routes_all_state_paths(tmp_path, monkeypatch):
    captured = {}

    def fake_run(config, **kwargs):
        captured["config"] = config
        captured.update(kwargs)
        return {}

    monkeypatch.setattr("logfusion.cli.load_kafka_config", lambda path: {
        "bootstrap_servers": "broker:9092", "group_id": "group", "topics": ["audit"],
        "source_id": "source", "source_type": "sso",
    })
    monkeypatch.setattr("logfusion.cli.run_kafka_ueba_pipeline", fake_run)
    config = tmp_path / "kafka.yaml"
    config.write_text("ignored", encoding="utf-8")
    assert main([
        "consume", "kafka-ueba", "--config", str(config), "--output", str(tmp_path / "normalized.jsonl"),
        "--feature-state", str(tmp_path / "features.db"), "--baseline-state", str(tmp_path / "baseline.db"),
        "--detection-state", str(tmp_path / "detection.db"), "--incident-state", str(tmp_path / "incidents.db"),
        "--risk-state", str(tmp_path / "risk.db"), "--watermark-lag-seconds", "15", "--once",
    ]) == 0
    assert captured["stop_when_idle"] is True
    assert captured["orchestrator_config"].watermark_lag_seconds == 15
    assert captured["feature_state"] == tmp_path / "features.db"
