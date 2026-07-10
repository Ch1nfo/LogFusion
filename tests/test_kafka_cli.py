from pathlib import Path

from logfusion.cli import main
from logfusion.config import load_kafka_config


def _write_kafka_config(path: Path) -> None:
    path.write_text(
        "kafka:\n"
        "  bootstrap_servers: kafka-1:9092\n"
        "  group_id: logfusion-audit\n"
        "  topics:\n"
        "    - audit\n"
        "    - auth\n"
        "  source_id: audit_kafka\n"
        "  source_type: unknown\n"
        "  include_raw_text: true\n"
        "  sasl_username_env: KAFKA_USERNAME\n",
        encoding="utf-8",
    )


def test_load_kafka_config_reads_source_contract_and_topics(tmp_path):
    config_path = tmp_path / "kafka.yaml"
    _write_kafka_config(config_path)

    config = load_kafka_config(config_path)

    assert config == {
        "bootstrap_servers": "kafka-1:9092",
        "group_id": "logfusion-audit",
        "topics": ["audit", "auth"],
        "source_id": "audit_kafka",
        "source_type": "unknown",
        "include_raw_text": True,
        "sasl_username_env": "KAFKA_USERNAME",
    }


def test_consume_kafka_cli_forwards_outputs_and_checkpoint_options(tmp_path, monkeypatch):
    config_path = tmp_path / "kafka.yaml"
    _write_kafka_config(config_path)
    calls = []

    def fake_runtime(config, **kwargs):
        calls.append((config, kwargs))
        return {"total_records": 0}

    monkeypatch.setattr("logfusion.cli.run_kafka_streaming_pipeline", fake_runtime)
    exit_code = main([
        "consume", "kafka",
        "--config", str(config_path),
        "--output", str(tmp_path / "normalized.jsonl"),
        "--unknown-output", str(tmp_path / "unknown.jsonl"),
        "--summary-output", str(tmp_path / "summary.json"),
        "--raw-store-output", str(tmp_path / "raw.jsonl"),
        "--checkpoint", str(tmp_path / "checkpoint.json"),
        "--checkpoint-every", "25",
        "--once",
    ])

    assert exit_code == 0
    assert calls == [(
        load_kafka_config(config_path),
        {
            "normalized_output": tmp_path / "normalized.jsonl",
            "unknown_output": tmp_path / "unknown.jsonl",
            "summary_output": tmp_path / "summary.json",
            "raw_output": tmp_path / "raw.jsonl",
            "registry_path": None,
            "checkpoint_path": tmp_path / "checkpoint.json",
            "checkpoint_every": 25,
            "resume": False,
            "stop_when_idle": True,
        },
    )]
