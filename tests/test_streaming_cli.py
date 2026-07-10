import os
from pathlib import Path

from logfusion.cli import main


def test_parse_cli_uses_streaming_runtime_and_forwards_checkpoint_options(tmp_path, monkeypatch):
    calls = []

    def fake_run_streaming_pipeline(
        sources,
        normalized_output,
        unknown_output,
        summary_output,
        raw_output=None,
        registry_path=None,
        checkpoint_path=None,
        checkpoint_every=10_000,
        resume=False,
    ):
        calls.append({
            "sources": sources,
            "normalized_output": normalized_output,
            "unknown_output": unknown_output,
            "summary_output": summary_output,
            "raw_output": raw_output,
            "registry_path": registry_path,
            "checkpoint_path": checkpoint_path,
            "checkpoint_every": checkpoint_every,
            "resume": resume,
        })
        return {"total_records": 0}

    monkeypatch.setattr("logfusion.cli.run_streaming_pipeline", fake_run_streaming_pipeline)
    normalized = tmp_path / "normalized.jsonl"
    unknown = tmp_path / "unknown.jsonl"
    summary = tmp_path / "summary.json"
    raw = tmp_path / "raw.jsonl"
    registry = tmp_path / "registry.jsonl"
    checkpoint = tmp_path / "parse.checkpoint.json"

    exit_code = main([
        "parse",
        "--config", "config/sources.yaml",
        "--output", str(normalized),
        "--unknown-output", str(unknown),
        "--summary-output", str(summary),
        "--raw-store-output", str(raw),
        "--registry", str(registry),
        "--checkpoint", str(checkpoint),
        "--checkpoint-every", "7",
        "--resume",
    ])

    assert exit_code == 0
    assert len(calls) == 1
    call = calls[0]
    assert len(call["sources"]) == 4
    assert call["normalized_output"] == normalized
    assert call["unknown_output"] == unknown
    assert call["summary_output"] == summary
    assert call["raw_output"] == raw
    assert call["registry_path"] == registry
    assert call["checkpoint_path"] == checkpoint
    assert call["checkpoint_every"] == 7
    assert call["resume"] is True


def test_replay_raw_cli_uses_streaming_runtime_with_default_checkpoint_interval(tmp_path, monkeypatch):
    calls = []

    def fake_run_streaming_raw_replay(
        raw_input,
        normalized_output,
        unknown_output,
        summary_output,
        registry_path=None,
        checkpoint_path=None,
        checkpoint_every=10_000,
        resume=False,
    ):
        calls.append({
            "raw_input": raw_input,
            "normalized_output": normalized_output,
            "unknown_output": unknown_output,
            "summary_output": summary_output,
            "registry_path": registry_path,
            "checkpoint_path": checkpoint_path,
            "checkpoint_every": checkpoint_every,
            "resume": resume,
        })
        return {"total_records": 0}

    monkeypatch.setattr("logfusion.cli.run_streaming_raw_replay", fake_run_streaming_raw_replay)
    raw_input = tmp_path / "raw.jsonl"
    output = tmp_path / "normalized.jsonl"
    unknown = tmp_path / "unknown.jsonl"
    summary = tmp_path / "summary.json"
    checkpoint = tmp_path / "replay.checkpoint.json"

    exit_code = main([
        "replay", "raw",
        "--raw-input", str(raw_input),
        "--output", str(output),
        "--unknown-output", str(unknown),
        "--summary-output", str(summary),
        "--checkpoint", str(checkpoint),
    ])

    assert exit_code == 0
    assert calls == [{
        "raw_input": raw_input,
        "normalized_output": output,
        "unknown_output": unknown,
        "summary_output": summary,
        "registry_path": None,
        "checkpoint_path": checkpoint,
        "checkpoint_every": 10_000,
        "resume": False,
    }]


def test_parse_cli_rejects_resume_without_checkpoint(tmp_path, capsys):
    exit_code = main([
        "parse",
        "--config", "config/sources.yaml",
        "--output", str(tmp_path / "normalized.jsonl"),
        "--resume",
    ])

    assert exit_code == 2
    assert "--resume requires --checkpoint" in capsys.readouterr().out


def test_parse_cli_rejects_non_positive_checkpoint_interval(tmp_path, capsys):
    exit_code = main([
        "parse",
        "--config", "config/sources.yaml",
        "--output", str(tmp_path / "normalized.jsonl"),
        "--checkpoint-every", "0",
    ])

    assert exit_code == 2
    assert "--checkpoint-every must be greater than zero" in capsys.readouterr().out


def test_parse_cli_reports_missing_resume_checkpoint_without_traceback(tmp_path, capsys):
    exit_code = main([
        "parse",
        "--config", "config/sources.yaml",
        "--output", str(tmp_path / "normalized.jsonl"),
        "--checkpoint", str(tmp_path / "missing-checkpoint.json"),
        "--resume",
    ])

    assert exit_code == 1
    assert "checkpoint does not exist" in capsys.readouterr().out


def test_replay_raw_cli_rejects_input_output_collision_without_data_loss(tmp_path, capsys):
    raw_path = tmp_path / "raw.jsonl"
    original = b'{"sentinel": true}\n'
    raw_path.write_bytes(original)

    exit_code = main([
        "replay", "raw",
        "--raw-input", str(raw_path),
        "--output", str(raw_path),
    ])

    assert exit_code == 1
    assert "conflicts with input" in capsys.readouterr().out
    assert raw_path.read_bytes() == original


def test_parse_cli_rejects_config_output_collision_without_data_loss(tmp_path, capsys):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\n", encoding="utf-8")
    config_path = tmp_path / "sources.yaml"
    config_path.write_text(
        "sources:\n"
        "  - source_id: protected-config\n"
        "    source_type: new_source\n"
        "    paths:\n"
        f"      - {source_path}\n",
        encoding="utf-8",
    )
    original = config_path.read_bytes()

    exit_code = main([
        "parse",
        "--config", str(config_path),
        "--output", str(config_path),
        "--unknown-output", str(tmp_path / "unknown.jsonl"),
        "--summary-output", str(tmp_path / "summary.json"),
    ])

    assert exit_code == 1
    assert "config path conflicts with output" in capsys.readouterr().out
    assert config_path.read_bytes() == original


def test_parse_cli_rejects_hardlinked_config_output_collision(tmp_path, capsys):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\n", encoding="utf-8")
    config_path = tmp_path / "sources.yaml"
    config_path.write_text(
        "sources:\n"
        "  - source_id: hardlink-config\n"
        "    source_type: new_source\n"
        "    paths:\n"
        f"      - {source_path}\n",
        encoding="utf-8",
    )
    output_alias = tmp_path / "config-output-hardlink.jsonl"
    os.link(config_path, output_alias)
    original = config_path.read_bytes()

    exit_code = main([
        "parse",
        "--config", str(config_path),
        "--output", str(output_alias),
        "--unknown-output", str(tmp_path / "unknown.jsonl"),
        "--summary-output", str(tmp_path / "summary.json"),
    ])

    assert exit_code == 1
    assert "config path conflicts with output" in capsys.readouterr().out
    assert config_path.read_bytes() == original
