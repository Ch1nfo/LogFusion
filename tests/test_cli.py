import json
from pathlib import Path

from logfusion.cli import main


def test_cli_writes_normalized_and_unknown_jsonl(tmp_path):
    output = tmp_path / "normalized.jsonl"
    unknown = tmp_path / "unknown.jsonl"
    summary = tmp_path / "summary.json"

    exit_code = main([
        "parse",
        "--config",
        "config/sources.yaml",
        "--output",
        str(output),
        "--unknown-output",
        str(unknown),
        "--summary-output",
        str(summary),
    ])

    assert exit_code == 0
    normalized_events = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    unknown_events = unknown.read_text(encoding="utf-8").splitlines()
    assert len(normalized_events) == 8
    assert unknown_events == []
    assert normalized_events[0]["parser"]["status"] == "success"
    summary_doc = json.loads(summary.read_text(encoding="utf-8"))
    assert summary_doc["total_records"] == 8
    assert summary_doc["parse_success_rate"] == 1.0
