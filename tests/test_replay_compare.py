import json
from pathlib import Path

from logfusion.cli import main
from logfusion.config import load_sources_config
from logfusion.raw_store import write_raw_store
from logfusion.replay_compare import compare_raw_replays


def _active_registry_for_unknown(path: Path):
    parser = {
        "parser_id": "candidate_runtime",
        "source_type": "unknown",
        "format": "key_value",
        "version": "0.1.0",
        "status": "active",
        "priority": 100,
        "owner": "system",
        "created_at": "2026-07-09T00:00:00Z",
        "updated_at": "2026-07-09T00:00:00Z",
        "candidate_id": "candidate_runtime",
        "template_hint": "level=<VAR> user=<VAR> action=<VAR> bytes=<NUM>",
        "parser_type": "key_value",
        "field_candidates": {"user": "alice", "action": "download", "bytes": "123"},
        "schema_mapping": {
            "user": "user.name",
            "action": "event.action",
            "bytes": "network.bytes",
        },
        "test_case_count": 1,
        "success_rate": 1.0,
        "schema_mapping_rate": 1.0,
        "shadow_replay_record_count": 1,
        "shadow_replay_success_rate": 1.0,
        "shadow_replay_schema_mapping_rate": 1.0,
    }
    path.write_text(json.dumps({"version": 1, "parsers": [parser]}), encoding="utf-8")


def test_compare_raw_replays_reports_unknown_resolved_with_current_registry(tmp_path):
    raw_store = tmp_path / "unknown_raw.jsonl"
    registry = tmp_path / "registry.json"
    write_raw_store([{
        "source_id": "unknown_sample",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": ["data/unknown/*.log"],
    }], raw_store)
    _active_registry_for_unknown(registry)

    report = compare_raw_replays(raw_store, baseline_registry_path=None, current_registry_path=registry)

    assert report["summary_delta"]["normalized_records"] == 1
    assert report["summary_delta"]["unknown_records"] == -1
    assert report["record_changes"]["unknown_resolved"] == 1
    assert report["record_changes"]["new_unknown"] == 0
    assert report["record_changes"]["parser_changed"] == 1
    assert len(report["changes"]) == 1
    change = report["changes"][0]
    assert change["change_type"] == "unknown_resolved"
    assert change["current_parser_id"] == "candidate_runtime"


def test_compare_raw_replays_reports_no_change_for_same_parser_state(tmp_path):
    raw_store = tmp_path / "raw_records.jsonl"
    write_raw_store(load_sources_config(Path("config/sources.yaml")), raw_store)

    report = compare_raw_replays(raw_store)

    assert report["record_changes"]["unchanged"] == 8
    assert report["record_changes"]["parser_changed"] == 0
    assert report["changes"] == []


def test_cli_replay_compare_writes_report(tmp_path):
    raw_store = tmp_path / "unknown_raw.jsonl"
    registry = tmp_path / "registry.json"
    output = tmp_path / "compare.json"
    write_raw_store([{
        "source_id": "unknown_sample",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": ["data/unknown/*.log"],
    }], raw_store)
    _active_registry_for_unknown(registry)

    assert main([
        "replay",
        "compare",
        "--raw-input", str(raw_store),
        "--current-registry", str(registry),
        "--output", str(output),
    ]) == 0

    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["record_changes"]["unknown_resolved"] == 1
