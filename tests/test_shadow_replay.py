import json

import pytest

from logfusion.cli import main
from logfusion.parser_registry import ParserRegistryError, register_candidates, set_parser_status
from logfusion.parser_test_harness import run_registry_tests
from logfusion.shadow_replay import run_shadow_replay


def _candidate():
    return {
        "candidate_id": "candidate_f1691c2e6bbab95d",
        "status": "draft",
        "suggested_source_type": "unknown",
        "detected_format": "key_value",
        "template_hint": "level=<VAR> user=<VAR> action=<VAR> bytes=<NUM>",
        "samples": [{
            "unknown_id": "u1",
            "record_id": "r1",
            "source_id": "unknown_sample",
            "raw_text": "level=INFO user=alice action=download bytes=123",
        }],
        "suggested_parser": {
            "parser_type": "key_value",
            "field_candidates": {
                "level": "INFO",
                "user": "alice",
                "action": "download",
                "bytes": "123",
            },
            "schema_mapping": {
                "level": "event.severity",
                "user": "user.name",
                "action": "event.action",
                "bytes": "network.bytes",
            },
            "confidence": 0.7,
        },
        "sample_count": 1,
    }


def _unknown_record():
    return {
        "unknown_id": "u1",
        "reason": "no_parser",
        "detected_format": "key_value",
        "suggested_source_type": "unknown",
        "template_hint": "level=<VAR> user=<VAR> action=<VAR> bytes=<NUM>",
        "raw": {
            "record_id": "r1",
            "source_id": "unknown_sample",
            "source_type": "unknown",
            "text": "level=INFO user=alice action=download bytes=123",
        },
    }


def _shadow_registry(registry_path):
    register_candidates([_candidate()], registry_path)
    set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "testing")
    run_registry_tests(registry_path, "candidate_f1691c2e6bbab95d")
    set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "shadow")


def test_shadow_replay_updates_registry_metrics(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    unknown_path = tmp_path / "unknown.jsonl"
    _shadow_registry(registry_path)
    unknown_path.write_text(json.dumps(_unknown_record()) + "\n", encoding="utf-8")

    report = run_shadow_replay(registry_path, unknown_path, "candidate_f1691c2e6bbab95d")

    assert report["parser_id"] == "candidate_f1691c2e6bbab95d"
    assert report["replay_record_count"] == 1
    assert report["matched_count"] == 1
    assert report["success_rate"] == 1.0
    assert report["schema_mapping_rate"] == 1.0
    assert report["results"][0]["shadow_only"] is True

    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    parser = registry["parsers"][0]
    assert parser["shadow_replay_record_count"] == 1
    assert parser["shadow_replay_success_rate"] == 1.0
    assert parser["last_shadow_replay_report"]["matched_count"] == 1


def test_active_transition_requires_successful_shadow_replay(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    unknown_path = tmp_path / "unknown.jsonl"
    _shadow_registry(registry_path)
    unknown_path.write_text(json.dumps(_unknown_record()) + "\n", encoding="utf-8")

    with pytest.raises(ParserRegistryError, match="successful shadow replay"):
        set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "active")

    run_shadow_replay(registry_path, unknown_path, "candidate_f1691c2e6bbab95d")
    registry = set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "active")

    assert registry["parsers"][0]["status"] == "active"


def test_cli_registry_replay_writes_report_and_allows_active(tmp_path):
    candidates_path = tmp_path / "candidates.jsonl"
    registry_path = tmp_path / "parser_registry.json"
    unknown_path = tmp_path / "unknown.jsonl"
    report_path = tmp_path / "shadow_replay_report.json"
    candidates_path.write_text(json.dumps(_candidate()) + "\n", encoding="utf-8")
    unknown_path.write_text(json.dumps(_unknown_record()) + "\n", encoding="utf-8")

    assert main([
        "registry", "register-candidates",
        "--candidates", str(candidates_path),
        "--registry", str(registry_path),
    ]) == 0
    assert main([
        "registry", "set-status",
        "--registry", str(registry_path),
        "--parser-id", "candidate_f1691c2e6bbab95d",
        "--status", "testing",
    ]) == 0
    assert main([
        "registry", "test",
        "--registry", str(registry_path),
        "--parser-id", "candidate_f1691c2e6bbab95d",
    ]) == 0
    assert main([
        "registry", "set-status",
        "--registry", str(registry_path),
        "--parser-id", "candidate_f1691c2e6bbab95d",
        "--status", "shadow",
    ]) == 0
    assert main([
        "registry", "replay",
        "--registry", str(registry_path),
        "--unknown-input", str(unknown_path),
        "--parser-id", "candidate_f1691c2e6bbab95d",
        "--output", str(report_path),
    ]) == 0
    assert main([
        "registry", "set-status",
        "--registry", str(registry_path),
        "--parser-id", "candidate_f1691c2e6bbab95d",
        "--status", "active",
    ]) == 0

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["success_rate"] == 1.0
