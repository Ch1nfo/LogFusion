import json

import pytest

from logfusion.cli import main
from logfusion.parser_registry import ParserRegistryError, register_candidates, set_parser_status
from logfusion.parser_test_harness import run_registry_tests


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


def test_registry_tests_update_success_metrics(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    register_candidates([_candidate()], registry_path)

    report = run_registry_tests(registry_path, "candidate_f1691c2e6bbab95d")

    assert report["parser_id"] == "candidate_f1691c2e6bbab95d"
    assert report["test_case_count"] == 1
    assert report["passed_count"] == 1
    assert report["success_rate"] == 1.0
    assert report["schema_mapping_rate"] == 1.0
    assert report["results"][0]["passed"] is True

    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    parser = registry["parsers"][0]
    assert parser["test_case_count"] == 1
    assert parser["success_rate"] == 1.0
    assert parser["last_test_report"]["passed_count"] == 1


def test_shadow_transition_requires_passing_tests(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    register_candidates([_candidate()], registry_path)
    set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "testing")

    with pytest.raises(ParserRegistryError, match="passing parser tests"):
        set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "shadow")

    run_registry_tests(registry_path, "candidate_f1691c2e6bbab95d")
    registry = set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "shadow")

    assert registry["parsers"][0]["status"] == "shadow"


def test_cli_registry_test_writes_report_and_updates_registry(tmp_path):
    candidates_path = tmp_path / "candidates.jsonl"
    registry_path = tmp_path / "parser_registry.json"
    report_path = tmp_path / "parser_test_report.json"
    candidates_path.write_text(json.dumps(_candidate()) + "\n", encoding="utf-8")

    assert main([
        "registry",
        "register-candidates",
        "--candidates",
        str(candidates_path),
        "--registry",
        str(registry_path),
    ]) == 0
    assert main([
        "registry",
        "test",
        "--registry",
        str(registry_path),
        "--parser-id",
        "candidate_f1691c2e6bbab95d",
        "--output",
        str(report_path),
    ]) == 0

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["success_rate"] == 1.0
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    assert registry["parsers"][0]["test_case_count"] == 1
