import json

import pytest

from logfusion.parser_registry import (
    ParserRegistryError,
    list_registry,
    register_candidates,
    set_parser_status,
)
from logfusion.parser_test_harness import run_registry_tests
from logfusion.shadow_replay import run_shadow_replay


def _candidate():
    return {
        "candidate_id": "candidate_f1691c2e6bbab95d",
        "status": "draft",
        "suggested_source_type": "unknown",
        "detected_format": "key_value",
        "template_hint": "level=<VAR> user=<VAR> action=<VAR> bytes=<NUM>",
        "suggested_parser": {
            "parser_type": "key_value",
            "field_candidates": {"user": "alice", "action": "download"},
            "schema_mapping": {"user": "user.name", "action": "event.action"},
            "confidence": 0.7,
        },
        "sample_count": 1,
        "samples": [{
            "unknown_id": "u1",
            "record_id": "r1",
            "source_id": "unknown_sample",
            "raw_text": "user=alice action=download",
        }],
    }


def test_register_candidates_creates_draft_registry_records(tmp_path):
    registry_path = tmp_path / "parser_registry.json"

    registry = register_candidates([_candidate()], registry_path)

    assert registry["version"] == 1
    assert len(registry["parsers"]) == 1
    parser = registry["parsers"][0]
    assert parser["parser_id"] == "candidate_f1691c2e6bbab95d"
    assert parser["source_type"] == "unknown"
    assert parser["format"] == "key_value"
    assert parser["version"] == "0.1.0"
    assert parser["status"] == "draft"
    assert parser["priority"] == 100
    assert parser["owner"] == "system"
    assert parser["candidate_id"] == "candidate_f1691c2e6bbab95d"
    assert parser["parser_type"] == "key_value"
    assert parser["field_candidates"]["user"] == "alice"
    assert parser["schema_mapping"]["user"] == "user.name"
    assert parser["test_case_count"] == 0
    assert parser["success_rate"] == 0.0
    assert parser["schema_mapping_rate"] == 1.0
    assert parser["created_at"].endswith("Z")
    assert parser["updated_at"].endswith("Z")


def test_register_candidates_is_idempotent_by_parser_id(tmp_path):
    registry_path = tmp_path / "parser_registry.json"

    first = register_candidates([_candidate()], registry_path)
    second = register_candidates([_candidate()], registry_path)

    assert len(first["parsers"]) == 1
    assert len(second["parsers"]) == 1


def test_registry_status_transitions_are_guarded(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    unknown_path = tmp_path / "unknown.jsonl"
    register_candidates([_candidate()], registry_path)
    unknown_path.write_text(
        json.dumps({
            "unknown_id": "u1",
            "raw": {
                "record_id": "r1",
                "text": "user=alice action=download",
            },
        }) + "\n",
        encoding="utf-8",
    )

    testing = set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "testing")
    run_registry_tests(registry_path, "candidate_f1691c2e6bbab95d")
    shadow = set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "shadow")
    run_shadow_replay(registry_path, unknown_path, "candidate_f1691c2e6bbab95d")
    active = set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "active")

    assert testing["parsers"][0]["status"] == "testing"
    assert shadow["parsers"][0]["status"] == "shadow"
    assert active["parsers"][0]["status"] == "active"

    with pytest.raises(ParserRegistryError, match="Invalid status transition"):
        set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "draft")


def test_list_registry_returns_sorted_parsers(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    candidate_b = _candidate()
    candidate_b["candidate_id"] = "candidate_b"
    candidate_b["suggested_source_type"] = "b"
    candidate_a = _candidate()
    candidate_a["candidate_id"] = "candidate_a"
    candidate_a["suggested_source_type"] = "a"
    register_candidates([candidate_b, candidate_a], registry_path)

    parsers = list_registry(registry_path)

    assert [parser["parser_id"] for parser in parsers] == ["candidate_a", "candidate_b"]


def test_cli_registry_commands_register_list_and_set_status(tmp_path):
    from logfusion.cli import main

    candidates_path = tmp_path / "candidates.jsonl"
    registry_path = tmp_path / "parser_registry.json"
    list_path = tmp_path / "registry_list.jsonl"
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
        "set-status",
        "--registry",
        str(registry_path),
        "--parser-id",
        "candidate_f1691c2e6bbab95d",
        "--status",
        "testing",
    ]) == 0
    assert main([
        "registry",
        "list",
        "--registry",
        str(registry_path),
        "--output",
        str(list_path),
    ]) == 0

    listed = [json.loads(line) for line in list_path.read_text(encoding="utf-8").splitlines()]
    assert listed[0]["parser_id"] == "candidate_f1691c2e6bbab95d"
    assert listed[0]["status"] == "testing"
