import json

from logfusion.cli import main
from logfusion.parser_registry import register_candidates, set_parser_status
from logfusion.parser_test_harness import run_registry_tests
from logfusion.pipeline import run_pipeline
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


def _candidate_with_id(candidate_id, priority=100, success_rate=1.0, schema_mapping_rate=1.0):
    candidate = _candidate()
    candidate["candidate_id"] = candidate_id
    candidate["suggested_parser"]["schema_mapping"] = {
        "user": "user.name",
        "action": "event.action",
        "bytes": "network.bytes",
    }
    candidate["suggested_parser"]["field_candidates"] = {
        "user": "alice",
        "action": "download",
        "bytes": "123",
    }
    candidate["_test_priority"] = priority
    candidate["_test_success_rate"] = success_rate
    candidate["_test_schema_mapping_rate"] = schema_mapping_rate
    return candidate


def _write_conflicting_active_registry(registry_path, candidates):
    parsers = []
    for candidate in candidates:
        suggested = candidate["suggested_parser"]
        parsers.append({
            "parser_id": candidate["candidate_id"],
            "source_type": "unknown",
            "format": "key_value",
            "version": "0.1.0",
            "status": "active",
            "priority": candidate["_test_priority"],
            "owner": "system",
            "created_at": "2026-07-09T00:00:00Z",
            "updated_at": "2026-07-09T00:00:00Z",
            "candidate_id": candidate["candidate_id"],
            "template_hint": candidate["template_hint"],
            "parser_type": "key_value",
            "field_candidates": suggested["field_candidates"],
            "schema_mapping": suggested["schema_mapping"],
            "test_case_count": 1,
            "success_rate": candidate["_test_success_rate"],
            "schema_mapping_rate": candidate["_test_schema_mapping_rate"],
            "shadow_replay_record_count": 1,
            "shadow_replay_success_rate": 1.0,
            "shadow_replay_schema_mapping_rate": 1.0,
        })
    registry_path.write_text(json.dumps({"version": 1, "parsers": parsers}), encoding="utf-8")


def _active_registry(registry_path, unknown_path):
    register_candidates([_candidate()], registry_path)
    set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "testing")
    run_registry_tests(registry_path, "candidate_f1691c2e6bbab95d")
    set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "shadow")
    run_shadow_replay(registry_path, unknown_path, "candidate_f1691c2e6bbab95d")
    set_parser_status(registry_path, "candidate_f1691c2e6bbab95d", "active")


def test_active_registry_parser_normalizes_unknown_key_value_record(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    unknown_path = tmp_path / "unknown.jsonl"
    unknown_path.write_text(json.dumps({
        "unknown_id": "u1",
        "raw": {
            "record_id": "r1",
            "text": "level=INFO user=alice action=download bytes=123",
        },
    }) + "\n", encoding="utf-8")
    _active_registry(registry_path, unknown_path)

    result = run_pipeline([{
        "source_id": "runtime_unknown",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": ["data/unknown/*.log"],
    }], registry_path=registry_path)

    assert result.unknown == []
    assert len(result.normalized) == 1
    event = result.normalized[0]
    assert event["parser"]["id"] == "candidate_f1691c2e6bbab95d"
    assert event["parser"]["origin"] == "registry_active"
    assert event["event"]["action"] == "download"
    assert event["event"]["category"] == "application"
    assert event["event"]["outcome"] == "unknown"
    assert event["user"]["name"] == "alice"
    assert event["network"]["bytes"] == 123
    assert event["extensions"]["active_parser"]["level"] == "INFO"


def test_handwritten_parser_takes_priority_over_active_registry(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    unknown_path = tmp_path / "unknown.jsonl"
    unknown_path.write_text(json.dumps({
        "unknown_id": "u1",
        "raw": {"record_id": "r1", "text": "level=INFO user=alice action=download bytes=123"},
    }) + "\n", encoding="utf-8")
    _active_registry(registry_path, unknown_path)

    result = run_pipeline([{
        "source_id": "svn_sample",
        "source_type": "svn",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": ["data/svn/*.log"],
    }], registry_path=registry_path)

    assert result.unknown == []
    assert result.normalized[0]["parser"]["id"] == "svn_apache_access"


def test_cli_parse_uses_active_registry_parser(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    unknown_path = tmp_path / "unknown.jsonl"
    output = tmp_path / "normalized.jsonl"
    unknown_output = tmp_path / "unknown_out.jsonl"
    summary = tmp_path / "summary.json"
    unknown_path.write_text(json.dumps({
        "unknown_id": "u1",
        "raw": {"record_id": "r1", "text": "level=INFO user=alice action=download bytes=123"},
    }) + "\n", encoding="utf-8")
    _active_registry(registry_path, unknown_path)

    assert main([
        "parse",
        "--config", "config/unknown-source.yaml",
        "--registry", str(registry_path),
        "--output", str(output),
        "--unknown-output", str(unknown_output),
        "--summary-output", str(summary),
    ]) == 0

    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["parser"]["id"] == "candidate_f1691c2e6bbab95d"
    assert unknown_output.read_text(encoding="utf-8") == ""


def test_active_parser_conflict_prefers_lower_priority(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    _write_conflicting_active_registry(registry_path, [
        _candidate_with_id("candidate_priority_200", priority=200),
        _candidate_with_id("candidate_priority_10", priority=10),
    ])

    result = run_pipeline([{
        "source_id": "runtime_unknown",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": ["data/unknown/*.log"],
    }], registry_path=registry_path)

    event = result.normalized[0]
    assert event["parser"]["id"] == "candidate_priority_10"
    assert event["parser"]["conflict"]["candidate_count"] == 2
    assert event["parser"]["conflict"]["selected_parser_id"] == "candidate_priority_10"
    assert event["parser"]["conflict"]["competing_parser_ids"] == ["candidate_priority_200"]
    assert event["parser"]["conflict"]["resolution"] == "priority"


def test_active_parser_conflict_uses_success_rate_when_priority_ties(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    _write_conflicting_active_registry(registry_path, [
        _candidate_with_id("candidate_success_low", priority=100, success_rate=0.8),
        _candidate_with_id("candidate_success_high", priority=100, success_rate=1.0),
    ])

    result = run_pipeline([{
        "source_id": "runtime_unknown",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": ["data/unknown/*.log"],
    }], registry_path=registry_path)

    event = result.normalized[0]
    assert event["parser"]["id"] == "candidate_success_high"
    assert event["parser"]["conflict"]["resolution"] == "success_rate"


def test_active_parser_without_conflict_has_no_conflict_metadata(tmp_path):
    registry_path = tmp_path / "parser_registry.json"
    _write_conflicting_active_registry(registry_path, [
        _candidate_with_id("candidate_only", priority=100),
    ])

    result = run_pipeline([{
        "source_id": "runtime_unknown",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": ["data/unknown/*.log"],
    }], registry_path=registry_path)

    assert result.normalized[0]["parser"]["id"] == "candidate_only"
    assert "conflict" not in result.normalized[0]["parser"]


def test_active_parser_converts_apache_timestamp_and_http_status(tmp_path):
    registry_path = tmp_path / "registry.json"
    log_path = tmp_path / "apache.log"
    log_path.write_text("[02/Aug/2024:12:34:56 +0800] status=207 user=alice\n", encoding="utf-8")
    registry_path.write_text(json.dumps({"version": 1, "parsers": [{
        "parser_id": "apache_llm_parser",
        "source_type": "apache_test",
        "format": "text",
        "version": "0.1.0",
        "status": "active",
        "priority": 100,
        "field_candidates": {"timestamp": "02/Aug/2024:12:34:56 +0800", "status": "207", "user": "alice"},
        "field_extractors": {"pattern": r"\[(?P<timestamp>[^\]]+)\] status=(?P<status>\d+) user=(?P<user>\S+)"},
        "parser_type": "regex",
        "schema_mapping": {"timestamp": "event.time", "status": "http.response.status_code", "user": "user.name"},
        "success_rate": 1.0,
        "schema_mapping_rate": 1.0,
        "shadow_replay_success_rate": 1.0,
    }]}), encoding="utf-8")

    result = run_pipeline([{
        "source_id": "apache_test",
        "source_type": "apache_test",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(log_path)],
    }], registry_path=registry_path)

    assert result.unknown == []
    assert result.normalized[0]["event"]["time"] == "2024-08-02T12:34:56+08:00"
    assert result.normalized[0]["http"]["response"]["status_code"] == 207


def test_active_parser_applies_event_defaults_action_taxonomy_and_http_outcome(tmp_path):
    registry_path = tmp_path / "registry.json"
    log_path = tmp_path / "svn.log"
    log_path.write_text("operation=get-file status=207 user=alice\n", encoding="utf-8")
    registry_path.write_text(json.dumps({"version": 1, "parsers": [{
        "parser_id": "svn_llm_parser",
        "source_type": "svn_test",
        "format": "key_value",
        "version": "0.1.0",
        "status": "active",
        "priority": 100,
        "field_candidates": {"operation": "get-file", "status": "207", "user": "alice"},
        "field_extractors": {},
        "parser_type": "key_value",
        "schema_mapping": {"operation": "extensions.svn.operation", "status": "http.response.status_code", "user": "user.name"},
        "event_defaults": {"event.category": "repository", "event.type": "access"},
        "action_source_field": "operation",
        "action_mapping": {"get-file": "repo_read"},
        "success_rate": 1.0,
        "schema_mapping_rate": 1.0,
        "shadow_replay_success_rate": 1.0,
    }]}), encoding="utf-8")

    result = run_pipeline([{
        "source_id": "svn_test",
        "source_type": "svn_test",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(log_path)],
    }], registry_path=registry_path)

    event = result.normalized[0]
    assert event["event"]["category"] == "repository"
    assert event["event"]["type"] == "access"
    assert event["event"]["action"] == "repo_read"
    assert event["event"]["outcome"] == "success"
