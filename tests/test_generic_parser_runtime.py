import json

from logfusion.parser_registry import register_candidates, set_parser_status
from logfusion.parser_test_harness import run_registry_tests
from logfusion.pipeline import run_pipeline
from logfusion.shadow_replay import run_shadow_replay


def _json_candidate():
    return {
        "candidate_id": "candidate_json_login",
        "suggested_source_type": "unknown",
        "detected_format": "json",
        "template_hint": '{"level":"<VAR>","user":"<VAR>","action":"<VAR>","bytes":"<NUM>"}',
        "samples": [{
            "unknown_id": "u-json",
            "record_id": "r-json",
            "source_id": "json_unknown",
            "raw_text": '{"level":"INFO","user":"alice","action":"download","bytes":123}',
        }],
        "suggested_parser": {
            "parser_type": "json",
            "field_extractors": {
                "level": "level",
                "user": "user",
                "action": "action",
                "bytes": "bytes",
            },
            "field_candidates": {
                "level": "INFO",
                "user": "alice",
                "action": "download",
                "bytes": 123,
            },
            "schema_mapping": {
                "level": "event.severity",
                "user": "user.name",
                "action": "event.action",
                "bytes": "network.bytes",
            },
        },
        "sample_count": 1,
    }


def _regex_candidate():
    return {
        "candidate_id": "candidate_regex_login",
        "suggested_source_type": "unknown",
        "detected_format": "regex",
        "template_hint": "user=<VAR> action=<VAR> bytes=<NUM>",
        "samples": [{
            "unknown_id": "u-regex",
            "record_id": "r-regex",
            "source_id": "regex_unknown",
            "raw_text": "user=alice action=download bytes=123",
        }],
        "suggested_parser": {
            "parser_type": "regex",
            "field_extractors": {
                "pattern": r"user=(?P<user>\w+) action=(?P<action>\w+) bytes=(?P<bytes>\d+)"
            },
            "field_candidates": {
                "user": "alice",
                "action": "download",
                "bytes": "123",
            },
            "schema_mapping": {
                "user": "user.name",
                "action": "event.action",
                "bytes": "network.bytes",
            },
        },
        "sample_count": 1,
    }


def _activate_candidate(candidate, registry_path, unknown_path, raw_text):
    unknown_path.write_text(json.dumps({
        "unknown_id": candidate["samples"][0]["unknown_id"],
        "raw": {
            "record_id": candidate["samples"][0]["record_id"],
            "text": raw_text,
        },
    }) + "\n", encoding="utf-8")
    register_candidates([candidate], registry_path)
    set_parser_status(registry_path, candidate["candidate_id"], "testing")
    test_report = run_registry_tests(registry_path, candidate["candidate_id"])
    set_parser_status(registry_path, candidate["candidate_id"], "shadow")
    replay_report = run_shadow_replay(registry_path, unknown_path, candidate["candidate_id"])
    set_parser_status(registry_path, candidate["candidate_id"], "active")
    return test_report, replay_report


def test_json_parser_runtime_supports_test_replay_and_active_parse(tmp_path):
    registry_path = tmp_path / "registry.json"
    unknown_path = tmp_path / "unknown.jsonl"
    candidate = _json_candidate()
    raw_text = '{"level":"INFO","user":"alice","action":"download","bytes":123}'

    test_report, replay_report = _activate_candidate(candidate, registry_path, unknown_path, raw_text)

    assert test_report["success_rate"] == 1.0
    assert replay_report["success_rate"] == 1.0

    data_file = tmp_path / "json.log"
    data_file.write_text(raw_text + "\n", encoding="utf-8")
    result = run_pipeline([{
        "source_id": "json_unknown",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(data_file)],
    }], registry_path=registry_path)

    assert result.unknown == []
    event = result.normalized[0]
    assert event["parser"]["id"] == "candidate_json_login"
    assert event["event"]["action"] == "download"
    assert event["user"]["name"] == "alice"
    assert event["network"]["bytes"] == 123


def test_regex_parser_runtime_supports_test_replay_and_active_parse(tmp_path):
    registry_path = tmp_path / "registry.json"
    unknown_path = tmp_path / "unknown.jsonl"
    candidate = _regex_candidate()
    raw_text = "user=alice action=download bytes=123"

    test_report, replay_report = _activate_candidate(candidate, registry_path, unknown_path, raw_text)

    assert test_report["success_rate"] == 1.0
    assert replay_report["success_rate"] == 1.0

    data_file = tmp_path / "regex.log"
    data_file.write_text(raw_text + "\n", encoding="utf-8")
    result = run_pipeline([{
        "source_id": "regex_unknown",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(data_file)],
    }], registry_path=registry_path)

    assert result.unknown == []
    event = result.normalized[0]
    assert event["parser"]["id"] == "candidate_regex_login"
    assert event["event"]["action"] == "download"
    assert event["user"]["name"] == "alice"
    assert event["network"]["bytes"] == 123
