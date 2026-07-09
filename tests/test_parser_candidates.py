import json

from logfusion.cli import main
from logfusion.parser_candidates import propose_parser_candidates
from logfusion.pipeline import run_pipeline


def test_propose_parser_candidates_groups_unknown_records_by_template():
    result = run_pipeline([{
        "source_id": "unknown_sample",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": ["data/unknown/*.log"],
    }])

    candidates = propose_parser_candidates(result.unknown)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["status"] == "draft"
    assert candidate["source"] == "unknown_pool"
    assert candidate["detected_format"] == "key_value"
    assert candidate["sample_count"] == 1
    assert candidate["template_hint"] == "level=<VAR> user=<VAR> action=<VAR> bytes=<NUM>"
    assert candidate["suggested_parser"]["parser_type"] == "key_value"
    assert candidate["suggested_parser"]["field_candidates"]["user"] == "alice"
    assert candidate["suggested_parser"]["schema_mapping"]["user"] == "user.name"
    assert candidate["llm_prompt"]["task"] == "generate_parser_candidate"
    assert "level=INFO user=alice" in candidate["samples"][0]["raw_text"]


def test_cli_writes_parser_candidate_jsonl(tmp_path):
    unknown_path = tmp_path / "unknown.jsonl"
    candidates_path = tmp_path / "candidates.jsonl"
    unknown = {
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
    unknown_path.write_text(json.dumps(unknown) + "\n", encoding="utf-8")

    exit_code = main([
        "propose-parsers",
        "--unknown-input",
        str(unknown_path),
        "--output",
        str(candidates_path),
    ])

    assert exit_code == 0
    rows = [json.loads(line) for line in candidates_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["status"] == "draft"
    assert rows[0]["candidate_id"].startswith("candidate_")


def test_json_unknown_candidate_includes_field_extractors_and_schema_mapping():
    unknown = [{
        "unknown_id": "u-json",
        "reason": "no_parser",
        "detected_format": "json",
        "suggested_source_type": "unknown",
        "template_hint": '{"user":"<VAR>","action":"<VAR>","bytes":"<NUM>"}',
        "raw": {
            "record_id": "r-json",
            "source_id": "json_unknown",
            "source_type": "unknown",
            "text": '{"user":"alice","action":"download","bytes":123}',
        },
    }]

    candidates = propose_parser_candidates(unknown)

    parser = candidates[0]["suggested_parser"]
    assert parser["parser_type"] == "json"
    assert parser["field_extractors"] == {
        "user": "user",
        "action": "action",
        "bytes": "bytes",
    }
    assert parser["field_candidates"]["user"] == "alice"
    assert parser["schema_mapping"]["user"] == "user.name"
    assert parser["schema_mapping"]["action"] == "event.action"
