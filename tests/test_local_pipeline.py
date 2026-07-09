from pathlib import Path

from logfusion.config import load_sources_config
from logfusion.file_source import iter_raw_records
from logfusion.pipeline import parse_sources, run_pipeline
from logfusion.schema import validate_event


def test_file_source_reads_line_and_object_records():
    config = load_sources_config(Path("config/sources.yaml"))

    records = list(iter_raw_records(config))

    assert len(records) == 8
    assert records[0].source_type == "svn"
    assert records[0].line_start == 1
    assert records[2].source_type == "sso"
    assert records[2].line_start == 1
    assert records[2].raw_text.startswith("{")
    assert records[6].source_type == "hiklink"
    assert records[6].raw_text.startswith("{")


def test_pipeline_maps_sample_sources_to_canonical_events():
    config = load_sources_config(Path("config/sources.yaml"))

    normalized, unknown = parse_sources(config)

    assert unknown == []
    assert len(normalized) == 8

    svn = normalized[0]
    assert svn["event"]["category"] == "repository"
    assert svn["event"]["action"] == "repo_read"
    assert svn["source"]["ip"] == "10.1.75.130"
    assert svn["user"]["name"] == "luyuantong"
    assert svn["extensions"]["svn"]["revision"] == "859687"
    assert svn["parser"]["confidence"] >= 0.9

    sso = normalized[3]
    assert sso["event"]["category"] == "authentication"
    assert sso["event"]["action"] == "user_login"
    assert sso["event"]["outcome"] == "success"
    assert sso["user"]["name"] == "longgengxi"

    gitlab = normalized[4]
    assert gitlab["event"]["category"] == "repository"
    assert gitlab["event"]["action"] == "repo_api_read"
    assert gitlab["http"]["request"]["method"] == "GET"
    assert gitlab["user"]["name"] == "root"

    hiklink = normalized[6]
    assert hiklink["event"]["category"] == "authentication"
    assert hiklink["event"]["action"] == "user_login"
    assert hiklink["source"]["ip"] == "112.17.241.230"
    assert hiklink["device"]["name"] == "iPhone 15 Pro"


def test_raw_metadata_includes_integrity_location_and_optional_text():
    config = load_sources_config(Path("config/sources.yaml"))

    normalized, unknown = parse_sources(config)

    assert unknown == []
    raw = normalized[0]["raw"]
    assert raw["checksum"].startswith("sha256:")
    assert raw["size_bytes"] > 0
    assert raw["storage_ref"] == "file://data/svn/sample.log#L1-L1"
    assert raw["text"].startswith("10.1.75.130 - luyuantong")


def test_raw_text_can_be_disabled_for_large_scale_outputs():
    config = [{
        "source_id": "svn_no_text",
        "source_type": "svn",
        "record_mode": "line",
        "include_raw_text": False,
        "paths": ["data/svn/*.log"],
    }]

    normalized, unknown = parse_sources(config)

    assert unknown == []
    assert "text" not in normalized[0]["raw"]
    assert normalized[0]["raw"]["checksum"].startswith("sha256:")


def test_auto_source_type_falls_back_to_content_detection():
    config = [{
        "source_id": "auto_svn",
        "source_type": "auto",
        "record_mode": "line",
        "paths": ["data/svn/*.log"],
    }]

    normalized, unknown = parse_sources(config)

    assert unknown == []
    assert normalized[0]["raw"]["source_type"] == "svn"


def test_canonical_schema_validator_accepts_sample_events():
    config = load_sources_config(Path("config/sources.yaml"))

    normalized, unknown = parse_sources(config)

    assert unknown == []
    for event in normalized:
        assert validate_event(event) == []


def test_schema_validation_failure_becomes_structured_unknown():
    invalid_event = {
        "event": {
            "id": "bad",
            "category": "repository",
            "type": "access",
            "action": "repo_read",
            "outcome": "success",
        },
        "parser": {
            "id": "test",
            "version": "0.1.0",
            "status": "success",
            "confidence": 0.9,
        },
        "raw": {
            "record_id": "raw-bad",
            "source_id": "test",
            "source_type": "test",
            "checksum": "sha256:abc",
            "storage_ref": "file://test#L1-L1",
        },
    }

    errors = validate_event(invalid_event)

    assert errors == [
        "event.time is required",
        "raw.checksum must match sha256:<64 hex chars>",
    ]


def test_pipeline_returns_quality_summary():
    config = load_sources_config(Path("config/sources.yaml"))

    result = run_pipeline(config)

    assert len(result.normalized) == 8
    assert result.unknown == []
    assert result.summary["total_records"] == 8
    assert result.summary["normalized_records"] == 8
    assert result.summary["unknown_records"] == 0
    assert result.summary["parse_success_rate"] == 1.0
    assert result.summary["parser_confidence_avg"] > 0.9
    assert result.summary["source_type_count"]["svn"] == 2
    assert result.summary["parser_count"]["gitlab_jsonl"] == 2
    assert result.summary["missing_required_fields_count"] == 0


def test_unknown_pool_records_detection_and_template_hint():
    config = [{
        "source_id": "unknown_sample",
        "source_type": "auto",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": ["data/unknown/*.log"],
    }]

    result = run_pipeline(config)

    assert result.normalized == []
    assert len(result.unknown) == 1
    unknown = result.unknown[0]
    assert unknown["reason"] == "no_parser"
    assert unknown["detected_format"] == "key_value"
    assert unknown["suggested_source_type"] == "unknown"
    assert "user=<VAR>" in unknown["template_hint"]
    assert unknown["raw"]["text"] == "level=INFO user=alice action=download bytes=123"
