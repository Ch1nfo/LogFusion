import json

import pytest

from logfusion.llm_generation import (
    LLMProviderError,
    generate_and_validate_llm_candidates,
    generate_llm_candidates,
)
from logfusion.config import load_sources_config
from logfusion.parser_registry import load_registry, set_parser_status
from logfusion.pipeline import run_pipeline
from logfusion.unknown import template_hint


class FakeProvider:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def generate_parser(self, request):
        self.requests.append(request)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def _unknown(source_id="edr_acme", text="time=2026-07-10T00:00:00Z user=alice action=process_start"):
    return {
        "unknown_id": f"u-{source_id}",
        "reason": "no_parser",
        "detected_format": "key_value",
        "suggested_source_type": "edr",
        "template_hint": template_hint(text),
        "raw": {
            "record_id": f"r-{source_id}",
            "source_id": source_id,
            "source_type": "edr",
            "text": text,
        },
    }


def _proposal():
    return {
        "parser_type": "key_value",
        "field_extractors": {},
        "field_candidates": {
            "time": "2026-07-10T00:00:00Z",
            "user": "alice",
            "action": "process_start",
        },
        "schema_mapping": {
            "time": "event.time",
            "user": "user.name",
            "action": "event.action",
        },
        "action_mapping": {},
        "confidence": 0.9,
        "confidence_rationale": "Stable EDR key-value samples.",
        "test_cases": [],
    }


def test_llm_generation_groups_one_source_contract_once_and_redacts_samples():
    provider = FakeProvider(_proposal())
    secret_record = _unknown(
        text="time=2026-07-10T00:00:00Z user=alice action=process_start token=super-secret",
    )

    candidates, report = generate_llm_candidates([
        secret_record,
        _unknown(text="time=2026-07-10T00:00:00Z user=alice action=process_start token=another-secret"),
    ], provider, sample_limit=3)

    assert len(candidates) == 1
    assert report["generated_count"] == 1
    assert len(provider.requests) == 1
    assert "super-secret" not in json.dumps(provider.requests[0])
    candidate = candidates[0]
    assert candidate["source"] == "llm"
    assert candidate["source_contract"] == {
        "source_id": "edr_acme",
        "source_type": "edr",
        "format_fingerprint": candidate["format_fingerprint"],
    }
    assert candidate["suggested_parser"]["parser_type"] == "key_value"


def test_llm_workflow_leaves_validated_parser_pending_human_approval(tmp_path):
    registry_path = tmp_path / "registry.json"
    provider = FakeProvider(_proposal())
    unknown = _unknown()

    report = generate_and_validate_llm_candidates([unknown], registry_path, provider)

    assert report["generated_count"] == 1
    assert report["ready_for_approval_count"] == 1
    parser = load_registry(registry_path)["parsers"][0]
    assert parser["status"] == "pending_approval"
    assert parser["generated_by"] == "llm"
    assert parser["approval_required"] is True

    set_parser_status(registry_path, parser["parser_id"], "active")
    edr_log = tmp_path / "edr.log"
    edr_log.write_text("time=2026-07-10T00:00:00Z user=alice action=process_start\n", encoding="utf-8")
    result = run_pipeline([{
        "source_id": "edr_acme",
        "source_type": "edr",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(edr_log)],
    }], registry_path=registry_path)

    assert len(result.normalized) == 1
    assert result.normalized[0]["parser"]["origin"] == "registry_active"


def test_llm_groups_format_variants_into_one_generic_parser_with_per_sample_tests(tmp_path):
    registry_path = tmp_path / "registry.json"
    first = _unknown(text="user=alice action=process_start")
    second = _unknown(text="user=bob action=process_stop")
    second["unknown_id"] = "u-edr-acme-2"
    second["raw"]["record_id"] = "r-edr-acme-2"
    second["template_hint"] = "user=<VAR> action=<VAR> outcome=<VAR>"
    provider = FakeProvider({
        "parser_type": "key_value",
        "field_extractors": {},
        "field_candidates": {"user": "alice", "action": "process_start"},
        "schema_mapping": {"user": "user.name", "action": "event.action"},
        "action_mapping": {},
        "confidence": 0.9,
        "confidence_rationale": "Generic key-value format.",
    })

    report = generate_and_validate_llm_candidates([first, second], registry_path, provider)

    assert len(provider.requests) == 1
    assert len(provider.requests[0]["samples"]) == 2
    assert "expected_output" in provider.requests[0]
    assert report["generated_count"] == 1
    assert report["ready_for_approval_count"] == 1
    parser = load_registry(registry_path)["parsers"][0]
    assert parser["status"] == "pending_approval"
    assert parser["test_case_count"] == 2


def test_llm_parser_contract_does_not_parse_another_source(tmp_path):
    registry_path = tmp_path / "registry.json"
    report = generate_and_validate_llm_candidates([_unknown()], registry_path, FakeProvider(_proposal()))
    parser_id = report["candidates"][0]["candidate_id"]
    set_parser_status(registry_path, parser_id, "active")
    other_log = tmp_path / "other-edr.log"
    other_log.write_text("time=2026-07-10T00:00:00Z user=alice action=process_start\n", encoding="utf-8")

    result = run_pipeline([{
        "source_id": "edr_other",
        "source_type": "edr",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(other_log)],
    }], registry_path=registry_path)

    assert result.normalized == []
    assert len(result.unknown) == 1


def test_llm_workflow_does_not_call_provider_for_existing_contract(tmp_path):
    registry_path = tmp_path / "registry.json"
    unknown = _unknown()
    first_provider = FakeProvider(_proposal())
    first = generate_and_validate_llm_candidates([unknown], registry_path, first_provider)
    set_parser_status(registry_path, first["candidates"][0]["candidate_id"], "active")
    second_provider = FakeProvider(_proposal())

    second = generate_and_validate_llm_candidates([unknown], registry_path, second_provider)

    assert second_provider.requests == []
    assert second["generated_count"] == 0
    assert second["skipped_existing_contract_count"] == 1


def test_llm_generation_keeps_unknown_when_provider_fails():
    candidates, report = generate_llm_candidates([_unknown()], FakeProvider(LLMProviderError("timeout")))

    assert candidates == []
    assert report["generated_count"] == 0
    assert report["failures"][0]["error"] == "timeout"


def test_llm_generation_rejects_invalid_proposal():
    candidates, report = generate_llm_candidates([_unknown()], FakeProvider({"parser_type": "shell"}))

    assert candidates == []
    assert "Unsupported parser_type" in report["failures"][0]["error"]


def test_llm_generation_rejects_non_canonical_field_mapping():
    proposal = _proposal()
    proposal["schema_mapping"] = {"user": "subprocess.run"}

    candidates, report = generate_llm_candidates([_unknown()], FakeProvider(proposal))

    assert candidates == []
    assert "Unsupported Canonical Schema mapping" in report["failures"][0]["error"]


def test_llm_generation_normalizes_ecs_timestamp_mapping():
    proposal = _proposal()
    proposal["field_candidates"]["time"] = "2026-07-10T00:00:00Z"
    proposal["schema_mapping"]["time"] = "@timestamp"

    candidates, report = generate_llm_candidates([_unknown()], FakeProvider(proposal))

    assert report["generated_count"] == 1
    assert candidates[0]["suggested_parser"]["schema_mapping"]["time"] == "event.time"


def test_llm_generation_normalizes_ecs_url_path_mapping():
    proposal = _proposal()
    proposal["field_candidates"]["path"] = "/svn/repository"
    proposal["schema_mapping"]["path"] = "url.path"

    candidates, report = generate_llm_candidates([_unknown()], FakeProvider(proposal))

    assert report["generated_count"] == 1
    assert candidates[0]["suggested_parser"]["schema_mapping"]["path"] == "http.url.path"


def test_llm_generation_normalizes_known_source_extension_mapping():
    proposal = _proposal()
    proposal["field_candidates"]["operation"] = "get-file"
    proposal["schema_mapping"]["operation"] = "svn.operation"
    unknown = _unknown()
    unknown["suggested_source_type"] = "svn"
    unknown["raw"]["source_type"] = "svn"

    candidates, report = generate_llm_candidates([unknown], FakeProvider(proposal))

    assert report["generated_count"] == 1
    assert candidates[0]["suggested_parser"]["schema_mapping"]["operation"] == "extensions.svn.operation"


def test_llm_request_includes_canonical_schema_and_extension_contract():
    provider = FakeProvider(_proposal())

    generate_llm_candidates([_unknown()], provider)

    request = provider.requests[0]
    assert request["canonical_schema"]["mapping_direction"] == "raw_field -> canonical_field"
    assert "event.time" in request["canonical_schema"]["core_fields"]
    assert request["canonical_schema"]["extension_rule"] == "extensions.<source_family>.<field>"
    assert "svn.operation -> extensions.svn.operation" in request["canonical_schema"]["mapping_examples"]


def test_llm_generation_skips_sources_that_disable_llm():
    unknown = _unknown()
    unknown["raw"]["llm_enabled"] = False
    provider = FakeProvider(_proposal())

    candidates, report = generate_llm_candidates([unknown], provider)

    assert candidates == []
    assert provider.requests == []
    assert report["skipped_count"] == 1


def test_source_config_metadata_reaches_unknown_records(tmp_path):
    source_file = tmp_path / "edr.log"
    source_file.write_text("user=alice action=process_start\n", encoding="utf-8")
    config_file = tmp_path / "sources.yaml"
    config_file.write_text(
        "sources:\n"
        "  - source_id: edr_acme\n"
        "    source_type: edr\n"
        "    product: acme-edr\n"
        "    format_version: v1\n"
        "    llm_enabled: true\n"
        "    record_mode: line\n"
        "    paths:\n"
        f"      - {source_file}\n",
        encoding="utf-8",
    )

    result = run_pipeline(load_sources_config(config_file))

    raw = result.unknown[0]["raw"]
    assert raw["product"] == "acme-edr"
    assert raw["format_version"] == "v1"
    assert raw["llm_enabled"] is True
