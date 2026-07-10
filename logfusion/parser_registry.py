from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ParserRegistryError(ValueError):
    pass


ALLOWED_TRANSITIONS = {
    "draft": {"testing", "deprecated", "blocked"},
    "testing": {"shadow", "deprecated", "blocked"},
    "shadow": {"active", "pending_approval", "deprecated"},
    "pending_approval": {"active", "deprecated"},
    "active": {"deprecated"},
    "deprecated": set(),
    "blocked": set(),
}


def register_candidates(candidates: list[dict[str, Any]], registry_path: Path) -> dict[str, Any]:
    registry = load_registry(registry_path)
    existing = {parser["parser_id"]: parser for parser in registry["parsers"]}
    now = _now()

    for candidate in candidates:
        parser_id = candidate["candidate_id"]
        if parser_id in existing:
            continue
        parser = _candidate_to_parser_record(candidate, now)
        registry["parsers"].append(parser)

    registry["parsers"] = sorted(registry["parsers"], key=lambda parser: parser["parser_id"])
    save_registry(registry, registry_path)
    return registry


def list_registry(registry_path: Path) -> list[dict[str, Any]]:
    return sorted(load_registry(registry_path)["parsers"], key=lambda parser: parser["parser_id"])


def set_parser_status(registry_path: Path, parser_id: str, status: str) -> dict[str, Any]:
    registry = load_registry(registry_path)
    parser = _find_parser(registry, parser_id)
    current = parser["status"]
    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if status not in allowed:
        raise ParserRegistryError(f"Invalid status transition: {current} -> {status}")
    if status == "shadow" and not _has_passing_tests(parser):
        raise ParserRegistryError("Parser must have passing parser tests before entering shadow")
    if status == "active" and not _has_successful_shadow_replay(parser):
        raise ParserRegistryError("Parser must have successful shadow replay before entering active")
    if status == "active" and parser.get("approval_required") and current != "pending_approval":
        raise ParserRegistryError("LLM parser must enter pending_approval before entering active")
    parser["status"] = status
    parser["updated_at"] = _now()
    save_registry(registry, registry_path)
    return registry


def load_registry(registry_path: Path) -> dict[str, Any]:
    if not registry_path.exists():
        return {"version": 1, "parsers": []}
    return json.loads(registry_path.read_text(encoding="utf-8"))


def save_registry(registry: dict[str, Any], registry_path: Path) -> None:
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(registry, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _candidate_to_parser_record(candidate: dict[str, Any], now: str) -> dict[str, Any]:
    suggested_parser = candidate.get("suggested_parser", {})
    schema_mapping = suggested_parser.get("schema_mapping", {})
    field_candidates = suggested_parser.get("field_candidates", {})
    mapping_rate = len(schema_mapping) / len(field_candidates) if field_candidates else 0.0
    return {
        "parser_id": candidate["candidate_id"],
        "source_type": candidate.get("suggested_source_type", "unknown"),
        "source_contract": candidate.get("source_contract"),
        "format_fingerprint": candidate.get("format_fingerprint"),
        "enforce_source_contract": candidate.get("source") == "llm",
        "generated_by": candidate.get("generated_by", "heuristic"),
        "approval_required": candidate.get("source") == "llm",
        "format": candidate.get("detected_format", "text"),
        "version": "0.1.0",
        "status": "draft",
        "priority": 100,
        "owner": "system",
        "created_at": now,
        "updated_at": now,
        "candidate_id": candidate["candidate_id"],
        "template_hint": candidate.get("template_hint", ""),
        "parser_type": suggested_parser.get("parser_type", candidate.get("detected_format", "text")),
        "field_extractors": suggested_parser.get("field_extractors", {}),
        "field_candidates": field_candidates,
        "schema_mapping": schema_mapping,
        "action_mapping": suggested_parser.get("action_mapping", {}),
        "action_source_field": suggested_parser.get("action_source_field"),
        "event_defaults": suggested_parser.get("event_defaults", {}),
        "test_case_count": 0,
        "success_rate": 0.0,
        "schema_mapping_rate": round(mapping_rate, 6),
        "sample_count": candidate.get("sample_count", 0),
        "test_cases": candidate.get("test_cases") or _candidate_test_cases(candidate, field_candidates),
    }


def _candidate_test_cases(candidate: dict[str, Any], expected_fields: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "unknown_id": sample.get("unknown_id"),
            "record_id": sample.get("record_id"),
            "source_id": sample.get("source_id"),
            "raw_text": sample.get("raw_text", ""),
            "expected_fields": expected_fields,
        }
        for sample in candidate.get("samples", [])
        if sample.get("raw_text")
    ]


def _find_parser(registry: dict[str, Any], parser_id: str) -> dict[str, Any]:
    for parser in registry["parsers"]:
        if parser["parser_id"] == parser_id:
            return parser
    raise ParserRegistryError(f"Parser not found: {parser_id}")


def _has_passing_tests(parser: dict[str, Any]) -> bool:
    return parser.get("test_case_count", 0) > 0 and parser.get("success_rate", 0.0) >= 1.0


def _has_successful_shadow_replay(parser: dict[str, Any]) -> bool:
    return (
        parser.get("shadow_replay_record_count", 0) > 0
        and parser.get("shadow_replay_success_rate", 0.0) >= 1.0
        and parser.get("shadow_replay_schema_mapping_rate", 0.0) >= 1.0
    )


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
