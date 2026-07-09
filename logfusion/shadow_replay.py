from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from logfusion.parser_registry import ParserRegistryError, _find_parser, _now, load_registry, save_registry
from logfusion.parser_runtime import extract_fields


def run_shadow_replay(
    registry_path: Path,
    unknown_input: Path,
    parser_id: str | None = None,
) -> dict[str, Any]:
    registry = load_registry(registry_path)
    unknown_records = _read_jsonl(unknown_input)
    shadow_parsers = _shadow_parsers(registry, parser_id)
    reports = [_replay_parser(parser, unknown_records) for parser in shadow_parsers]

    for parser, report in zip(shadow_parsers, reports, strict=True):
        parser["shadow_replay_record_count"] = report["replay_record_count"]
        parser["shadow_replay_success_rate"] = report["success_rate"]
        parser["shadow_replay_schema_mapping_rate"] = report["schema_mapping_rate"]
        parser["last_shadow_replay_report"] = report
        parser["updated_at"] = _now()

    save_registry(registry, registry_path)
    return reports[0] if parser_id else {"reports": reports}


def _shadow_parsers(registry: dict[str, Any], parser_id: str | None) -> list[dict[str, Any]]:
    if parser_id:
        parser = _find_parser(registry, parser_id)
        if parser["status"] != "shadow":
            raise ParserRegistryError(f"Parser is not in shadow status: {parser_id}")
        return [parser]
    return [parser for parser in registry["parsers"] if parser["status"] == "shadow"]


def _replay_parser(parser: dict[str, Any], unknown_records: list[dict[str, Any]]) -> dict[str, Any]:
    results = [_replay_record(parser, record) for record in unknown_records]
    matched_count = sum(1 for result in results if result["matched"])
    replay_count = len(results)
    return {
        "parser_id": parser["parser_id"],
        "replay_record_count": replay_count,
        "matched_count": matched_count,
        "unmatched_count": replay_count - matched_count,
        "success_rate": round(matched_count / replay_count, 6) if replay_count else 0.0,
        "schema_mapping_rate": _schema_mapping_rate(parser),
        "shadow_only": True,
        "results": results,
    }


def _replay_record(parser: dict[str, Any], unknown_record: dict[str, Any]) -> dict[str, Any]:
    raw = unknown_record.get("raw", {})
    raw_text = raw.get("text", "")
    extracted = extract_fields(parser, raw_text)
    expected_fields = parser.get("field_candidates", {})
    missing_or_mismatched = {
        key: {"expected": value, "actual": extracted.get(key)}
        for key, value in expected_fields.items()
        if extracted.get(key) != value
    }
    return {
        "unknown_id": unknown_record.get("unknown_id"),
        "record_id": raw.get("record_id"),
        "shadow_only": True,
        "matched": not missing_or_mismatched,
        "extracted_fields": extracted,
        "missing_or_mismatched": missing_or_mismatched,
    }


def _schema_mapping_rate(parser: dict[str, Any]) -> float:
    fields = parser.get("field_candidates", {})
    mapping = parser.get("schema_mapping", {})
    return round(len(mapping) / len(fields), 6) if fields else 0.0


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
