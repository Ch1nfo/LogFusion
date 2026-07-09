from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from typing import Any


def propose_parser_candidates(unknown_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in unknown_records:
        key = (
            record.get("suggested_source_type", "unknown"),
            record.get("detected_format", "text"),
            record.get("template_hint", ""),
        )
        groups[key].append(record)

    candidates = []
    for (source_type, detected_format, template_hint), records in sorted(groups.items()):
        examples = _samples(records)
        candidate_id = _candidate_id(source_type, detected_format, template_hint)
        candidates.append({
            "candidate_id": candidate_id,
            "status": "draft",
            "source": "unknown_pool",
            "suggested_source_type": source_type,
            "detected_format": detected_format,
            "template_hint": template_hint,
            "sample_count": len(records),
            "samples": examples,
            "suggested_parser": _suggest_parser(detected_format, examples),
            "llm_prompt": _llm_prompt(source_type, detected_format, template_hint, examples),
        })
    return candidates


def _candidate_id(source_type: str, detected_format: str, template_hint: str) -> str:
    digest = hashlib.sha256(f"{source_type}:{detected_format}:{template_hint}".encode("utf-8")).hexdigest()[:16]
    return f"candidate_{digest}"


def _samples(records: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    samples = []
    for record in records[:limit]:
        raw = record.get("raw", {})
        samples.append({
            "unknown_id": record.get("unknown_id"),
            "record_id": raw.get("record_id"),
            "source_id": raw.get("source_id"),
            "raw_text": raw.get("text", ""),
        })
    return samples


def _suggest_parser(detected_format: str, samples: list[dict[str, Any]]) -> dict[str, Any]:
    raw_text = samples[0].get("raw_text", "") if samples else ""
    if detected_format == "key_value":
        fields = _parse_key_value(raw_text)
        return {
            "parser_type": "key_value",
            "field_candidates": fields,
            "schema_mapping": _schema_mapping(fields),
            "confidence": 0.7,
            "notes": "Heuristic draft. Requires human review before activation.",
        }
    if detected_format == "json":
        fields = _parse_json_fields(raw_text)
        return {
            "parser_type": "json",
            "field_extractors": {field: field for field in fields},
            "field_candidates": fields,
            "schema_mapping": _schema_mapping(fields),
            "confidence": 0.75,
            "notes": "Heuristic JSON draft. Requires human review before activation.",
        }
    return {
        "parser_type": detected_format,
        "field_candidates": {},
        "schema_mapping": {},
        "confidence": 0.3,
        "notes": "LLM or human review required to infer fields.",
    }


def _parse_key_value(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for key, value in re.findall(r"(\w+)=([^\s]+)", text):
        fields[key] = value
    return fields


def _parse_json_fields(text: str) -> dict[str, Any]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        key: value
        for key, value in data.items()
        if isinstance(value, str | int | float | bool) or value is None
    }


def _schema_mapping(fields: dict[str, str]) -> dict[str, str]:
    dictionary = {
        "user": "user.name",
        "username": "user.name",
        "account": "user.name",
        "src_ip": "source.ip",
        "client_ip": "source.ip",
        "ip": "source.ip",
        "action": "event.action",
        "status": "event.outcome",
        "level": "event.severity",
        "bytes": "network.bytes",
    }
    return {field: dictionary[field] for field in fields if field in dictionary}


def _llm_prompt(
    source_type: str,
    detected_format: str,
    template_hint: str,
    samples: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "task": "generate_parser_candidate",
        "constraints": [
            "Do not parse production logs in the hot path.",
            "Return a draft parser proposal only.",
            "Map fields to Logfusion Canonical Event Schema v0.",
            "Preserve raw references and parser confidence.",
        ],
        "input": {
            "suggested_source_type": source_type,
            "detected_format": detected_format,
            "template_hint": template_hint,
            "samples": samples,
        },
        "expected_output": {
            "parser_type": "regex | grok | key_value | json | json_like",
            "field_extractors": {},
            "schema_mapping": {},
            "action_mapping": {},
            "confidence_rationale": "",
            "test_cases": [],
        },
    }
