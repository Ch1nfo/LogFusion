from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from logfusion.field_mapping import (
    is_canonical_mapping_path,
    normalize_schema_mapping,
    parser_generation_schema_contract,
)
from logfusion.llm_provider import LLMProvider, LLMProviderError
from logfusion.parser_contract import source_format_fingerprint
from logfusion.parser_registry import load_registry, register_candidates, set_parser_status
from logfusion.parser_runtime import extract_fields
from logfusion.parser_test_harness import run_registry_tests
from logfusion.shadow_replay import run_shadow_replay_records


SUPPORTED_PARSER_TYPES = {"key_value", "json", "regex"}
_SENSITIVE_KEY_VALUE = re.compile(r"(?i)\b(password|passwd|token|authorization|api[_-]?key|secret)=([^\s,]+)")


def generate_llm_candidates(
    unknown_records: list[dict[str, Any]],
    provider: LLMProvider,
    sample_limit: int = 5,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    groups: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    skipped_count = 0
    for record in unknown_records:
        raw = record.get("raw", {})
        if raw.get("llm_enabled") is False:
            skipped_count += 1
            continue
        key = (
            str(raw.get("source_id", "unknown")),
            str(record.get("suggested_source_type") or raw.get("source_type", "unknown")),
            str(record.get("detected_format", "text")),
            str(raw.get("product", "")),
            str(raw.get("format_version", "")),
        )
        groups[key].append(record)

    candidates: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for (source_id, source_type, detected_format, product, format_version), records in sorted(groups.items()):
        templates = sorted({str(record.get("template_hint", "")) for record in records})
        fingerprint = source_format_fingerprint(source_id, source_type, detected_format, product, format_version)
        request = _request(source_id, source_type, detected_format, templates, records[:sample_limit], fingerprint)
        try:
            proposal = _validate_proposal(provider.generate_parser(request))
            candidate = _candidate(
                source_id, source_type, detected_format, templates, fingerprint, records, proposal, product, format_version,
            )
        except (LLMProviderError, ValueError) as exc:
            failures.append({"format_fingerprint": fingerprint, "error": str(exc)})
            continue
        candidates.append(candidate)
    return candidates, {
        "group_count": len(groups),
        "generated_count": len(candidates),
        "skipped_count": skipped_count,
        "failures": failures,
    }


def generate_and_validate_llm_candidates(
    unknown_records: list[dict[str, Any]],
    registry_path: Path,
    provider: LLMProvider,
    sample_limit: int = 5,
) -> dict[str, Any]:
    existing_contracts = [
        parser.get("source_contract")
        for parser in load_registry(registry_path)["parsers"]
        if parser.get("status") != "deprecated" and parser.get("source_contract")
    ]
    eligible_records = [
        record for record in unknown_records
        if not any(_same_contract(record, contract) for contract in existing_contracts)
    ]
    candidates, report = generate_llm_candidates(eligible_records, provider, sample_limit)
    report["skipped_existing_contract_count"] = len(unknown_records) - len(eligible_records)
    register_candidates(candidates, registry_path)
    ready = 0
    validation_reports = []
    for candidate in candidates:
        parser_id = candidate["candidate_id"]
        set_parser_status(registry_path, parser_id, "testing")
        test_report = run_registry_tests(registry_path, parser_id)
        parser_records = [record for record in unknown_records if _same_contract(record, candidate["source_contract"])]
        shadow_report = None
        status = "testing"
        if test_report["success_rate"] >= 1.0:
            set_parser_status(registry_path, parser_id, "shadow")
            shadow_report = run_shadow_replay_records(registry_path, parser_records, parser_id)
            if shadow_report["success_rate"] >= 1.0 and shadow_report["schema_mapping_rate"] >= 1.0:
                set_parser_status(registry_path, parser_id, "pending_approval")
                status = "pending_approval"
                ready += 1
            else:
                status = "shadow"
        validation_reports.append({
            "parser_id": parser_id,
            "status": status,
            "test_report": test_report,
            "shadow_report": shadow_report,
        })
    return {
        **report,
        "candidates": candidates,
        "ready_for_approval_count": ready,
        "validation_reports": validation_reports,
    }


def _request(
    source_id: str,
    source_type: str,
    detected_format: str,
    templates: list[str],
    records: list[dict[str, Any]],
    fingerprint: str,
) -> dict[str, Any]:
    return {
        "task": "generate_source_specific_parser",
        "source_contract": {
            "source_id": source_id,
            "source_type": source_type,
            "format_fingerprint": fingerprint,
        },
        "detected_format": detected_format,
        "canonical_schema": parser_generation_schema_contract(),
        "template_hints": templates,
        "samples": [{
            "record_id": record.get("raw", {}).get("record_id"),
            "storage_ref": record.get("raw", {}).get("storage_ref"),
            "raw_text": _redact(str(record.get("raw", {}).get("text", ""))),
        } for record in records],
        "constraints": [
            "Return JSON only.",
            "Generate a parser for this source contract only.",
            "Use only key_value, json, or regex parser_type.",
            "The parser must match every provided sample, not just one sample.",
            "field_candidates must list every extracted field with an example value from the samples.",
            "For regex use field_extractors.pattern with named capture groups.",
            "schema_mapping must follow canonical_schema.mapping_direction exactly.",
            "Use canonical_schema.extension_rule for source-specific fields; never emit bare source namespaces such as svn.operation.",
            "event_defaults may only set event.category, event.type, event.action, or event.outcome to a literal allowed value.",
            "action_source_field must name one field_candidates key; action_mapping must be a plain raw-value to canonical-action dictionary, never a rule expression.",
            "Return Canonical Event field mappings; do not return credentials, explanations, markdown, or extra keys.",
        ],
        "expected_output": {
            "parser_type": "regex",
            "field_extractors": {"pattern": "^(?P<source_ip>...) - (?P<user>...) ...$"},
            "field_candidates": {"source_ip": "10.0.0.1", "user": "alice"},
            "schema_mapping": {"source_ip": "source.ip", "user": "user.name"},
            "event_defaults": {"event.category": "repository", "event.type": "access"},
            "action_source_field": "svn_operation",
            "action_mapping": {"get-file": "repo_read", "proppatch": "repo_write"},
            "confidence": 0.9,
            "confidence_rationale": "The named groups match every supplied sample.",
        },
    }


def _candidate(
    source_id: str,
    source_type: str,
    detected_format: str,
    templates: list[str],
    fingerprint: str,
    records: list[dict[str, Any]],
    proposal: dict[str, Any],
    product: str | None,
    format_version: str | None,
) -> dict[str, Any]:
    candidate_id = "candidate_llm_" + hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:16]
    samples = [{
        "unknown_id": record.get("unknown_id"),
        "record_id": record.get("raw", {}).get("record_id"),
        "source_id": source_id,
        "raw_text": record.get("raw", {}).get("text", ""),
    } for record in records[:5]]
    parser_spec = {
        "parser_type": proposal["parser_type"],
        "field_extractors": proposal["field_extractors"],
    }
    test_cases = []
    for sample in samples:
        try:
            expected_fields = extract_fields(parser_spec, sample["raw_text"])
        except Exception as exc:
            raise ValueError(f"Generated parser cannot read sample {sample['record_id']}: {exc}") from exc
        if not expected_fields:
            raise ValueError(f"Generated parser did not match sample {sample['record_id']}")
        test_cases.append({**sample, "expected_fields": expected_fields})

    contract = {
        "source_id": source_id,
        "source_type": source_type,
        "format_fingerprint": fingerprint,
    }
    if product:
        contract["product"] = product
    if format_version:
        contract["format_version"] = format_version
    return {
        "candidate_id": candidate_id,
        "status": "draft",
        "source": "llm",
        "generated_by": "llm",
        "suggested_source_type": source_type,
        "detected_format": detected_format,
        "template_hint": templates[0] if templates else "",
        "template_hints": templates,
        "format_fingerprint": fingerprint,
        "source_contract": contract,
        "sample_count": len(records),
        "samples": samples,
        "test_cases": test_cases,
        "suggested_parser": proposal,
    }


def _validate_proposal(proposal: Any) -> dict[str, Any]:
    if not isinstance(proposal, dict):
        raise ValueError("LLM proposal must be a JSON object")
    parser_type = proposal.get("parser_type")
    if parser_type not in SUPPORTED_PARSER_TYPES:
        raise ValueError(f"Unsupported parser_type: {parser_type}")
    fields = proposal.get("field_candidates", {})
    extractors = proposal.get("field_extractors", {})
    mapping = normalize_schema_mapping(proposal.get("schema_mapping", {}))
    if not all(isinstance(value, dict) for value in (fields, extractors, mapping)):
        raise ValueError("field_candidates, field_extractors, and schema_mapping must be objects")
    if not fields:
        raise ValueError("field_candidates must not be empty")
    if not set(mapping).issubset(fields):
        raise ValueError("schema_mapping keys must exist in field_candidates")
    for target_path in mapping.values():
        if not is_canonical_mapping_path(target_path):
            raise ValueError(f"Unsupported Canonical Schema mapping: {target_path}")
    if parser_type == "regex" and not isinstance(extractors.get("pattern"), str):
        raise ValueError("Regex parser requires field_extractors.pattern")
    event_defaults = proposal.get("event_defaults", {})
    if not isinstance(event_defaults, dict) or not set(event_defaults).issubset({
        "event.category", "event.type", "event.action", "event.outcome",
    }):
        raise ValueError("event_defaults may only set event.category, event.type, event.action, or event.outcome")
    action_source_field = proposal.get("action_source_field")
    if action_source_field is not None and action_source_field not in fields:
        raise ValueError("action_source_field must exist in field_candidates")
    return {
        "parser_type": parser_type,
        "field_extractors": extractors,
        "field_candidates": fields,
        "schema_mapping": mapping,
        "action_mapping": proposal.get("action_mapping", {}),
        "action_source_field": action_source_field,
        "event_defaults": event_defaults,
        "confidence": float(proposal.get("confidence", 0.0)),
        "confidence_rationale": str(proposal.get("confidence_rationale", "")),
        "test_cases": proposal.get("test_cases", []),
    }


def _same_contract(record: dict[str, Any], contract: dict[str, str]) -> bool:
    raw = record.get("raw", {})
    source_type = record.get("suggested_source_type") or raw.get("source_type", "unknown")
    return (
        raw.get("source_id") == contract["source_id"]
        and source_type == contract["source_type"]
        and source_format_fingerprint(
            str(raw.get("source_id")),
            str(source_type),
            str(record.get("detected_format", "text")),
            raw.get("product"),
            raw.get("format_version"),
        ) == contract["format_fingerprint"]
        and (not contract.get("product") or raw.get("product") == contract["product"])
        and (not contract.get("format_version") or raw.get("format_version") == contract["format_version"])
    )


def _redact(raw_text: str) -> str:
    try:
        document = json.loads(raw_text)
    except json.JSONDecodeError:
        return _SENSITIVE_KEY_VALUE.sub(lambda match: f"{match.group(1)}=<REDACTED>", raw_text)
    if not isinstance(document, dict):
        return raw_text
    return json.dumps(_redact_document(document), ensure_ascii=False, sort_keys=True)


def _redact_document(document: dict[str, Any]) -> dict[str, Any]:
    result = {}
    for key, value in document.items():
        if re.search(r"(?i)password|passwd|token|authorization|api[_-]?key|secret", key):
            result[key] = "<REDACTED>"
        elif isinstance(value, dict):
            result[key] = _redact_document(value)
        else:
            result[key] = value
    return result
