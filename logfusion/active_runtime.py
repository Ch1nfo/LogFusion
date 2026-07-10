from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from logfusion.models import RawRecord
from logfusion.parser_contract import record_format_fingerprint
from logfusion.parser_registry import load_registry
from logfusion.parser_runtime import extract_fields


def load_active_parsers(registry_path: Path | None) -> list[dict[str, Any]]:
    if registry_path is None:
        return []
    registry = load_registry(registry_path)
    return [parser for parser in registry["parsers"] if parser.get("status") == "active"]


def parse_with_active_parsers(record: RawRecord, active_parsers: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for parser in active_parsers:
        event = _try_active_parser(record, parser)
        if event is not None:
            matches.append((parser, event))
    if not matches:
        return None

    selected_parser, selected_event = _select_parser_match(matches)
    if len(matches) > 1:
        selected_event["parser"]["conflict"] = _conflict_metadata(selected_parser, matches)
    return selected_event


def _try_active_parser(record: RawRecord, parser: dict[str, Any]) -> dict[str, Any] | None:
    if not _matches_source_contract(record, parser):
        return None
    try:
        fields = extract_fields(parser, record.raw_text)
    except Exception:
        return None
    expected = parser.get("field_candidates", {})
    if expected and not set(expected).issubset(fields):
        return None

    event = _base_active_event(record, parser)
    _apply_event_defaults(event, parser.get("event_defaults", {}))
    _apply_schema_mapping(event, fields, parser.get("schema_mapping", {}))
    _apply_action_mapping(
        event,
        fields,
        parser.get("schema_mapping", {}),
        parser.get("action_mapping", {}),
        parser.get("action_source_field"),
    )
    _apply_http_outcome(event)
    event["extensions"]["active_parser"] = fields
    return event


def _matches_source_contract(record: RawRecord, parser: dict[str, Any]) -> bool:
    if not parser.get("enforce_source_contract"):
        return True
    contract = parser.get("source_contract") or {}
    return (
        contract.get("source_id") == record.source_id
        and contract.get("source_type") == record.source_type
        and contract.get("format_fingerprint")
        == record_format_fingerprint(
            record.source_id, record.source_type, record.raw_text, record.product, record.format_version,
        )
        and (not contract.get("product") or contract["product"] == record.product)
        and (not contract.get("format_version") or contract["format_version"] == record.format_version)
    )


def _select_parser_match(matches: list[tuple[dict[str, Any], dict[str, Any]]]) -> tuple[dict[str, Any], dict[str, Any]]:
    return sorted(matches, key=lambda item: _selection_key(item[0]))[0]


def _selection_key(parser: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(parser.get("priority", 100)),
        -float(parser.get("success_rate", 0.0) or 0.0),
        -float(parser.get("schema_mapping_rate", 0.0) or 0.0),
        -float(parser.get("shadow_replay_success_rate", 0.0) or 0.0),
        parser.get("parser_id", ""),
    )


def _conflict_metadata(
    selected_parser: dict[str, Any],
    matches: list[tuple[dict[str, Any], dict[str, Any]]],
) -> dict[str, Any]:
    selected_id = selected_parser["parser_id"]
    competing_ids = sorted(parser["parser_id"] for parser, _ in matches if parser["parser_id"] != selected_id)
    return {
        "candidate_count": len(matches),
        "selected_parser_id": selected_id,
        "competing_parser_ids": competing_ids,
        "resolution": _resolution_reason(selected_parser, [parser for parser, _ in matches]),
    }


def _resolution_reason(selected_parser: dict[str, Any], matched_parsers: list[dict[str, Any]]) -> str:
    if _is_unique_min(selected_parser, matched_parsers, "priority"):
        return "priority"
    if _is_unique_max(selected_parser, matched_parsers, "success_rate"):
        return "success_rate"
    if _is_unique_max(selected_parser, matched_parsers, "schema_mapping_rate"):
        return "schema_mapping_rate"
    if _is_unique_max(selected_parser, matched_parsers, "shadow_replay_success_rate"):
        return "shadow_replay_success_rate"
    return "parser_id"


def _is_unique_min(selected_parser: dict[str, Any], parsers: list[dict[str, Any]], field: str) -> bool:
    values = [parser.get(field, 100) for parser in parsers]
    selected = selected_parser.get(field, 100)
    return selected == min(values) and values.count(selected) == 1


def _is_unique_max(selected_parser: dict[str, Any], parsers: list[dict[str, Any]], field: str) -> bool:
    values = [parser.get(field, 0.0) for parser in parsers]
    selected = selected_parser.get(field, 0.0)
    return selected == max(values) and values.count(selected) == 1


def _base_active_event(record: RawRecord, parser: dict[str, Any]) -> dict[str, Any]:
    event = {
        "event": {
            "id": record.record_id,
            "time": _now(),
            "category": "application",
            "type": "activity",
            "action": "unknown",
            "outcome": "unknown",
        },
        "user": {},
        "source": {},
        "destination": {},
        "host": {},
        "device": {},
        "resource": {},
        "http": {},
        "network": {},
        "parser": {
            "id": parser["parser_id"],
            "version": parser.get("version", "0.1.0"),
            "confidence": min(float(parser.get("success_rate", 0.0) or 0.0), 0.9),
            "status": "success",
            "origin": "registry_active",
        },
        "raw": {
            "record_id": record.record_id,
            "source_id": record.source_id,
            "source_type": parser.get("source_type", record.source_type),
            "file_path": record.file_path,
            "line_start": record.line_start,
            "line_end": record.line_end,
            "record_index": record.record_index,
            "checksum": record.checksum,
            "size_bytes": record.size_bytes,
            "storage_ref": record.storage_ref,
        },
        "extensions": {},
    }
    if record.include_raw_text:
        event["raw"]["text"] = record.raw_text
    return event


def _apply_schema_mapping(event: dict[str, Any], fields: dict[str, str], mapping: dict[str, str]) -> None:
    for source_field, target_path in mapping.items():
        if source_field not in fields:
            continue
        value = _normalize_mapped_value(fields[source_field], target_path)
        _set_path(event, target_path, value)


def _normalize_mapped_value(value: Any, target_path: str) -> Any:
    if target_path in {"network.bytes", "http.response.status_code"}:
        try:
            return int(value)
        except (TypeError, ValueError):
            return value
    if target_path == "event.time" and isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized).isoformat()
        except ValueError:
            pass
        try:
            return datetime.strptime(value, "%d/%b/%Y:%H:%M:%S %z").isoformat()
        except ValueError:
            return value
    return value


def _apply_action_mapping(
    event: dict[str, Any],
    fields: dict[str, Any],
    schema_mapping: dict[str, str],
    action_mapping: dict[str, str],
    action_source_field: str | None,
) -> None:
    if not action_mapping:
        return
    if action_source_field and action_source_field in fields:
        event["event"]["action"] = action_mapping.get(
            str(fields[action_source_field]), fields[action_source_field],
        )
        return
    for source_field, target_path in schema_mapping.items():
        if target_path == "event.action" and source_field in fields:
            event["event"]["action"] = action_mapping.get(str(fields[source_field]), fields[source_field])
            return


def _apply_event_defaults(event: dict[str, Any], defaults: dict[str, Any]) -> None:
    for target_path in ("event.category", "event.type", "event.action", "event.outcome"):
        if target_path in defaults:
            _set_path(event, target_path, defaults[target_path])


def _apply_http_outcome(event: dict[str, Any]) -> None:
    status_code = event.get("http", {}).get("response", {}).get("status_code")
    if event["event"].get("outcome") != "unknown" or not isinstance(status_code, int):
        return
    event["event"]["outcome"] = "success" if 200 <= status_code < 400 else "failure"


def _set_path(document: dict[str, Any], field_path: str, value: Any) -> None:
    current = document
    parts = field_path.split(".")
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = value


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
