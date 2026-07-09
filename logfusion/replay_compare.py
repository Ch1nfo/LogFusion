from __future__ import annotations

from pathlib import Path
from typing import Any

from logfusion.pipeline import PipelineResult, replay_raw_records


def compare_raw_replays(
    raw_input: Path,
    baseline_registry_path: Path | None = None,
    current_registry_path: Path | None = None,
) -> dict[str, Any]:
    baseline = replay_raw_records(raw_input, registry_path=baseline_registry_path)
    current = replay_raw_records(raw_input, registry_path=current_registry_path)
    changes = _record_changes(baseline, current)
    return {
        "raw_input": str(raw_input),
        "baseline_registry": str(baseline_registry_path) if baseline_registry_path else None,
        "current_registry": str(current_registry_path) if current_registry_path else None,
        "baseline_summary": baseline.summary,
        "current_summary": current.summary,
        "summary_delta": _summary_delta(baseline.summary, current.summary),
        "record_changes": _change_counts(changes, baseline, current),
        "changes": [change for change in changes if change["change_type"] != "unchanged"],
    }


def _record_changes(baseline: PipelineResult, current: PipelineResult) -> list[dict[str, Any]]:
    baseline_index = _index_result(baseline)
    current_index = _index_result(current)
    changes = []
    for record_id in sorted(set(baseline_index) | set(current_index)):
        before = baseline_index.get(record_id)
        after = current_index.get(record_id)
        changes.append(_compare_record(record_id, before, after))
    return changes


def _index_result(result: PipelineResult) -> dict[str, dict[str, Any]]:
    indexed = {}
    for event in result.normalized:
        indexed[event["raw"]["record_id"]] = {"kind": "normalized", "event": event}
    for unknown in result.unknown:
        indexed[unknown["raw"]["record_id"]] = {"kind": "unknown", "event": unknown}
    return indexed


def _compare_record(
    record_id: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
) -> dict[str, Any]:
    if before is None:
        return {"record_id": record_id, "change_type": "new_record"}
    if after is None:
        return {"record_id": record_id, "change_type": "removed_record"}
    before_kind = before["kind"]
    after_kind = after["kind"]
    before_parser = _parser_id(before)
    after_parser = _parser_id(after)
    if before_kind == "unknown" and after_kind == "normalized":
        return {
            "record_id": record_id,
            "change_type": "unknown_resolved",
            "baseline_parser_id": before_parser,
            "current_parser_id": after_parser,
        }
    if before_kind == "normalized" and after_kind == "unknown":
        return {
            "record_id": record_id,
            "change_type": "new_unknown",
            "baseline_parser_id": before_parser,
            "current_parser_id": after_parser,
        }
    if before_parser != after_parser:
        return {
            "record_id": record_id,
            "change_type": "parser_changed",
            "baseline_parser_id": before_parser,
            "current_parser_id": after_parser,
        }
    if _event_projection(before) != _event_projection(after):
        return {
            "record_id": record_id,
            "change_type": "event_changed",
            "baseline_parser_id": before_parser,
            "current_parser_id": after_parser,
        }
    return {
        "record_id": record_id,
        "change_type": "unchanged",
        "baseline_parser_id": before_parser,
        "current_parser_id": after_parser,
    }


def _parser_id(item: dict[str, Any]) -> str:
    return item["event"].get("parser", {}).get("id", "unknown")


def _event_projection(item: dict[str, Any]) -> dict[str, Any]:
    event = item["event"]
    if item["kind"] == "unknown":
        return {"kind": "unknown", "reason": event.get("reason")}
    return {
        "kind": "normalized",
        "event": event.get("event"),
        "user": event.get("user"),
        "source": event.get("source"),
        "resource": event.get("resource"),
        "network": event.get("network"),
    }


def _change_counts(
    changes: list[dict[str, Any]],
    baseline: PipelineResult,
    current: PipelineResult,
) -> dict[str, int]:
    counts = {
        "total_records": len(changes),
        "unchanged": 0,
        "unknown_resolved": 0,
        "new_unknown": 0,
        "parser_changed": 0,
        "event_changed": 0,
        "new_record": 0,
        "removed_record": 0,
    }
    for change in changes:
        counts[change["change_type"]] += 1
        if change["change_type"] == "unknown_resolved":
            counts["parser_changed"] += 1
        elif change["change_type"] == "new_unknown":
            counts["parser_changed"] += 1
    counts["baseline_normalized_records"] = len(baseline.normalized)
    counts["current_normalized_records"] = len(current.normalized)
    counts["baseline_unknown_records"] = len(baseline.unknown)
    counts["current_unknown_records"] = len(current.unknown)
    return counts


def _summary_delta(baseline: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "total_records",
        "normalized_records",
        "unknown_records",
        "parse_success_rate",
        "parser_confidence_avg",
        "unknown_template_count",
    )
    return {
        field: _numeric(current.get(field, 0)) - _numeric(baseline.get(field, 0))
        for field in fields
    }


def _numeric(value: Any) -> float:
    return value if isinstance(value, int | float) else 0
