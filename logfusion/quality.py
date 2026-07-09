from __future__ import annotations

from collections import Counter
from typing import Any

TRACKED_FIELD_PATHS = (
    "event.category",
    "event.action",
    "event.outcome",
    "user.name",
    "source.ip",
    "raw.checksum",
    "raw.storage_ref",
)


def build_summary(normalized: list[dict[str, Any]], unknown: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(normalized) + len(unknown)
    confidences = [
        event["parser"]["confidence"]
        for event in normalized
        if isinstance(event.get("parser", {}).get("confidence"), int | float)
    ]
    confidence_avg = round(sum(confidences) / len(confidences), 6) if confidences else 0.0
    missing_required = sum(len(event.get("schema_errors", [])) for event in unknown)
    unknown_templates = [event.get("template_hint", "") for event in unknown if event.get("template_hint")]
    return {
        "total_records": total,
        "normalized_records": len(normalized),
        "unknown_records": len(unknown),
        "parse_success_rate": round(len(normalized) / total, 6) if total else 0.0,
        "parser_confidence_avg": confidence_avg,
        "parser_confidence_min": min(confidences) if confidences else 0.0,
        "parser_confidence_p50": _percentile(confidences, 0.5),
        "source_type_count": dict(Counter(
            event.get("raw", {}).get("source_type", "unknown")
            for event in [*normalized, *unknown]
        )),
        "parser_count": dict(Counter(event["parser"]["id"] for event in normalized)),
        "missing_required_fields_count": missing_required,
        "required_field_coverage": _field_coverage(normalized),
        "unknown_template_count": len(set(unknown_templates)),
        "unknown_template_top": dict(Counter(unknown_templates).most_common(10)),
        "unknown_reason_count": dict(Counter(event.get("reason", "unknown") for event in unknown)),
    }


def _field_coverage(events: list[dict[str, Any]]) -> dict[str, float]:
    if not events:
        return {field: 0.0 for field in TRACKED_FIELD_PATHS}
    coverage = {}
    for field in TRACKED_FIELD_PATHS:
        present = sum(1 for event in events if _get_path(event, field) not in (None, ""))
        coverage[field] = round(present / len(events), 6)
    return coverage


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, int(len(sorted_values) * q))
    return sorted_values[index]


def _get_path(document: dict[str, Any], field_path: str) -> Any:
    current: Any = document
    for part in field_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current
