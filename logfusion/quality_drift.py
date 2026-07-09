from __future__ import annotations

from typing import Any


DEFAULT_THRESHOLDS = {
    "parse_success_rate_drop": 0.10,
    "unknown_template_count_multiplier": 2.0,
    "unknown_template_count_min_delta": 3,
    "parser_confidence_avg_drop": 0.10,
    "required_field_coverage_drop": 0.20,
}


def detect_drift(
    baseline: dict[str, Any],
    current: dict[str, Any],
    thresholds: dict[str, float] | None = None,
) -> dict[str, Any]:
    limits = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    flags: dict[str, dict[str, Any]] = {}

    flags["parse_success_rate_drop"] = _drop_flag(
        baseline.get("parse_success_rate", 0.0),
        current.get("parse_success_rate", 0.0),
        limits["parse_success_rate_drop"],
    )
    flags["unknown_template_count_spike"] = _spike_flag(
        baseline.get("unknown_template_count", 0),
        current.get("unknown_template_count", 0),
        limits["unknown_template_count_multiplier"],
        limits["unknown_template_count_min_delta"],
    )
    flags["parser_confidence_avg_drop"] = _drop_flag(
        baseline.get("parser_confidence_avg", 0.0),
        current.get("parser_confidence_avg", 0.0),
        limits["parser_confidence_avg_drop"],
    )

    baseline_coverage = baseline.get("required_field_coverage", {})
    current_coverage = current.get("required_field_coverage", {})
    for field in sorted(set(baseline_coverage) | set(current_coverage)):
        flags[f"required_field_coverage_drop:{field}"] = _drop_flag(
            baseline_coverage.get(field, 0.0),
            current_coverage.get(field, 0.0),
            limits["required_field_coverage_drop"],
        )

    return {
        "drift_detected": any(flag["triggered"] for flag in flags.values()),
        "flags": flags,
        "baseline": baseline,
        "current": current,
        "thresholds": limits,
    }


def _drop_flag(baseline_value: float, current_value: float, threshold: float) -> dict[str, Any]:
    delta = round(baseline_value - current_value, 6)
    return {
        "triggered": delta >= threshold,
        "baseline": baseline_value,
        "current": current_value,
        "delta": delta,
        "threshold": threshold,
    }


def _spike_flag(
    baseline_value: int,
    current_value: int,
    multiplier: float,
    min_delta: float,
) -> dict[str, Any]:
    delta = current_value - baseline_value
    triggered = delta >= min_delta and current_value >= baseline_value * multiplier
    return {
        "triggered": triggered,
        "baseline": baseline_value,
        "current": current_value,
        "delta": delta,
        "multiplier": multiplier,
        "min_delta": min_delta,
    }
