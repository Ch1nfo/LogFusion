from __future__ import annotations

from pathlib import Path
from typing import Any


def load_sources_config(path: Path) -> list[dict[str, Any]]:
    """Load the small YAML subset used by the MVP source config."""
    lines = path.read_text(encoding="utf-8").splitlines()
    sources: list[dict[str, Any]] = []
    raw_options: dict[str, Any] = {}
    current: dict[str, Any] | None = None
    in_paths = False
    section: str | None = None

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if stripped == "raw:":
            section = "raw"
            current = None
            in_paths = False
            continue

        if stripped == "sources:":
            section = "sources"
            current = None
            in_paths = False
            continue

        if section == "raw" and ":" in stripped:
            key, value = stripped.split(":", 1)
            raw_options[key.strip()] = _typed_value(value)
            continue

        if stripped.startswith("- source_id:"):
            current = {
                "source_id": _value(stripped.split(":", 1)[1]),
                "include_raw_text": bool(raw_options.get("include_text_in_event", False)),
            }
            sources.append(current)
            in_paths = False
            continue

        if current is None:
            continue

        if stripped == "paths:":
            current["paths"] = []
            in_paths = True
            continue

        if in_paths and stripped.startswith("- "):
            current.setdefault("paths", []).append(_value(stripped[2:]))
            continue

        if ":" in stripped:
            key, value = stripped.split(":", 1)
            current[key.strip()] = _typed_value(value)
            in_paths = False

    return sources


def load_llm_config(path: Path) -> dict[str, Any]:
    """Load the small `llm:` YAML section used by the local provider config."""
    settings: dict[str, Any] = {}
    in_llm_section = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        stripped = raw_line.strip()
        if stripped == "llm:":
            in_llm_section = True
            continue
        if not raw_line[0].isspace() and stripped.endswith(":"):
            in_llm_section = False
            continue
        if in_llm_section and ":" in stripped:
            key, value = stripped.split(":", 1)
            settings[key.strip()] = _typed_value(value)
    for numeric_key in ("timeout_seconds", "sample_limit"):
        if numeric_key in settings:
            settings[numeric_key] = int(settings[numeric_key])
    return settings


def load_kafka_config(path: Path) -> dict[str, Any]:
    """Load the small YAML subset used by the Kafka consumer configuration."""
    settings: dict[str, Any] = {}
    in_kafka_section = False
    in_topics = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        stripped = raw_line.strip()
        if stripped == "kafka:":
            in_kafka_section = True
            in_topics = False
            continue
        if not raw_line[0].isspace() and stripped.endswith(":"):
            in_kafka_section = False
            in_topics = False
            continue
        if not in_kafka_section:
            continue
        if stripped == "topics:":
            settings["topics"] = []
            in_topics = True
            continue
        if in_topics and stripped.startswith("- "):
            settings["topics"].append(_value(stripped[2:]))
            continue
        if ":" in stripped:
            key, value = stripped.split(":", 1)
            settings[key.strip()] = _typed_value(value)
            in_topics = False
    return settings


def load_fusion_policy(path: Path) -> dict[str, Any]:
    """Load scalar settings from a local ``fusion:`` policy YAML section."""
    settings: dict[str, Any] = {}
    active = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        stripped = raw_line.strip()
        if stripped == "fusion:":
            active = True
            continue
        if not raw_line[0].isspace() and stripped.endswith(":"):
            active = False
            continue
        if active and ":" in stripped:
            key, value = stripped.split(":", 1)
            settings[key.strip()] = _typed_value(value)
    for key in ("alert_threshold", "critical_threshold", "cross_system_bonus", "detector_family_bonus", "detector_family_bonus_cap", "per_user_daily_alert_limit"):
        if key in settings:
            try:
                settings[key] = int(settings[key])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"fusion policy {key} must be an integer") from exc
    return settings


def load_detection_policy(path: Path) -> dict[str, Any]:
    settings: dict[str, Any] = {}
    active = False
    source_type: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        stripped = raw_line.strip()
        if stripped == "detection:":
            active = True
            continue
        if not raw_line[0].isspace() and stripped.endswith(":"):
            active = False
            continue
        if active and stripped == "source_type:":
            settings["source_type_overrides"] = {}
            source_type = None
            continue
        if active and raw_line.startswith("    ") and stripped.endswith(":"):
            source_type = stripped[:-1]
            settings.setdefault("source_type_overrides", {})[source_type] = {}
            continue
        if active and source_type is not None and raw_line.startswith("      ") and ":" in stripped:
            key, value = stripped.split(":", 1)
            settings["source_type_overrides"][source_type][key.strip()] = _typed_value(value)
            continue
        if active and ":" in stripped and not stripped.endswith(":"):
            key, value = stripped.split(":", 1)
            settings[key.strip()] = _typed_value(value)
    for key in ("rare_event_count_max", "p95_score", "p99_score", "new_value_score", "rare_value_score"):
        if key in settings:
            settings[key] = int(settings[key])
    if "rare_share_threshold" in settings:
        settings["rare_share_threshold"] = float(settings["rare_share_threshold"])
    for override in settings.get("source_type_overrides", {}).values():
        for key in ("rare_event_count_max", "p95_score", "p99_score", "new_value_score", "rare_value_score"):
            if key in override:
                override[key] = int(override[key])
        if "rare_share_threshold" in override:
            override["rare_share_threshold"] = float(override["rare_share_threshold"])
    return settings


def load_model_detection_config(path: Path) -> dict[str, Any]:
    """Load scalar model backtest settings from ``model_detection:``."""
    settings: dict[str, Any] = {}
    active = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        stripped = raw_line.strip()
        if stripped == "model_detection:":
            active = True
            continue
        if not raw_line[0].isspace() and stripped.endswith(":"):
            active = False
            continue
        if active and ":" in stripped:
            key, value = stripped.split(":", 1)
            settings[key.strip()] = _typed_value(value)
    integer_keys = (
        "min_training_samples", "min_peer_members", "top_feature_count", "hbos_min_bins", "hbos_max_bins",
        "isolation_forest_estimators", "isolation_forest_max_samples", "random_state",
    )
    for key in integer_keys:
        if key in settings:
            try:
                settings[key] = int(settings[key])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"model detection config {key} must be an integer") from exc
    if "threshold_quantile" in settings:
        try:
            settings["threshold_quantile"] = float(settings["threshold_quantile"])
        except (TypeError, ValueError) as exc:
            raise ValueError("model detection config threshold_quantile must be numeric") from exc
    return settings


def _value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _typed_value(value: str) -> Any:
    value = _value(value)
    if value == "true":
        return True
    if value == "false":
        return False
    return value
