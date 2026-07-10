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
