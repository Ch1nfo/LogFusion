from __future__ import annotations

import json
import re
from typing import Any

from logfusion.parser_registry import ParserRegistryError


def extract_fields(parser: dict[str, Any], raw_text: str) -> dict[str, Any]:
    parser_type = parser.get("parser_type")
    if parser_type == "key_value":
        return _extract_key_value(raw_text)
    if parser_type == "json":
        return _extract_json(parser, raw_text)
    if parser_type == "regex":
        return _extract_regex(parser, raw_text)
    raise ParserRegistryError(f"Unsupported parser runtime type: {parser_type}")


def _extract_key_value(raw_text: str) -> dict[str, str]:
    return {key: value for key, value in re.findall(r"(\w+)=([^\s]+)", raw_text)}


def _extract_json(parser: dict[str, Any], raw_text: str) -> dict[str, Any]:
    document = json.loads(raw_text)
    extractors = parser.get("field_extractors", {})
    if not extractors:
        return dict(document) if isinstance(document, dict) else {}
    return {
        field_name: value
        for field_name, path in extractors.items()
        if (value := _get_path(document, str(path))) is not None
    }


def _extract_regex(parser: dict[str, Any], raw_text: str) -> dict[str, str]:
    pattern = parser.get("field_extractors", {}).get("pattern")
    if not pattern:
        raise ParserRegistryError("Regex parser requires field_extractors.pattern")
    match = re.search(pattern, raw_text)
    if not match:
        return {}
    return match.groupdict()


def _get_path(document: Any, field_path: str) -> Any:
    current = document
    for part in field_path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        return None
    return current
