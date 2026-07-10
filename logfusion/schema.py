from __future__ import annotations

import ipaddress
import re
from datetime import datetime
from typing import Any


REQUIRED_FIELDS = (
    "event.id",
    "event.time",
    "event.category",
    "event.type",
    "event.action",
    "event.outcome",
    "parser.id",
    "parser.version",
    "parser.status",
    "parser.confidence",
    "raw.record_id",
    "raw.source_id",
    "raw.source_type",
    "raw.checksum",
    "raw.storage_ref",
)

EVENT_CATEGORIES = ("application", "authentication", "endpoint", "network", "repository", "unknown")
EVENT_TYPES = ("access", "activity", "api", "behavior", "login", "redirect", "unknown")
EVENT_OUTCOMES = ("failure", "success", "unknown")
PARSER_STATUSES = ("success", "failed")
CHECKSUM_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")


def validate_event(event: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field_path in REQUIRED_FIELDS:
        value = _get_path(event, field_path)
        if value is None or value == "":
            errors.append(f"{field_path} is required")
    confidence = _get_path(event, "parser.confidence")
    is_numeric_confidence = type(confidence) in (int, float)
    if confidence is not None and not is_numeric_confidence:
        errors.append("parser.confidence must be numeric")
    if is_numeric_confidence and not 0 <= confidence <= 1:
        errors.append("parser.confidence must be between 0 and 1")
    _validate_enum(errors, event, "event.category", EVENT_CATEGORIES)
    _validate_enum(errors, event, "event.type", EVENT_TYPES)
    _validate_enum(errors, event, "event.outcome", EVENT_OUTCOMES)
    _validate_enum(errors, event, "parser.status", PARSER_STATUSES)
    _validate_iso_time(errors, event, "event.time")
    _validate_ip(errors, event, "source.ip")
    _validate_ip(errors, event, "destination.ip")
    _validate_checksum(errors, event)
    _validate_storage_ref(errors, event)
    _validate_non_negative_int(errors, event, "raw.size_bytes")
    _validate_non_negative_int(errors, event, "network.bytes")
    _validate_int(errors, event, "http.response.status_code")
    return errors


def _get_path(document: dict[str, Any], field_path: str) -> Any:
    current: Any = document
    for part in field_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _validate_enum(errors: list[str], event: dict[str, Any], field_path: str, allowed: tuple[str, ...]) -> None:
    value = _get_path(event, field_path)
    if value is None or value == "":
        return
    if value not in allowed:
        errors.append(f"{field_path} must be one of: {', '.join(allowed)}")


def _validate_iso_time(errors: list[str], event: dict[str, Any], field_path: str) -> None:
    value = _get_path(event, field_path)
    if value is None or value == "":
        return
    if not isinstance(value, str):
        errors.append(f"{field_path} must be ISO-8601")
        return
    normalized = value.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError:
        errors.append(f"{field_path} must be ISO-8601")


def _validate_ip(errors: list[str], event: dict[str, Any], field_path: str) -> None:
    value = _get_path(event, field_path)
    if value is None or value == "":
        return
    try:
        ipaddress.ip_address(value)
    except ValueError:
        errors.append(f"{field_path} must be a valid IP address")


def _validate_checksum(errors: list[str], event: dict[str, Any]) -> None:
    value = _get_path(event, "raw.checksum")
    if value is None or value == "":
        return
    if not isinstance(value, str) or not CHECKSUM_RE.match(value):
        errors.append("raw.checksum must match sha256:<64 hex chars>")


def _validate_storage_ref(errors: list[str], event: dict[str, Any]) -> None:
    value = _get_path(event, "raw.storage_ref")
    if value is None or value == "":
        return
    if not isinstance(value, str) or not (value.startswith("file://") or value.startswith("kafka://")):
        errors.append("raw.storage_ref must start with file:// or kafka://")


def _validate_non_negative_int(errors: list[str], event: dict[str, Any], field_path: str) -> None:
    value = _get_path(event, field_path)
    if value is None:
        return
    if not isinstance(value, int) or value < 0:
        errors.append(f"{field_path} must be a non-negative integer")


def _validate_int(errors: list[str], event: dict[str, Any], field_path: str) -> None:
    value = _get_path(event, field_path)
    if value is None:
        return
    if not isinstance(value, int):
        errors.append(f"{field_path} must be an integer")
