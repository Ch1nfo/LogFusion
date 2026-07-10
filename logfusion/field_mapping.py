from __future__ import annotations

from typing import Any


CORE_MAPPING_PREFIXES = (
    "event.", "user.", "source.", "destination.", "host.", "device.",
    "resource.", "http.", "network.", "extensions.",
)

ECS_ALIASES = {
    "@timestamp": "event.time",
    "url.path": "http.url.path",
}

SOURCE_EXTENSION_FAMILIES = {
    "svn", "git", "gitlab", "edr", "waf", "vpn", "sso", "hiklink", "database", "db",
}


def normalize_schema_mapping(mapping: dict[str, Any]) -> dict[str, Any]:
    """Normalize known ECS and source-extension paths into Canonical Schema paths."""
    normalized: dict[str, Any] = {}
    for raw_field, target_path in mapping.items():
        target = ECS_ALIASES.get(target_path, target_path)
        if isinstance(target, str):
            source_family = target.split(".", 1)[0]
            if source_family in SOURCE_EXTENSION_FAMILIES and not target.startswith("extensions."):
                target = f"extensions.{target}"
        normalized[raw_field] = target
    return normalized


def is_canonical_mapping_path(target_path: Any) -> bool:
    return isinstance(target_path, str) and target_path.startswith(CORE_MAPPING_PREFIXES)


def parser_generation_schema_contract() -> dict[str, Any]:
    return {
        "mapping_direction": "raw_field -> canonical_field",
        "core_fields": [
            "event.time", "event.category", "event.type", "event.action", "event.outcome",
            "user.id", "user.name", "source.ip", "destination.ip", "host.id", "host.name",
            "resource.type", "resource.name", "http.request.method", "http.response.status_code",
            "http.url.path", "network.bytes",
        ],
        "event_enums": {
            "event.category": ["application", "authentication", "endpoint", "network", "repository", "unknown"],
            "event.type": ["access", "activity", "api", "behavior", "login", "redirect", "unknown"],
            "event.outcome": ["success", "failure", "unknown"],
        },
        "extension_rule": "extensions.<source_family>.<field>",
        "mapping_examples": [
            "@timestamp -> event.time",
            "url.path -> http.url.path",
            "svn.operation -> extensions.svn.operation",
            "edr.process_guid -> extensions.edr.process_guid",
        ],
        "forbidden_targets": ["@timestamp", "url.path", "svn.operation", "edr.process_guid"],
    }
