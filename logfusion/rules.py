from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import tempfile
from pathlib import Path
from typing import Any


RULE_SCHEMA_VERSION = 1
RULE_STATUSES = ("draft", "shadow", "active", "disabled", "deprecated")
RULE_SCOPES = ("event", "window")
RULE_OPERATORS = (
    "eq", "neq", "in", "not_in", "gt", "gte", "lt", "lte",
    "exists", "contains", "starts_with", "ends_with", "cidr_contains",
)
WINDOW_SIZES = (60, 300, 3600, 86400)
EVENT_FIELDS = {
    "event.category", "event.type", "event.action", "event.outcome",
    "user.name", "source.ip", "destination.ip", "host.name",
    "resource.type", "resource.name", "http.request.method",
    "http.response.status_code", "parser.id", "raw.source_type",
}
WINDOW_FIELDS = {
    "event_count", "success_count", "failure_count", "other_outcome_count",
    "source_type_distinct_count", "source_ip_distinct_count", "resource_distinct_count",
    "action_distinct_count", "read_action_count", "write_action_count", "network_bytes_total",
    "first_seen_ip_count", "first_seen_resource_count", "first_seen_action_count",
}
NUMERIC_FIELDS = {*WINDOW_FIELDS, "http.response.status_code"}
IP_FIELDS = {"source.ip", "destination.ip"}
STRING_FIELDS = EVENT_FIELDS - NUMERIC_FIELDS
RULE_KEYS = {
    "rule_id", "version", "name", "description", "status", "scope", "source_types",
    "window_size", "score", "match", "conditions", "tags", "tests",
}
CONDITION_KEYS = {"field", "operator", "value"}
TEST_KEYS = {"name", "input", "expected_match"}
TRANSITIONS = {
    "draft": {"shadow", "disabled", "deprecated"},
    "shadow": {"active", "disabled", "deprecated"},
    "active": {"disabled", "deprecated"},
    "disabled": {"shadow", "active", "deprecated"},
    "deprecated": set(),
}


class RuleError(ValueError):
    """Raised when a detection rule or registry is invalid."""


def load_rules(path: Path | str) -> dict[str, Any]:
    rule_path = Path(path)
    if not rule_path.exists():
        return {"schema_version": RULE_SCHEMA_VERSION, "rules": []}
    try:
        document = json.loads(rule_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuleError(f"invalid rule config: {exc}") from exc
    return validate_rules(document)


def validate_rules(document: Any) -> dict[str, Any]:
    if not isinstance(document, dict) or set(document) != {"schema_version", "rules"}:
        raise RuleError("rule config must contain only schema_version and rules")
    if document.get("schema_version") != RULE_SCHEMA_VERSION or not isinstance(document.get("rules"), list):
        raise RuleError(f"rule schema_version must be {RULE_SCHEMA_VERSION} and rules must be a list")
    seen: set[tuple[str, int]] = set()
    active_ids: set[str] = set()
    validated = []
    for item in document["rules"]:
        rule = validate_rule(item)
        key = (rule["rule_id"], rule["version"])
        if key in seen:
            raise RuleError(f"duplicate rule version: {rule['rule_id']} v{rule['version']}")
        seen.add(key)
        if rule["status"] in {"active", "shadow"}:
            if rule["rule_id"] in active_ids:
                raise RuleError(f"multiple active/shadow versions: {rule['rule_id']}")
            active_ids.add(rule["rule_id"])
        validated.append(rule)
    return {"schema_version": RULE_SCHEMA_VERSION, "rules": validated}


def validate_rule(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict) or not set(item).issubset(RULE_KEYS):
        unknown = sorted(set(item) - RULE_KEYS) if isinstance(item, dict) else []
        raise RuleError(f"invalid rule object; unknown fields: {', '.join(unknown)}")
    required = {"rule_id", "version", "name", "status", "scope", "score", "match", "conditions"}
    missing = sorted(required - set(item))
    if missing:
        raise RuleError(f"rule is missing fields: {', '.join(missing)}")
    rule_id = item["rule_id"]
    if not isinstance(rule_id, str) or not rule_id or len(rule_id) > 80 or any(not (c.isalnum() or c in "._-") for c in rule_id):
        raise RuleError("rule_id must use 1-80 letters, numbers, dot, underscore, or hyphen")
    version = item["version"]
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise RuleError("rule version must be a positive integer")
    name = item["name"]
    if not isinstance(name, str) or not name.strip() or len(name) > 120:
        raise RuleError("rule name must be 1-120 characters")
    description = item.get("description", "")
    if not isinstance(description, str) or len(description) > 1000:
        raise RuleError("rule description must be a string up to 1000 characters")
    status, scope = item["status"], item["scope"]
    if status not in RULE_STATUSES or scope not in RULE_SCOPES:
        raise RuleError("unsupported rule status or scope")
    score = item["score"]
    if isinstance(score, bool) or not isinstance(score, int) or not 0 <= score <= 100:
        raise RuleError("rule score must be an integer from 0 to 100")
    match = item["match"]
    if match not in {"all", "any"}:
        raise RuleError("rule match must be all or any")
    source_types = item.get("source_types", [])
    tags = item.get("tags", [])
    if not isinstance(source_types, list) or any(not isinstance(value, str) or not value for value in source_types):
        raise RuleError("source_types must contain non-empty strings")
    if not isinstance(tags, list) or any(not isinstance(value, str) or not value for value in tags):
        raise RuleError("tags must contain non-empty strings")
    conditions = item["conditions"]
    if not isinstance(conditions, list) or not 1 <= len(conditions) <= 20:
        raise RuleError("rules require 1-20 conditions")
    allowed_fields = EVENT_FIELDS if scope == "event" else WINDOW_FIELDS
    normalized_conditions = [_validate_condition(condition, allowed_fields) for condition in conditions]
    window_size = item.get("window_size")
    if scope == "window" and window_size not in WINDOW_SIZES:
        raise RuleError("window rules require window_size 60, 300, 3600, or 86400")
    if scope == "event" and window_size is not None:
        raise RuleError("event rules must not set window_size")
    tests = item.get("tests", [])
    if not isinstance(tests, list):
        raise RuleError("rule tests must be a list")
    normalized_tests = [_validate_test(test) for test in tests]
    result = {
        "rule_id": rule_id,
        "version": version,
        "name": name.strip(),
        "description": description,
        "status": status,
        "scope": scope,
        "source_types": sorted(set(source_types)),
        "score": score,
        "match": match,
        "conditions": normalized_conditions,
        "tags": sorted(set(tags)),
        "tests": normalized_tests,
    }
    if scope == "window":
        result["window_size"] = window_size
    if status == "active":
        report = test_rule(result)
        if not report["passed"] or report["positive_count"] < 1 or report["negative_count"] < 1:
            raise RuleError("active rules require passing positive and negative tests")
    return result


def test_rule(rule: dict[str, Any]) -> dict[str, Any]:
    results = []
    for test in rule.get("tests", []):
        actual = match_rule(rule, test["input"])
        results.append({"name": test["name"], "expected_match": test["expected_match"], "actual_match": actual, "passed": actual == test["expected_match"]})
    return {
        "rule_id": rule["rule_id"],
        "version": rule["version"],
        "test_count": len(results),
        "positive_count": sum(test["expected_match"] for test in rule.get("tests", [])),
        "negative_count": sum(not test["expected_match"] for test in rule.get("tests", [])),
        "passed": bool(results) and all(item["passed"] for item in results),
        "results": results,
    }


def test_rules(document: dict[str, Any]) -> dict[str, Any]:
    validated = validate_rules(document)
    reports = [test_rule(rule) for rule in validated["rules"]]
    return {"passed": all(report["passed"] for report in reports), "reports": reports}


def match_rule(rule: dict[str, Any], document: dict[str, Any]) -> bool:
    source_types = rule.get("source_types", [])
    if source_types:
        observed = _get(document, "raw.source_type") if rule["scope"] == "event" else document.get("source_types", [])
        values = [observed] if isinstance(observed, str) else observed
        if not isinstance(values, list) or not set(source_types).intersection(values):
            return False
    matches = [_match_condition(_get(document, condition["field"]), condition) for condition in rule["conditions"]]
    return all(matches) if rule["match"] == "all" else any(matches)


def rule_fingerprint(rule: dict[str, Any]) -> str:
    payload = json.dumps(rule, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def save_rules(document: dict[str, Any], path: Path | str) -> None:
    validated = validate_rules(document)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(validated, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def add_rule_version(path: Path | str, item: dict[str, Any]) -> dict[str, Any]:
    document = load_rules(path)
    rule = validate_rule(item)
    versions = [candidate for candidate in document["rules"] if candidate["rule_id"] == rule["rule_id"]]
    if versions and rule["version"] != max(candidate["version"] for candidate in versions) + 1:
        raise RuleError("new rule versions must increment the latest version by one")
    if not versions and rule["version"] != 1:
        raise RuleError("a new rule must start at version 1")
    document["rules"].append(rule)
    save_rules(document, path)
    return rule


def set_rule_status(path: Path | str, rule_id: str, version: int, status: str) -> dict[str, Any]:
    document = load_rules(path)
    rule = next((item for item in document["rules"] if item["rule_id"] == rule_id and item["version"] == version), None)
    if rule is None:
        raise RuleError(f"rule does not exist: {rule_id} v{version}")
    if status not in TRANSITIONS[rule["status"]]:
        raise RuleError(f"invalid rule status transition: {rule['status']} -> {status}")
    if status in {"active", "shadow"}:
        for other in document["rules"]:
            if other is not rule and other["rule_id"] == rule_id and other["status"] in {"active", "shadow"}:
                other["status"] = "disabled"
    rule["status"] = status
    validated = validate_rule(rule)
    document["rules"] = [validated if item is rule else item for item in document["rules"]]
    save_rules(document, path)
    return validated


def _validate_condition(condition: Any, allowed_fields: set[str]) -> dict[str, Any]:
    if not isinstance(condition, dict) or set(condition) - CONDITION_KEYS or not {"field", "operator"}.issubset(condition):
        raise RuleError("rule conditions support only field, operator, and value")
    field, operator = condition["field"], condition["operator"]
    if field not in allowed_fields or field == "raw.text":
        raise RuleError(f"unsupported rule field: {field}")
    if operator not in RULE_OPERATORS:
        raise RuleError(f"unsupported rule operator: {operator}")
    if "value" not in condition and operator != "exists":
        raise RuleError(f"{operator} requires a value")
    value = condition.get("value", True)
    if operator in {"in", "not_in"} and (not isinstance(value, list) or not value):
        raise RuleError(f"{operator} requires a non-empty list")
    if operator in {"eq", "neq", "in", "not_in"}:
        values = value if isinstance(value, list) else [value]
        if field in NUMERIC_FIELDS and any(isinstance(item, bool) or not isinstance(item, (int, float)) for item in values):
            raise RuleError(f"{operator} requires numeric values for {field}")
        if field in STRING_FIELDS | IP_FIELDS and any(not isinstance(item, str) for item in values):
            raise RuleError(f"{operator} requires string values for {field}")
    if operator in {"gt", "gte", "lt", "lte"}:
        if field not in NUMERIC_FIELDS or isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RuleError(f"{operator} requires a numeric field and value")
    if operator == "exists" and not isinstance(value, bool):
        raise RuleError("exists requires a boolean value")
    if operator in {"contains", "starts_with", "ends_with"} and (field not in STRING_FIELDS or not isinstance(value, str)):
        raise RuleError(f"{operator} requires a string field and value")
    if operator == "cidr_contains":
        if field not in IP_FIELDS:
            raise RuleError("cidr_contains requires a source.ip or destination.ip field")
        try:
            ipaddress.ip_network(value, strict=False)
        except (TypeError, ValueError) as exc:
            raise RuleError("cidr_contains requires a valid CIDR") from exc
    return {"field": field, "operator": operator, "value": value}


def _validate_test(test: Any) -> dict[str, Any]:
    if not isinstance(test, dict) or set(test) != TEST_KEYS:
        raise RuleError("rule tests require name, input, and expected_match")
    if not isinstance(test["name"], str) or not test["name"] or not isinstance(test["input"], dict) or not isinstance(test["expected_match"], bool):
        raise RuleError("invalid rule test")
    return {"name": test["name"][:120], "input": test["input"], "expected_match": test["expected_match"]}


def _match_condition(actual: Any, condition: dict[str, Any]) -> bool:
    operator, expected = condition["operator"], condition.get("value")
    if operator == "exists":
        return (actual is not None) == (expected is not False)
    if operator == "eq":
        return actual == expected
    if operator == "neq":
        return actual != expected
    if operator == "in":
        return actual in expected
    if operator == "not_in":
        return actual not in expected
    if operator in {"gt", "gte", "lt", "lte"}:
        if isinstance(actual, bool) or not isinstance(actual, (int, float)):
            return False
        return {"gt": actual > expected, "gte": actual >= expected, "lt": actual < expected, "lte": actual <= expected}[operator]
    if not isinstance(actual, str):
        return False
    if operator == "contains":
        return str(expected) in actual
    if operator == "starts_with":
        return actual.startswith(str(expected))
    if operator == "ends_with":
        return actual.endswith(str(expected))
    if operator == "cidr_contains":
        try:
            return ipaddress.ip_address(actual) in ipaddress.ip_network(expected, strict=False)
        except ValueError:
            return False
    return False


def _get(document: dict[str, Any], path: str) -> Any:
    if path in document:
        return document[path]
    current: Any = document
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current
