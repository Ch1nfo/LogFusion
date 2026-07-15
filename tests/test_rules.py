from __future__ import annotations

import json

import pytest

from logfusion.rules import RuleError, add_rule_version, load_rules, match_rule, save_rules, set_rule_status, test_rules as run_rule_tests, validate_rule


def _rule(**overrides):
    rule = {
        "rule_id": "sso-brute-force",
        "version": 1,
        "name": "SSO repeated failures",
        "description": "Repeated authentication failures",
        "status": "draft",
        "scope": "window",
        "source_types": ["sso"],
        "window_size": 300,
        "score": 85,
        "match": "all",
        "conditions": [{"field": "failure_count", "operator": "gte", "value": 10}],
        "tags": ["authentication"],
        "tests": [
            {"name": "positive", "input": {"failure_count": 12, "source_types": ["sso"]}, "expected_match": True},
            {"name": "negative", "input": {"failure_count": 2, "source_types": ["sso"]}, "expected_match": False},
        ],
    }
    rule.update(overrides)
    return rule


@pytest.mark.parametrize(("operator", "value", "actual", "expected"), [
    ("eq", "failure", "failure", True), ("neq", "success", "failure", True),
    ("in", ["failure", "unknown"], "failure", True), ("not_in", ["success"], "failure", True),
    ("exists", True, "failure", True),
    ("gt", 4, 5, True), ("gte", 5, 5, True), ("lt", 6, 5, True), ("lte", 5, 5, True),
    ("contains", "admin", "user-admin-login", True), ("starts_with", "user_", "user_login", True),
    ("ends_with", "_login", "user_login", True), ("cidr_contains", "10.0.0.0/8", "10.1.2.3", True),
])
def test_rule_operators(operator, value, actual, expected):
    rule = validate_rule({
        **_rule(scope="event", source_types=[], window_size=None, tests=[]),
        "conditions": [{"field": "source.ip" if operator == "cidr_contains" else ("http.response.status_code" if operator in {"gt", "gte", "lt", "lte"} else "event.action"), "operator": operator, "value": value}],
    })
    field = rule["conditions"][0]["field"]
    assert match_rule(rule, {field: actual}) is expected


def test_registry_is_atomic_versioned_and_activation_requires_tests(tmp_path):
    path = tmp_path / "rules.json"
    add_rule_version(path, _rule())
    set_rule_status(path, "sso-brute-force", 1, "shadow")
    active = set_rule_status(path, "sso-brute-force", 1, "active")
    assert active["status"] == "active"
    assert run_rule_tests(load_rules(path))["passed"] is True

    second = _rule(version=2, status="draft", score=90)
    add_rule_version(path, second)
    assert [item["version"] for item in load_rules(path)["rules"]] == [1, 2]
    with pytest.raises(RuleError, match="increment"):
        add_rule_version(path, _rule(version=4))


def test_rule_validation_rejects_raw_unknown_fields_duplicates_and_untested_active(tmp_path):
    with pytest.raises(RuleError, match="unsupported rule field"):
        validate_rule({**_rule(scope="event", source_types=[], window_size=None), "conditions": [{"field": "raw.text", "operator": "contains", "value": "password"}]})
    with pytest.raises(RuleError, match="unknown fields"):
        validate_rule({**_rule(), "execute": "python"})
    with pytest.raises(RuleError, match="positive and negative"):
        validate_rule({**_rule(status="active"), "tests": []})
    with pytest.raises(RuleError, match="numeric field"):
        validate_rule({**_rule(scope="event", source_types=[], window_size=None), "conditions": [{"field": "event.action", "operator": "gte", "value": 2}]})
    with pytest.raises(RuleError, match="unsupported rule operator"):
        validate_rule({**_rule(), "conditions": [{"field": "failure_count", "operator": "regex", "value": ".*"}]})

    path = tmp_path / "rules.json"
    document = {"schema_version": 1, "rules": [_rule(), _rule()]}
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(RuleError, match="duplicate rule version"):
        load_rules(path)


def test_any_rule_matches_one_flat_condition():
    rule = validate_rule({
        **_rule(match="any", source_types=[]),
        "conditions": [
            {"field": "failure_count", "operator": "gte", "value": 10},
            {"field": "event_count", "operator": "gte", "value": 100},
        ],
    })
    assert match_rule(rule, {"failure_count": 1, "event_count": 100}) is True
