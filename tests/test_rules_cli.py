from __future__ import annotations

import json

from logfusion.cli import main


def test_rules_validate_and_test_cli(tmp_path, capsys):
    path = tmp_path / "rules.json"
    path.write_text(json.dumps({"schema_version": 1, "rules": [{
        "rule_id": "login-failure", "version": 1, "name": "Login failure", "description": "", "status": "draft",
        "scope": "event", "source_types": ["sso"], "score": 80, "match": "all",
        "conditions": [{"field": "event.outcome", "operator": "eq", "value": "failure"}], "tags": [],
        "tests": [
            {"name": "positive", "input": {"event.outcome": "failure", "raw.source_type": "sso"}, "expected_match": True},
            {"name": "negative", "input": {"event.outcome": "success", "raw.source_type": "sso"}, "expected_match": False},
        ],
    }]}), encoding="utf-8")
    assert main(["rules", "validate", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["rule_count"] == 1
    assert main(["rules", "test", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["passed"] is True


def test_rules_cli_rejects_invalid_config(tmp_path, capsys):
    path = tmp_path / "rules.json"
    path.write_text('{"schema_version":1,"rules":[{"execute":"code"}]}', encoding="utf-8")
    assert main(["rules", "validate", "--config", str(path)]) == 1
    assert "unknown fields" in capsys.readouterr().out
