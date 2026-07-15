from __future__ import annotations

import json

import pytest

from logfusion.config import load_kafka_config, load_sources_config
from logfusion.source_management import (
    SourceManagementError,
    add_kafka_source,
    add_local_source,
    preview_source,
    validate_source_settings,
)


SSO_SAMPLE = json.dumps({
    "ctime": "2026-07-15 10:20:30",
    "eventType": "authenticateSuccess",
    "status": "0",
    "shortName": "alice",
    "userIp": "10.1.2.3",
    "casServer": "10.2.3.4",
    "appUrl": "https://example.internal/app",
})


def _local(**overrides):
    values = {
        "kind": "local",
        "source_id": "sso-main",
        "source_type": "auto",
        "record_mode": "line",
        "path": "data/sso/*.jsonl",
        "product": "sso",
        "format_version": "v1",
        "include_raw_text": "",
    }
    values.update(overrides)
    return validate_source_settings(values)


def _kafka(**overrides):
    values = {
        "kind": "kafka",
        "source_id": "audit-kafka",
        "source_type": "auto",
        "bootstrap_servers": "kafka-1:9092,kafka-2:9092",
        "topics": "audit.raw, audit.backfill",
        "group_id": "logfusion-security",
        "product": "audit",
        "format_version": "v2",
        "include_raw_text": "true",
    }
    values.update(overrides)
    return validate_source_settings(values)


def test_preview_uses_real_parser_and_reports_canonical_coverage():
    result = preview_source(_local(), SSO_SAMPLE)
    assert result["ok"] is True
    assert result["detected_source_type"] == "sso"
    assert result["effective_source_type"] == "sso"
    assert result["parser_id"] == "sso_json"
    assert result["confidence"] == 0.98
    assert result["field_coverage"] == 1.0
    assert result["normalized"]["user"]["name"] == "alice"


def test_preview_failure_returns_unknown_and_cannot_masquerade_as_success():
    result = preview_source(_local(source_type="gitlab"), SSO_SAMPLE)
    assert result["ok"] is False
    assert result["normalized"] is None
    assert result["unknown"]["reason"] == "schema_validation_failed"
    assert result["confidence"] == 0.0


def test_local_source_is_appended_atomically_and_duplicate_is_rejected(tmp_path):
    path = tmp_path / "sources.yaml"
    path.write_text("# keep this comment\nraw:\n  include_text_in_event: false\n\nsources:\n", encoding="utf-8")
    before_inode = path.stat().st_ino
    settings = _local(include_raw_text="true")
    add_local_source(path, settings)
    assert "# keep this comment" in path.read_text(encoding="utf-8")
    assert path.stat().st_ino != before_inode
    assert load_sources_config(path) == [{
        "source_id": "sso-main",
        "include_raw_text": True,
        "source_type": "auto",
        "record_mode": "line",
        "product": "sso",
        "format_version": "v1",
        "paths": ["data/sso/*.jsonl"],
    }]
    with pytest.raises(SourceManagementError, match="来源 ID 已存在"):
        add_local_source(path, settings)


def test_kafka_source_writes_only_non_secret_connection_settings(tmp_path):
    path = tmp_path / "kafka.local.yaml"
    add_kafka_source(path, _kafka())
    text = path.read_text(encoding="utf-8")
    assert "password" not in text.lower()
    assert "username" not in text.lower()
    loaded = load_kafka_config(path)
    assert loaded["topics"] == ["audit.raw", "audit.backfill"]
    assert loaded["source_id"] == "audit-kafka"
    assert loaded["include_raw_text"] is True
    with pytest.raises(SourceManagementError, match="Kafka 配置已存在"):
        add_kafka_source(path, _kafka(source_id="another"))


@pytest.mark.parametrize("source_id", ["", "bad id", "../escape", "a" * 65])
def test_source_id_is_constrained(source_id):
    with pytest.raises(SourceManagementError, match="来源 ID"):
        _local(source_id=source_id)
