from __future__ import annotations

import json

import pytest

from logfusion.kafka_source import FakeKafkaConsumer, KafkaConsumerError, KafkaMessage
from logfusion.kafka_ueba import run_kafka_ueba_pipeline
from logfusion.pipeline import ParseOutcome
from tests.test_features import _event


def _config() -> dict:
    return {
        "bootstrap_servers": "kafka-1:9092",
        "group_id": "logfusion-ueba-test",
        "topics": ["audit"],
        "source_id": "audit_kafka",
        "source_type": "sso",
        "include_raw_text": True,
    }


def _paths(tmp_path) -> dict:
    return {
        "normalized_output": tmp_path / "normalized.jsonl",
        "unknown_output": tmp_path / "unknown.jsonl",
        "summary_output": tmp_path / "summary.json",
        "raw_output": tmp_path / "raw.jsonl",
        "feature_state": tmp_path / "features.db",
        "baseline_state": tmp_path / "baseline.db",
        "detection_state": tmp_path / "detection.db",
        "incident_state": tmp_path / "incidents.db",
        "risk_state": tmp_path / "risk.db",
        "checkpoint_path": tmp_path / "kafka-ueba.checkpoint.json",
    }


def _parser(events):
    iterator = iter(events)

    def parse(records, registry_path=None):
        for record in records:
            event = next(iterator)
            event["parser"] = {"id": "test-kafka-parser", "version": "1", "confidence": 1.0}
            yield ParseOutcome(record, event, None)

    return parse


def test_kafka_ueba_persists_full_chain_before_committing_offsets(tmp_path, monkeypatch):
    consumer = FakeKafkaConsumer([KafkaMessage("audit", 0, 5, b"first", None)])
    monkeypatch.setattr("logfusion.kafka_ueba.iter_parse_records", _parser([_event("kafka-5", "2026-07-01T10:00:00Z")]))

    report = run_kafka_ueba_pipeline(
        _config(), consumer=consumer, checkpoint_every=1, stop_when_idle=True, **_paths(tmp_path),
    )

    assert report["processed_records"] == 1
    assert report["downstream"] is not None
    assert consumer.commits == [{("audit", 0): 6}]
    normalized = [json.loads(line) for line in (tmp_path / "normalized.jsonl").read_text().splitlines()]
    assert normalized[0]["event"]["id"] == "kafka-5"
    assert (tmp_path / "risk.db").exists()


def test_kafka_ueba_does_not_commit_when_downstream_fails(tmp_path, monkeypatch):
    consumer = FakeKafkaConsumer([KafkaMessage("audit", 0, 0, b"first", None)])
    monkeypatch.setattr("logfusion.kafka_ueba.iter_parse_records", _parser([_event("kafka-0", "2026-07-01T10:00:00Z")]))

    def fail_downstream(*args, **kwargs):
        raise RuntimeError("downstream failed")

    monkeypatch.setattr("logfusion.kafka_ueba._run_downstream", fail_downstream)
    with pytest.raises(RuntimeError, match="downstream failed"):
        run_kafka_ueba_pipeline(
            _config(), consumer=consumer, checkpoint_every=1, stop_when_idle=True, **_paths(tmp_path),
        )
    assert consumer.commits == []


def test_kafka_ueba_resume_commits_checkpointed_offset_after_broker_failure(tmp_path, monkeypatch):
    paths = _paths(tmp_path)
    first = FakeKafkaConsumer([KafkaMessage("audit", 0, 0, b"first", None)], fail_commit=True)
    monkeypatch.setattr("logfusion.kafka_ueba.iter_parse_records", _parser([_event("kafka-0", "2026-07-01T10:00:00Z")]))
    with pytest.raises(KafkaConsumerError, match="commit failed"):
        run_kafka_ueba_pipeline(_config(), consumer=first, checkpoint_every=1, stop_when_idle=True, **paths)

    second = FakeKafkaConsumer([KafkaMessage("audit", 0, 0, b"first", None)])
    report = run_kafka_ueba_pipeline(
        _config(), consumer=second, checkpoint_every=1, stop_when_idle=True, resume=True, **paths,
    )
    assert report["processed_records"] == 1
    assert second.seeks == [{("audit", 0): 1}]
    assert second.commits == [{("audit", 0): 1}]
