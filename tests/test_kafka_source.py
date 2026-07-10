import json
import sys

import pytest

from logfusion.kafka_source import (
    ConfluentKafkaConsumer,
    FakeKafkaConsumer,
    KafkaConsumerError,
    KafkaMessage,
    run_kafka_streaming_pipeline,
)


def _config() -> dict:
    return {
        "bootstrap_servers": "kafka-1:9092",
        "group_id": "logfusion-test",
        "topics": ["audit"],
        "source_id": "audit_kafka",
        "source_type": "unknown",
        "include_raw_text": True,
    }


def _read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_kafka_runtime_writes_raw_metadata_and_commits_next_offsets(tmp_path):
    consumer = FakeKafkaConsumer([
        KafkaMessage("audit", 0, 5, b"first event", 1_700_000_000_000),
        KafkaMessage("audit", 1, 2, b"second event", 1_700_000_000_100),
        KafkaMessage("audit", 0, 6, b"third event", 1_700_000_000_200),
    ])
    raw_output = tmp_path / "raw.jsonl"
    summary = run_kafka_streaming_pipeline(
        _config(),
        consumer=consumer,
        normalized_output=tmp_path / "normalized.jsonl",
        unknown_output=tmp_path / "unknown.jsonl",
        summary_output=tmp_path / "summary.json",
        raw_output=raw_output,
        checkpoint_path=tmp_path / "checkpoint.json",
        checkpoint_every=2,
        stop_when_idle=True,
    )

    raw = _read_jsonl(raw_output)
    unknown = _read_jsonl(tmp_path / "unknown.jsonl")
    assert summary["total_records"] == 3
    assert raw[0]["storage_ref"] == "kafka://audit/0/5"
    assert raw[0]["kafka_topic"] == "audit"
    assert raw[0]["kafka_partition"] == 0
    assert raw[0]["kafka_offset"] == 5
    assert raw[0]["kafka_timestamp_ms"] == 1_700_000_000_000
    assert unknown[0]["raw"]["kafka"] == {
        "topic": "audit",
        "partition": 0,
        "offset": 5,
        "timestamp_ms": 1_700_000_000_000,
    }
    assert consumer.commits == [
        {("audit", 0): 6, ("audit", 1): 3},
        {("audit", 0): 7, ("audit", 1): 3},
    ]


def test_kafka_runtime_does_not_commit_when_parser_fails(tmp_path, monkeypatch):
    consumer = FakeKafkaConsumer([KafkaMessage("audit", 0, 0, b"broken", None)])

    def fail_parse(records, registry_path=None):
        raise RuntimeError("parser failed")
        yield  # pragma: no cover

    monkeypatch.setattr("logfusion.kafka_source.iter_parse_records", fail_parse)
    with pytest.raises(RuntimeError, match="parser failed"):
        run_kafka_streaming_pipeline(
            _config(),
            consumer=consumer,
            normalized_output=tmp_path / "normalized.jsonl",
            unknown_output=tmp_path / "unknown.jsonl",
            summary_output=tmp_path / "summary.json",
            checkpoint_path=tmp_path / "checkpoint.json",
            stop_when_idle=True,
        )

    assert consumer.commits == []


def test_kafka_runtime_does_not_commit_when_checkpoint_write_fails(tmp_path, monkeypatch):
    consumer = FakeKafkaConsumer([KafkaMessage("audit", 0, 0, b"event", None)])
    from logfusion import kafka_source

    original_save = kafka_source._save_checkpoint
    calls = 0

    def fail_after_initial_checkpoint(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("checkpoint disk failure")
        return original_save(*args, **kwargs)

    monkeypatch.setattr(kafka_source, "_save_checkpoint", fail_after_initial_checkpoint)
    with pytest.raises(OSError, match="checkpoint disk failure"):
        run_kafka_streaming_pipeline(
            _config(),
            consumer=consumer,
            normalized_output=tmp_path / "normalized.jsonl",
            unknown_output=tmp_path / "unknown.jsonl",
            summary_output=tmp_path / "summary.json",
            checkpoint_path=tmp_path / "checkpoint.json",
            checkpoint_every=1,
            stop_when_idle=True,
        )

    assert consumer.commits == []


def test_optional_confluent_client_reports_a_clear_missing_dependency_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "confluent_kafka", None)

    with pytest.raises(KafkaConsumerError, match="confluent-kafka"):
        ConfluentKafkaConsumer(_config())


def test_kafka_resume_seeks_checkpoint_offset_after_commit_failure(tmp_path):
    checkpoint = tmp_path / "checkpoint.json"
    outputs = {
        "normalized_output": tmp_path / "normalized.jsonl",
        "unknown_output": tmp_path / "unknown.jsonl",
        "summary_output": tmp_path / "summary.json",
        "checkpoint_path": checkpoint,
    }
    first = FakeKafkaConsumer(
        [
            KafkaMessage("audit", 0, 5, b"first", None),
            KafkaMessage("audit", 0, 6, b"second", None),
        ],
        fail_commit=True,
    )
    with pytest.raises(KafkaConsumerError, match="commit failed"):
        run_kafka_streaming_pipeline(
            _config(), consumer=first, checkpoint_every=1, stop_when_idle=True, **outputs
        )

    second = FakeKafkaConsumer([
        KafkaMessage("audit", 0, 5, b"first", None),
        KafkaMessage("audit", 0, 6, b"second", None),
    ])
    summary = run_kafka_streaming_pipeline(
        _config(), consumer=second, checkpoint_every=1, stop_when_idle=True, resume=True, **outputs
    )

    assert second.seeks == [{("audit", 0): 6}]
    record_ids = [row["raw"]["record_id"] for row in _read_jsonl(outputs["unknown_output"])]
    assert len(record_ids) == len(set(record_ids)) == 2
    assert summary["total_records"] == 2
    assert second.commits[-1] == {("audit", 0): 7}
