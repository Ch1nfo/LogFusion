from __future__ import annotations

import hashlib
import json
import os
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from logfusion.models import RawRecord
from logfusion.pipeline import ParseOutcome, iter_parse_records
from logfusion.quality import QualityAccumulator
from logfusion.raw_store import raw_record_to_dict
from logfusion.streaming import (
    CheckpointError,
    JsonlSink,
    _atomic_write_json,
    _compact_quality_snapshot,
    _expand_quality_snapshot,
    _file_fingerprint,
    _hash_json,
)


KAFKA_CHECKPOINT_VERSION = 1


class KafkaConsumerError(RuntimeError):
    pass


@dataclass(frozen=True)
class KafkaMessage:
    topic: str
    partition: int
    offset: int
    value: bytes | str
    timestamp_ms: int | None


class KafkaConsumer(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout_seconds: float) -> KafkaMessage | None: ...

    def commit(self, offsets: dict[tuple[str, int], int]) -> None: ...

    def seek(self, offsets: dict[tuple[str, int], int]) -> None: ...

    def close(self) -> None: ...


class FakeKafkaConsumer:
    """In-memory consumer used by unit tests and local runtime simulations."""

    def __init__(self, messages: list[KafkaMessage], fail_commit: bool = False) -> None:
        self._messages = list(messages)
        self.fail_commit = fail_commit
        self.subscriptions: list[list[str]] = []
        self.commits: list[dict[tuple[str, int], int]] = []
        self.seeks: list[dict[tuple[str, int], int]] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(list(topics))

    def poll(self, timeout_seconds: float) -> KafkaMessage | None:
        while self._messages:
            return self._messages.pop(0)
        return None

    def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        if self.fail_commit:
            raise KafkaConsumerError("Kafka offset commit failed")
        self.commits.append(dict(offsets))

    def seek(self, offsets: dict[tuple[str, int], int]) -> None:
        self.seeks.append(dict(offsets))
        self._messages = [
            message
            for message in self._messages
            if message.offset >= offsets.get((message.topic, message.partition), 0)
        ]

    def close(self) -> None:
        self.closed = True


class ConfluentKafkaConsumer:
    """Optional confluent-kafka adapter, imported only when real Kafka is used."""

    def __init__(self, config: dict[str, Any]) -> None:
        try:
            from confluent_kafka import Consumer, KafkaError, TopicPartition
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise KafkaConsumerError(
                "Kafka support requires the optional 'confluent-kafka' dependency"
            ) from exc
        self._Consumer = Consumer
        self._KafkaError = KafkaError
        self._TopicPartition = TopicPartition
        self._pending_seek: dict[tuple[str, int], int] = {}
        self._consumer = Consumer(_confluent_config(config))

    def subscribe(self, topics: list[str]) -> None:
        def on_assign(consumer, partitions):
            for partition in partitions:
                offset = self._pending_seek.get((partition.topic, partition.partition))
                if offset is not None:
                    partition.offset = offset
            consumer.assign(partitions)

        self._consumer.subscribe(topics, on_assign=on_assign)

    def poll(self, timeout_seconds: float) -> KafkaMessage | None:
        message = self._consumer.poll(timeout_seconds)
        if message is None:
            return None
        if message.error():
            if message.error().code() == self._KafkaError._PARTITION_EOF:
                return None
            raise KafkaConsumerError(str(message.error()))
        timestamp = message.timestamp()
        timestamp_ms = timestamp[1] if timestamp else None
        return KafkaMessage(
            message.topic(), message.partition(), message.offset(), message.value(), timestamp_ms,
        )

    def commit(self, offsets: dict[tuple[str, int], int]) -> None:
        partitions = [
            self._TopicPartition(topic, partition, offset)
            for (topic, partition), offset in sorted(offsets.items())
        ]
        try:
            self._consumer.commit(offsets=partitions, asynchronous=False)
        except Exception as exc:  # pragma: no cover - depends on broker client
            raise KafkaConsumerError(f"Kafka offset commit failed: {exc}") from exc

    def seek(self, offsets: dict[tuple[str, int], int]) -> None:
        self._pending_seek = dict(offsets)

    def close(self) -> None:
        self._consumer.close()


def run_kafka_streaming_pipeline(
    config: dict[str, Any],
    normalized_output: Path,
    unknown_output: Path,
    summary_output: Path,
    raw_output: Path | None = None,
    registry_path: Path | None = None,
    checkpoint_path: Path | None = None,
    checkpoint_every: int = 10_000,
    resume: bool = False,
    consumer: KafkaConsumer | None = None,
    stop_when_idle: bool = False,
) -> dict[str, Any]:
    _validate_config(config)
    if checkpoint_every <= 0:
        raise ValueError("checkpoint_every must be greater than zero")
    if resume and checkpoint_path is None:
        raise CheckpointError("--resume requires a checkpoint path")
    _validate_output_paths(
        normalized_output, unknown_output, summary_output, raw_output, checkpoint_path, registry_path,
    )
    expected = {
        "config_fingerprint": _hash_json(config),
        "registry_fingerprint": _file_fingerprint(registry_path),
        "output_paths": _output_paths(normalized_output, unknown_output, summary_output, raw_output),
    }
    state = _load_checkpoint(checkpoint_path, expected) if resume else None
    if state is not None:
        accumulator = QualityAccumulator.from_snapshot(_expand_quality_snapshot(state["quality"]))
        if accumulator.finalize()["total_records"] != state["processed_records"]:
            raise CheckpointError("invalid Kafka checkpoint: processed_records does not match quality state")
        next_offsets = _decode_offsets(state["next_offsets"])
    else:
        accumulator = QualityAccumulator()
        next_offsets = {}
    processed = state["processed_records"] if state is not None else 0
    client = consumer or ConfluentKafkaConsumer(config)
    sinks: list[JsonlSink] = []
    try:
        normalized_sink = JsonlSink(normalized_output, state["output_offsets"]["normalized"] if state else None)
        unknown_sink = JsonlSink(unknown_output, state["output_offsets"]["unknown"] if state else None)
        sinks.extend([normalized_sink, unknown_sink])
        raw_sink = JsonlSink(raw_output, state["output_offsets"]["raw"] if state and raw_output else None) if raw_output else None
        if raw_sink is not None:
            sinks.append(raw_sink)
        client.subscribe(list(config["topics"]))
        if next_offsets:
            client.seek(next_offsets)
        if checkpoint_path is not None and state is None:
            _save_checkpoint(checkpoint_path, expected, accumulator, sinks, processed, next_offsets)

        pending: deque[tuple[KafkaMessage, RawRecord]] = deque()

        def records():
            while True:
                message = client.poll(float(config.get("poll_timeout_seconds", 1.0)))
                if message is None:
                    if stop_when_idle:
                        return
                    continue
                record = raw_record_from_kafka_message(config, message)
                pending.append((message, record))
                yield record

        for outcome in iter_parse_records(records(), registry_path):
            message, record = pending.popleft()
            _write_outcome(outcome, normalized_sink, unknown_sink, raw_sink, accumulator)
            next_offsets[(message.topic, message.partition)] = message.offset + 1
            processed += 1
            if checkpoint_path is not None and processed % checkpoint_every == 0:
                _save_checkpoint(checkpoint_path, expected, accumulator, sinks, processed, next_offsets)
                if next_offsets:
                    client.commit(next_offsets)

        if pending:
            raise RuntimeError("parser runtime did not produce an outcome for every Kafka message")
        if checkpoint_path is not None:
            _save_checkpoint(checkpoint_path, expected, accumulator, sinks, processed, next_offsets)
        else:
            for sink in sinks:
                sink.commit()
        if next_offsets:
            client.commit(next_offsets)
        summary = accumulator.finalize()
        _atomic_write_json(summary_output, summary)
        return summary
    finally:
        for sink in sinks:
            sink.close()
        client.close()


def raw_record_from_kafka_message(config: dict[str, Any], message: KafkaMessage) -> RawRecord:
    raw_text = message.value if isinstance(message.value, str) else message.value.decode("utf-8", "replace")
    source_id = str(config["source_id"])
    identity = f"{source_id}:{message.topic}:{message.partition}:{message.offset}"
    return RawRecord(
        record_id=hashlib.sha256(identity.encode("utf-8")).hexdigest(),
        source_id=source_id,
        source_type=str(config["source_type"]),
        file_path=f"kafka://{message.topic}/{message.partition}",
        line_start=message.offset,
        line_end=message.offset,
        record_index=message.offset,
        raw_text=raw_text,
        checksum=f"sha256:{hashlib.sha256(raw_text.encode('utf-8')).hexdigest()}",
        size_bytes=len(raw_text.encode("utf-8")),
        storage_ref=f"kafka://{message.topic}/{message.partition}/{message.offset}",
        include_raw_text=bool(config.get("include_raw_text", False)),
        product=config.get("product"),
        format_version=config.get("format_version"),
        llm_enabled=bool(config.get("llm_enabled", False)),
        kafka_topic=message.topic,
        kafka_partition=message.partition,
        kafka_offset=message.offset,
        kafka_timestamp_ms=message.timestamp_ms,
    )


def _write_outcome(
    outcome: ParseOutcome,
    normalized_sink: JsonlSink,
    unknown_sink: JsonlSink,
    raw_sink: JsonlSink | None,
    accumulator: QualityAccumulator,
) -> None:
    if raw_sink is not None:
        raw_sink.write(raw_record_to_dict(outcome.record))
    if outcome.normalized is not None:
        normalized_sink.write(outcome.normalized)
        accumulator.add_normalized(outcome.normalized)
    if outcome.unknown is not None:
        unknown_sink.write(outcome.unknown)
        accumulator.add_unknown(outcome.unknown)


def _save_checkpoint(
    path: Path,
    expected: dict[str, Any],
    accumulator: QualityAccumulator,
    sinks: list[JsonlSink],
    processed: int,
    next_offsets: dict[tuple[str, int], int],
) -> None:
    for sink in sinks:
        sink.commit()
    offsets = {"normalized": sinks[0].position(), "unknown": sinks[1].position()}
    if len(sinks) == 3:
        offsets["raw"] = sinks[2].position()
    _atomic_write_json(
        path,
        {
            "version": KAFKA_CHECKPOINT_VERSION,
            **expected,
            "processed_records": processed,
            "next_offsets": _encode_offsets(next_offsets),
            "output_offsets": offsets,
            "quality": _compact_quality_snapshot(accumulator.snapshot()),
        },
        pretty=False,
    )


def _load_checkpoint(path: Path | None, expected: dict[str, Any]) -> dict[str, Any]:
    if path is None or not path.exists():
        raise CheckpointError("Kafka checkpoint does not exist")
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"invalid Kafka checkpoint: {exc}") from exc
    required = {"version", "config_fingerprint", "registry_fingerprint", "output_paths", "processed_records", "next_offsets", "output_offsets", "quality"}
    if not isinstance(state, dict) or required.difference(state):
        raise CheckpointError("invalid Kafka checkpoint")
    if state["version"] != KAFKA_CHECKPOINT_VERSION:
        raise CheckpointError("unsupported Kafka checkpoint version")
    for key in ("config_fingerprint", "registry_fingerprint", "output_paths"):
        if state[key] != expected[key]:
            raise CheckpointError(f"Kafka checkpoint {key} changed")
    if not isinstance(state["processed_records"], int) or state["processed_records"] < 0:
        raise CheckpointError("invalid Kafka checkpoint processed_records")
    required_offsets = {"normalized", "unknown"}
    if expected["output_paths"]["raw"] is not None:
        required_offsets.add("raw")
    if not isinstance(state["output_offsets"], dict) or required_offsets.difference(state["output_offsets"]):
        raise CheckpointError("invalid Kafka checkpoint output offsets")
    if any(not isinstance(state["output_offsets"][name], int) or state["output_offsets"][name] < 0 for name in required_offsets):
        raise CheckpointError("invalid Kafka checkpoint output offsets")
    _decode_offsets(state["next_offsets"])
    if not isinstance(state["quality"], dict):
        raise CheckpointError("invalid Kafka checkpoint quality state")
    return state


def _encode_offsets(offsets: dict[tuple[str, int], int]) -> dict[str, int]:
    return {f"{topic}\x1f{partition}": offset for (topic, partition), offset in sorted(offsets.items())}


def _decode_offsets(encoded: Any) -> dict[tuple[str, int], int]:
    if not isinstance(encoded, dict):
        raise CheckpointError("invalid Kafka checkpoint offsets")
    decoded: dict[tuple[str, int], int] = {}
    for key, offset in encoded.items():
        try:
            topic, partition = key.rsplit("\x1f", 1)
            partition_number = int(partition)
        except (AttributeError, TypeError, ValueError) as exc:
            raise CheckpointError("invalid Kafka checkpoint offset key") from exc
        if not topic or partition_number < 0 or not isinstance(offset, int) or offset < 0:
            raise CheckpointError("invalid Kafka checkpoint offset")
        decoded[(topic, partition_number)] = offset
    return decoded


def _validate_config(config: dict[str, Any]) -> None:
    required = ("bootstrap_servers", "group_id", "topics", "source_id", "source_type")
    if not isinstance(config, dict) or any(not config.get(key) for key in required):
        raise ValueError("Kafka config requires bootstrap_servers, group_id, topics, source_id, and source_type")
    if not isinstance(config["topics"], list) or any(not isinstance(topic, str) or not topic for topic in config["topics"]):
        raise ValueError("Kafka config topics must be a non-empty string list")


def _output_paths(normalized: Path, unknown: Path, summary: Path, raw: Path | None) -> dict[str, str | None]:
    return {
        "normalized": str(normalized.resolve()),
        "unknown": str(unknown.resolve()),
        "summary": str(summary.resolve()),
        "raw": str(raw.resolve()) if raw else None,
    }


def _validate_output_paths(
    normalized: Path,
    unknown: Path,
    summary: Path,
    raw: Path | None,
    checkpoint: Path | None,
    registry: Path | None,
) -> None:
    paths = [("normalized", normalized.resolve()), ("unknown", unknown.resolve()), ("summary", summary.resolve())]
    if raw is not None:
        paths.append(("raw", raw.resolve()))
    if checkpoint is not None:
        paths.append(("checkpoint", checkpoint.resolve()))
    for index, (name, path) in enumerate(paths):
        if any(path == other_path for _, other_path in paths[:index]):
            raise CheckpointError(f"Kafka output paths must be distinct: {name}")
    if registry is not None and any(path == registry.resolve() for _, path in paths):
        raise CheckpointError("Kafka output path conflicts with registry")


def _confluent_config(config: dict[str, Any]) -> dict[str, Any]:
    result = {
        "bootstrap.servers": config["bootstrap_servers"],
        "group.id": config["group_id"],
        "enable.auto.commit": False,
        "auto.offset.reset": config.get("auto_offset_reset", "earliest"),
    }
    for config_key, kafka_key in (
        ("security_protocol", "security.protocol"),
        ("sasl_mechanism", "sasl.mechanisms"),
        ("ssl_ca_location", "ssl.ca.location"),
    ):
        if config.get(config_key):
            result[kafka_key] = config[config_key]
    for config_key, kafka_key in (("sasl_username_env", "sasl.username"), ("sasl_password_env", "sasl.password")):
        if config.get(config_key):
            secret = os.environ.get(str(config[config_key]))
            if not secret:
                raise KafkaConsumerError(f"Kafka credential environment variable is not set: {config[config_key]}")
            result[kafka_key] = secret
    return result
