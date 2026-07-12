from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Any

from logfusion.features import FeatureEngine
from logfusion.kafka_source import (
    ConfluentKafkaConsumer,
    KafkaConsumer,
    KafkaConsumerError,
    KafkaMessage,
    _decode_offsets,
    _encode_offsets,
    _output_paths,
    _validate_config,
    _validate_output_paths,
    raw_record_from_kafka_message,
)
from logfusion.orchestrator import OrchestratorConfig, _event_time_ms, _iso, _max_time, _run_downstream
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


KAFKA_UEBA_CHECKPOINT_VERSION = 1


def run_kafka_ueba_pipeline(
    config: dict[str, Any],
    normalized_output: Path,
    unknown_output: Path,
    summary_output: Path,
    feature_state: Path,
    baseline_state: Path,
    detection_state: Path,
    incident_state: Path,
    risk_state: Path,
    *,
    raw_output: Path | None = None,
    registry_path: Path | None = None,
    checkpoint_path: Path | None = None,
    checkpoint_every: int = 10_000,
    resume: bool = False,
    consumer: KafkaConsumer | None = None,
    stop_when_idle: bool = False,
    orchestrator_config: OrchestratorConfig | None = None,
) -> dict[str, Any]:
    """Consume Kafka records through parsing and the complete persistent UEBA chain.

    The local Kafka UEBA checkpoint is written only after all local output and
    state stores are durable. Broker offsets are committed synchronously after
    that checkpoint. A crash in between can replay data, which FeatureEngine
    handles by stable Canonical ``event.id`` deduplication.
    """
    _validate_config(config)
    if checkpoint_every <= 0:
        raise ValueError("checkpoint_every must be greater than zero")
    if resume and checkpoint_path is None:
        raise CheckpointError("--resume requires a checkpoint path")
    _validate_output_paths(normalized_output, unknown_output, summary_output, raw_output, checkpoint_path, registry_path)
    settings = orchestrator_config or OrchestratorConfig()
    states = {
        "feature_state": Path(feature_state), "baseline_state": Path(baseline_state),
        "detection_state": Path(detection_state), "incident_state": Path(incident_state), "risk_state": Path(risk_state),
    }
    _validate_state_paths(states, normalized_output, unknown_output, summary_output, raw_output, checkpoint_path, registry_path)
    expected = {
        "config_fingerprint": _hash_json(config),
        "registry_fingerprint": _file_fingerprint(registry_path),
        "output_paths": _output_paths(normalized_output, unknown_output, summary_output, raw_output),
        "state_paths": {key: str(path.resolve()) for key, path in states.items()},
        "orchestrator_config": {"batch_size": settings.batch_size, "watermark_lag_seconds": settings.watermark_lag_seconds, "peer_groups_path": str(Path(settings.peer_groups_path).resolve()) if settings.peer_groups_path else None},
    }
    state = _load_checkpoint(checkpoint_path, expected) if resume else None
    accumulator = QualityAccumulator.from_snapshot(_expand_quality_snapshot(state["quality"])) if state else QualityAccumulator()
    if state is not None and accumulator.finalize()["total_records"] != state["processed_records"]:
        raise CheckpointError("invalid Kafka UEBA checkpoint: processed_records does not match quality state")
    next_offsets = _decode_offsets(state["next_offsets"]) if state else {}
    processed = int(state["processed_records"]) if state else 0
    max_event_time = state.get("max_event_time_ms") if state else None
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
            _save_checkpoint(checkpoint_path, expected, accumulator, sinks, processed, next_offsets, max_event_time)

        pending: deque[KafkaMessage] = deque()
        dirty = False
        downstream: dict[str, Any] | None = None
        with FeatureEngine(states["feature_state"], mode="realtime") as features:
            def records():
                while True:
                    message = client.poll(float(config.get("poll_timeout_seconds", 1.0)))
                    if message is None:
                        if stop_when_idle:
                            return
                        continue
                    pending.append(message)
                    yield raw_record_from_kafka_message(config, message)

            for outcome in iter_parse_records(records(), registry_path):
                if not pending:
                    raise RuntimeError("parser runtime did not preserve Kafka message order")
                message = pending.popleft()
                _write_outcome(outcome, normalized_sink, unknown_sink, raw_sink, accumulator)
                if outcome.normalized is not None:
                    features.process_event(outcome.normalized)
                    max_event_time = _max_time(max_event_time, [_event_time_ms(outcome.normalized)])
                next_offsets[(message.topic, message.partition)] = message.offset + 1
                processed += 1
                dirty = True
                if processed % checkpoint_every == 0:
                    downstream = _checkpoint_ueba(
                        checkpoint_path, expected, accumulator, sinks, processed, next_offsets, max_event_time,
                        features, states, settings,
                    )
                    if next_offsets:
                        client.commit(next_offsets)
                    dirty = False
            if pending:
                raise RuntimeError("parser runtime did not produce an outcome for every Kafka message")
            if dirty:
                downstream = _checkpoint_ueba(
                    checkpoint_path, expected, accumulator, sinks, processed, next_offsets, max_event_time,
                    features, states, settings,
                )
                if next_offsets:
                    client.commit(next_offsets)
            elif state is not None and next_offsets:
                # A prior process may have crashed after the durable local checkpoint
                # and before broker commit. It is safe to finish that commit now.
                client.commit(next_offsets)
        summary = accumulator.finalize()
        _atomic_write_json(summary_output, summary)
        return {
            **summary,
            "processed_records": processed,
            "next_offsets": _encode_offsets(next_offsets),
            "watermark": _iso(max_event_time - settings.watermark_lag_seconds * 1000) if max_event_time is not None else None,
            "downstream": downstream,
        }
    finally:
        for sink in sinks:
            sink.close()
        client.close()


def _checkpoint_ueba(
    checkpoint_path: Path | None,
    expected: dict[str, Any],
    accumulator: QualityAccumulator,
    sinks: list[JsonlSink],
    processed: int,
    next_offsets: dict[tuple[str, int], int],
    max_event_time: int | None,
    features: FeatureEngine,
    states: dict[str, Path],
    config: OrchestratorConfig,
) -> dict[str, Any]:
    if max_event_time is not None:
        features.advance_watermark(max_event_time - config.watermark_lag_seconds * 1000)
    # All Feature transactions, session state and downstream DBs must succeed before
    # the durable local commit point that authorizes a Kafka offset commit.
    downstream = _run_downstream(states, max_event_time - config.watermark_lag_seconds * 1000 if max_event_time is not None else None, config.peer_groups_path)
    if checkpoint_path is not None:
        _save_checkpoint(checkpoint_path, expected, accumulator, sinks, processed, next_offsets, max_event_time)
    else:
        for sink in sinks:
            sink.commit()
    return downstream


def _write_outcome(outcome: ParseOutcome, normalized: JsonlSink, unknown: JsonlSink, raw: JsonlSink | None, accumulator: QualityAccumulator) -> None:
    if raw is not None:
        raw.write(raw_record_to_dict(outcome.record))
    if outcome.normalized is not None:
        normalized.write(outcome.normalized)
        accumulator.add_normalized(outcome.normalized)
    if outcome.unknown is not None:
        unknown.write(outcome.unknown)
        accumulator.add_unknown(outcome.unknown)


def _save_checkpoint(
    path: Path,
    expected: dict[str, Any],
    accumulator: QualityAccumulator,
    sinks: list[JsonlSink],
    processed: int,
    next_offsets: dict[tuple[str, int], int],
    max_event_time: int | None,
) -> None:
    for sink in sinks:
        sink.commit()
    offsets = {"normalized": sinks[0].position(), "unknown": sinks[1].position()}
    if len(sinks) == 3:
        offsets["raw"] = sinks[2].position()
    _atomic_write_json(path, {
        "version": KAFKA_UEBA_CHECKPOINT_VERSION,
        **expected,
        "processed_records": processed,
        "next_offsets": _encode_offsets(next_offsets),
        "output_offsets": offsets,
        "quality": _compact_quality_snapshot(accumulator.snapshot()),
        "max_event_time_ms": max_event_time,
    }, pretty=False)


def _load_checkpoint(path: Path | None, expected: dict[str, Any]) -> dict[str, Any]:
    if path is None or not path.exists():
        raise CheckpointError("Kafka UEBA checkpoint does not exist")
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"invalid Kafka UEBA checkpoint: {exc}") from exc
    required = {"version", "config_fingerprint", "registry_fingerprint", "output_paths", "state_paths", "orchestrator_config", "processed_records", "next_offsets", "output_offsets", "quality", "max_event_time_ms"}
    if not isinstance(state, dict) or required.difference(state) or state["version"] != KAFKA_UEBA_CHECKPOINT_VERSION:
        raise CheckpointError("invalid Kafka UEBA checkpoint")
    for key in ("config_fingerprint", "registry_fingerprint", "output_paths", "state_paths", "orchestrator_config"):
        if state[key] != expected[key]:
            raise CheckpointError(f"Kafka UEBA checkpoint {key} changed")
    if not isinstance(state["processed_records"], int) or state["processed_records"] < 0:
        raise CheckpointError("invalid Kafka UEBA checkpoint processed_records")
    required_offsets = {"normalized", "unknown"}
    if expected["output_paths"]["raw"] is not None:
        required_offsets.add("raw")
    if not isinstance(state["output_offsets"], dict) or required_offsets.difference(state["output_offsets"]):
        raise CheckpointError("invalid Kafka UEBA checkpoint output offsets")
    if any(not isinstance(state["output_offsets"][name], int) or state["output_offsets"][name] < 0 for name in required_offsets):
        raise CheckpointError("invalid Kafka UEBA checkpoint output offsets")
    _decode_offsets(state["next_offsets"])
    if not isinstance(state["quality"], dict) or state["max_event_time_ms"] is not None and not isinstance(state["max_event_time_ms"], int):
        raise CheckpointError("invalid Kafka UEBA checkpoint state")
    return state


def _validate_state_paths(states: dict[str, Path], normalized: Path, unknown: Path, summary: Path, raw: Path | None, checkpoint: Path | None, registry: Path | None) -> None:
    resolved = [path.resolve() for path in states.values()]
    if len(set(resolved)) != len(resolved):
        raise CheckpointError("Kafka UEBA state paths must be distinct")
    forbidden = {normalized.resolve(), unknown.resolve(), summary.resolve()}
    if raw is not None:
        forbidden.add(raw.resolve())
    if checkpoint is not None:
        forbidden.add(checkpoint.resolve())
    if registry is not None:
        forbidden.add(registry.resolve())
    if any(path in forbidden for path in resolved):
        raise CheckpointError("Kafka UEBA state path conflicts with output, checkpoint, or registry")
