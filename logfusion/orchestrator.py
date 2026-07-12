from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from logfusion.baseline import BaselineEngine
from logfusion.correlation import CorrelationEngine
from logfusion.detection import DetectionEngine
from logfusion.features import FeatureEngine, FeatureError
from logfusion.fusion import FusionEngine


ORCHESTRATOR_CHECKPOINT_VERSION = 1


class OrchestratorError(ValueError):
    """Raised when a continuous UEBA run cannot be resumed safely."""


@dataclass(frozen=True)
class OrchestratorConfig:
    batch_size: int = 1_000
    watermark_lag_seconds: int = 5 * 60

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise OrchestratorError("batch_size must be greater than zero")
        if self.watermark_lag_seconds < 0:
            raise OrchestratorError("watermark_lag_seconds must not be negative")


def run_continuous_pipeline(
    normalized_input: Path | str,
    feature_state: Path | str,
    baseline_state: Path | str,
    detection_state: Path | str,
    incident_state: Path | str,
    risk_state: Path | str,
    *,
    checkpoint_path: Path | str | None = None,
    resume: bool = False,
    config: OrchestratorConfig | None = None,
    run_downstream: bool = True,
) -> dict[str, Any]:
    """Process appended Canonical-event JSONL and advance the complete UEBA chain.

    The input contract is append-only JSONL. A checkpoint commits only after Feature,
    Baseline, Detection, Correlation and Fusion have all completed. Replaying the last
    uncommitted input range is safe because FeatureEngine deduplicates by ``event.id``.
    """
    settings = config or OrchestratorConfig()
    input_path = Path(normalized_input)
    if not input_path.exists() or not input_path.is_file():
        raise OrchestratorError(f"normalized input does not exist: {input_path}")
    paths = _state_paths(feature_state, baseline_state, detection_state, incident_state, risk_state)
    checkpoint = Path(checkpoint_path) if checkpoint_path is not None else None
    if resume and checkpoint is None:
        raise OrchestratorError("resume requires a checkpoint path")
    state = _load_checkpoint(checkpoint, input_path, paths, settings) if resume else None
    start_offset = int(state["committed_offset"]) if state else 0
    max_event_time = state.get("max_event_time_ms") if state else None

    counts = {"read": 0, "processed": 0, "duplicates": 0, "rejected": 0}
    with FeatureEngine(paths["feature_state"], mode="realtime") as features:
        batch: list[dict[str, Any]] = []
        event_times: list[int | None] = []
        committed_offset = start_offset
        with input_path.open("rb") as handle:
            handle.seek(start_offset)
            while True:
                line_start = handle.tell()
                raw_line = handle.readline()
                if not raw_line:
                    break
                if not raw_line.endswith(b"\n"):
                    # Leave a writer's incomplete final record for its next append.
                    handle.seek(line_start)
                    break
                event = _decode_event(raw_line, line_start)
                batch.append(event)
                event_times.append(_event_time_ms(event))
                counts["read"] += 1
                if len(batch) >= settings.batch_size:
                    _apply_batch(features, batch, event_times, counts)
                    max_event_time = _max_time(max_event_time, event_times)
                    batch, event_times = [], []
                    committed_offset = handle.tell()
            if batch:
                _apply_batch(features, batch, event_times, counts)
                max_event_time = _max_time(max_event_time, event_times)
                committed_offset = handle.tell()
        watermark = None
        sessions_closed = 0
        if max_event_time is not None:
            watermark = max_event_time - settings.watermark_lag_seconds * 1000
            sessions_closed = features.advance_watermark(watermark)
        feature_revision = features.current_revision()

    downstream: dict[str, Any] | None = None
    if run_downstream:
        downstream = _run_downstream(paths, watermark)
    if checkpoint is not None:
        _save_checkpoint(checkpoint, input_path, paths, settings, committed_offset, max_event_time)
    return {
        "input": str(input_path.resolve()),
        "committed_offset": committed_offset,
        "feature_revision": feature_revision,
        "watermark": _iso(watermark) if watermark is not None else None,
        "sessions_closed": sessions_closed,
        "counts": counts,
        "downstream": downstream,
    }


def _apply_batch(
    engine: FeatureEngine, batch: list[dict[str, Any]], times: list[int | None], counts: dict[str, int]) -> None:
    for result in engine.process_events(batch, batch_size=len(batch)):
        if result.status == "processed":
            counts["processed"] += 1
        elif result.status == "duplicate":
            counts["duplicates"] += 1
        else:
            counts["rejected"] += 1


def _run_downstream(paths: dict[str, Path], watermark: int | None) -> dict[str, Any]:
    # Baseline keeps its own rolling cutoff. Supplying the persisted event-time
    # watermark makes repeated runs deterministic with respect to late data.
    as_of = _iso(watermark) if watermark is not None else None
    with BaselineEngine(paths["feature_state"], paths["baseline_state"]) as baseline:
        baseline_report = baseline.update(as_of=as_of)
    with DetectionEngine(paths["feature_state"], paths["baseline_state"], paths["detection_state"]) as detection:
        detection_report = detection.run()
    with CorrelationEngine(paths["feature_state"], paths["detection_state"], paths["incident_state"]) as correlation:
        correlation_report = correlation.run()
    with FusionEngine(paths["detection_state"], paths["incident_state"], paths["risk_state"]) as fusion:
        fusion_report = fusion.run()
    return {
        "baseline": baseline_report,
        "detection": detection_report,
        "correlation": correlation_report,
        "fusion": fusion_report,
    }


def _state_paths(feature: Path | str, baseline: Path | str, detection: Path | str, incident: Path | str, risk: Path | str) -> dict[str, Path]:
    values = {
        "feature_state": Path(feature), "baseline_state": Path(baseline), "detection_state": Path(detection),
        "incident_state": Path(incident), "risk_state": Path(risk),
    }
    resolved = {key: value.resolve() for key, value in values.items()}
    if len(set(resolved.values())) != len(resolved):
        raise OrchestratorError("Feature, Baseline, Detection, Incident and Risk state paths must be distinct")
    return values


def _decode_event(raw_line: bytes, offset: int) -> dict[str, Any]:
    try:
        value = json.loads(raw_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrchestratorError(f"invalid normalized JSONL at byte offset {offset}") from exc
    if not isinstance(value, dict):
        raise OrchestratorError(f"normalized JSONL record at byte offset {offset} must be an object")
    return value


def _event_time_ms(event: dict[str, Any]) -> int | None:
    value = event.get("event")
    if not isinstance(value, dict) or not isinstance(value.get("time"), str):
        return None
    try:
        parsed = datetime.fromisoformat(value["time"].replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _max_time(current: int | None, values: list[int | None]) -> int | None:
    candidates = [value for value in values if value is not None]
    if current is not None:
        candidates.append(current)
    return max(candidates) if candidates else None


def _load_checkpoint(checkpoint: Path | None, input_path: Path, paths: dict[str, Path], config: OrchestratorConfig) -> dict[str, Any]:
    if checkpoint is None or not checkpoint.exists():
        raise OrchestratorError("orchestrator checkpoint does not exist")
    try:
        state = json.loads(checkpoint.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OrchestratorError(f"invalid orchestrator checkpoint: {exc}") from exc
    expected_paths = {key: str(value.resolve()) for key, value in paths.items()}
    if not isinstance(state, dict) or state.get("version") != ORCHESTRATOR_CHECKPOINT_VERSION:
        raise OrchestratorError("unsupported orchestrator checkpoint")
    if state.get("input_path") != str(input_path.resolve()) or state.get("state_paths") != expected_paths:
        raise OrchestratorError("orchestrator checkpoint input or state paths changed")
    if state.get("config") != asdict(config):
        raise OrchestratorError("orchestrator checkpoint configuration changed")
    offset = state.get("committed_offset")
    if not isinstance(offset, int) or offset < 0 or offset > input_path.stat().st_size:
        raise OrchestratorError("invalid orchestrator checkpoint offset")
    identity = state.get("input_identity")
    if not isinstance(identity, dict) or identity != _file_identity(input_path):
        raise OrchestratorError("orchestrator input file identity changed")
    if state.get("committed_prefix_sha256") != _prefix_sha256(input_path, offset):
        raise OrchestratorError("orchestrator input before checkpoint changed")
    if state.get("max_event_time_ms") is not None and not isinstance(state["max_event_time_ms"], int):
        raise OrchestratorError("invalid orchestrator checkpoint watermark state")
    return state


def _save_checkpoint(checkpoint: Path, input_path: Path, paths: dict[str, Path], config: OrchestratorConfig, offset: int, max_event_time: int | None) -> None:
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "version": ORCHESTRATOR_CHECKPOINT_VERSION,
        "input_path": str(input_path.resolve()),
        "input_identity": _file_identity(input_path),
        "state_paths": {key: str(value.resolve()) for key, value in paths.items()},
        "config": asdict(config),
        "committed_offset": offset,
        "committed_prefix_sha256": _prefix_sha256(input_path, offset),
        "max_event_time_ms": max_event_time,
    }
    descriptor, temporary = tempfile.mkstemp(prefix=f".{checkpoint.name}.", dir=checkpoint.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(document, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, checkpoint)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _file_identity(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"device": int(stat.st_dev), "inode": int(stat.st_ino)}


def _prefix_sha256(path: Path, end: int) -> str:
    digest = hashlib.sha256()
    remaining = end
    with path.open("rb") as handle:
        while remaining:
            chunk = handle.read(min(1_048_576, remaining))
            if not chunk:
                raise OrchestratorError("input is shorter than orchestrator checkpoint")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
