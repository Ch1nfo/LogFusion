from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from logfusion.baseline import BASELINE_SCHEMA_VERSION, BaselineConfig
from logfusion.detection import FEATURE_SCHEMA_VERSION, WINDOW_SIZES
from logfusion.model_detection import ModelDetectionConfig, ModelDetectionError


DAY_MS = 86_400_000


class ReadinessError(ValueError):
    """Raised when readiness inputs are missing or incompatible."""


@dataclass(frozen=True)
class ReadinessTargets:
    history_days: int = 30


class ReadinessEngine:
    """Produces a read-only readiness report for unlabeled detector training."""

    def __init__(
        self,
        feature_state: Path | str,
        baseline_state: Path | str,
        model_config: ModelDetectionConfig | None = None,
        targets: ReadinessTargets | None = None,
    ) -> None:
        self.feature_path, self.baseline_path = map(Path, (feature_state, baseline_state))
        if not all(path.exists() for path in (self.feature_path, self.baseline_path)):
            raise ReadinessError("feature state and baseline state must exist")
        self.model_config = model_config or ModelDetectionConfig()
        self.targets = targets or ReadinessTargets()
        try:
            self.model_config.validate()
        except ModelDetectionError as exc:
            raise ReadinessError(str(exc)) from exc
        if self.targets.history_days <= 0:
            raise ReadinessError("readiness targets must be positive")
        self.feature = _readonly(self.feature_path)
        self.baseline = _readonly(self.baseline_path)
        try:
            self._verify_inputs()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        self.feature.close()
        self.baseline.close()

    def __enter__(self) -> "ReadinessEngine":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def report(self, as_of: str | int | datetime | None = None) -> dict[str, Any]:
        first_event, latest_event = self._event_bounds()
        if latest_event is None:
            raise ReadinessError("Feature DB contains no user activity")
        as_of_ms = _utc_boundary(as_of) if as_of is not None else (latest_event // DAY_MS + 1) * DAY_MS
        if first_event is not None and as_of_ms <= first_event:
            raise ReadinessError("readiness as_of must be after the first event")
        history_start = as_of_ms - self.targets.history_days * DAY_MS
        history = self._history(first_event, latest_event, as_of_ms)
        windows = {str(size): self._window_readiness(size, history_start, as_of_ms) for size in WINDOW_SIZES}
        peer_groups = self._peer_group_readiness(history_start, as_of_ms)
        statistical_training = history["ready"] and all(item["statistical"]["ready"] for item in windows.values())
        model_global = history["ready"] and all(item["model"]["ready"] for item in windows.values())
        statistical_peer = history["ready"] and any(all(group["windows"][str(size)]["statistical"]["ready"] for size in WINDOW_SIZES) for group in peer_groups)
        model_peer = history["ready"] and any(all(group["windows"][str(size)]["model"]["ready"] for size in WINDOW_SIZES) for group in peer_groups)
        detectors = {
            "statistical": _detector_status(statistical_training, statistical_peer),
            "hbos": _detector_status(model_global, model_peer),
            "isolation_forest": _detector_status(model_global, model_peer),
        }
        actions = self._next_actions(history, windows, peer_groups, detectors)
        return {
            "as_of": _iso(as_of_ms),
            "collection_window": {"from": _iso(history_start), "to": _iso(as_of_ms)},
            "history": history,
            "windows": windows,
            "peer_groups": peer_groups,
            "detectors": detectors,
            "next_actions": actions,
        }

    def _history(self, first_event: int | None, latest_event: int, as_of_ms: int) -> dict[str, Any]:
        first_day = first_event // DAY_MS * DAY_MS if first_event is not None else as_of_ms
        latest_day = latest_event // DAY_MS * DAY_MS
        span_days = max(0, min(as_of_ms, latest_day + DAY_MS) - first_day) // DAY_MS
        target_start = as_of_ms - self.targets.history_days * DAY_MS
        coverage_start = max(first_day, target_start)
        coverage_end = min(as_of_ms, latest_day + DAY_MS)
        observed_days = max(0, coverage_end - coverage_start) // DAY_MS
        active_days = int(self.feature.execute(
            "SELECT COUNT(DISTINCT window_start / ?) FROM user_windows WHERE window_size = 86400 AND event_count > 0 AND window_start >= ? AND window_start < ?",
            (DAY_MS, target_start, as_of_ms),
        ).fetchone()[0])
        return {
            "first_event": _iso(first_event) if first_event is not None else None,
            "latest_event": _iso(latest_event),
            "calendar_span_days": span_days,
            "active_utc_days": active_days,
            "target_days": self.targets.history_days,
            "observed_days": observed_days,
            "missing_days": max(0, self.targets.history_days - observed_days),
            "ready": observed_days >= self.targets.history_days,
        }

    def _window_readiness(self, window_size: int, start: int, end: int, users: list[str] | None = None) -> dict[str, Any]:
        clause = ""
        params: list[Any] = [window_size, start, end]
        if users is not None:
            if not users:
                return _empty_window_readiness(self.baseline_config, self.model_config)
            clause = f" AND user_name IN ({','.join('?' for _ in users)})"
            params.extend(users)
        row = self.feature.execute(
            f"SELECT COUNT(*) sample_count,COUNT(DISTINCT user_name) user_count,COUNT(DISTINCT window_start / {DAY_MS}) active_days FROM user_windows WHERE window_size = ? AND window_start >= ? AND window_start < ? AND event_count > 0{clause}",
            params,
        ).fetchone()
        samples, users_count, active_days = int(row["sample_count"]), int(row["user_count"]), int(row["active_days"])
        statistical_ready = active_days >= self.baseline_config.min_active_days and samples >= self.baseline_config.min_samples
        model_ready = samples >= self.model_config.min_training_samples
        return {
            "sample_count": samples,
            "active_user_count": users_count,
            "active_day_count": active_days,
            "statistical": {
                "ready": statistical_ready,
                "missing_samples": max(0, self.baseline_config.min_samples - samples),
                "missing_active_days": max(0, self.baseline_config.min_active_days - active_days),
            },
            "model": {
                "ready": model_ready,
                "missing_samples": max(0, self.model_config.min_training_samples - samples),
            },
        }

    def _peer_group_readiness(self, start: int, end: int) -> list[dict[str, Any]]:
        memberships: dict[str, list[str]] = {}
        for row in self.baseline.execute("SELECT user_name,group_id FROM peer_group_memberships ORDER BY group_id,user_name"):
            memberships.setdefault(row["group_id"], []).append(row["user_name"])
        result = []
        for group_id, members in memberships.items():
            windows = {str(size): self._window_readiness(size, start, end, members) for size in WINDOW_SIZES}
            active_members = max((item["active_user_count"] for item in windows.values()), default=0)
            for item in windows.values():
                statistical = item["statistical"]
                statistical["missing_members"] = max(0, self.baseline_config.min_peer_members - item["active_user_count"])
                statistical["ready"] = statistical["ready"] and item["active_user_count"] >= self.baseline_config.min_peer_members and item["sample_count"] >= self.baseline_config.min_peer_samples and item["active_day_count"] >= self.baseline_config.min_peer_active_days
                statistical["missing_samples"] = max(0, self.baseline_config.min_peer_samples - item["sample_count"])
                statistical["missing_active_days"] = max(0, self.baseline_config.min_peer_active_days - item["active_day_count"])
                model = item["model"]
                model["missing_members"] = max(0, self.model_config.min_peer_members - item["active_user_count"])
                model["ready"] = model["ready"] and item["active_user_count"] >= self.model_config.min_peer_members
            result.append({
                "group_id": group_id,
                "configured_member_count": len(members),
                "active_member_count": active_members,
                "windows": windows,
            })
        return result

    def _next_actions(
        self, history: dict[str, Any], windows: dict[str, Any], peer_groups: list[dict[str, Any]],
        detectors: dict[str, Any],
    ) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        if history["missing_days"]:
            actions.append({"priority": 1, "kind": "collect_history", "message": f"Collect {history['missing_days']} more UTC calendar days of Feature data."})
        for size, item in windows.items():
            if item["model"]["missing_samples"]:
                actions.append({"priority": 2, "kind": "collect_windows", "window_size": int(size), "message": f"Collect {item['model']['missing_samples']} more global {size}s windows for model training."})
        unassigned = [group for group in peer_groups if group["configured_member_count"] < self.model_config.min_peer_members]
        if unassigned:
            actions.append({"priority": 3, "kind": "expand_peer_groups", "groups": [group["group_id"] for group in unassigned], "message": "Add members or consolidate undersized peer groups."})
        if all(item["evaluation_ready"] for item in detectors.values()):
            actions.append({"priority": 4, "kind": "run_benchmark", "message": "Run statistical, HBOS, and Isolation Forest experiments over the same UTC range."})
        return sorted(actions, key=lambda item: (item["priority"], item["kind"], item.get("window_size", 0)))

    def _event_bounds(self) -> tuple[int | None, int | None]:
        row = self.feature.execute("SELECT MIN(first_event_time),MAX(last_event_time) FROM user_activity_bounds").fetchone()
        return (int(row[0]) if row and row[0] is not None else None, int(row[1]) if row and row[1] is not None else None)

    def _verify_inputs(self) -> None:
        feature_meta = _meta(self.feature, "feature_meta")
        baseline_meta = _meta(self.baseline, "baseline_meta")
        if feature_meta.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
            raise ReadinessError("feature schema version changed; rebuild Feature DB")
        if baseline_meta.get("baseline_schema_version") != BASELINE_SCHEMA_VERSION:
            raise ReadinessError("baseline schema version changed; rebuild Baseline DB")
        try:
            self.baseline_config = BaselineConfig(**json.loads(baseline_meta["baseline_config_json"]))
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ReadinessError("Baseline DB lacks a valid baseline_config_json") from exc


def _empty_window_readiness(baseline: BaselineConfig, model: ModelDetectionConfig) -> dict[str, Any]:
    return {
        "sample_count": 0, "active_user_count": 0, "active_day_count": 0,
        "statistical": {"ready": False, "missing_samples": baseline.min_samples, "missing_active_days": baseline.min_active_days},
        "model": {"ready": False, "missing_samples": model.min_training_samples},
    }


def _detector_status(global_ready: bool, peer_ready: bool) -> dict[str, Any]:
    training_status = "ready" if global_ready else ("partial" if peer_ready else "not_ready")
    return {
        "training_status": training_status,
        "training_ready": training_status == "ready",
        "evaluation_ready": training_status == "ready",
        "evaluation_mode": "unsupervised",
    }


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _meta(connection: sqlite3.Connection, table: str) -> dict[str, str]:
    return {row["key"]: row["value"] for row in connection.execute(f"SELECT key,value FROM {table}")}


def _utc_boundary(value: str | int | datetime) -> int:
    if isinstance(value, int):
        timestamp = value
    elif isinstance(value, datetime):
        timestamp = int((value if value.tzinfo else value.replace(tzinfo=timezone.utc)).timestamp() * 1000)
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ReadinessError("invalid readiness timestamp") from exc
        timestamp = int((parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp() * 1000)
    else:
        raise ReadinessError("invalid readiness timestamp")
    if timestamp % DAY_MS:
        raise ReadinessError("readiness as_of must align to a UTC natural-day boundary")
    return timestamp


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
