from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from logfusion.baseline import BASELINE_SCHEMA_VERSION, BaselineConfig, METRICS, _active_days, _personal_metric_values, _statistics
from logfusion.cases import CASE_SCHEMA_VERSION
from logfusion.detection import DetectionConfig, FEATURE_SCHEMA_VERSION, WINDOW_SIZES
from logfusion.model_detection import (
    MODEL_DETECTORS,
    ModelDetectionConfig,
    ModelDetectionError,
    fit_model_detector,
    model_versions,
)


EVALUATION_SCHEMA_VERSION = "3"
EVALUATION_DETECTORS = ("statistical", *MODEL_DETECTORS)


class EvaluationError(ValueError):
    """Raised when an evaluation experiment cannot be reproduced safely."""


class EvaluationEngine:
    """Point-in-time backtesting for statistical and model-based detectors."""

    def __init__(
        self,
        feature_state: Path | str,
        baseline_state: Path | str,
        case_state: Path | str,
        evaluation_state: Path | str,
        detection_config: DetectionConfig | None = None,
        detector_id: str = "statistical",
        model_config: ModelDetectionConfig | None = None,
    ) -> None:
        self.feature_path, self.baseline_path, self.case_path = map(Path, (feature_state, baseline_state, case_state))
        if not all(path.exists() for path in (self.feature_path, self.baseline_path, self.case_path)):
            raise EvaluationError("feature state, baseline state, and case state must exist")
        if detector_id not in EVALUATION_DETECTORS:
            raise EvaluationError(f"unsupported evaluation detector: {detector_id}")
        self.evaluation_path = Path(evaluation_state)
        self.evaluation_path.parent.mkdir(parents=True, exist_ok=True)
        self.detection_config = detection_config or DetectionConfig()
        self.detector_id = detector_id
        self.model_config = model_config or ModelDetectionConfig()
        try:
            self.model_config.validate()
            self.detector_versions = model_versions(detector_id)
        except ModelDetectionError as exc:
            raise EvaluationError(str(exc)) from exc
        self.feature = _readonly(self.feature_path)
        self.baseline = _readonly(self.baseline_path)
        self.cases = _readonly(self.case_path)
        self.connection = sqlite3.connect(self.evaluation_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        try:
            self._verify_inputs()
            self._create_schema()
            self._initialize_meta()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        self.feature.close()
        self.baseline.close()
        self.cases.close()
        self.connection.close()

    def __enter__(self) -> "EvaluationEngine":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def run(self, name: str, start: str | int | datetime, end: str | int | datetime) -> dict[str, Any]:
        if not name.strip():
            raise EvaluationError("experiment name is required")
        start_ms, end_ms = _utc_day(start), _utc_day(end)
        if end_ms <= start_ms:
            raise EvaluationError("evaluation end must be after start")
        experiment_id = self._experiment_id(name, start_ms, end_ms)
        if self.connection.execute("SELECT 1 FROM experiments WHERE experiment_id = ?", (experiment_id,)).fetchone():
            return self.query(experiment_id)

        predictions: list[dict[str, Any]] = []
        baseline_snapshots: list[tuple[Any, ...]] = []
        model_snapshots: list[tuple[Any, ...]] = []
        for day_start in range(start_ms, end_ms, 86_400_000):
            day_end = day_start + 86_400_000
            if self.detector_id == "statistical":
                snapshot, rows = self._build_statistical_snapshot(day_start)
                baseline_snapshots.extend((experiment_id, day_start, *row) for row in rows)
                predictions.extend(self._detect_statistical_day(day_start, day_end, snapshot))
            else:
                day_predictions, day_snapshots = self._detect_model_day(day_start, day_end)
                predictions.extend(day_predictions)
                model_snapshots.extend((experiment_id, *row) for row in day_snapshots)
        labels = self._labels(start_ms, end_ms)
        return self._persist(
            experiment_id, name, start_ms, end_ms, baseline_snapshots, model_snapshots, predictions, labels,
        )

    def query(self, experiment_id: str) -> dict[str, Any]:
        return _query_connection(self.connection, experiment_id)

    def compare(self, left_experiment_id: str, right_experiment_id: str) -> dict[str, Any]:
        return _compare_connection(self.connection, left_experiment_id, right_experiment_id)

    def _build_statistical_snapshot(self, as_of_ms: int) -> tuple[dict[str, Any], list[tuple[Any, ...]]]:
        config = self.baseline_config
        history_start = as_of_ms - config.history_days * 86_400_000
        users = [row[0] for row in self.feature.execute("SELECT user_name FROM user_activity_bounds ORDER BY user_name")]
        groups = self._groups()
        metric: dict[tuple[str, str, int, str], dict[str, Any]] = {}
        values: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        rows: list[tuple[Any, ...]] = []
        for user in users:
            for window_size in config.window_sizes_seconds:
                records = self.feature.execute(
                    "SELECT * FROM user_windows WHERE user_name = ? AND window_size = ? AND window_start >= ? AND window_start < ? ORDER BY window_start",
                    (user, window_size, history_start, as_of_ms),
                ).fetchall()
                status = "ready" if _active_days(records) >= config.min_active_days and len(records) >= config.min_samples else "warming_up"
                filled = _personal_metric_values(records, history_start, as_of_ms - 1, window_size)
                for name, numbers in filled.items():
                    payload = {**_statistics(numbers), "status": status}
                    metric[("personal", user, window_size, name)] = payload
                    rows.append(("personal", user, "metric", f"{window_size}:{name}", json.dumps(payload, sort_keys=True)))
            self._value_snapshot("personal", user, user, history_start, as_of_ms, values, rows)
        for window_size in config.window_sizes_seconds:
            records = self.feature.execute(
                "SELECT * FROM user_windows WHERE window_size = ? AND window_start >= ? AND window_start < ? ORDER BY window_start, user_name",
                (window_size, history_start, as_of_ms),
            ).fetchall()
            status = "ready" if _active_days(records) >= config.min_active_days and len(records) >= config.min_samples else "warming_up"
            for name in METRICS:
                payload = {**_statistics([float(record[name]) for record in records]), "status": status}
                metric[("global", "", window_size, name)] = payload
                rows.append(("global", "", "metric", f"{window_size}:{name}", json.dumps(payload, sort_keys=True)))
        self._value_snapshot("global", "", None, history_start, as_of_ms, values, rows)
        for group in sorted(set(groups.values())):
            members = [user for user, value in groups.items() if value == group]
            marks = ",".join("?" for _ in members)
            for window_size in config.window_sizes_seconds:
                records = self.feature.execute(
                    f"SELECT * FROM user_windows WHERE user_name IN ({marks}) AND window_size = ? AND window_start >= ? AND window_start < ? ORDER BY window_start, user_name",
                    (*members, window_size, history_start, as_of_ms),
                ).fetchall()
                member_count = len({record["user_name"] for record in records if record["event_count"] > 0})
                status = "ready" if member_count >= config.min_peer_members and _active_days(records) >= config.min_peer_active_days and len(records) >= config.min_peer_samples else "warming_up"
                for name in METRICS:
                    payload = {**_statistics([float(record[name]) for record in records]), "status": status}
                    metric[("peer_group", group, window_size, name)] = payload
                    rows.append(("peer_group", group, "metric", f"{window_size}:{name}", json.dumps(payload, sort_keys=True)))
            self._value_snapshot("peer_group", group, members, history_start, as_of_ms, values, rows)
        return {"metric": metric, "values": values, "groups": groups}, rows

    def _value_snapshot(
        self, scope: str, subject: str, users: str | list[str] | None, start: int, end: int,
        values: dict[tuple[str, str, str, str], dict[str, Any]], rows: list[tuple[Any, ...]],
    ) -> None:
        params: list[Any] = []
        clause = ""
        if isinstance(users, str):
            clause, params = "AND user_name = ?", [users]
        elif isinstance(users, list):
            clause, params = f"AND user_name IN ({','.join('?' for _ in users)})", users
        records = self.feature.execute(
            f"SELECT value_type,value_key,SUM(event_count) event_count,COUNT(*) window_count,MIN(window_start) first_seen FROM window_value_counts WHERE window_size = ? AND window_start >= ? AND window_start < ? {clause} GROUP BY value_type,value_key",
            (self.baseline_config.value_window_size_seconds, start, end, *params),
        ).fetchall()
        totals: dict[str, int] = {}
        for record in records:
            totals[record["value_type"]] = totals.get(record["value_type"], 0) + int(record["event_count"])
        for record in records:
            payload = {
                "event_count": int(record["event_count"]), "window_count": int(record["window_count"]),
                "first_seen": int(record["first_seen"]), "share": int(record["event_count"]) / totals[record["value_type"]],
            }
            values[(scope, subject, record["value_type"], record["value_key"])] = payload
            rows.append((scope, subject, "value", f"{record['value_type']}:{record['value_key']}", json.dumps(payload, sort_keys=True)))

    def _detect_statistical_day(self, start: int, end: int, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        windows = self._day_windows(start, end)
        candidates: list[dict[str, Any]] = []
        for window in windows:
            user, size = window["user_name"], int(window["window_size"])
            group = snapshot["groups"].get(user)
            scope, subject = self._statistical_scope(snapshot, user, group, size)
            if scope is None:
                continue
            policy = self._policy(window)
            for metric in METRICS:
                reference = snapshot["metric"].get((scope, subject, size, metric))
                if not reference or reference["p95"] is None:
                    continue
                detector = None
                score = None
                if float(window[metric]) > reference["p99"]:
                    detector, score = "numeric_p99", int(policy["p99_score"])
                elif float(window[metric]) > reference["p95"]:
                    detector, score = "numeric_p95", int(policy["p95_score"])
                if detector:
                    candidates.append(self._statistical_candidate(window, detector, score, scope, group, {"metric": metric, "reference": reference}))
            if size == self.baseline_config.value_window_size_seconds:
                for value in self.feature.execute(
                    "SELECT value_type,value_key FROM window_value_counts WHERE user_name = ? AND window_size = ? AND window_start = ?",
                    (user, size, window["window_start"]),
                ):
                    reference = snapshot["values"].get((scope, subject, value["value_type"], value["value_key"]))
                    if not reference:
                        continue
                    if reference["first_seen"] == window["window_start"]:
                        candidates.append(self._statistical_candidate(window, "new_value_30d", int(policy["new_value_score"]), scope, group, {"value_type": value["value_type"], "value_key": value["value_key"]}))
                    if reference["share"] <= float(policy["rare_share_threshold"]) and reference["event_count"] <= int(policy["rare_event_count_max"]):
                        candidates.append(self._statistical_candidate(window, "rare_value", int(policy["rare_value_score"]), scope, group, {"value_type": value["value_type"], "value_key": value["value_key"]}))
        return candidates

    def _detect_model_day(self, start: int, end: int) -> tuple[list[dict[str, Any]], list[tuple[Any, ...]]]:
        windows = self._day_windows(start, end)
        groups = self._groups()
        history_start = start - self.baseline_config.history_days * 86_400_000
        cache: dict[tuple[str, str, int], Any] = {}
        snapshots: dict[tuple[str, str, int], tuple[Any, ...]] = {}
        candidates: list[dict[str, Any]] = []
        for window in windows:
            size, user = int(window["window_size"]), window["user_name"]
            group = groups.get(user)
            scope, subject, training, member_count = self._model_training_rows(group, size, history_start, start)
            key = (scope, subject, size)
            fitted = cache.get(key)
            if key not in cache:
                try:
                    fitted = fit_model_detector(self.detector_id, training, self.model_config)
                    status, threshold = "ready", fitted.threshold
                except ModelDetectionError as exc:
                    if str(exc) != "model_not_ready":
                        raise EvaluationError(str(exc)) from exc
                    fitted, status, threshold = None, "model_not_ready", None
                cache[key] = fitted
                snapshots[key] = (
                    start, size, scope, subject, status, len(training), member_count, history_start, start,
                    threshold, self.model_config.threshold_quantile, json.dumps(asdict(self.model_config), sort_keys=True),
                    json.dumps(self.detector_versions, sort_keys=True),
                )
            if fitted is None:
                continue
            model_score = fitted.score(window)
            if model_score.raw_score < fitted.threshold:
                continue
            explanation = dict(model_score.explanation)
            explanation["training"] = {
                "scope": scope, "subject": subject, "sample_count": len(training), "member_count": member_count,
                "history_start": _iso(history_start), "history_end": _iso(start),
            }
            candidates.append({
                "user_name": user,
                "day_start": start,
                "window_start": int(window["window_start"]),
                "window_end": int(window["window_end"]),
                "detector_id": self.detector_id,
                "score": model_score.score,
                "severity": _severity(model_score.score),
                "baseline_scope": scope,
                "baseline_group_id": subject if scope == "peer_group" else None,
                "value_type": None,
                "value_key": None,
                "raw_score": model_score.raw_score,
                "score_percentile": model_score.percentile,
                "explanation": explanation,
            })
        return candidates, list(snapshots.values())

    def _model_training_rows(self, group: str | None, size: int, start: int, end: int) -> tuple[str, str, list[sqlite3.Row], int]:
        if group:
            members = [user for user, value in self._groups().items() if value == group]
            marks = ",".join("?" for _ in members)
            peer = self.feature.execute(
                f"SELECT * FROM user_windows WHERE user_name IN ({marks}) AND window_size = ? AND window_start >= ? AND window_start < ? ORDER BY window_start,user_name",
                (*members, size, start, end),
            ).fetchall()
            member_count = len({row["user_name"] for row in peer if row["event_count"] > 0})
            if member_count >= self.model_config.min_peer_members and len(peer) >= self.model_config.min_training_samples:
                return "peer_group", group, peer, member_count
        global_rows = self.feature.execute(
            "SELECT * FROM user_windows WHERE window_size = ? AND window_start >= ? AND window_start < ? ORDER BY window_start,user_name",
            (size, start, end),
        ).fetchall()
        member_count = len({row["user_name"] for row in global_rows if row["event_count"] > 0})
        return "global", "", global_rows, member_count

    def _day_windows(self, start: int, end: int) -> list[sqlite3.Row]:
        return self.feature.execute(
            "SELECT * FROM user_windows WHERE window_size IN (?,?) AND window_start >= ? AND window_start < ? ORDER BY user_name,window_size,window_start",
            (*WINDOW_SIZES, start, end),
        ).fetchall()

    def _groups(self) -> dict[str, str]:
        return {row["user_name"]: row["group_id"] for row in self.baseline.execute("SELECT user_name,group_id FROM peer_group_memberships")}

    def _statistical_scope(self, snapshot: dict[str, Any], user: str, group: str | None, size: int) -> tuple[str | None, str]:
        for scope, subject in (("personal", user), ("peer_group", group), ("global", "")):
            if subject is not None and snapshot["metric"].get((scope, subject, size, "event_count"), {}).get("status") == "ready":
                return scope, subject
        return None, ""

    def _policy(self, window: sqlite3.Row) -> dict[str, int | float]:
        result = {key: getattr(self.detection_config, key) for key in ("rare_share_threshold", "rare_event_count_max", "p95_score", "p99_score", "new_value_score", "rare_value_score")}
        sources = self.feature.execute(
            "SELECT value_key FROM window_value_counts WHERE user_name = ? AND window_size = ? AND window_start = ? AND value_type = 'source_type' ORDER BY value_key",
            (window["user_name"], window["window_size"], window["window_start"]),
        ).fetchall()
        for source in sources:
            if override := self.detection_config.source_type_overrides.get(source["value_key"]):
                result.update(override)
                break
        return result

    def _statistical_candidate(self, window: sqlite3.Row, detector: str, score: int, scope: str, group: str | None, explanation: dict[str, Any]) -> dict[str, Any]:
        return {
            "user_name": window["user_name"], "day_start": int(window["window_start"]) // 86_400_000 * 86_400_000,
            "window_start": int(window["window_start"]), "window_end": int(window["window_end"]),
            "detector_id": detector, "score": score, "severity": _severity(score), "baseline_scope": scope,
            "baseline_group_id": group if scope == "peer_group" else None,
            "value_type": explanation.get("value_type"), "value_key": explanation.get("value_key"),
            "raw_score": None, "score_percentile": None, "explanation": explanation,
        }

    def _labels(self, start: int, end: int) -> dict[tuple[str, int], str]:
        result: dict[tuple[str, int], str] = {}
        rows = self.cases.execute(
            "SELECT user_name,first_seen,last_seen,disposition FROM cases WHERE last_seen >= ? AND first_seen < ?",
            (start, end),
        ).fetchall()
        for row in rows:
            first_day = max(start, int(row["first_seen"]) // 86_400_000 * 86_400_000)
            last_day = min(end, int(row["last_seen"]) // 86_400_000 * 86_400_000 + 86_400_000)
            for day in range(first_day, last_day, 86_400_000):
                key, current = (row["user_name"], day), result.get((row["user_name"], day))
                if row["disposition"] == "confirmed_threat" or (row["disposition"] == "benign" and current != "confirmed_threat"):
                    result[key] = row["disposition"]
        return result

    def _persist(
        self, experiment_id: str, name: str, start: int, end: int,
        baseline_snapshots: list[tuple[Any, ...]], model_snapshots: list[tuple[Any, ...]],
        candidates: list[dict[str, Any]], labels: dict[tuple[str, int], str],
    ) -> dict[str, Any]:
        by_day: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for candidate in candidates:
            by_day.setdefault((candidate["user_name"], candidate["day_start"]), []).append(candidate)
        keys = sorted(set(by_day) | set(labels))
        tp = fp = fn = unlabeled = 0
        day_count = (end - start) // 86_400_000
        config_json = self._detector_config_json()
        config_fingerprint = self._detector_config_fingerprint()
        with self._transaction():
            self.connection.execute(
                "INSERT INTO experiments(experiment_id,name,detector_id,detector_config_json,detector_config_fingerprint,dependency_versions_json,start_ms,end_ms,input_fingerprint,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (experiment_id, name, self.detector_id, config_json, config_fingerprint, json.dumps(self.detector_versions, sort_keys=True), start, end, self._input_fingerprint(), _now_ms()),
            )
            self.connection.executemany(
                "INSERT INTO daily_baseline_snapshots(experiment_id,day_start,scope,subject,kind,item_key,payload_json) VALUES (?,?,?,?,?,?,?)",
                baseline_snapshots,
            )
            self.connection.executemany(
                "INSERT INTO daily_model_snapshots(experiment_id,day_start,window_size,scope,subject,status,sample_count,member_count,history_start,history_end,threshold,threshold_quantile,config_json,dependency_versions_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                model_snapshots,
            )
            for candidate in candidates:
                fields = ("user_name", "day_start", "window_start", "window_end", "detector_id", "score", "severity", "baseline_scope", "baseline_group_id", "value_type", "value_key", "raw_score", "score_percentile")
                self.connection.execute(
                    "INSERT INTO candidate_predictions(experiment_id,user_name,day_start,window_start,window_end,detector_id,score,severity,baseline_scope,baseline_group_id,value_type,value_key,raw_score,score_percentile,explanation_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (experiment_id, *[candidate[field] for field in fields], json.dumps(candidate["explanation"], sort_keys=True)),
                )
            for user, day in keys:
                predicted, label = by_day.get((user, day), []), labels.get((user, day))
                max_score = max((item["score"] for item in predicted), default=None)
                self.connection.execute(
                    "INSERT INTO user_day_predictions(experiment_id,user_name,day_start,max_score,candidate_count,detectors_json,severities_json,baseline_scopes_json) VALUES (?,?,?,?,?,?,?,?)",
                    (experiment_id, user, day, max_score, len(predicted), json.dumps(sorted({item["detector_id"] for item in predicted})), json.dumps(sorted({item["severity"] for item in predicted})), json.dumps(sorted({item["baseline_scope"] for item in predicted}))),
                )
                if label:
                    self.connection.execute("INSERT INTO user_day_labels(experiment_id,user_name,day_start,disposition) VALUES (?,?,?,?)", (experiment_id, user, day, label))
                if label == "confirmed_threat":
                    if predicted:
                        tp += 1
                    else:
                        fn += 1
                elif label == "benign" and predicted:
                    fp += 1
                elif predicted:
                    unlabeled += 1
            precision = tp / (tp + fp) if tp + fp else 0.0
            recall = tp / (tp + fn) if tp + fn else 0.0
            f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
            predicted_user_days = len(by_day)
            metrics = (tp, fp, fn, precision, recall, f1, unlabeled, len(candidates), predicted_user_days, predicted_user_days / day_count)
            self.connection.execute(
                "INSERT INTO experiment_metrics(experiment_id,true_positive,false_positive,false_negative,precision,recall,f1,unlabeled_predictions,candidate_count,predicted_user_days,average_daily_predictions) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (experiment_id, *metrics),
            )
        return self.query(experiment_id)

    def _verify_inputs(self) -> None:
        feature, baseline, case = _meta(self.feature, "feature_meta"), _meta(self.baseline, "baseline_meta"), _meta(self.cases, "case_meta")
        if feature.get("feature_schema_version") != FEATURE_SCHEMA_VERSION or baseline.get("baseline_schema_version") != BASELINE_SCHEMA_VERSION or case.get("case_schema_version") != CASE_SCHEMA_VERSION:
            raise EvaluationError("input state schema changed; rebuild the affected state")
        config_json = baseline.get("baseline_config_json")
        if not config_json:
            raise EvaluationError("Baseline DB lacks baseline_config_json; rebuild it before evaluation")
        try:
            self.baseline_config = BaselineConfig(**json.loads(config_json))
        except (TypeError, json.JSONDecodeError) as exc:
            raise EvaluationError("Baseline DB has invalid baseline_config_json") from exc
        self.feature_meta, self.baseline_meta = feature, baseline

    def _create_schema(self) -> None:
        existing = _existing_evaluation_version(self.connection)
        if existing is not None and existing != EVALUATION_SCHEMA_VERSION:
            raise EvaluationError("evaluation schema version changed; rebuild Evaluation DB")
        self.connection.executescript("""
        CREATE TABLE IF NOT EXISTS evaluation_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS experiments (experiment_id TEXT PRIMARY KEY,name TEXT NOT NULL,detector_id TEXT NOT NULL,detector_config_json TEXT NOT NULL,detector_config_fingerprint TEXT NOT NULL,dependency_versions_json TEXT NOT NULL,start_ms INTEGER NOT NULL,end_ms INTEGER NOT NULL,input_fingerprint TEXT NOT NULL,created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS daily_baseline_snapshots (experiment_id TEXT NOT NULL,day_start INTEGER NOT NULL,scope TEXT NOT NULL,subject TEXT NOT NULL,kind TEXT NOT NULL,item_key TEXT NOT NULL,payload_json TEXT NOT NULL,PRIMARY KEY(experiment_id,day_start,scope,subject,kind,item_key));
        CREATE TABLE IF NOT EXISTS daily_model_snapshots (experiment_id TEXT NOT NULL,day_start INTEGER NOT NULL,window_size INTEGER NOT NULL,scope TEXT NOT NULL,subject TEXT NOT NULL,status TEXT NOT NULL,sample_count INTEGER NOT NULL,member_count INTEGER NOT NULL,history_start INTEGER NOT NULL,history_end INTEGER NOT NULL,threshold REAL,threshold_quantile REAL NOT NULL,config_json TEXT NOT NULL,dependency_versions_json TEXT NOT NULL,PRIMARY KEY(experiment_id,day_start,window_size,scope,subject));
        CREATE TABLE IF NOT EXISTS candidate_predictions (experiment_id TEXT NOT NULL,user_name TEXT NOT NULL,day_start INTEGER NOT NULL,window_start INTEGER NOT NULL,window_end INTEGER NOT NULL,detector_id TEXT NOT NULL,score INTEGER NOT NULL,severity TEXT NOT NULL,baseline_scope TEXT NOT NULL,baseline_group_id TEXT,value_type TEXT,value_key TEXT,raw_score REAL,score_percentile REAL,explanation_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS user_day_predictions (experiment_id TEXT NOT NULL,user_name TEXT NOT NULL,day_start INTEGER NOT NULL,max_score INTEGER,candidate_count INTEGER NOT NULL,detectors_json TEXT NOT NULL,severities_json TEXT NOT NULL,baseline_scopes_json TEXT NOT NULL,PRIMARY KEY(experiment_id,user_name,day_start));
        CREATE TABLE IF NOT EXISTS user_day_labels (experiment_id TEXT NOT NULL,user_name TEXT NOT NULL,day_start INTEGER NOT NULL,disposition TEXT NOT NULL,PRIMARY KEY(experiment_id,user_name,day_start));
        CREATE TABLE IF NOT EXISTS experiment_metrics (experiment_id TEXT PRIMARY KEY,true_positive INTEGER NOT NULL,false_positive INTEGER NOT NULL,false_negative INTEGER NOT NULL,precision REAL NOT NULL,recall REAL NOT NULL,f1 REAL NOT NULL,unlabeled_predictions INTEGER NOT NULL,candidate_count INTEGER NOT NULL,predicted_user_days INTEGER NOT NULL,average_daily_predictions REAL NOT NULL);
        """)
        self.connection.commit()

    def _initialize_meta(self) -> None:
        if _existing_evaluation_version(self.connection) is None:
            self.connection.execute("INSERT INTO evaluation_meta(key,value) VALUES ('evaluation_schema_version',?)", (EVALUATION_SCHEMA_VERSION,))
            self.connection.commit()

    def _detector_config_json(self) -> str:
        config = asdict(self.detection_config) if self.detector_id == "statistical" else asdict(self.model_config)
        return json.dumps(config, sort_keys=True, separators=(",", ":"))

    def _detector_config_fingerprint(self) -> str:
        if self.detector_id == "statistical":
            return self.detection_config.fingerprint()
        return self.model_config.fingerprint(self.detector_id, self.detector_versions)

    def _input_fingerprint(self) -> str:
        payload = {
            "feature": self.feature_meta.get("config_fingerprint"), "feature_revision": self.feature_meta.get("current_revision"),
            "baseline": self.baseline_meta.get("baseline_config_fingerprint"), "peer_groups": self.baseline_meta.get("peer_groups_fingerprint"),
            "detector": self.detector_id, "detector_config": self._detector_config_fingerprint(), "cases": _case_fingerprint(self.cases),
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def _experiment_id(self, name: str, start: int, end: int) -> str:
        payload = {"name": name, "start": start, "end": end, "input": self._input_fingerprint()}
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()


def query_evaluation_state(evaluation_state: Path | str, experiment_id: str) -> dict[str, Any]:
    connection = _open_evaluation(evaluation_state)
    try:
        return _query_connection(connection, experiment_id)
    finally:
        connection.close()


def compare_evaluation_state(evaluation_state: Path | str, left: str, right: str) -> dict[str, Any]:
    connection = _open_evaluation(evaluation_state)
    try:
        return _compare_connection(connection, left, right)
    finally:
        connection.close()


def _open_evaluation(path: Path | str) -> sqlite3.Connection:
    state = Path(path)
    if not state.exists():
        raise EvaluationError(f"evaluation state does not exist: {state}")
    connection = _readonly(state)
    if _existing_evaluation_version(connection) != EVALUATION_SCHEMA_VERSION:
        connection.close()
        raise EvaluationError("evaluation schema version changed; rebuild Evaluation DB")
    return connection


def _query_connection(connection: sqlite3.Connection, experiment_id: str) -> dict[str, Any]:
    row = connection.execute("SELECT * FROM experiments WHERE experiment_id = ?", (experiment_id,)).fetchone()
    if row is None:
        raise EvaluationError(f"experiment does not exist: {experiment_id}")
    metrics = connection.execute("SELECT * FROM experiment_metrics WHERE experiment_id = ?", (experiment_id,)).fetchone()
    result = dict(row)
    result["from"] = _iso(result.pop("start_ms"))
    result["to"] = _iso(result.pop("end_ms"))
    result["detector_config"] = json.loads(result.pop("detector_config_json"))
    result["dependency_versions"] = json.loads(result.pop("dependency_versions_json"))
    result["metrics"] = {key: value for key, value in dict(metrics).items() if key != "experiment_id"} if metrics else {}
    return result


def _compare_connection(connection: sqlite3.Connection, left_id: str, right_id: str) -> dict[str, Any]:
    left, right = _query_connection(connection, left_id), _query_connection(connection, right_id)
    fields = (
        "true_positive", "false_positive", "false_negative", "precision", "recall", "f1",
        "unlabeled_predictions", "candidate_count", "predicted_user_days", "average_daily_predictions",
    )
    return {"left": left, "right": right, "delta": {field: right["metrics"].get(field, 0) - left["metrics"].get(field, 0) for field in fields}}


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _meta(connection: sqlite3.Connection, table: str) -> dict[str, str]:
    return {row["key"]: row["value"] for row in connection.execute(f"SELECT key,value FROM {table}")}


def _existing_evaluation_version(connection: sqlite3.Connection) -> str | None:
    table = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='evaluation_meta'").fetchone()
    if table is None:
        return None
    row = connection.execute("SELECT value FROM evaluation_meta WHERE key='evaluation_schema_version'").fetchone()
    return row[0] if row else None


def _case_fingerprint(connection: sqlite3.Connection) -> str:
    rows = [tuple(row) for row in connection.execute("SELECT case_id,user_name,first_seen,last_seen,disposition,updated_at FROM cases ORDER BY case_id")]
    return hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()


def _utc_day(value: str | int | datetime) -> int:
    if isinstance(value, int):
        timestamp = value
    elif isinstance(value, datetime):
        timestamp = int((value if value.tzinfo else value.replace(tzinfo=timezone.utc)).timestamp() * 1000)
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise EvaluationError("invalid timestamp") from exc
        timestamp = int((parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp() * 1000)
    else:
        raise EvaluationError("invalid timestamp")
    if timestamp % 86_400_000:
        raise EvaluationError("evaluation range must align to UTC natural-day boundaries")
    return timestamp


def _severity(score: int) -> str:
    return "high" if score >= 80 else ("medium" if score >= 60 else "low")


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
