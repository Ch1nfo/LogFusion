from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from logfusion.baseline import BASELINE_SCHEMA_VERSION, BaselineConfig, METRICS, _active_days, _personal_metric_values, _statistics
from logfusion.cases import CASE_SCHEMA_VERSION
from logfusion.detection import DetectionConfig, FEATURE_SCHEMA_VERSION, WINDOW_SIZES


EVALUATION_SCHEMA_VERSION = "1"


class EvaluationError(ValueError):
    """Raised when an evaluation experiment cannot be reproduced safely."""


class EvaluationEngine:
    """Point-in-time, read-only backtesting for the statistical detector."""

    def __init__(
        self, feature_state: Path | str, baseline_state: Path | str, case_state: Path | str,
        evaluation_state: Path | str, detection_config: DetectionConfig | None = None,
    ) -> None:
        self.feature_path, self.baseline_path, self.case_path = map(Path, (feature_state, baseline_state, case_state))
        if not all(path.exists() for path in (self.feature_path, self.baseline_path, self.case_path)):
            raise EvaluationError("feature state, baseline state, and case state must exist")
        self.evaluation_path = Path(evaluation_state)
        self.evaluation_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = detection_config or DetectionConfig()
        self.feature = _readonly(self.feature_path)
        self.baseline = _readonly(self.baseline_path)
        self.cases = _readonly(self.case_path)
        self.connection = sqlite3.connect(self.evaluation_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self._verify_inputs()
        self._create_schema()
        self._initialize_meta()

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
        existing = self.connection.execute("SELECT experiment_id FROM experiments WHERE experiment_id = ?", (experiment_id,)).fetchone()
        if existing:
            return self.query(experiment_id)

        predictions: list[dict[str, Any]] = []
        snapshots: list[tuple[Any, ...]] = []
        for day_start in range(start_ms, end_ms, 86_400_000):
            day_end = day_start + 86_400_000
            snapshot, snapshot_rows = self._build_snapshot(day_start)
            snapshots.extend((experiment_id, day_start, *row) for row in snapshot_rows)
            predictions.extend(self._detect_day(day_start, day_end, snapshot))
        labels = self._labels(start_ms, end_ms)
        report = self._persist(experiment_id, name, start_ms, end_ms, snapshots, predictions, labels)
        return report

    def query(self, experiment_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM experiments WHERE experiment_id = ?", (experiment_id,)).fetchone()
        if row is None:
            raise EvaluationError(f"experiment does not exist: {experiment_id}")
        metrics = self.connection.execute("SELECT * FROM experiment_metrics WHERE experiment_id = ?", (experiment_id,)).fetchone()
        result = dict(row)
        result["from"] = _iso(result.pop("start_ms"))
        result["to"] = _iso(result.pop("end_ms"))
        result["metrics"] = dict(metrics) if metrics else {}
        return result

    def compare(self, left_experiment_id: str, right_experiment_id: str) -> dict[str, Any]:
        left, right = self.query(left_experiment_id), self.query(right_experiment_id)
        fields = ("true_positive", "false_positive", "false_negative", "precision", "recall", "f1", "unlabeled_predictions")
        return {"left": left, "right": right, "delta": {field: right["metrics"].get(field, 0) - left["metrics"].get(field, 0) for field in fields}}

    def _build_snapshot(self, as_of_ms: int) -> tuple[dict[str, Any], list[tuple[Any, ...]]]:
        """Create a baseline strictly from windows preceding the evaluated day."""
        config = self.baseline_config
        history_start = as_of_ms - config.history_days * 86_400_000
        users = [r[0] for r in self.feature.execute("SELECT user_name FROM user_activity_bounds ORDER BY user_name")]
        groups = {r["user_name"]: r["group_id"] for r in self.baseline.execute("SELECT user_name, group_id FROM peer_group_memberships")}
        metric: dict[tuple[str, str, int, str], dict[str, Any]] = {}
        values: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        rows: list[tuple[Any, ...]] = []
        for user in users:
            for window_size in config.window_sizes_seconds:
                records = self.feature.execute("SELECT * FROM user_windows WHERE user_name = ? AND window_size = ? AND window_start >= ? AND window_start < ? ORDER BY window_start", (user, window_size, history_start, as_of_ms)).fetchall()
                active_days = _active_days(records)
                status = "ready" if active_days >= config.min_active_days and len(records) >= config.min_samples else "warming_up"
                filled = _personal_metric_values(records, history_start, as_of_ms - 1, window_size)
                for name, numbers in filled.items():
                    payload = {**_statistics(numbers), "status": status}
                    metric[("personal", user, window_size, name)] = payload
                    rows.append(("personal", user, "metric", f"{window_size}:{name}", json.dumps(payload, sort_keys=True)))
            self._value_snapshot("personal", user, user, history_start, as_of_ms, config, values, rows)
        for window_size in config.window_sizes_seconds:
            records = self.feature.execute("SELECT * FROM user_windows WHERE window_size = ? AND window_start >= ? AND window_start < ? ORDER BY window_start, user_name", (window_size, history_start, as_of_ms)).fetchall()
            status = "ready" if _active_days(records) >= config.min_active_days and len(records) >= config.min_samples else "warming_up"
            for name in METRICS:
                payload = {**_statistics([float(record[name]) for record in records]), "status": status}
                metric[("global", "", window_size, name)] = payload
                rows.append(("global", "", "metric", f"{window_size}:{name}", json.dumps(payload, sort_keys=True)))
        self._value_snapshot("global", "", None, history_start, as_of_ms, config, values, rows)
        for group in sorted(set(groups.values())):
            members = [user for user, value in groups.items() if value == group]
            if not members:
                continue
            marks = ",".join("?" for _ in members)
            for window_size in config.window_sizes_seconds:
                records = self.feature.execute(f"SELECT * FROM user_windows WHERE user_name IN ({marks}) AND window_size = ? AND window_start >= ? AND window_start < ? ORDER BY window_start, user_name", (*members, window_size, history_start, as_of_ms)).fetchall()
                member_count = len({record["user_name"] for record in records if record["event_count"] > 0})
                status = "ready" if member_count >= config.min_peer_members and _active_days(records) >= config.min_peer_active_days and len(records) >= config.min_peer_samples else "warming_up"
                for name in METRICS:
                    payload = {**_statistics([float(record[name]) for record in records]), "status": status}
                    metric[("peer_group", group, window_size, name)] = payload
                    rows.append(("peer_group", group, "metric", f"{window_size}:{name}", json.dumps(payload, sort_keys=True)))
            self._value_snapshot("peer_group", group, members, history_start, as_of_ms, config, values, rows)
        return {"metric": metric, "values": values, "groups": groups}, rows

    def _value_snapshot(self, scope: str, subject: str, users: str | list[str] | None, start: int, end: int, config: BaselineConfig, values: dict[tuple[str, str, str, str], dict[str, Any]], rows: list[tuple[Any, ...]]) -> None:
        params: list[Any] = []
        clause = ""
        if isinstance(users, str):
            clause, params = "AND user_name = ?", [users]
        elif isinstance(users, list):
            clause, params = f"AND user_name IN ({','.join('?' for _ in users)})", users
        records = self.feature.execute(f"SELECT value_type, value_key, SUM(event_count) AS event_count, COUNT(*) AS window_count, MIN(window_start) AS first_seen FROM window_value_counts WHERE window_size = ? AND window_start >= ? AND window_start < ? {clause} GROUP BY value_type, value_key", (config.value_window_size_seconds, start, end, *params)).fetchall()
        totals: dict[str, int] = {}
        for record in records:
            totals[record["value_type"]] = totals.get(record["value_type"], 0) + int(record["event_count"])
        for record in records:
            payload = {"event_count": int(record["event_count"]), "window_count": int(record["window_count"]), "first_seen": int(record["first_seen"]), "share": int(record["event_count"]) / totals[record["value_type"]]}
            values[(scope, subject, record["value_type"], record["value_key"])] = payload
            rows.append((scope, subject, "value", f"{record['value_type']}:{record['value_key']}", json.dumps(payload, sort_keys=True)))

    def _detect_day(self, start: int, end: int, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        windows = self.feature.execute("SELECT * FROM user_windows WHERE window_size IN (?, ?) AND window_start >= ? AND window_start < ? ORDER BY user_name, window_size, window_start", (*WINDOW_SIZES, start, end)).fetchall()
        candidates: list[dict[str, Any]] = []
        for window in windows:
            user, size = window["user_name"], int(window["window_size"])
            group = snapshot["groups"].get(user)
            scope, subject = self._scope(snapshot, user, group, size)
            if scope is None:
                continue
            policy = self._policy(window)
            for metric in METRICS:
                reference = snapshot["metric"].get((scope, subject, size, metric))
                if not reference or reference["p95"] is None:
                    continue
                score = int(policy["p99_score"]) if float(window[metric]) > reference["p99"] else (int(policy["p95_score"]) if float(window[metric]) > reference["p95"] else None)
                if score is not None:
                    candidates.append(self._candidate(window, "numeric_p99" if score == int(policy["p99_score"]) else "numeric_p95", score, scope, group))
            if size == self.baseline_config.value_window_size_seconds:
                for value in self.feature.execute("SELECT value_type, value_key FROM window_value_counts WHERE user_name = ? AND window_size = ? AND window_start = ?", (user, size, window["window_start"])):
                    reference = snapshot["values"].get((scope, subject, value["value_type"], value["value_key"]))
                    if not reference:
                        continue
                    if reference["first_seen"] == window["window_start"]:
                        candidates.append(self._candidate(window, "new_value_30d", int(policy["new_value_score"]), scope, group, value))
                    if reference["share"] <= float(policy["rare_share_threshold"]) and reference["event_count"] <= int(policy["rare_event_count_max"]):
                        candidates.append(self._candidate(window, "rare_value", int(policy["rare_value_score"]), scope, group, value))
        return candidates

    def _scope(self, snapshot: dict[str, Any], user: str, group: str | None, size: int) -> tuple[str | None, str]:
        for scope, subject in (("personal", user), ("peer_group", group), ("global", "")):
            if subject is not None and snapshot["metric"].get((scope, subject, size, "event_count"), {}).get("status") == "ready":
                return scope, subject
        return None, ""

    def _policy(self, window: sqlite3.Row) -> dict[str, int | float]:
        result = {key: getattr(self.config, key) for key in ("rare_share_threshold", "rare_event_count_max", "p95_score", "p99_score", "new_value_score", "rare_value_score")}
        sources = self.feature.execute("SELECT value_key FROM window_value_counts WHERE user_name = ? AND window_size = ? AND window_start = ? AND value_type = 'source_type' ORDER BY value_key", (window["user_name"], window["window_size"], window["window_start"])).fetchall()
        for source in sources:
            if override := self.config.source_type_overrides.get(source["value_key"]):
                result.update(override)
                break
        return result

    def _candidate(self, window: sqlite3.Row, detector: str, score: int, scope: str, group: str | None, value: sqlite3.Row | None = None) -> dict[str, Any]:
        return {"user_name": window["user_name"], "day_start": int(window["window_start"]) // 86_400_000 * 86_400_000, "window_start": int(window["window_start"]), "window_end": int(window["window_end"]), "detector_id": detector, "score": score, "severity": _severity(score), "baseline_scope": scope, "baseline_group_id": group if scope == "peer_group" else None, "value_type": value["value_type"] if value else None, "value_key": value["value_key"] if value else None}

    def _labels(self, start: int, end: int) -> dict[tuple[str, int], str]:
        result: dict[tuple[str, int], str] = {}
        cases = self.cases.execute("SELECT user_name, first_seen, last_seen, disposition FROM cases WHERE last_seen >= ? AND first_seen < ?", (start, end)).fetchall()
        for row in cases:
            for day in range(max(start, int(row["first_seen"]) // 86_400_000 * 86_400_000), min(end, int(row["last_seen"]) // 86_400_000 * 86_400_000 + 86_400_000), 86_400_000):
                key, current = (row["user_name"], day), result.get((row["user_name"], day))
                if row["disposition"] == "confirmed_threat" or (row["disposition"] == "benign" and current != "confirmed_threat"):
                    result[key] = row["disposition"]
        return result

    def _persist(self, experiment_id: str, name: str, start: int, end: int, snapshots: list[tuple[Any, ...]], candidates: list[dict[str, Any]], labels: dict[tuple[str, int], str]) -> dict[str, Any]:
        by_day: dict[tuple[str, int], list[dict[str, Any]]] = {}
        for candidate in candidates:
            by_day.setdefault((candidate["user_name"], candidate["day_start"]), []).append(candidate)
        keys = sorted(set(by_day) | set(labels))
        tp = fp = fn = unlabeled = 0
        with self._transaction():
            self.connection.execute("INSERT INTO experiments(experiment_id,name,start_ms,end_ms,input_fingerprint,created_at) VALUES (?,?,?,?,?,?)", (experiment_id, name, start, end, self._input_fingerprint(), _now_ms()))
            self.connection.executemany("INSERT INTO daily_baseline_snapshots(experiment_id,day_start,scope,subject,kind,item_key,payload_json) VALUES (?,?,?,?,?,?,?)", snapshots)
            for candidate in candidates:
                self.connection.execute("INSERT INTO candidate_predictions(experiment_id,user_name,day_start,window_start,window_end,detector_id,score,severity,baseline_scope,baseline_group_id,value_type,value_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (experiment_id, *[candidate[key] for key in ("user_name", "day_start", "window_start", "window_end", "detector_id", "score", "severity", "baseline_scope", "baseline_group_id", "value_type", "value_key")]))
            for user, day in keys:
                predicted, label = by_day.get((user, day), []), labels.get((user, day))
                max_score = max((item["score"] for item in predicted), default=None)
                self.connection.execute("INSERT INTO user_day_predictions(experiment_id,user_name,day_start,max_score,candidate_count,detectors_json,severities_json,baseline_scopes_json) VALUES (?,?,?,?,?,?,?,?)", (experiment_id, user, day, max_score, len(predicted), json.dumps(sorted({item["detector_id"] for item in predicted})), json.dumps(sorted({item["severity"] for item in predicted})), json.dumps(sorted({item["baseline_scope"] for item in predicted}))))
                if label:
                    self.connection.execute("INSERT INTO user_day_labels(experiment_id,user_name,day_start,disposition) VALUES (?,?,?,?)", (experiment_id, user, day, label))
                if label == "confirmed_threat":
                    if predicted: tp += 1
                    else: fn += 1
                elif label == "benign" and predicted:
                    fp += 1
                elif predicted:
                    unlabeled += 1
            precision = tp / (tp + fp) if tp + fp else 0.0
            recall = tp / (tp + fn) if tp + fn else 0.0
            f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
            metrics = {"true_positive": tp, "false_positive": fp, "false_negative": fn, "precision": precision, "recall": recall, "f1": f1, "unlabeled_predictions": unlabeled}
            self.connection.execute("INSERT INTO experiment_metrics(experiment_id,true_positive,false_positive,false_negative,precision,recall,f1,unlabeled_predictions) VALUES (?,?,?,?,?,?,?,?)", (experiment_id, *metrics.values()))
        return self.query(experiment_id)

    def _verify_inputs(self) -> None:
        feature = _meta(self.feature, "feature_meta")
        baseline = _meta(self.baseline, "baseline_meta")
        case = _meta(self.cases, "case_meta")
        if feature.get("feature_schema_version") != FEATURE_SCHEMA_VERSION or baseline.get("baseline_schema_version") != BASELINE_SCHEMA_VERSION or case.get("case_schema_version") != CASE_SCHEMA_VERSION:
            raise EvaluationError("input state schema changed; rebuild the affected state")
        config_json = baseline.get("baseline_config_json")
        if not config_json:
            raise EvaluationError("Baseline DB lacks baseline_config_json; rebuild it before evaluation")
        try:
            self.baseline_config = BaselineConfig(**json.loads(config_json))
        except (TypeError, json.JSONDecodeError) as exc:
            raise EvaluationError("Baseline DB has invalid baseline_config_json") from exc
        self.feature_meta, self.baseline_meta, self.case_meta = feature, baseline, case

    def _create_schema(self) -> None:
        self.connection.executescript("""
        CREATE TABLE IF NOT EXISTS evaluation_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS experiments (experiment_id TEXT PRIMARY KEY, name TEXT NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL, input_fingerprint TEXT NOT NULL, created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS daily_baseline_snapshots (experiment_id TEXT NOT NULL, day_start INTEGER NOT NULL, scope TEXT NOT NULL, subject TEXT NOT NULL, kind TEXT NOT NULL, item_key TEXT NOT NULL, payload_json TEXT NOT NULL, PRIMARY KEY(experiment_id,day_start,scope,subject,kind,item_key));
        CREATE TABLE IF NOT EXISTS candidate_predictions (experiment_id TEXT NOT NULL, user_name TEXT NOT NULL, day_start INTEGER NOT NULL, window_start INTEGER NOT NULL, window_end INTEGER NOT NULL, detector_id TEXT NOT NULL, score INTEGER NOT NULL, severity TEXT NOT NULL, baseline_scope TEXT NOT NULL, baseline_group_id TEXT, value_type TEXT, value_key TEXT);
        CREATE TABLE IF NOT EXISTS user_day_predictions (experiment_id TEXT NOT NULL, user_name TEXT NOT NULL, day_start INTEGER NOT NULL, max_score INTEGER, candidate_count INTEGER NOT NULL, detectors_json TEXT NOT NULL, severities_json TEXT NOT NULL, baseline_scopes_json TEXT NOT NULL, PRIMARY KEY(experiment_id,user_name,day_start));
        CREATE TABLE IF NOT EXISTS user_day_labels (experiment_id TEXT NOT NULL, user_name TEXT NOT NULL, day_start INTEGER NOT NULL, disposition TEXT NOT NULL, PRIMARY KEY(experiment_id,user_name,day_start));
        CREATE TABLE IF NOT EXISTS experiment_metrics (experiment_id TEXT PRIMARY KEY, true_positive INTEGER NOT NULL, false_positive INTEGER NOT NULL, false_negative INTEGER NOT NULL, precision REAL NOT NULL, recall REAL NOT NULL, f1 REAL NOT NULL, unlabeled_predictions INTEGER NOT NULL);
        """)
        self.connection.commit()

    def _initialize_meta(self) -> None:
        current = self.connection.execute("SELECT value FROM evaluation_meta WHERE key = 'evaluation_schema_version'").fetchone()
        if current is None:
            self.connection.execute("INSERT INTO evaluation_meta(key,value) VALUES ('evaluation_schema_version',?)", (EVALUATION_SCHEMA_VERSION,))
            self.connection.commit()
        elif current[0] != EVALUATION_SCHEMA_VERSION:
            raise EvaluationError("evaluation schema version changed; rebuild Evaluation DB")

    def _input_fingerprint(self) -> str:
        payload = {"feature": self.feature_meta.get("config_fingerprint"), "feature_revision": self.feature_meta.get("current_revision"), "baseline": self.baseline_meta.get("baseline_config_fingerprint"), "peer_groups": self.baseline_meta.get("peer_groups_fingerprint"), "detection": self.config.fingerprint(), "cases": _case_fingerprint(self.cases)}
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def _experiment_id(self, name: str, start: int, end: int) -> str:
        return hashlib.sha256(json.dumps({"name": name, "start": start, "end": end, "input": self._input_fingerprint()}, sort_keys=True).encode()).hexdigest()

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


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _meta(connection: sqlite3.Connection, table: str) -> dict[str, str]:
    return {row["key"]: row["value"] for row in connection.execute(f"SELECT key, value FROM {table}")}


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
