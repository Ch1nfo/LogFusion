from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


DETECTION_SCHEMA_VERSION = "1"
FEATURE_SCHEMA_VERSION = "1"
BASELINE_SCHEMA_VERSION = "1"
WINDOW_SIZES = (3600, 86400)
METRICS = (
    "event_count", "success_count", "failure_count", "other_outcome_count",
    "source_type_distinct_count", "source_ip_distinct_count", "resource_distinct_count",
    "action_distinct_count", "read_action_count", "write_action_count", "network_bytes_total",
    "first_seen_ip_count", "first_seen_resource_count", "first_seen_action_count",
)


class DetectionError(ValueError):
    """Raised when detection inputs are incompatible or stale."""


@dataclass(frozen=True)
class DetectionConfig:
    window_sizes_seconds: tuple[int, ...] = WINDOW_SIZES
    rare_share_threshold: float = 0.01
    rare_event_count_max: int = 3

    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class DetectionEngine:
    """Revision-aware statistical anomaly candidate generator."""

    def __init__(
        self,
        feature_state: Path | str,
        baseline_state: Path | str,
        detection_state: Path | str,
        config: DetectionConfig | None = None,
    ) -> None:
        self.feature_path = Path(feature_state)
        self.baseline_path = Path(baseline_state)
        if not self.feature_path.exists() or not self.baseline_path.exists():
            raise DetectionError("feature state and baseline state must exist")
        self.detection_path = Path(detection_state)
        self.detection_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or DetectionConfig()
        if not 0 < self.config.rare_share_threshold <= 1 or self.config.rare_event_count_max <= 0:
            raise DetectionError("invalid detection configuration")
        self.feature = _readonly(self.feature_path)
        self.baseline = _readonly(self.baseline_path)
        self.connection = sqlite3.connect(self.detection_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self._verify_inputs()
        self._create_schema()
        self._initialize_meta()

    def close(self) -> None:
        self.feature.close()
        self.baseline.close()
        self.connection.close()

    def __enter__(self) -> DetectionEngine:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def run(self) -> dict[str, Any]:
        feature_revision = self._feature_revision()
        baseline_revision = self._baseline_revision()
        if baseline_revision != feature_revision:
            raise DetectionError("Baseline DB is behind Feature DB; run baseline build first")
        previous_revision = self._meta_int("last_consumed_feature_revision", 0)
        rows = self.feature.execute(
            """
            SELECT * FROM user_windows
            WHERE updated_revision > ? AND window_size IN (?, ?)
            ORDER BY updated_revision, user_name, window_size, window_start
            """,
            (previous_revision, *self.config.window_sizes_seconds),
        ).fetchall()
        detector_count: dict[str, int] = {}
        candidate_count = 0
        superseded_count = 0
        skipped: dict[str, int] = {}
        with self._transaction():
            for window in rows:
                superseded_count += self._supersede_window(window)
                candidates, reasons = self._detect_window(window, baseline_revision)
                for reason in reasons:
                    skipped[reason] = skipped.get(reason, 0) + 1
                for candidate in candidates:
                    if self._insert_candidate(candidate):
                        candidate_count += 1
                        detector_count[candidate["detector_id"]] = detector_count.get(candidate["detector_id"], 0) + 1
            self._set_meta("last_consumed_feature_revision", str(feature_revision))
            self._set_meta("last_consumed_baseline_revision", str(baseline_revision))
        return {
            "processed_feature_revision": feature_revision,
            "processed_baseline_revision": baseline_revision,
            "scanned_windows": len(rows),
            "candidate_count": candidate_count,
            "superseded_count": superseded_count,
            "by_detector": detector_count,
            "skipped": skipped,
        }

    def query(self, user_name: str, start: str | int | datetime, end: str | int | datetime) -> list[dict[str, Any]]:
        start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
        if end_ms <= start_ms:
            raise DetectionError("query end must be after start")
        rows = self.connection.execute(
            """
            SELECT * FROM anomaly_candidates
            WHERE user_name = ? AND status = 'active'
              AND window_start < ? AND window_end > ?
            ORDER BY score DESC, window_start DESC, candidate_id
            """,
            (user_name, end_ms, start_ms),
        ).fetchall()
        return [_serialize_candidate(row) for row in rows]

    def _detect_window(self, window: sqlite3.Row, baseline_revision: int) -> tuple[list[dict[str, Any]], list[str]]:
        candidates: list[dict[str, Any]] = []
        skipped: list[str] = []
        user_name = window["user_name"]
        window_size = int(window["window_size"])
        user_ready = self._baseline_status("user_metric_baselines", user_name, window_size)
        global_ready = self._baseline_status("global_metric_baselines", None, window_size)
        if user_ready:
            scope = "personal"
        elif global_ready:
            scope = "global"
        else:
            scope = None
            skipped.append("baseline_not_ready")
        if scope is not None:
            for metric in METRICS:
                reference = self._metric_reference(scope, user_name, window_size, metric)
                if reference is None or reference["p95"] is None or reference["p99"] is None:
                    continue
                value = float(window[metric])
                if value > float(reference["p99"]):
                    candidates.append(self._numeric_candidate(window, metric, value, reference, scope, "numeric_p99", 80, baseline_revision))
                elif value > float(reference["p95"]):
                    candidates.append(self._numeric_candidate(window, metric, value, reference, scope, "numeric_p95", 60, baseline_revision))
        candidates.extend(self._value_candidates(window, user_ready, global_ready, baseline_revision))
        return candidates, skipped

    def _value_candidates(
        self, window: sqlite3.Row, user_ready: bool, global_ready: bool, baseline_revision: int,
    ) -> list[dict[str, Any]]:
        if not user_ready and not global_ready:
            return []
        scope = "personal" if user_ready else "global"
        values = self.feature.execute(
            """
            SELECT value_type, value_key, display_value, event_count
            FROM window_value_counts
            WHERE user_name = ? AND window_size = ? AND window_start = ?
            ORDER BY value_type, value_key
            """,
            (window["user_name"], window["window_size"], window["window_start"]),
        ).fetchall()
        candidates: list[dict[str, Any]] = []
        for value in values:
            reference = self._value_reference(scope, window["user_name"], value["value_type"], value["value_key"])
            if reference is None:
                continue
            first_seen = int(reference["first_seen"])
            if first_seen == int(window["window_start"]):
                candidates.append(self._value_candidate(window, value, reference, scope, "new_value_30d", 75, baseline_revision))
            if float(reference["share"]) <= self.config.rare_share_threshold and int(reference["event_count"]) <= self.config.rare_event_count_max:
                candidates.append(self._value_candidate(window, value, reference, scope, "rare_value", 55, baseline_revision))
        return candidates

    def _numeric_candidate(
        self, window: sqlite3.Row, metric: str, value: float, reference: sqlite3.Row, scope: str,
        detector_id: str, score: int, baseline_revision: int,
    ) -> dict[str, Any]:
        explanation = {
            "kind": "numeric_deviation",
            "metric": metric,
            "current_value": value,
            "reference": {
                "scope": scope, "mean": reference["mean"], "mad": reference["mad"],
                "p95": reference["p95"], "p99": reference["p99"], "sample_count": reference["sample_count"],
            },
            "trigger": detector_id,
        }
        return self._candidate_base(window, detector_id, metric, None, None, score, scope, explanation, baseline_revision)

    def _value_candidate(
        self, window: sqlite3.Row, value: sqlite3.Row, reference: sqlite3.Row, scope: str,
        detector_id: str, score: int, baseline_revision: int,
    ) -> dict[str, Any]:
        explanation = {
            "kind": "value_rarity",
            "value_type": value["value_type"], "value_key": value["value_key"],
            "display_value": value["display_value"], "window_event_count": value["event_count"],
            "reference": {
                "scope": scope, "event_count": reference["event_count"], "window_count": reference["window_count"],
                "share": reference["share"], "first_seen": _iso(int(reference["first_seen"])),
                "last_seen": _iso(int(reference["last_seen"])),
            },
            "trigger": detector_id,
        }
        return self._candidate_base(
            window, detector_id, None, value["value_type"], value["value_key"], score, scope, explanation, baseline_revision,
        )

    def _candidate_base(
        self, window: sqlite3.Row, detector_id: str, metric: str | None, value_type: str | None,
        value_key: str | None, score: int, scope: str, explanation: dict[str, Any], baseline_revision: int,
    ) -> dict[str, Any]:
        identity = ":".join(str(value) for value in (
            window["user_name"], window["window_size"], window["window_start"], detector_id, metric or "", value_type or "", value_key or "",
        ))
        candidate_key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        revision = int(window["updated_revision"])
        candidate_id = hashlib.sha256(f"{candidate_key}:{revision}".encode("utf-8")).hexdigest()
        return {
            "candidate_id": candidate_id, "candidate_key": candidate_key, "user_name": window["user_name"],
            "window_size": int(window["window_size"]), "window_start": int(window["window_start"]), "window_end": int(window["window_end"]),
            "detector_id": detector_id, "metric": metric, "value_type": value_type, "value_key": value_key,
            "score": score, "severity": _severity(score), "baseline_scope": scope,
            "feature_revision": revision, "baseline_revision": baseline_revision, "explanation": explanation,
        }

    def _insert_candidate(self, candidate: dict[str, Any]) -> bool:
        previous = self.connection.execute(
            "SELECT candidate_id FROM anomaly_candidates WHERE candidate_key = ? ORDER BY feature_revision DESC LIMIT 1",
            (candidate["candidate_key"],),
        ).fetchone()
        try:
            self.connection.execute(
                """
                INSERT INTO anomaly_candidates(
                    candidate_id, candidate_key, user_name, window_size, window_start, window_end,
                    detector_id, metric, value_type, value_key, score, severity, baseline_scope,
                    feature_revision, baseline_revision, explanation_json, status, supersedes_candidate_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
                """,
                (
                    candidate["candidate_id"], candidate["candidate_key"], candidate["user_name"], candidate["window_size"],
                    candidate["window_start"], candidate["window_end"], candidate["detector_id"], candidate["metric"],
                    candidate["value_type"], candidate["value_key"], candidate["score"], candidate["severity"],
                    candidate["baseline_scope"], candidate["feature_revision"], candidate["baseline_revision"],
                    json.dumps(candidate["explanation"], ensure_ascii=False, sort_keys=True), previous["candidate_id"] if previous else None,
                    _now_ms(), _now_ms(),
                ),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def _supersede_window(self, window: sqlite3.Row) -> int:
        cursor = self.connection.execute(
            """
            UPDATE anomaly_candidates SET status = 'superseded', updated_at = ?
            WHERE user_name = ? AND window_size = ? AND window_start = ? AND status = 'active'
              AND feature_revision < ?
            """,
            (_now_ms(), window["user_name"], window["window_size"], window["window_start"], window["updated_revision"]),
        )
        return cursor.rowcount

    def _baseline_status(self, table: str, user_name: str | None, window_size: int) -> bool:
        if table == "user_metric_baselines":
            row = self.baseline.execute(
                "SELECT status FROM user_metric_baselines WHERE user_name = ? AND window_size = ? AND metric = 'event_count'",
                (user_name, window_size),
            ).fetchone()
        else:
            row = self.baseline.execute(
                "SELECT status FROM global_metric_baselines WHERE window_size = ? AND metric = 'event_count'",
                (window_size,),
            ).fetchone()
        return row is not None and row["status"] == "ready"

    def _metric_reference(self, scope: str, user_name: str, window_size: int, metric: str) -> sqlite3.Row | None:
        if scope == "personal":
            return self.baseline.execute(
                "SELECT * FROM user_metric_baselines WHERE user_name = ? AND window_size = ? AND metric = ?",
                (user_name, window_size, metric),
            ).fetchone()
        return self.baseline.execute(
            "SELECT * FROM global_metric_baselines WHERE window_size = ? AND metric = ?", (window_size, metric)
        ).fetchone()

    def _value_reference(self, scope: str, user_name: str, value_type: str, value_key: str) -> sqlite3.Row | None:
        if scope == "personal":
            return self.baseline.execute(
                "SELECT * FROM user_value_baselines WHERE user_name = ? AND value_type = ? AND value_key = ?",
                (user_name, value_type, value_key),
            ).fetchone()
        return self.baseline.execute(
            "SELECT * FROM global_value_baselines WHERE value_type = ? AND value_key = ?", (value_type, value_key)
        ).fetchone()

    def _verify_inputs(self) -> None:
        feature_meta = _meta_map(self.feature, "feature_meta")
        baseline_meta = _meta_map(self.baseline, "baseline_meta")
        if feature_meta.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
            raise DetectionError("feature schema version changed; rebuild Detection DB")
        if baseline_meta.get("baseline_schema_version") != BASELINE_SCHEMA_VERSION:
            raise DetectionError("baseline schema version changed; rebuild Detection DB")
        if baseline_meta.get("feature_config_fingerprint") != feature_meta.get("config_fingerprint"):
            raise DetectionError("baseline does not match feature configuration; rebuild Baseline DB")
        self.feature_fingerprint = feature_meta["config_fingerprint"]
        self.baseline_fingerprint = baseline_meta["baseline_config_fingerprint"]

    def _feature_revision(self) -> int:
        return int(_meta_map(self.feature, "feature_meta").get("current_revision", "0"))

    def _baseline_revision(self) -> int:
        return int(_meta_map(self.baseline, "baseline_meta").get("last_consumed_revision", "0"))

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS detection_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS anomaly_candidates (
                candidate_id TEXT PRIMARY KEY, candidate_key TEXT NOT NULL, user_name TEXT NOT NULL,
                window_size INTEGER NOT NULL, window_start INTEGER NOT NULL, window_end INTEGER NOT NULL,
                detector_id TEXT NOT NULL, metric TEXT, value_type TEXT, value_key TEXT,
                score INTEGER NOT NULL, severity TEXT NOT NULL, baseline_scope TEXT NOT NULL,
                feature_revision INTEGER NOT NULL, baseline_revision INTEGER NOT NULL,
                explanation_json TEXT NOT NULL, status TEXT NOT NULL, supersedes_candidate_id TEXT,
                created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                UNIQUE(candidate_key, feature_revision)
            );
            CREATE INDEX IF NOT EXISTS idx_candidates_user_time ON anomaly_candidates(user_name, window_start, status);
            CREATE INDEX IF NOT EXISTS idx_candidates_key ON anomaly_candidates(candidate_key, feature_revision);
            """
        )
        self.connection.commit()

    def _initialize_meta(self) -> None:
        fingerprint = self.config.fingerprint()
        existing = self._meta("detection_schema_version")
        if existing is None:
            values = {
                "detection_schema_version": DETECTION_SCHEMA_VERSION,
                "feature_config_fingerprint": self.feature_fingerprint,
                "baseline_config_fingerprint": self.baseline_fingerprint,
                "detection_config_fingerprint": fingerprint,
                "last_consumed_feature_revision": "0",
                "last_consumed_baseline_revision": "0",
            }
            self.connection.executemany("INSERT INTO detection_meta(key, value) VALUES (?, ?)", values.items())
            self.connection.commit()
            return
        if existing != DETECTION_SCHEMA_VERSION:
            raise DetectionError("detection schema version changed; rebuild Detection DB")
        if self._meta("feature_config_fingerprint") != self.feature_fingerprint:
            raise DetectionError("feature configuration changed; rebuild Detection DB")
        if self._meta("baseline_config_fingerprint") != self.baseline_fingerprint:
            raise DetectionError("baseline configuration changed; rebuild Detection DB")
        if self._meta("detection_config_fingerprint") != fingerprint:
            raise DetectionError("detection configuration changed; rebuild Detection DB")

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

    def _meta(self, key: str) -> str | None:
        row = self.connection.execute("SELECT value FROM detection_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def _meta_int(self, key: str, default: int = 0) -> int:
        value = self._meta(key)
        return int(value) if value is not None else default

    def _set_meta(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO detection_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def query_detection_state(
    detection_state: Path | str, user_name: str, start: str | int | datetime, end: str | int | datetime,
) -> dict[str, Any]:
    path = Path(detection_state)
    if not path.exists():
        raise DetectionError(f"detection state does not exist: {path}")
    connection = _readonly(path)
    try:
        meta = _meta_map(connection, "detection_meta")
        if meta.get("detection_schema_version") != DETECTION_SCHEMA_VERSION:
            raise DetectionError("detection schema version changed; rebuild Detection DB")
        start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
        rows = connection.execute(
            """
            SELECT * FROM anomaly_candidates WHERE user_name = ? AND status = 'active'
              AND window_start < ? AND window_end > ?
            ORDER BY score DESC, window_start DESC, candidate_id
            """,
            (user_name, end_ms, start_ms),
        ).fetchall()
        return {
            "user_name": user_name,
            "from": _iso(start_ms), "to": _iso(end_ms),
            "processed_feature_revision": int(meta.get("last_consumed_feature_revision", "0")),
            "candidates": [_serialize_candidate(row) for row in rows],
        }
    finally:
        connection.close()


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _meta_map(connection: sqlite3.Connection, table: str) -> dict[str, str]:
    return {row["key"]: row["value"] for row in connection.execute(f"SELECT key, value FROM {table}")}


def _severity(score: int) -> str:
    if score >= 80:
        return "high"
    if score >= 60:
        return "medium"
    return "low"


def _timestamp_ms(value: str | int | datetime) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DetectionError("invalid timestamp") from exc
    else:
        raise DetectionError("invalid timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _serialize_candidate(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    for field in ("window_start", "window_end", "created_at", "updated_at"):
        result[field] = _iso(int(result[field]))
    result["explanation"] = json.loads(result.pop("explanation_json"))
    return result
