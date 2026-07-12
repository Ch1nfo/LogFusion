from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


CORRELATION_SCHEMA_VERSION = "1"
FEATURE_SCHEMA_VERSION = "1"
DETECTION_SCHEMA_VERSION = "1"


class CorrelationError(ValueError):
    """Raised when correlation inputs are stale or incompatible."""


@dataclass(frozen=True)
class CorrelationConfig:
    fallback_bucket_seconds: int = 3600
    session_window_size_seconds: int = 3600
    candidate_bonus: int = 5
    candidate_bonus_cap: int = 20

    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class CorrelationEngine:
    """Groups active anomaly candidates into versioned user incidents."""

    def __init__(
        self,
        feature_state: Path | str,
        detection_state: Path | str,
        incident_state: Path | str,
        config: CorrelationConfig | None = None,
    ) -> None:
        self.feature_path = Path(feature_state)
        self.detection_path = Path(detection_state)
        if not self.feature_path.exists() or not self.detection_path.exists():
            raise CorrelationError("feature state and detection state must exist")
        self.incident_path = Path(incident_state)
        self.incident_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or CorrelationConfig()
        if self.config.fallback_bucket_seconds <= 0 or self.config.candidate_bonus < 0:
            raise CorrelationError("invalid correlation configuration")
        self.feature = _readonly(self.feature_path)
        self.detection = _readonly(self.detection_path)
        self.connection = sqlite3.connect(self.incident_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self._verify_inputs()
        self._create_schema()
        self._initialize_meta()

    def close(self) -> None:
        self.feature.close()
        self.detection.close()
        self.connection.close()

    def __enter__(self) -> CorrelationEngine:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def run(self) -> dict[str, Any]:
        feature_revision = int(_meta_map(self.feature, "feature_meta").get("current_revision", "0"))
        detection_revision = int(_meta_map(self.detection, "detection_meta").get("last_consumed_feature_revision", "0"))
        if detection_revision != feature_revision:
            raise CorrelationError("Detection DB is behind Feature DB; run detect run first")
        previous_revision = self._meta_int("last_consumed_detection_revision", 0)
        if previous_revision == detection_revision:
            return {
                "processed_detection_revision": detection_revision,
                "candidate_count": 0,
                "incident_count": 0,
                "superseded_count": 0,
                "by_type": {},
            }
        candidates = self.detection.execute(
            "SELECT * FROM anomaly_candidates WHERE status = 'active' ORDER BY user_name, window_start, score DESC, candidate_id"
        ).fetchall()
        groups: dict[tuple[str, str], list[sqlite3.Row]] = {}
        group_sessions: dict[tuple[str, str], sqlite3.Row | None] = {}
        for candidate in candidates:
            group_key, session = self._group_key(candidate)
            identity = (candidate["user_name"], group_key)
            groups.setdefault(identity, []).append(candidate)
            group_sessions[identity] = session

        by_type: dict[str, int] = {}
        with self._transaction():
            previous = {
                row["incident_key"]: row["incident_id"]
                for row in self.connection.execute("SELECT incident_key, incident_id FROM incidents WHERE status = 'active'")
            }
            cursor = self.connection.execute(
                "UPDATE incidents SET status = 'superseded', updated_at = ? WHERE status = 'active'", (_now_ms(),)
            )
            superseded_count = cursor.rowcount
            for (user_name, group_key), group in groups.items():
                incident = self._build_incident(
                    user_name, group_key, group, group_sessions[(user_name, group_key)], detection_revision, previous,
                )
                self._insert_incident(incident, group)
                by_type[incident["incident_type"]] = by_type.get(incident["incident_type"], 0) + 1
            self._set_meta("last_consumed_detection_revision", str(detection_revision))
        return {
            "processed_detection_revision": detection_revision,
            "candidate_count": len(candidates),
            "incident_count": len(groups),
            "superseded_count": superseded_count,
            "by_type": by_type,
        }

    def query(self, user_name: str, start: str | int | datetime, end: str | int | datetime) -> list[dict[str, Any]]:
        start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
        if end_ms <= start_ms:
            raise CorrelationError("query end must be after start")
        rows = self.connection.execute(
            """
            SELECT * FROM incidents WHERE user_name = ? AND status = 'active'
              AND first_seen < ? AND last_seen > ?
            ORDER BY aggregation_score DESC, first_seen DESC, incident_id
            """,
            (user_name, end_ms, start_ms),
        ).fetchall()
        return [self._serialize_incident(row) for row in rows]

    def _group_key(self, candidate: sqlite3.Row) -> tuple[str, sqlite3.Row | None]:
        if int(candidate["window_size"]) == self.config.session_window_size_seconds:
            sessions = self.feature.execute(
                """
                SELECT * FROM user_sessions
                WHERE user_name = ? AND status != 'merged'
                  AND start_time < ? AND last_event_time >= ?
                ORDER BY start_time, first_event_id, session_row_id
                """,
                (candidate["user_name"], candidate["window_end"], candidate["window_start"]),
            ).fetchall()
            if sessions:
                session = max(
                    sessions,
                    key=lambda row: (
                        _overlap(row["start_time"], row["last_event_time"] + 1, candidate["window_start"], candidate["window_end"]),
                        -int(row["start_time"]),
                        str(row["first_event_id"]),
                    ),
                )
                session_key = session["session_id"] or f"provisional:{session['session_row_id']}"
                return f"session:{session_key}", session
        width = self.config.fallback_bucket_seconds * 1000
        bucket = (int(candidate["window_start"]) // width) * width
        return f"bucket:{candidate['window_size']}:{bucket}", None

    def _build_incident(
        self,
        user_name: str,
        group_key: str,
        candidates: list[sqlite3.Row],
        session: sqlite3.Row | None,
        revision: int,
        previous: dict[str, str],
    ) -> dict[str, Any]:
        incident_key = hashlib.sha256(f"{user_name}:{group_key}".encode("utf-8")).hexdigest()
        incident_id = hashlib.sha256(f"{incident_key}:{revision}".encode("utf-8")).hexdigest()
        source_types: set[str] = set()
        source_ips: set[str] = set()
        resources: set[str] = set()
        detectors: set[str] = set()
        timeline: list[dict[str, Any]] = []
        for candidate in candidates:
            detectors.add(candidate["detector_id"])
            for row in self.feature.execute(
                """
                SELECT value_type, value_key FROM window_value_counts
                WHERE user_name = ? AND window_size = ? AND window_start = ?
                """,
                (candidate["user_name"], candidate["window_size"], candidate["window_start"]),
            ):
                if row["value_type"] == "source_type":
                    source_types.add(row["value_key"])
            if candidate["value_type"] == "source_ip" and candidate["value_key"]:
                source_ips.add(candidate["value_key"])
            if candidate["value_type"] == "resource" and candidate["value_key"]:
                resources.add(candidate["value_key"])
            timeline.append({
                "candidate_id": candidate["candidate_id"],
                "window_start": _iso(int(candidate["window_start"])),
                "window_end": _iso(int(candidate["window_end"])),
                "detector_id": candidate["detector_id"],
                "metric": candidate["metric"],
                "value_type": candidate["value_type"],
                "value_key": candidate["value_key"],
                "score": int(candidate["score"]),
                "severity": candidate["severity"],
            })
        if session is not None:
            source_types.update(json.loads(session["source_type_samples_json"]))
            source_ips.update(json.loads(session["source_ip_samples_json"]))
            resources.update(json.loads(session["resource_samples_json"]))
        timeline.sort(key=lambda item: (item["window_start"], -item["score"], item["candidate_id"]))
        max_score = max(int(candidate["score"]) for candidate in candidates)
        bonus = min(self.config.candidate_bonus_cap, self.config.candidate_bonus * (len(candidates) - 1))
        score = min(100, max_score + bonus)
        if len(source_types) > 1:
            incident_type = "cross_system_behavior_anomaly"
        elif len(detectors) > 1:
            incident_type = "compound_behavior_anomaly"
        else:
            incident_type = "user_behavior_anomaly"
        return {
            "incident_id": incident_id,
            "incident_key": incident_key,
            "user_name": user_name,
            "correlation_key": group_key,
            "session_id": (session["session_id"] if session is not None else None),
            "first_seen": min(int(candidate["window_start"]) for candidate in candidates),
            "last_seen": max(int(candidate["window_end"]) for candidate in candidates),
            "incident_type": incident_type,
            "aggregation_score": score,
            "severity": _severity(score),
            "candidate_count": len(candidates),
            "detector_count": len(detectors),
            "source_types": sorted(source_types),
            "source_ips": sorted(source_ips),
            "resources": sorted(resources),
            "timeline": timeline,
            "evidence": {
                "candidate_ids": [item["candidate_id"] for item in timeline],
                "aggregation_method": "max_score_plus_candidate_bonus",
                "max_candidate_score": max_score,
                "candidate_bonus": bonus,
            },
            "source_detection_revision": revision,
            "supersedes_incident_id": previous.get(incident_key),
        }

    def _insert_incident(self, incident: dict[str, Any], candidates: list[sqlite3.Row]) -> None:
        self.connection.execute(
            """
            INSERT INTO incidents(
                incident_id, incident_key, user_name, correlation_key, session_id,
                first_seen, last_seen, incident_type, aggregation_score, severity,
                candidate_count, detector_count, source_types_json, source_ips_json, resources_json,
                timeline_json, evidence_json, source_detection_revision, status,
                supersedes_incident_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
            """,
            (
                incident["incident_id"], incident["incident_key"], incident["user_name"], incident["correlation_key"],
                incident["session_id"], incident["first_seen"], incident["last_seen"], incident["incident_type"],
                incident["aggregation_score"], incident["severity"], incident["candidate_count"], incident["detector_count"],
                json.dumps(incident["source_types"], ensure_ascii=False), json.dumps(incident["source_ips"], ensure_ascii=False),
                json.dumps(incident["resources"], ensure_ascii=False), json.dumps(incident["timeline"], ensure_ascii=False, sort_keys=True),
                json.dumps(incident["evidence"], ensure_ascii=False, sort_keys=True), incident["source_detection_revision"],
                incident["supersedes_incident_id"], _now_ms(), _now_ms(),
            ),
        )
        self.connection.executemany(
            "INSERT INTO incident_candidates(incident_id, candidate_id) VALUES (?, ?)",
            [(incident["incident_id"], candidate["candidate_id"]) for candidate in candidates],
        )

    def _serialize_incident(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for field in ("first_seen", "last_seen", "created_at", "updated_at"):
            result[field] = _iso(int(result[field]))
        for field in ("source_types", "source_ips", "resources", "timeline", "evidence"):
            result[field] = json.loads(result.pop(f"{field}_json"))
        return result

    def _verify_inputs(self) -> None:
        feature_meta = _meta_map(self.feature, "feature_meta")
        detection_meta = _meta_map(self.detection, "detection_meta")
        if feature_meta.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
            raise CorrelationError("feature schema version changed; rebuild Incident DB")
        if detection_meta.get("detection_schema_version") != DETECTION_SCHEMA_VERSION:
            raise CorrelationError("detection schema version changed; rebuild Incident DB")
        if detection_meta.get("feature_config_fingerprint") != feature_meta.get("config_fingerprint"):
            raise CorrelationError("detection does not match feature configuration")
        self.feature_fingerprint = feature_meta["config_fingerprint"]
        self.detection_fingerprint = detection_meta["detection_config_fingerprint"]

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS correlation_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id TEXT PRIMARY KEY, incident_key TEXT NOT NULL, user_name TEXT NOT NULL,
                correlation_key TEXT NOT NULL, session_id TEXT, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL,
                incident_type TEXT NOT NULL, aggregation_score INTEGER NOT NULL, severity TEXT NOT NULL,
                candidate_count INTEGER NOT NULL, detector_count INTEGER NOT NULL,
                source_types_json TEXT NOT NULL, source_ips_json TEXT NOT NULL, resources_json TEXT NOT NULL,
                timeline_json TEXT NOT NULL, evidence_json TEXT NOT NULL, source_detection_revision INTEGER NOT NULL,
                status TEXT NOT NULL, supersedes_incident_id TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                UNIQUE(incident_key, source_detection_revision)
            );
            CREATE TABLE IF NOT EXISTS incident_candidates (
                incident_id TEXT NOT NULL, candidate_id TEXT NOT NULL,
                PRIMARY KEY(incident_id, candidate_id)
            );
            CREATE INDEX IF NOT EXISTS idx_incidents_user_time ON incidents(user_name, first_seen, status);
            CREATE INDEX IF NOT EXISTS idx_incidents_key_revision ON incidents(incident_key, source_detection_revision);
            """
        )
        self.connection.commit()

    def _initialize_meta(self) -> None:
        fingerprint = self.config.fingerprint()
        existing = self._meta("correlation_schema_version")
        if existing is None:
            values = {
                "correlation_schema_version": CORRELATION_SCHEMA_VERSION,
                "feature_config_fingerprint": self.feature_fingerprint,
                "detection_config_fingerprint": self.detection_fingerprint,
                "correlation_config_fingerprint": fingerprint,
                "last_consumed_detection_revision": "0",
            }
            self.connection.executemany("INSERT INTO correlation_meta(key, value) VALUES (?, ?)", values.items())
            self.connection.commit()
            return
        if existing != CORRELATION_SCHEMA_VERSION:
            raise CorrelationError("correlation schema version changed; rebuild Incident DB")
        if self._meta("feature_config_fingerprint") != self.feature_fingerprint:
            raise CorrelationError("feature configuration changed; rebuild Incident DB")
        if self._meta("detection_config_fingerprint") != self.detection_fingerprint:
            raise CorrelationError("detection configuration changed; rebuild Incident DB")
        if self._meta("correlation_config_fingerprint") != fingerprint:
            raise CorrelationError("correlation configuration changed; rebuild Incident DB")

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
        row = self.connection.execute("SELECT value FROM correlation_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def _meta_int(self, key: str, default: int = 0) -> int:
        value = self._meta(key)
        return int(value) if value is not None else default

    def _set_meta(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO correlation_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def query_incident_state(
    incident_state: Path | str, user_name: str, start: str | int | datetime, end: str | int | datetime,
) -> dict[str, Any]:
    path = Path(incident_state)
    if not path.exists():
        raise CorrelationError(f"incident state does not exist: {path}")
    connection = _readonly(path)
    try:
        meta = _meta_map(connection, "correlation_meta")
        start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
        rows = connection.execute(
            """
            SELECT * FROM incidents WHERE user_name = ? AND status = 'active'
              AND first_seen < ? AND last_seen > ?
            ORDER BY aggregation_score DESC, first_seen DESC, incident_id
            """,
            (user_name, end_ms, start_ms),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            document = dict(row)
            for field in ("first_seen", "last_seen", "created_at", "updated_at"):
                document[field] = _iso(int(document[field]))
            for field in ("source_types", "source_ips", "resources", "timeline", "evidence"):
                document[field] = json.loads(document.pop(f"{field}_json"))
            result.append(document)
        return {
            "user_name": user_name,
            "from": _iso(start_ms),
            "to": _iso(end_ms),
            "processed_detection_revision": int(meta.get("last_consumed_detection_revision", "0")),
            "incidents": result,
        }
    finally:
        connection.close()


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _meta_map(connection: sqlite3.Connection, table: str) -> dict[str, str]:
    return {row["key"]: row["value"] for row in connection.execute(f"SELECT key, value FROM {table}")}


def _overlap(left_start: int, left_end: int, right_start: int, right_end: int) -> int:
    return max(0, min(left_end, right_end) - max(left_start, right_start))


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
            raise CorrelationError("invalid timestamp") from exc
    else:
        raise CorrelationError("invalid timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
