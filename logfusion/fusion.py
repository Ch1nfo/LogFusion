from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


FUSION_SCHEMA_VERSION = "1"
CORRELATION_SCHEMA_VERSION = "1"
DETECTION_SCHEMA_VERSION = "1"


class FusionError(ValueError):
    """Raised when risk fusion inputs are stale or incompatible."""


@dataclass(frozen=True)
class FusionConfig:
    alert_threshold: int = 80
    critical_threshold: int = 90
    cross_system_bonus: int = 10
    detector_bonus: int = 3
    detector_bonus_cap: int = 10
    per_user_daily_alert_limit: int = 3
    case_suppression_enabled: bool = True

    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class FusionEngine:
    """Converts active incidents into risk assessments and policy-controlled alerts."""

    def __init__(
        self,
        detection_state: Path | str,
        incident_state: Path | str,
        risk_state: Path | str,
        config: FusionConfig | None = None,
        case_state: Path | str | None = None,
    ) -> None:
        self.detection_path = Path(detection_state)
        self.incident_path = Path(incident_state)
        if not self.detection_path.exists() or not self.incident_path.exists():
            raise FusionError("detection state and incident state must exist")
        self.risk_path = Path(risk_state)
        self.risk_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or FusionConfig()
        self.case_path = Path(case_state) if case_state is not None else None
        if not 0 <= self.config.alert_threshold <= 100 or not 0 <= self.config.critical_threshold <= 100:
            raise FusionError("risk thresholds must be between 0 and 100")
        self.detection = _readonly(self.detection_path)
        self.incidents = _readonly(self.incident_path)
        self.cases = _readonly(self.case_path) if self.case_path is not None else None
        self.connection = sqlite3.connect(self.risk_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self._verify_inputs()
        self._create_schema()
        self._initialize_meta()

    def close(self) -> None:
        self.detection.close()
        self.incidents.close()
        if self.cases is not None:
            self.cases.close()
        self.connection.close()

    def __enter__(self) -> FusionEngine:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def run(self, refresh_policy: bool = False) -> dict[str, Any]:
        detection_revision = int(_meta_map(self.detection, "detection_meta").get("last_consumed_feature_revision", "0"))
        correlation_revision = int(_meta_map(self.incidents, "correlation_meta").get("last_consumed_detection_revision", "0"))
        if correlation_revision != detection_revision:
            raise FusionError("Incident DB is behind Detection DB; run correlate run first")
        previous_revision = self._meta_int("last_consumed_correlation_revision", 0)
        if previous_revision == correlation_revision and not refresh_policy:
            return {
                "processed_correlation_revision": correlation_revision,
                "assessment_count": 0,
                "alert_count": 0,
                "suppressed_count": 0,
                "superseded_count": 0,
            }
        incidents = self.incidents.execute(
            "SELECT * FROM incidents WHERE status = 'active' ORDER BY aggregation_score DESC, first_seen, incident_id"
        ).fetchall()
        assessments = [self._assessment(row, correlation_revision) for row in incidents]
        assessments.sort(key=lambda item: (-item["risk_score"], item["first_seen"], item["incident_id"]))
        per_user_day: dict[tuple[str, int], int] = {}
        alert_count = 0
        suppressed_count = 0
        with self._transaction():
            previous = {
                row["assessment_key"]: row["assessment_id"]
                for row in self.connection.execute("SELECT assessment_key, assessment_id FROM risk_assessments WHERE status = 'active'")
            }
            superseded_assessments = self.connection.execute(
                "UPDATE risk_assessments SET status = 'superseded', updated_at = ? WHERE status = 'active'", (_now_ms(),)
            ).rowcount
            superseded_alerts = self.connection.execute(
                "UPDATE alerts SET status = 'superseded', updated_at = ? WHERE status = 'active'", (_now_ms(),)
            ).rowcount
            for assessment in assessments:
                user_day = (assessment["user_name"], assessment["first_seen"] // 86_400_000)
                if assessment["risk_score"] < self.config.alert_threshold:
                    action = "below_threshold"
                elif self._case_suppressed(assessment["user_name"], assessment["incident_type"], assessment["first_seen"]):
                    action = "suppressed_by_case_policy"
                    suppressed_count += 1
                elif per_user_day.get(user_day, 0) >= self.config.per_user_daily_alert_limit:
                    action = "suppressed_budget"
                    suppressed_count += 1
                else:
                    action = "alert"
                    per_user_day[user_day] = per_user_day.get(user_day, 0) + 1
                    alert_count += 1
                assessment["policy_action"] = action
                assessment["supersedes_assessment_id"] = previous.get(assessment["assessment_key"])
                if assessment["supersedes_assessment_id"] == assessment["assessment_id"]:
                    assessment["supersedes_assessment_id"] = None
                self._insert_assessment(assessment)
                if action == "alert":
                    self._insert_alert(assessment, correlation_revision)
            self._set_meta("last_consumed_correlation_revision", str(correlation_revision))
        return {
            "processed_correlation_revision": correlation_revision,
            "assessment_count": len(assessments),
            "alert_count": alert_count,
            "suppressed_count": suppressed_count,
            "superseded_count": superseded_assessments + superseded_alerts,
        }

    def _case_suppressed(self, user_name: str, incident_type: str, first_seen: int) -> bool:
        if self.cases is None or not self.config.case_suppression_enabled:
            return False
        day = first_seen // 86_400_000
        key = hashlib.sha256(f"{user_name}:{incident_type}:{day}".encode("utf-8")).hexdigest()
        row = self.cases.execute(
            "SELECT 1 FROM cases WHERE alert_key = ? AND status = 'suppressed' AND suppression_until > ?",
            (key, _now_ms()),
        ).fetchone()
        return row is not None

    def query(self, user_name: str, start: str | int | datetime, end: str | int | datetime) -> dict[str, Any]:
        start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
        if end_ms <= start_ms:
            raise FusionError("query end must be after start")
        assessments = self.connection.execute(
            """
            SELECT * FROM risk_assessments WHERE user_name = ? AND status = 'active'
              AND first_seen < ? AND last_seen > ?
            ORDER BY risk_score DESC, first_seen DESC, assessment_id
            """,
            (user_name, end_ms, start_ms),
        ).fetchall()
        alerts = self.connection.execute(
            """
            SELECT * FROM alerts WHERE user_name = ? AND status = 'active'
              AND first_seen < ? AND last_seen > ?
            ORDER BY risk_score DESC, first_seen DESC, alert_id
            """,
            (user_name, end_ms, start_ms),
        ).fetchall()
        return {
            "user_name": user_name,
            "from": _iso(start_ms), "to": _iso(end_ms),
            "processed_correlation_revision": self._meta_int("last_consumed_correlation_revision", 0),
            "assessments": [_serialize_assessment(row) for row in assessments],
            "alerts": [_serialize_alert(row) for row in alerts],
        }

    def _assessment(self, incident: sqlite3.Row, revision: int) -> dict[str, Any]:
        source_types = json.loads(incident["source_types_json"])
        base = int(incident["aggregation_score"])
        cross_bonus = self.config.cross_system_bonus if len(source_types) > 1 else 0
        detector_bonus = min(self.config.detector_bonus_cap, self.config.detector_bonus * max(0, int(incident["detector_count"]) - 1))
        score = min(100, base + cross_bonus + detector_bonus)
        key = hashlib.sha256(incident["incident_key"].encode("utf-8")).hexdigest()
        assessment_id = hashlib.sha256(f"{key}:{revision}".encode("utf-8")).hexdigest()
        return {
            "assessment_id": assessment_id,
            "assessment_key": key,
            "incident_id": incident["incident_id"],
            "incident_key": incident["incident_key"],
            "user_name": incident["user_name"],
            "first_seen": int(incident["first_seen"]),
            "last_seen": int(incident["last_seen"]),
            "incident_type": incident["incident_type"],
            "base_score": base,
            "risk_score": score,
            "severity": _severity(score, self.config),
            "risk_breakdown": {
                "incident_aggregation_score": base,
                "cross_system_bonus": cross_bonus,
                "detector_diversity_bonus": detector_bonus,
                "source_type_count": len(source_types),
                "detector_count": int(incident["detector_count"]),
            },
            "source_correlation_revision": revision,
        }

    def _insert_assessment(self, assessment: dict[str, Any]) -> None:
        try:
            self.connection.execute(
            """
            INSERT INTO risk_assessments(
                assessment_id, assessment_key, incident_id, incident_key, user_name,
                first_seen, last_seen, incident_type, base_score, risk_score, severity,
                risk_breakdown_json, policy_action, source_correlation_revision,
                status, supersedes_assessment_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
            """,
            (
                assessment["assessment_id"], assessment["assessment_key"], assessment["incident_id"], assessment["incident_key"],
                assessment["user_name"], assessment["first_seen"], assessment["last_seen"], assessment["incident_type"],
                assessment["base_score"], assessment["risk_score"], assessment["severity"],
                json.dumps(assessment["risk_breakdown"], ensure_ascii=False, sort_keys=True), assessment["policy_action"],
                assessment["source_correlation_revision"], assessment["supersedes_assessment_id"], _now_ms(), _now_ms(),
            ),
            )
        except sqlite3.IntegrityError:
            # A policy-only refresh intentionally reuses the same correlation revision.
            self.connection.execute(
                """
                UPDATE risk_assessments SET incident_id = ?, incident_key = ?, user_name = ?, first_seen = ?, last_seen = ?,
                    incident_type = ?, base_score = ?, risk_score = ?, severity = ?, risk_breakdown_json = ?, policy_action = ?,
                    status = 'active', supersedes_assessment_id = ?, updated_at = ?
                WHERE assessment_key = ? AND source_correlation_revision = ?
                """,
                (
                    assessment["incident_id"], assessment["incident_key"], assessment["user_name"], assessment["first_seen"],
                    assessment["last_seen"], assessment["incident_type"], assessment["base_score"], assessment["risk_score"],
                    assessment["severity"], json.dumps(assessment["risk_breakdown"], ensure_ascii=False, sort_keys=True),
                    assessment["policy_action"], assessment["supersedes_assessment_id"], _now_ms(),
                    assessment["assessment_key"], assessment["source_correlation_revision"],
                ),
            )

    def _insert_alert(self, assessment: dict[str, Any], revision: int) -> None:
        day = assessment["first_seen"] // 86_400_000
        alert_key = hashlib.sha256(f"{assessment['user_name']}:{assessment['incident_type']}:{day}".encode("utf-8")).hexdigest()
        alert_id = hashlib.sha256(f"{alert_key}:{revision}:{assessment['assessment_id']}".encode("utf-8")).hexdigest()
        try:
            self.connection.execute(
                """
            INSERT INTO alerts(
                alert_id, alert_key, assessment_id, incident_id, user_name, first_seen, last_seen,
                risk_score, severity, policy_reason, source_correlation_revision, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'threshold_and_budget', ?, 'active', ?, ?)
            """,
                (
                alert_id, alert_key, assessment["assessment_id"], assessment["incident_id"], assessment["user_name"],
                assessment["first_seen"], assessment["last_seen"], assessment["risk_score"], assessment["severity"],
                revision, _now_ms(), _now_ms(),
                ),
            )
        except sqlite3.IntegrityError:
            self.connection.execute(
                """
                UPDATE alerts SET incident_id = ?, user_name = ?, first_seen = ?, last_seen = ?, risk_score = ?,
                    severity = ?, status = 'active', updated_at = ? WHERE alert_id = ?
                """,
                (assessment["incident_id"], assessment["user_name"], assessment["first_seen"], assessment["last_seen"],
                 assessment["risk_score"], assessment["severity"], _now_ms(), alert_id),
            )

    def _verify_inputs(self) -> None:
        detection_meta = _meta_map(self.detection, "detection_meta")
        correlation_meta = _meta_map(self.incidents, "correlation_meta")
        if detection_meta.get("detection_schema_version") != DETECTION_SCHEMA_VERSION:
            raise FusionError("detection schema version changed; rebuild Risk DB")
        if correlation_meta.get("correlation_schema_version") != CORRELATION_SCHEMA_VERSION:
            raise FusionError("correlation schema version changed; rebuild Risk DB")
        if correlation_meta.get("detection_config_fingerprint") != detection_meta.get("detection_config_fingerprint"):
            raise FusionError("correlation does not match detection configuration")
        self.detection_fingerprint = detection_meta["detection_config_fingerprint"]
        self.correlation_fingerprint = correlation_meta["correlation_config_fingerprint"]

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS fusion_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS risk_assessments (
                assessment_id TEXT PRIMARY KEY, assessment_key TEXT NOT NULL, incident_id TEXT NOT NULL, incident_key TEXT NOT NULL,
                user_name TEXT NOT NULL, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, incident_type TEXT NOT NULL,
                base_score INTEGER NOT NULL, risk_score INTEGER NOT NULL, severity TEXT NOT NULL,
                risk_breakdown_json TEXT NOT NULL, policy_action TEXT NOT NULL, source_correlation_revision INTEGER NOT NULL,
                status TEXT NOT NULL, supersedes_assessment_id TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                UNIQUE(assessment_key, source_correlation_revision)
            );
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY, alert_key TEXT NOT NULL, assessment_id TEXT NOT NULL, incident_id TEXT NOT NULL,
                user_name TEXT NOT NULL, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL,
                risk_score INTEGER NOT NULL, severity TEXT NOT NULL, policy_reason TEXT NOT NULL,
                source_correlation_revision INTEGER NOT NULL, status TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_assessments_user_time ON risk_assessments(user_name, first_seen, status);
            CREATE INDEX IF NOT EXISTS idx_alerts_user_time ON alerts(user_name, first_seen, status);
            """
        )
        self.connection.commit()

    def _initialize_meta(self) -> None:
        fingerprint = self.config.fingerprint()
        existing = self._meta("fusion_schema_version")
        if existing is None:
            values = {
                "fusion_schema_version": FUSION_SCHEMA_VERSION,
                "detection_config_fingerprint": self.detection_fingerprint,
                "correlation_config_fingerprint": self.correlation_fingerprint,
                "fusion_config_fingerprint": fingerprint,
                "last_consumed_correlation_revision": "0",
            }
            self.connection.executemany("INSERT INTO fusion_meta(key, value) VALUES (?, ?)", values.items())
            self.connection.commit()
            return
        if existing != FUSION_SCHEMA_VERSION:
            raise FusionError("fusion schema version changed; rebuild Risk DB")
        if self._meta("detection_config_fingerprint") != self.detection_fingerprint:
            raise FusionError("detection configuration changed; rebuild Risk DB")
        if self._meta("correlation_config_fingerprint") != self.correlation_fingerprint:
            raise FusionError("correlation configuration changed; rebuild Risk DB")
        if self._meta("fusion_config_fingerprint") != fingerprint:
            raise FusionError("fusion configuration changed; rebuild Risk DB")

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
        row = self.connection.execute("SELECT value FROM fusion_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def _meta_int(self, key: str, default: int = 0) -> int:
        value = self._meta(key)
        return int(value) if value is not None else default

    def _set_meta(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO fusion_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def query_risk_state(risk_state: Path | str, user_name: str, start: str | int | datetime, end: str | int | datetime) -> dict[str, Any]:
    path = Path(risk_state)
    if not path.exists():
        raise FusionError(f"risk state does not exist: {path}")
    connection = _readonly(path)
    try:
        meta = _meta_map(connection, "fusion_meta")
        start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
        assessments = connection.execute(
            "SELECT * FROM risk_assessments WHERE user_name = ? AND status = 'active' AND first_seen < ? AND last_seen > ? ORDER BY risk_score DESC, first_seen DESC",
            (user_name, end_ms, start_ms),
        ).fetchall()
        alerts = connection.execute(
            "SELECT * FROM alerts WHERE user_name = ? AND status = 'active' AND first_seen < ? AND last_seen > ? ORDER BY risk_score DESC, first_seen DESC",
            (user_name, end_ms, start_ms),
        ).fetchall()
        return {
            "user_name": user_name, "from": _iso(start_ms), "to": _iso(end_ms),
            "processed_correlation_revision": int(meta.get("last_consumed_correlation_revision", "0")),
            "assessments": [_serialize_assessment(row) for row in assessments],
            "alerts": [_serialize_alert(row) for row in alerts],
        }
    finally:
        connection.close()


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _meta_map(connection: sqlite3.Connection, table: str) -> dict[str, str]:
    return {row["key"]: row["value"] for row in connection.execute(f"SELECT key, value FROM {table}")}


def _severity(score: int, config: FusionConfig) -> str:
    if score >= config.critical_threshold:
        return "critical"
    if score >= config.alert_threshold:
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
            raise FusionError("invalid timestamp") from exc
    else:
        raise FusionError("invalid timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _serialize_assessment(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    for field in ("first_seen", "last_seen", "created_at", "updated_at"):
        result[field] = _iso(int(result[field]))
    result["risk_breakdown"] = json.loads(result.pop("risk_breakdown_json"))
    return result


def _serialize_alert(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    for field in ("first_seen", "last_seen", "created_at", "updated_at"):
        result[field] = _iso(int(result[field]))
    return result
