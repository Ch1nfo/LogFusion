from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


CASE_SCHEMA_VERSION = "3"
FUSION_SCHEMA_VERSION = "2"
CASE_STATUSES = ("new", "acknowledged", "investigating", "resolved", "false_positive", "suppressed")
TERMINAL_CASE_STATUSES = {"resolved", "false_positive"}


class CaseError(ValueError):
    """Raised when analyst case state is invalid or incompatible."""


class CaseEngine:
    """Maintains analyst-owned case state separately from versioned Fusion alerts."""

    def __init__(self, risk_state: Path | str, case_state: Path | str) -> None:
        self.risk_path = Path(risk_state)
        if not self.risk_path.exists():
            raise CaseError(f"risk state does not exist: {self.risk_path}")
        self.case_path = Path(case_state)
        self.case_path.parent.mkdir(parents=True, exist_ok=True)
        self.risk = _readonly(self.risk_path)
        self.connection = sqlite3.connect(self.case_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self._verify_risk()
        self._create_schema()
        self._initialize_meta()

    def close(self) -> None:
        self.risk.close()
        self.connection.close()

    def __enter__(self) -> CaseEngine:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def sync(self) -> dict[str, Any]:
        alerts = self.risk.execute(
            "SELECT * FROM alerts WHERE status = 'active' ORDER BY first_seen, alert_id"
        ).fetchall()
        created = refreshed = expired = 0
        now = _now_ms()
        revision = int(_meta_map(self.risk, "fusion_meta").get("last_consumed_correlation_revision", "0"))
        with self._transaction():
            expired = self._expire_suppressions(now)
            for alert in alerts:
                existing = self.connection.execute(
                    "SELECT * FROM cases WHERE alert_key = ?", (alert["alert_key"],)
                ).fetchone()
                if existing is None:
                    case_id = hashlib.sha256(alert["alert_key"].encode("utf-8")).hexdigest()
                    self.connection.execute(
                        """
                        INSERT INTO cases(
                            case_id, alert_key, current_alert_id, user_name, incident_id, first_seen, last_seen,
                            risk_score, severity, status, owner, tags_json, suppression_until, requires_review,
                            source_correlation_revision, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', NULL, '[]', NULL, 0, ?, ?, ?)
                        """,
                        (
                            case_id, alert["alert_key"], alert["alert_id"], alert["user_name"], alert["incident_id"],
                            alert["first_seen"], alert["last_seen"], alert["risk_score"], alert["severity"],
                            alert["source_correlation_revision"], now, now,
                        ),
                    )
                    self._append_event(case_id, "created", "system", {"alert_id": alert["alert_id"]}, now)
                    created += 1
                elif existing["current_alert_id"] != alert["alert_id"]:
                    requires_review = int(existing["status"] in TERMINAL_CASE_STATUSES)
                    self.connection.execute(
                        """
                        UPDATE cases SET current_alert_id = ?, incident_id = ?, last_seen = ?, risk_score = ?, severity = ?,
                            requires_review = ?, source_correlation_revision = ?, updated_at = ? WHERE case_id = ?
                        """,
                        (
                            alert["alert_id"], alert["incident_id"], alert["last_seen"], alert["risk_score"],
                            alert["severity"], requires_review, alert["source_correlation_revision"], now, existing["case_id"],
                        ),
                    )
                    self._append_event(existing["case_id"], "risk_alert_refreshed", "system", {
                        "previous_alert_id": existing["current_alert_id"], "alert_id": alert["alert_id"],
                        "requires_review": bool(requires_review),
                    }, now)
                    refreshed += 1
            self._set_meta("last_synced_correlation_revision", str(revision))
        return {
            "processed_correlation_revision": revision,
            "active_alert_count": len(alerts),
            "created_count": created,
            "refreshed_count": refreshed,
            "expired_suppression_count": expired,
        }

    def transition(
        self,
        case_id: str,
        status: str,
        actor: str,
        note: str | None = None,
        suppression_until: str | int | datetime | None = None,
    ) -> dict[str, Any]:
        if status not in CASE_STATUSES:
            raise CaseError(f"unsupported case status: {status}")
        if not actor.strip():
            raise CaseError("actor is required")
        until = _timestamp_ms(suppression_until) if suppression_until is not None else None
        now = _now_ms()
        with self._transaction():
            row = self._case_row(case_id)
            if status == "suppressed":
                if until is None or until <= now:
                    raise CaseError("suppressed status requires a future suppression_until")
            elif until is not None:
                raise CaseError("suppression_until is only valid for suppressed status")
            self.connection.execute(
                "UPDATE cases SET status = ?, suppression_until = ?, requires_review = 0, updated_at = ? WHERE case_id = ?",
                (status, until, now, case_id),
            )
            self._append_event(case_id, "status_changed", actor, {
                "from": row["status"], "to": status, "note": note, "suppression_until": until,
            }, now)
        return self.get(case_id)

    def assign(self, case_id: str, owner: str | None, actor: str, note: str | None = None) -> dict[str, Any]:
        if not actor.strip():
            raise CaseError("actor is required")
        normalized = owner.strip() if owner is not None and owner.strip() else None
        now = _now_ms()
        with self._transaction():
            row = self._case_row(case_id)
            self.connection.execute("UPDATE cases SET owner = ?, updated_at = ? WHERE case_id = ?", (normalized, now, case_id))
            self._append_event(case_id, "owner_changed", actor, {"from": row["owner"], "to": normalized, "note": note}, now)
        return self.get(case_id)

    def comment(self, case_id: str, actor: str, note: str) -> dict[str, Any]:
        if not actor.strip() or not note.strip():
            raise CaseError("actor and note are required")
        now = _now_ms()
        with self._transaction():
            self._case_row(case_id)
            self._append_event(case_id, "comment", actor, {"note": note}, now)
            self.connection.execute("UPDATE cases SET updated_at = ? WHERE case_id = ?", (now, case_id))
        return self.get(case_id)

    def set_tags(self, case_id: str, tags: list[str], actor: str) -> dict[str, Any]:
        if not actor.strip() or any(not isinstance(tag, str) or not tag.strip() for tag in tags):
            raise CaseError("actor and non-empty tags are required")
        normalized = sorted(set(tag.strip() for tag in tags))
        now = _now_ms()
        with self._transaction():
            row = self._case_row(case_id)
            self.connection.execute("UPDATE cases SET tags_json = ?, updated_at = ? WHERE case_id = ?", (json.dumps(normalized), now, case_id))
            self._append_event(case_id, "tags_changed", actor, {"from": json.loads(row["tags_json"]), "to": normalized}, now)
        return self.get(case_id)

    def list(self, user_name: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        if status is not None and status not in CASE_STATUSES:
            raise CaseError(f"unsupported case status: {status}")
        sql = "SELECT * FROM cases WHERE 1 = 1"
        params: list[Any] = []
        if user_name:
            sql += " AND user_name = ?"
            params.append(user_name)
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY requires_review DESC, risk_score DESC, last_seen DESC, case_id"
        return [_serialize_case(row) for row in self.connection.execute(sql, params).fetchall()]

    def get(self, case_id: str) -> dict[str, Any]:
        row = self._case_row(case_id)
        events = self.connection.execute(
            "SELECT * FROM case_events WHERE case_id = ? ORDER BY event_id", (case_id,)
        ).fetchall()
        result = _serialize_case(row)
        result["events"] = [_serialize_event(event) for event in events]
        return result

    def _verify_risk(self) -> None:
        meta = _meta_map(self.risk, "fusion_meta")
        if meta.get("fusion_schema_version") != FUSION_SCHEMA_VERSION:
            raise CaseError("risk fusion schema version changed; rebuild Case DB")
        self.risk_fusion_fingerprint = meta.get("fusion_config_fingerprint", "")

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS case_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS cases (
                case_id TEXT PRIMARY KEY, alert_key TEXT NOT NULL UNIQUE, current_alert_id TEXT NOT NULL,
                user_name TEXT NOT NULL, incident_id TEXT NOT NULL, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL,
                risk_score INTEGER NOT NULL, severity TEXT NOT NULL, status TEXT NOT NULL, owner TEXT,
                tags_json TEXT NOT NULL, suppression_until INTEGER, requires_review INTEGER NOT NULL,
                source_correlation_revision INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS case_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT, case_id TEXT NOT NULL, event_type TEXT NOT NULL,
                actor TEXT NOT NULL, details_json TEXT NOT NULL, created_at INTEGER NOT NULL,
                FOREIGN KEY(case_id) REFERENCES cases(case_id)
            );
            CREATE INDEX IF NOT EXISTS idx_cases_status_score ON cases(status, requires_review, risk_score, last_seen);
            CREATE INDEX IF NOT EXISTS idx_cases_user ON cases(user_name, last_seen);
            CREATE INDEX IF NOT EXISTS idx_case_events_case ON case_events(case_id, event_id);
            """
        )
        self.connection.commit()

    def _initialize_meta(self) -> None:
        existing = self._meta("case_schema_version")
        if existing is None:
            self.connection.executemany("INSERT INTO case_meta(key, value) VALUES (?, ?)", {
                "case_schema_version": CASE_SCHEMA_VERSION,
                "risk_fusion_schema_version": FUSION_SCHEMA_VERSION,
                "last_synced_correlation_revision": "0",
            }.items())
            self.connection.commit()
        elif existing != CASE_SCHEMA_VERSION or self._meta("risk_fusion_schema_version") != FUSION_SCHEMA_VERSION:
            raise CaseError("case schema version changed; rebuild Case DB")

    def _expire_suppressions(self, now: int) -> int:
        rows = self.connection.execute(
            "SELECT case_id, suppression_until FROM cases WHERE status = 'suppressed' AND suppression_until <= ?", (now,)
        ).fetchall()
        for row in rows:
            self.connection.execute(
                "UPDATE cases SET status = 'new', suppression_until = NULL, updated_at = ? WHERE case_id = ?", (now, row["case_id"])
            )
            self._append_event(row["case_id"], "suppression_expired", "system", {"suppression_until": row["suppression_until"]}, now)
        return len(rows)

    def _case_row(self, case_id: str) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)).fetchone()
        if row is None:
            raise CaseError(f"case does not exist: {case_id}")
        return row

    def _append_event(self, case_id: str, event_type: str, actor: str, details: dict[str, Any], now: int) -> None:
        self.connection.execute(
            "INSERT INTO case_events(case_id, event_type, actor, details_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (case_id, event_type, actor, json.dumps(details, ensure_ascii=False, sort_keys=True), now),
        )

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
        row = self.connection.execute("SELECT value FROM case_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO case_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value)
        )


def list_cases(case_state: Path | str, user_name: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
    path = Path(case_state)
    if not path.exists():
        raise CaseError(f"case state does not exist: {path}")
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        meta = _meta_map(connection, "case_meta")
        if meta.get("case_schema_version") != CASE_SCHEMA_VERSION:
            raise CaseError("case schema version changed; rebuild Case DB")
        engine = object.__new__(CaseEngine)
        engine.connection = connection
        return engine.list(user_name, status)
    finally:
        connection.close()


def get_case(case_state: Path | str, case_id: str) -> dict[str, Any]:
    path = Path(case_state)
    if not path.exists():
        raise CaseError(f"case state does not exist: {path}")
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        meta = _meta_map(connection, "case_meta")
        if meta.get("case_schema_version") != CASE_SCHEMA_VERSION:
            raise CaseError("case schema version changed; rebuild Case DB")
        engine = object.__new__(CaseEngine)
        engine.connection = connection
        return engine.get(case_id)
    finally:
        connection.close()


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _meta_map(connection: sqlite3.Connection, table: str) -> dict[str, str]:
    return {row["key"]: row["value"] for row in connection.execute(f"SELECT key, value FROM {table}")}


def _timestamp_ms(value: str | int | datetime) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CaseError("invalid timestamp") from exc
    else:
        raise CaseError("invalid timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _serialize_case(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    for field in ("first_seen", "last_seen", "suppression_until", "created_at", "updated_at"):
        result[field] = _iso(result[field])
    result["requires_review"] = bool(result["requires_review"])
    result["tags"] = json.loads(result.pop("tags_json"))
    return result


def _serialize_event(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    result["created_at"] = _iso(result["created_at"])
    result["details"] = json.loads(result.pop("details_json"))
    return result
