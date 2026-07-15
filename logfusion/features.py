from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


FEATURE_SCHEMA_VERSION = "2"
CANONICAL_SCHEMA_VERSION = "v0"
INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)


class FeatureError(ValueError):
    """Raised when an event or feature-state operation violates the v1 contract."""


@dataclass(frozen=True)
class FeatureConfig:
    window_sizes_seconds: tuple[int, ...] = (60, 300, 3600, 86400)
    session_gap_seconds: int = 30 * 60
    allowed_lateness_seconds: int = 5 * 60
    sample_limit: int = 64
    action_classes: dict[str, tuple[str, ...]] = field(default_factory=lambda: {
        "read": ("_read", "_clone", "_checkout", "_pull"),
        "write": ("_write", "_push", "_delete", "_create", "_modify", "_rename", "_move"),
    })
    outcome_classes: dict[str, tuple[str, ...]] = field(default_factory=lambda: {
        "success": ("success",),
        "failure": ("failure",),
        "other": ("unknown",),
    })

    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class FeatureResult:
    status: str
    revision: int | None = None
    skip_reason: str | None = None
    windows_updated: int = 0
    session_row_id: int | None = None


class FeatureEngine:
    """Single-writer SQLite feature state for Canonical Event v0."""

    def __init__(
        self,
        state_db: Path | str,
        config: FeatureConfig | None = None,
        *,
        mode: str | None = "replay",
    ) -> None:
        if mode not in {None, "replay", "realtime"}:
            raise FeatureError("mode must be replay, realtime, or None")
        self.path = Path(state_db)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or FeatureConfig()
        self.mode = mode
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self._reject_old_schema()
        self._create_schema()
        self._initialize_meta()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> FeatureEngine:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def process_event(self, event: dict[str, Any]) -> FeatureResult:
        event_id = _path(event, "event.id")
        if not isinstance(event_id, str) or not event_id:
            return FeatureResult("rejected", skip_reason="missing_event_id")
        try:
            with self._transaction():
                return self._process_event_in_transaction(event)
        except sqlite3.Error as exc:
            raise FeatureError(f"SQLite feature state failure: {exc}") from exc

    def process_events(self, events: Iterable[dict[str, Any]], batch_size: int = 1000) -> Iterator[FeatureResult]:
        if batch_size <= 0:
            raise FeatureError("batch_size must be greater than zero")
        batch: list[dict[str, Any]] = []
        for event in events:
            batch.append(event)
            if len(batch) >= batch_size:
                yield from self._process_batch(batch)
                batch = []
        if batch:
            yield from self._process_batch(batch)

    def _process_batch(self, batch: list[dict[str, Any]]) -> Iterator[FeatureResult]:
        results: list[FeatureResult] = []
        try:
            with self._transaction():
                for index, event in enumerate(batch):
                    event_id = _path(event, "event.id")
                    if not isinstance(event_id, str) or not event_id:
                        results.append(FeatureResult("rejected", skip_reason="missing_event_id"))
                        continue
                    savepoint = f"feature_event_{index}"
                    self.connection.execute(f"SAVEPOINT {savepoint}")
                    try:
                        results.append(self._process_event_in_transaction(event))
                    except FeatureError:
                        self.connection.execute(f"ROLLBACK TO {savepoint}")
                        self.connection.execute(f"RELEASE {savepoint}")
                        raise
                    self.connection.execute(f"RELEASE {savepoint}")
        except sqlite3.Error as exc:
            raise FeatureError(f"SQLite feature state failure: {exc}") from exc
        yield from results

    def advance_watermark(self, watermark: str | int | datetime) -> int:
        if self.mode != "realtime":
            raise FeatureError("advance_watermark is only available in realtime mode")
        watermark_ms = _timestamp_ms(watermark)
        previous = int(self._meta("current_watermark_ms", "-1"))
        if watermark_ms <= previous:
            return 0
        with self._transaction():
            revision = self._next_revision()
            self._set_meta("current_watermark_ms", str(watermark_ms))
            rows = self.connection.execute(
                "SELECT * FROM user_sessions WHERE status = 'provisional' AND last_event_time + ? < ?",
                (self.config.session_gap_seconds * 1000, watermark_ms),
            ).fetchall()
            for row in rows:
                self._close_session(row, "watermark", revision)
            return len(rows)

    def flush_sessions(self) -> int:
        with self._transaction():
            rows = self.connection.execute(
                "SELECT * FROM user_sessions WHERE status = 'provisional' ORDER BY session_row_id"
            ).fetchall()
            if not rows:
                if self.mode == "replay":
                    self._set_meta("replay_finalized", "1")
                return 0
            revision = self._next_revision()
            for row in rows:
                self._close_session(row, "end_of_input", revision)
            if self.mode == "replay":
                self._set_meta("replay_finalized", "1")
            return len(rows)

    def ensure_replay_build_allowed(self) -> None:
        if self.mode != "replay":
            raise FeatureError("features build requires replay mode")
        finalized = self._meta("replay_finalized", "0") == "1"
        count = int(self.connection.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0])
        if finalized or count:
            raise FeatureError("historical state already exists; rebuild a new Feature DB")

    def current_revision(self) -> int:
        return int(self._meta("current_revision", "0"))

    def query_windows(
        self, user_name: str, start: str | int | datetime, end: str | int | datetime, window_size: int | None = None,
    ) -> list[dict[str, Any]]:
        start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
        if end_ms <= start_ms:
            raise FeatureError("query end must be after start")
        sql = "SELECT * FROM user_windows WHERE user_name = ? AND window_start < ? AND window_end > ?"
        values: list[Any] = [user_name, end_ms, start_ms]
        if window_size is not None:
            sql += " AND window_size = ?"
            values.append(window_size)
        sql += " ORDER BY window_start, window_size"
        return [_serialize_row(row) for row in self.connection.execute(sql, values).fetchall()]

    def query_sessions(self, user_name: str, start: str | int | datetime, end: str | int | datetime) -> list[dict[str, Any]]:
        start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
        if end_ms <= start_ms:
            raise FeatureError("query end must be after start")
        rows = self.connection.execute(
            """
            SELECT * FROM user_sessions
            WHERE user_name = ? AND status != 'merged'
              AND start_time < ? AND last_event_time >= ?
            ORDER BY start_time, first_event_id
            """,
            (user_name, end_ms, start_ms),
        ).fetchall()
        return [_serialize_row(row) for row in rows]

    def query_event_evidence(
        self,
        user_name: str,
        start: str | int | datetime,
        end: str | int | datetime,
        *,
        limit: int = 20,
        source_types: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        """Query compact immutable event references without exposing raw event text."""
        start_ms, end_ms = _timestamp_ms(start), _timestamp_ms(end)
        if end_ms <= start_ms or limit <= 0:
            raise FeatureError("evidence query requires end after start and a positive limit")
        clauses = ["user_name = ?", "event_time >= ?", "event_time < ?"]
        values: list[Any] = [user_name, start_ms, end_ms]
        if source_types:
            clauses.append(f"source_type IN ({','.join('?' for _ in source_types)})")
            values.extend(source_types)
        where = " AND ".join(clauses)
        total = int(self.connection.execute(f"SELECT COUNT(*) FROM event_evidence WHERE {where}", values).fetchone()[0])
        rows = self.connection.execute(
            f"SELECT * FROM event_evidence WHERE {where} ORDER BY event_time,event_id LIMIT ?",
            [*values, limit],
        ).fetchall()
        return {
            "user_name": user_name,
            "from": _iso(start_ms),
            "to": _iso(end_ms),
            "event_count": total,
            "evidence_truncated": total > limit,
            "events": [_serialize_row(row) for row in rows],
        }

    def value_stats(self, user_name: str, value_type: str, value_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM user_value_stats WHERE user_name = ? AND value_type = ? AND value_key = ?",
            (user_name, value_type, value_key),
        ).fetchone()
        return _serialize_row(row) if row else None

    def _process_event_in_transaction(self, event: dict[str, Any]) -> FeatureResult:
        event_id = _path(event, "event.id")
        assert isinstance(event_id, str) and event_id
        existing = self.connection.execute(
            "SELECT status FROM processed_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if existing:
            return FeatureResult("duplicate")

        try:
            fields = _event_fields(event)
        except FeatureError as exc:
            self.connection.execute(
                """
                INSERT INTO processed_events(
                    event_id, event_time, user_name, status, skip_reason,
                    canonical_schema_version, feature_schema_version, processed_at
                ) VALUES (?, NULL, NULL, 'rejected', ?, ?, ?, ?)
                """,
                (event_id, str(exc), CANONICAL_SCHEMA_VERSION, FEATURE_SCHEMA_VERSION, _now_ms()),
            )
            return FeatureResult("rejected", skip_reason=str(exc))

        revision = self._next_revision()
        self.connection.execute(
            """
            INSERT INTO processed_events(
                event_id, event_time, user_name, status, skip_reason,
                canonical_schema_version, feature_schema_version, processed_at
            ) VALUES (?, ?, ?, 'processed', NULL, ?, ?, ?)
            """,
            (event_id, fields["event_time"], fields["user_name"], CANONICAL_SCHEMA_VERSION, FEATURE_SCHEMA_VERSION, _now_ms()),
        )
        self._insert_event_evidence(fields, revision)
        self._update_activity_bounds(fields, revision)
        self._update_windows(fields, revision)
        self._update_value_stats(fields, revision)
        session_row_id = self._update_sessions(fields, revision)
        return FeatureResult("processed", revision=revision, windows_updated=len(self.config.window_sizes_seconds), session_row_id=session_row_id)

    def _insert_event_evidence(self, fields: dict[str, Any], revision: int) -> None:
        values = {**fields, "feature_revision": revision}
        self.connection.execute(
            """
            INSERT INTO event_evidence(
                event_id, feature_revision, event_time, user_name, category, event_type, action, outcome,
                source_id, source_type, source_ip, destination_ip, host_name, resource_type, resource_name,
                http_method, http_status, parser_id, parser_version, record_id, checksum, storage_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(values[key] for key in (
                "event_id", "feature_revision", "event_time", "user_name", "category", "event_type", "action", "outcome",
                "source_id", "source_type", "source_ip", "destination_ip", "host_name", "resource_type", "resource_name",
                "http_method", "http_status", "parser_id", "parser_version", "record_id", "checksum", "storage_ref",
            )),
        )

    def _update_windows(self, fields: dict[str, Any], revision: int) -> None:
        outcome = _outcome_class(fields["outcome"], self.config)
        action_class = _action_class(fields["action"], self.config)
        for window_size in self.config.window_sizes_seconds:
            start = _window_start(fields["event_time"], window_size)
            end = start + window_size * 1000
            self.connection.execute(
                """
                INSERT OR IGNORE INTO user_windows(
                    user_name, window_size, window_start, window_end,
                    event_count, success_count, failure_count, other_outcome_count,
                    source_type_distinct_count, source_ip_distinct_count,
                    resource_distinct_count, action_distinct_count,
                    read_action_count, write_action_count, network_bytes_total,
                    first_seen_ip_count, first_seen_resource_count, first_seen_action_count,
                    updated_revision, updated_at
                ) VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, ?)
                """,
                (fields["user_name"], window_size, start, end, revision, _now_ms()),
            )
            current = self.connection.execute(
                "SELECT network_bytes_total FROM user_windows WHERE user_name = ? AND window_size = ? AND window_start = ?",
                (fields["user_name"], window_size, start),
            ).fetchone()
            network_total = _checked_add(current[0], fields["network_bytes"])
            self.connection.execute(
                """
                UPDATE user_windows SET
                    event_count = event_count + 1,
                    success_count = success_count + ?,
                    failure_count = failure_count + ?,
                    other_outcome_count = other_outcome_count + ?,
                    read_action_count = read_action_count + ?,
                    write_action_count = write_action_count + ?,
                    network_bytes_total = ?,
                    updated_revision = ?, updated_at = ?
                WHERE user_name = ? AND window_size = ? AND window_start = ?
                """,
                (
                    int(outcome == "success"), int(outcome == "failure"), int(outcome == "other"),
                    int(action_class == "read"), int(action_class == "write"), network_total, revision, _now_ms(),
                    fields["user_name"], window_size, start,
                ),
            )
            for value_type, value_key, display in _window_values(fields):
                is_new = self._increment_window_value(
                    fields["user_name"], window_size, start, value_type, value_key, display, revision,
                )
                if is_new:
                    column = {
                        "source_type": "source_type_distinct_count",
                        "source_ip": "source_ip_distinct_count",
                        "resource": "resource_distinct_count",
                        "action": "action_distinct_count",
                    }[value_type]
                    self.connection.execute(
                        f"UPDATE user_windows SET {column} = {column} + 1, updated_revision = ?, updated_at = ? "
                        "WHERE user_name = ? AND window_size = ? AND window_start = ?",
                        (revision, _now_ms(), fields["user_name"], window_size, start),
                    )

    def _increment_window_value(
        self, user_name: str, window_size: int, window_start: int, value_type: str,
        value_key: str, display_value: str, revision: int,
    ) -> bool:
        row = self.connection.execute(
            """
            SELECT event_count FROM window_value_counts
            WHERE user_name = ? AND window_size = ? AND window_start = ? AND value_type = ? AND value_key = ?
            """,
            (user_name, window_size, window_start, value_type, value_key),
        ).fetchone()
        if row is None:
            self.connection.execute(
                """
                INSERT INTO window_value_counts(
                    user_name, window_size, window_start, value_type, value_key, display_value,
                    event_count, updated_revision, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (user_name, window_size, window_start, value_type, value_key, display_value, revision, _now_ms()),
            )
            return True
        self.connection.execute(
            """
            UPDATE window_value_counts
            SET event_count = event_count + 1, updated_revision = ?, updated_at = ?
            WHERE user_name = ? AND window_size = ? AND window_start = ? AND value_type = ? AND value_key = ?
            """,
            (revision, _now_ms(), user_name, window_size, window_start, value_type, value_key),
        )
        return False

    def _update_value_stats(self, fields: dict[str, Any], revision: int) -> None:
        for value_type, value_key, display in _window_values(fields):
            old = self.connection.execute(
                """
                SELECT first_event_time, first_event_id, last_event_time, last_event_id
                FROM user_value_stats WHERE user_name = ? AND value_type = ? AND value_key = ?
                """,
                (fields["user_name"], value_type, value_key),
            ).fetchone()
            if old is None:
                self.connection.execute(
                    """
                    INSERT INTO user_value_stats(
                        user_name, value_type, value_key, display_value,
                        first_event_time, first_event_id, last_event_time, last_event_id,
                        event_count, updated_revision, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        fields["user_name"], value_type, value_key, display,
                        fields["event_time"], fields["event_id"], fields["event_time"], fields["event_id"],
                        revision, _now_ms(),
                    ),
                )
                self._change_first_seen(fields["user_name"], value_type, fields["event_time"], 1, revision)
                continue

            first_key = (old["first_event_time"], old["first_event_id"])
            current_key = (fields["event_time"], fields["event_id"])
            last_key = (old["last_event_time"], old["last_event_id"])
            first_time, first_id = first_key if first_key <= current_key else current_key
            last_time, last_id = last_key if last_key >= current_key else current_key
            self.connection.execute(
                """
                UPDATE user_value_stats SET
                    display_value = ?, first_event_time = ?, first_event_id = ?,
                    last_event_time = ?, last_event_id = ?, event_count = event_count + 1,
                    updated_revision = ?, updated_at = ?
                WHERE user_name = ? AND value_type = ? AND value_key = ?
                """,
                (
                    display, first_time, first_id, last_time, last_id, revision, _now_ms(),
                    fields["user_name"], value_type, value_key,
                ),
            )
            if current_key < first_key:
                self._change_first_seen(fields["user_name"], value_type, old["first_event_time"], -1, revision)
                self._change_first_seen(fields["user_name"], value_type, fields["event_time"], 1, revision)

    def _change_first_seen(self, user_name: str, value_type: str, event_time: int, delta: int, revision: int) -> None:
        column = {
            "source_ip": "first_seen_ip_count",
            "resource": "first_seen_resource_count",
            "action": "first_seen_action_count",
        }.get(value_type)
        if column is None:
            return
        for window_size in self.config.window_sizes_seconds:
            start = _window_start(event_time, window_size)
            self.connection.execute(
                f"UPDATE user_windows SET {column} = {column} + ?, updated_revision = ?, updated_at = ? "
                "WHERE user_name = ? AND window_size = ? AND window_start = ?",
                (delta, revision, _now_ms(), user_name, window_size, start),
            )

    def _update_activity_bounds(self, fields: dict[str, Any], revision: int) -> None:
        row = self.connection.execute(
            "SELECT * FROM user_activity_bounds WHERE user_name = ?", (fields["user_name"],)
        ).fetchone()
        if row is None:
            self.connection.execute(
                """
                INSERT INTO user_activity_bounds(
                    user_name, first_event_time, first_event_id, last_event_time, last_event_id,
                    updated_revision, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fields["user_name"], fields["event_time"], fields["event_id"],
                    fields["event_time"], fields["event_id"], revision, _now_ms(),
                ),
            )
            return
        candidate = (fields["event_time"], fields["event_id"])
        first = min((row["first_event_time"], row["first_event_id"]), candidate)
        last = max((row["last_event_time"], row["last_event_id"]), candidate)
        self.connection.execute(
            """
            UPDATE user_activity_bounds SET first_event_time = ?, first_event_id = ?,
                last_event_time = ?, last_event_id = ?, updated_revision = ?, updated_at = ?
            WHERE user_name = ?
            """,
            (*first, *last, revision, _now_ms(), fields["user_name"]),
        )

    def _update_sessions(self, fields: dict[str, Any], revision: int) -> int:
        gap = self.config.session_gap_seconds * 1000
        matches = self.connection.execute(
            """
            SELECT * FROM user_sessions
            WHERE user_name = ? AND status = 'provisional'
              AND start_time <= ? AND last_event_time >= ?
            ORDER BY start_time, first_event_id, session_row_id
            """,
            (fields["user_name"], fields["event_time"] + gap, fields["event_time"] - gap),
        ).fetchall()
        if not matches:
            return self._insert_session(fields, revision)
        survivor = min(matches, key=lambda row: (row["start_time"], row["first_event_id"], row["session_row_id"]))
        all_rows = list(matches)
        start_time, first_id = min(
            [(fields["event_time"], fields["event_id"])] + [(row["start_time"], row["first_event_id"]) for row in all_rows]
        )
        last_time, last_id = max(
            [(fields["event_time"], fields["event_id"])] + [(row["last_event_time"], row["last_event_id"]) for row in all_rows]
        )
        event_count = 1 + sum(int(row["event_count"]) for row in all_rows)
        action_count = int(bool(fields["action"])) + sum(int(row["action_count"]) for row in all_rows)
        source_samples, source_capped = _merge_samples(
            [row["source_type_samples_json"] for row in all_rows], [fields["source_type"]],
            [bool(row["source_type_samples_capped"]) for row in all_rows], self.config.sample_limit,
        )
        ip_samples, ip_capped = _merge_samples(
            [row["source_ip_samples_json"] for row in all_rows], _optional_list(fields["source_ip"]),
            [bool(row["source_ip_samples_capped"]) for row in all_rows], self.config.sample_limit,
        )
        resource_samples, resource_capped = _merge_samples(
            [row["resource_samples_json"] for row in all_rows], _optional_list(fields["resource_display"]),
            [bool(row["resource_samples_capped"]) for row in all_rows], self.config.sample_limit,
        )
        first_actions, last_actions, sequence_capped = _merge_actions(
            all_rows, fields, self.config.sample_limit, action_count,
        )
        self.connection.execute(
            """
            UPDATE user_sessions SET
                start_time = ?, first_event_id = ?, last_event_time = ?, last_event_id = ?,
                event_count = ?, action_count = ?, source_type_samples_json = ?, source_type_samples_capped = ?,
                source_ip_samples_json = ?, source_ip_samples_capped = ?,
                resource_samples_json = ?, resource_samples_capped = ?, first_actions_json = ?, last_actions_json = ?,
                sequence_capped = ?, updated_revision = ?, updated_at = ?
            WHERE session_row_id = ?
            """,
            (
                start_time, first_id, last_time, last_id, event_count, action_count,
                json.dumps(source_samples), int(source_capped), json.dumps(ip_samples), int(ip_capped),
                json.dumps(resource_samples), int(resource_capped), json.dumps(first_actions), json.dumps(last_actions),
                int(sequence_capped), revision, _now_ms(), survivor["session_row_id"],
            ),
        )
        for row in all_rows:
            if row["session_row_id"] != survivor["session_row_id"]:
                self.connection.execute(
                    """
                    UPDATE user_sessions SET status = 'merged', merged_into_row_id = ?,
                        updated_revision = ?, updated_at = ? WHERE session_row_id = ?
                    """,
                    (survivor["session_row_id"], revision, _now_ms(), row["session_row_id"]),
                )
        return int(survivor["session_row_id"])

    def _insert_session(self, fields: dict[str, Any], revision: int) -> int:
        action_rows = [[fields["event_time"], fields["event_id"], fields["action"]]] if fields["action"] else []
        cursor = self.connection.execute(
            """
            INSERT INTO user_sessions(
                session_id, user_name, start_time, first_event_id, last_event_time, last_event_id,
                event_count, action_count, status, closed_at, close_reason, merged_into_row_id,
                source_type_samples_json, source_type_samples_capped,
                source_ip_samples_json, source_ip_samples_capped,
                resource_samples_json, resource_samples_capped,
                first_actions_json, last_actions_json, sequence_capped, updated_revision, updated_at
            ) VALUES (NULL, ?, ?, ?, ?, ?, 1, ?, 'provisional', NULL, NULL, NULL, ?, 0, ?, 0, ?, 0, ?, ?, 0, ?, ?)
            """,
            (
                fields["user_name"], fields["event_time"], fields["event_id"], fields["event_time"], fields["event_id"],
                int(bool(fields["action"])), json.dumps([fields["source_type"]]),
                json.dumps(_optional_list(fields["source_ip"])), json.dumps(_optional_list(fields["resource_display"])),
                json.dumps(action_rows), json.dumps(action_rows), revision, _now_ms(),
            ),
        )
        return int(cursor.lastrowid)

    def _close_session(self, row: sqlite3.Row, reason: str, revision: int) -> None:
        session_id = _session_id(row["user_name"], row["start_time"], row["first_event_id"])
        self.connection.execute(
            """
            UPDATE user_sessions SET status = 'closed', session_id = ?, closed_at = ?, close_reason = ?,
                updated_revision = ?, updated_at = ? WHERE session_row_id = ?
            """,
            (session_id, _now_ms(), reason, revision, _now_ms(), row["session_row_id"]),
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

    def _next_revision(self) -> int:
        revision = int(self._meta("current_revision", "0")) + 1
        self._set_meta("current_revision", str(revision))
        return revision

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS feature_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT PRIMARY KEY, event_time INTEGER, user_name TEXT, status TEXT NOT NULL,
                skip_reason TEXT, canonical_schema_version TEXT NOT NULL, feature_schema_version TEXT NOT NULL,
                processed_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS event_evidence (
                event_id TEXT PRIMARY KEY, feature_revision INTEGER NOT NULL, event_time INTEGER NOT NULL, user_name TEXT NOT NULL,
                category TEXT, event_type TEXT, action TEXT NOT NULL, outcome TEXT NOT NULL,
                source_id TEXT NOT NULL, source_type TEXT NOT NULL, source_ip TEXT, destination_ip TEXT, host_name TEXT,
                resource_type TEXT, resource_name TEXT, http_method TEXT, http_status INTEGER,
                parser_id TEXT, parser_version TEXT, record_id TEXT, checksum TEXT, storage_ref TEXT
            );
            CREATE TABLE IF NOT EXISTS user_windows (
                user_name TEXT NOT NULL, window_size INTEGER NOT NULL, window_start INTEGER NOT NULL, window_end INTEGER NOT NULL,
                event_count INTEGER NOT NULL, success_count INTEGER NOT NULL, failure_count INTEGER NOT NULL,
                other_outcome_count INTEGER NOT NULL, source_type_distinct_count INTEGER NOT NULL,
                source_ip_distinct_count INTEGER NOT NULL, resource_distinct_count INTEGER NOT NULL,
                action_distinct_count INTEGER NOT NULL, read_action_count INTEGER NOT NULL, write_action_count INTEGER NOT NULL,
                network_bytes_total INTEGER NOT NULL, first_seen_ip_count INTEGER NOT NULL,
                first_seen_resource_count INTEGER NOT NULL, first_seen_action_count INTEGER NOT NULL,
                updated_revision INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY(user_name, window_size, window_start)
            );
            CREATE TABLE IF NOT EXISTS window_value_counts (
                user_name TEXT NOT NULL, window_size INTEGER NOT NULL, window_start INTEGER NOT NULL,
                value_type TEXT NOT NULL, value_key TEXT NOT NULL, display_value TEXT NOT NULL,
                event_count INTEGER NOT NULL, updated_revision INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY(user_name, window_size, window_start, value_type, value_key)
            );
            CREATE TABLE IF NOT EXISTS user_value_stats (
                user_name TEXT NOT NULL, value_type TEXT NOT NULL, value_key TEXT NOT NULL, display_value TEXT NOT NULL,
                first_event_time INTEGER NOT NULL, first_event_id TEXT NOT NULL, last_event_time INTEGER NOT NULL,
                last_event_id TEXT NOT NULL, event_count INTEGER NOT NULL, updated_revision INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY(user_name, value_type, value_key)
            );
            CREATE TABLE IF NOT EXISTS user_activity_bounds (
                user_name TEXT PRIMARY KEY, first_event_time INTEGER NOT NULL, first_event_id TEXT NOT NULL,
                last_event_time INTEGER NOT NULL, last_event_id TEXT NOT NULL,
                updated_revision INTEGER NOT NULL, updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_row_id INTEGER PRIMARY KEY, session_id TEXT, user_name TEXT NOT NULL,
                start_time INTEGER NOT NULL, first_event_id TEXT NOT NULL, last_event_time INTEGER NOT NULL, last_event_id TEXT NOT NULL,
                event_count INTEGER NOT NULL, action_count INTEGER NOT NULL, status TEXT NOT NULL,
                closed_at INTEGER, close_reason TEXT, merged_into_row_id INTEGER,
                source_type_samples_json TEXT NOT NULL, source_type_samples_capped INTEGER NOT NULL,
                source_ip_samples_json TEXT NOT NULL, source_ip_samples_capped INTEGER NOT NULL,
                resource_samples_json TEXT NOT NULL, resource_samples_capped INTEGER NOT NULL,
                first_actions_json TEXT NOT NULL, last_actions_json TEXT NOT NULL, sequence_capped INTEGER NOT NULL,
                updated_revision INTEGER NOT NULL, updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_windows_user_time ON user_windows(user_name, window_start);
            CREATE INDEX IF NOT EXISTS idx_event_evidence_user_time ON event_evidence(user_name, event_time, event_id);
            CREATE INDEX IF NOT EXISTS idx_event_evidence_revision ON event_evidence(feature_revision, event_id);
            CREATE INDEX IF NOT EXISTS idx_window_values_user_time ON window_value_counts(user_name, window_size, window_start);
            CREATE INDEX IF NOT EXISTS idx_value_stats_user ON user_value_stats(user_name, value_type, value_key);
            CREATE INDEX IF NOT EXISTS idx_sessions_user_status ON user_sessions(user_name, status, start_time, last_event_time);
            """
        )
        self.connection.commit()

    def _reject_old_schema(self) -> None:
        table = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='feature_meta'"
        ).fetchone()
        if table is None:
            return
        row = self.connection.execute(
            "SELECT value FROM feature_meta WHERE key='feature_schema_version'"
        ).fetchone()
        if row is not None and row[0] != FEATURE_SCHEMA_VERSION:
            raise FeatureError(
                "feature schema version changed; rebuild Feature, Baseline, Detection, Incident, Risk, Case, and Evaluation DBs from Canonical/Raw"
            )

    def _initialize_meta(self) -> None:
        existing_schema = self._meta("feature_schema_version")
        fingerprint = self.config.fingerprint()
        if existing_schema is None:
            values = {
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "canonical_schema_version": CANONICAL_SCHEMA_VERSION,
                "config_fingerprint": fingerprint,
                "mode": self.mode or "replay",
                "current_revision": "0",
                "current_watermark_ms": "-1",
                "replay_finalized": "0",
            }
            self.connection.executemany("INSERT INTO feature_meta(key, value) VALUES (?, ?)", values.items())
            self.connection.commit()
            return
        if existing_schema != FEATURE_SCHEMA_VERSION:
            raise FeatureError("feature schema version changed; rebuild Feature DB")
        if self._meta("canonical_schema_version") != CANONICAL_SCHEMA_VERSION:
            raise FeatureError("canonical schema version changed; rebuild Feature DB")
        if self._meta("config_fingerprint") != fingerprint:
            raise FeatureError("feature configuration changed; rebuild Feature DB")
        stored_mode = self._meta("mode")
        if self.mode == "replay" and stored_mode == "realtime":
            raise FeatureError("realtime feature state cannot be used for historical build; rebuild Feature DB")
        if self.mode == "realtime" and stored_mode == "replay":
            if self._meta("replay_finalized", "0") != "1":
                raise FeatureError("complete replay initialization before entering realtime mode")
            self._set_meta("mode", "realtime")
            self.connection.commit()

    def _meta(self, key: str, default: str | None = None) -> str | None:
        row = self.connection.execute("SELECT value FROM feature_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default

    def _set_meta(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO feature_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def build_features_from_jsonl(input_path: Path | str, state_db: Path | str, batch_size: int = 1000) -> dict[str, int]:
    """Create a replay-mode state database from one complete normalized JSONL history."""
    path = Path(input_path)
    if not path.exists():
        raise FeatureError(f"normalized input does not exist: {path}")
    summary = {
        "total_records": 0,
        "processed": 0,
        "duplicates": 0,
        "rejected": 0,
        "windows_updated": 0,
        "sessions_closed": 0,
    }
    with FeatureEngine(state_db, mode="replay") as engine:
        engine.ensure_replay_build_allowed()

        def events() -> Iterator[dict[str, Any]]:
            with path.open("rt", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    try:
                        value = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise FeatureError(f"invalid JSONL at line {line_number}") from exc
                    if not isinstance(value, dict):
                        raise FeatureError(f"JSONL record at line {line_number} must be an object")
                    yield value

        for result in engine.process_events(events(), batch_size=batch_size):
            summary["total_records"] += 1
            if result.status == "processed":
                summary["processed"] += 1
                summary["windows_updated"] += result.windows_updated
            elif result.status == "duplicate":
                summary["duplicates"] += 1
            else:
                summary["rejected"] += 1
        summary["sessions_closed"] = engine.flush_sessions()
    return summary


def query_feature_state(
    state_db: Path | str,
    user_name: str,
    start: str | int | datetime,
    end: str | int | datetime,
    window_size: int | None = None,
) -> dict[str, Any]:
    with FeatureEngine(state_db, mode=None) as engine:
        return {
            "user_name": user_name,
            "from": _iso(_timestamp_ms(start)),
            "to": _iso(_timestamp_ms(end)),
            "windows": engine.query_windows(user_name, start, end, window_size),
            "sessions": engine.query_sessions(user_name, start, end),
            "current_revision": engine.current_revision(),
        }


def _event_fields(event: dict[str, Any]) -> dict[str, Any]:
    event_id = _required_string(event, "event.id")
    event_time = _timestamp_ms(_required_string(event, "event.time"))
    user_name = _required_string(event, "user.name")
    source_type = _required_string(event, "raw.source_type")
    source_id = _required_string(event, "raw.source_id")
    action = _required_string(event, "event.action")
    outcome = _required_string(event, "event.outcome")
    source_ip = _optional_string(event, "source.ip")
    resource_type = _optional_string(event, "resource.type")
    resource_id = _optional_string(event, "resource.id")
    resource_name = _optional_string(event, "resource.name")
    resource_key, resource_display = _resource_key(resource_type, resource_id, resource_name)
    http_status = _path(event, "http.response.status_code")
    if http_status is not None and (isinstance(http_status, bool) or not isinstance(http_status, int)):
        raise FeatureError("http.response.status_code must be an integer")
    network_bytes = _path(event, "network.bytes")
    if network_bytes is None:
        network_bytes = 0
    if isinstance(network_bytes, bool) or not isinstance(network_bytes, int) or network_bytes < 0 or network_bytes > INT64_MAX:
        raise FeatureError("network.bytes must be a non-negative int64")
    return {
        "event_id": event_id,
        "event_time": event_time,
        "user_name": user_name,
        "category": _optional_string(event, "event.category"),
        "event_type": _optional_string(event, "event.type"),
        "source_type": source_type,
        "source_id": source_id,
        "source_ip": source_ip,
        "destination_ip": _optional_string(event, "destination.ip"),
        "host_name": _optional_string(event, "host.name"),
        "resource_type": resource_type,
        "resource_name": resource_name,
        "resource_key": resource_key,
        "resource_display": resource_display,
        "action": action,
        "outcome": outcome,
        "http_method": _optional_string(event, "http.request.method"),
        "http_status": http_status,
        "parser_id": _optional_string(event, "parser.id"),
        "parser_version": _optional_string(event, "parser.version"),
        "record_id": _optional_string(event, "raw.record_id"),
        "checksum": _optional_string(event, "raw.checksum"),
        "storage_ref": _optional_string(event, "raw.storage_ref"),
        "network_bytes": network_bytes,
    }


def _window_values(fields: dict[str, Any]) -> list[tuple[str, str, str]]:
    values = [("source_type", fields["source_type"], fields["source_type"]), ("action", fields["action"], fields["action"])]
    if fields["source_ip"] is not None:
        values.append(("source_ip", fields["source_ip"], fields["source_ip"]))
    if fields["resource_key"] is not None:
        values.append(("resource", fields["resource_key"], fields["resource_display"] or fields["resource_key"]))
    return values


def _resource_key(resource_type: str | None, resource_id: str | None, resource_name: str | None) -> tuple[str | None, str | None]:
    if resource_type is None and resource_id is None and resource_name is None:
        return None, None
    kind = resource_type or "<unknown_type>"
    value = resource_id if resource_id is not None else (resource_name or "<unknown_resource>")
    return f"{kind}:{value}", resource_name or value


def _required_string(event: dict[str, Any], field_path: str) -> str:
    value = _path(event, field_path)
    if not isinstance(value, str) or not value:
        raise FeatureError(f"missing_{field_path.replace('.', '_')}")
    return value


def _optional_string(event: dict[str, Any], field_path: str) -> str | None:
    value = _path(event, field_path)
    return value if isinstance(value, str) and value else None


def _path(document: dict[str, Any], field_path: str) -> Any:
    current: Any = document
    for part in field_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _timestamp_ms(value: str | int | datetime) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FeatureError("invalid_event_time") from exc
    else:
        raise FeatureError("invalid_event_time")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _window_start(event_time_ms: int, window_size_seconds: int) -> int:
    width = window_size_seconds * 1000
    return (event_time_ms // width) * width


def _checked_add(left: int, right: int) -> int:
    result = left + right
    if result < INT64_MIN or result > INT64_MAX:
        raise FeatureError("int64 aggregation overflow")
    return result


def _outcome_class(outcome: str, config: FeatureConfig) -> str:
    normalized = outcome.lower()
    for kind in ("success", "failure"):
        if normalized in config.outcome_classes.get(kind, ()):
            return kind
    return "other"


def _action_class(action: str, config: FeatureConfig) -> str | None:
    normalized = action.lower()
    for kind in ("read", "write"):
        if any(normalized.endswith(suffix) for suffix in config.action_classes.get(kind, ())):
            return kind
    return None


def _merge_samples(
    serialized_lists: list[str], additions: list[str], capped_values: list[bool], limit: int,
) -> tuple[list[str], bool]:
    values: set[str] = set(additions)
    for serialized in serialized_lists:
        values.update(str(value) for value in json.loads(serialized))
    ordered = sorted(values)
    return ordered[:limit], any(capped_values) or len(ordered) > limit


def _merge_actions(
    rows: list[sqlite3.Row], fields: dict[str, Any], limit: int, action_count: int,
) -> tuple[list[list[Any]], list[list[Any]], bool]:
    entries: dict[tuple[int, str], list[Any]] = {}
    for row in rows:
        for serialized in (row["first_actions_json"], row["last_actions_json"]):
            for entry in json.loads(serialized):
                entries[(int(entry[0]), str(entry[1]))] = entry
    if fields["action"]:
        entry = [fields["event_time"], fields["event_id"], fields["action"]]
        entries[(fields["event_time"], fields["event_id"])] = entry
    ordered = [entries[key] for key in sorted(entries)]
    capped = any(bool(row["sequence_capped"]) for row in rows) or action_count > limit
    return ordered[:limit], ordered[-limit:] if limit else [], capped


def _optional_list(value: str | None) -> list[str]:
    return [value] if value is not None else []


def _session_id(user_name: str, start_time: int, first_event_id: str) -> str:
    identity = f"{user_name}:{start_time}:{first_event_id}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _serialize_row(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    for field in (
        "window_start", "window_end", "first_event_time", "last_event_time", "closed_at", "updated_at", "processed_at",
    ):
        if field in result and result[field] is not None:
            result[field] = _iso(int(result[field]))
    for field, value in list(result.items()):
        if field.endswith("_json") and isinstance(value, str):
            result[field] = json.loads(value)
    return result


def _iso(value_ms: int) -> str:
    return datetime.fromtimestamp(value_ms / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
