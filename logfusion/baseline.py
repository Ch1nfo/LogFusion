from __future__ import annotations

import hashlib
import csv
import json
import math
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Iterator


BASELINE_SCHEMA_VERSION = "3"
FEATURE_SCHEMA_VERSION = "2"
WINDOW_SIZES = (3600, 86400)
METRICS = (
    "event_count", "success_count", "failure_count", "other_outcome_count",
    "source_type_distinct_count", "source_ip_distinct_count", "resource_distinct_count",
    "action_distinct_count", "read_action_count", "write_action_count", "network_bytes_total",
    "first_seen_ip_count", "first_seen_resource_count", "first_seen_action_count",
)


class BaselineError(ValueError):
    """Raised when a baseline state is incompatible or cannot be built."""


@dataclass(frozen=True)
class BaselineConfig:
    history_days: int = 30
    window_sizes_seconds: tuple[int, ...] = WINDOW_SIZES
    value_window_size_seconds: int = 3600
    min_active_days: int = 14
    min_samples: int = 20
    min_peer_members: int = 5
    min_peer_active_days: int = 14
    min_peer_samples: int = 100

    def fingerprint(self) -> str:
        document = json.dumps(asdict(self), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(document.encode("utf-8")).hexdigest()


class BaselineEngine:
    """Builds deterministic personal and observed-only global baselines from Feature DB."""

    def __init__(
        self,
        feature_state: Path | str,
        baseline_state: Path | str,
        config: BaselineConfig | None = None,
        peer_groups_path: Path | str | None = None,
    ) -> None:
        self.feature_path = Path(feature_state)
        if not self.feature_path.exists():
            raise BaselineError(f"feature state does not exist: {self.feature_path}")
        self.baseline_path = Path(baseline_state)
        self.baseline_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or BaselineConfig()
        self.peer_groups_path = Path(peer_groups_path) if peer_groups_path else None
        self.peer_groups = _load_peer_groups(self.peer_groups_path) if self.peer_groups_path else {}
        self.peer_groups_fingerprint = _peer_groups_fingerprint(self.peer_groups_path)
        if self.config.history_days <= 0 or self.config.min_active_days <= 0 or self.config.min_samples <= 0:
            raise BaselineError("baseline configuration values must be positive")
        if self.config.value_window_size_seconds not in self.config.window_sizes_seconds:
            raise BaselineError("value window size must be one of baseline window sizes")

        self.feature = sqlite3.connect(f"file:{self.feature_path}?mode=ro", uri=True)
        self.feature.row_factory = sqlite3.Row
        self.connection = sqlite3.connect(self.baseline_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self._verify_feature_contract()
        self._create_schema()
        self._initialize_meta()

    def close(self) -> None:
        self.feature.close()
        self.connection.close()

    def __enter__(self) -> BaselineEngine:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def update(self, as_of: str | int | datetime | None = None) -> dict[str, Any]:
        feature_revision = self._feature_revision()
        as_of_ms = _timestamp_ms(as_of) if as_of is not None else self._feature_latest_event_time()
        if as_of_ms is None:
            with self._transaction():
                self._set_meta("last_consumed_revision", str(feature_revision))
            return {
                "as_of": None,
                "processed_revision": feature_revision,
                "recomputed_users": 0,
                "global_recomputed": False,
                "warming_up_users": 0,
            }

        previous_as_of = self._meta_int("as_of_ms")
        previous_revision = self._meta_int("last_consumed_revision", 0)
        full_recompute = previous_as_of is None or previous_as_of != as_of_ms
        if full_recompute:
            users = self._all_users()
        else:
            users = self._changed_users(previous_revision)

        global_recompute = full_recompute or bool(users) or feature_revision != previous_revision
        with self._transaction():
            statuses: dict[str, str] = {}
            for user_name in users:
                statuses[user_name] = self._recompute_user(user_name, as_of_ms, feature_revision)
            if global_recompute:
                self._recompute_global(as_of_ms, feature_revision)
            if self.peer_groups:
                groups = sorted(set(self.peer_groups.values())) if full_recompute else sorted({self.peer_groups[user] for user in users if user in self.peer_groups})
                for group_id in groups:
                    self._recompute_peer_group(group_id, as_of_ms, feature_revision)
            self._set_meta("last_consumed_revision", str(feature_revision))
            self._set_meta("as_of_ms", str(as_of_ms))

        return {
            "as_of": _iso(as_of_ms),
            "processed_revision": feature_revision,
            "recomputed_users": len(users),
            "global_recomputed": global_recompute,
            "warming_up_users": sum(status == "warming_up" for status in statuses.values()),
            "recomputed_peer_groups": len(groups) if self.peer_groups else 0,
        }

    def query_user(self, user_name: str, window_size: int | None = None) -> dict[str, Any]:
        metric_sql = "SELECT * FROM user_metric_baselines WHERE user_name = ?"
        metric_params: list[Any] = [user_name]
        global_sql = "SELECT * FROM global_metric_baselines WHERE 1 = 1"
        global_params: list[Any] = []
        if window_size is not None:
            metric_sql += " AND window_size = ?"
            metric_params.append(window_size)
            global_sql += " AND window_size = ?"
            global_params.append(window_size)
        metric_sql += " ORDER BY window_size, metric"
        global_sql += " ORDER BY window_size, metric"
        user_values = self.connection.execute(
            "SELECT * FROM user_value_baselines WHERE user_name = ? ORDER BY value_type, event_count DESC, value_key",
            (user_name,),
        ).fetchall()
        value_keys = {(row["value_type"], row["value_key"]) for row in user_values}
        global_values = self.connection.execute(
            "SELECT * FROM global_value_baselines ORDER BY value_type, event_count DESC, value_key"
        ).fetchall()
        group_row = self.connection.execute("SELECT group_id FROM peer_group_memberships WHERE user_name = ?", (user_name,)).fetchone()
        group_id = group_row["group_id"] if group_row else None
        peer_metrics = self.connection.execute("SELECT * FROM peer_group_metric_baselines WHERE group_id = ? ORDER BY window_size, metric", (group_id,)).fetchall() if group_id else []
        peer_values = self.connection.execute("SELECT * FROM peer_group_value_baselines WHERE group_id = ? ORDER BY value_type, event_count DESC", (group_id,)).fetchall() if group_id else []
        return {
            "user_name": user_name,
            "as_of": _iso(self._meta_int("as_of_ms")) if self._meta_int("as_of_ms") is not None else None,
            "processed_revision": self._meta_int("last_consumed_revision", 0),
            "metrics": [_serialize(row) for row in self.connection.execute(metric_sql, metric_params).fetchall()],
            "values": [_serialize(row) for row in user_values],
            "global_metrics": [_serialize(row) for row in self.connection.execute(global_sql, global_params).fetchall()],
            "global_values": [_serialize(row) for row in global_values if (row["value_type"], row["value_key"]) in value_keys],
            "group_id": group_id,
            "peer_group_metrics": [_serialize(row) for row in peer_metrics],
            "peer_group_values": [_serialize(row) for row in peer_values],
        }

    def _recompute_user(self, user_name: str, as_of_ms: int, revision: int) -> str:
        self.connection.execute("DELETE FROM user_metric_baselines WHERE user_name = ?", (user_name,))
        self.connection.execute("DELETE FROM user_value_baselines WHERE user_name = ?", (user_name,))
        bounds = self.feature.execute(
            "SELECT first_event_time, last_event_time FROM user_activity_bounds WHERE user_name = ?", (user_name,)
        ).fetchone()
        if bounds is None:
            return "warming_up"
        history_start = as_of_ms - self.config.history_days * 86400 * 1000
        user_start = max(history_start, int(bounds["first_event_time"]))
        user_last = min(as_of_ms, int(bounds["last_event_time"]))
        overall_status = "ready"
        for window_size in self.config.window_sizes_seconds:
            width = window_size * 1000
            records = self._user_windows(user_name, window_size, (user_start // width) * width, user_last)
            active_days = _active_days(records)
            values_by_metric = _personal_metric_values(records, user_start, user_last, window_size)
            status = _status(active_days, len(next(iter(values_by_metric.values()), [])), self.config)
            if status != "ready":
                overall_status = "warming_up"
            for metric, values in values_by_metric.items():
                self._insert_metric("user_metric_baselines", (user_name,), window_size, metric, values, active_days, user_start, as_of_ms, revision, status)
        self._recompute_user_values(user_name, user_start, user_last, as_of_ms, revision, overall_status)
        return overall_status

    def _recompute_global(self, as_of_ms: int, revision: int) -> None:
        self.connection.execute("DELETE FROM global_metric_baselines")
        self.connection.execute("DELETE FROM global_value_baselines")
        history_start = as_of_ms - self.config.history_days * 86400 * 1000
        for window_size in self.config.window_sizes_seconds:
            records = self._global_windows(window_size, history_start, as_of_ms)
            active_days = _active_days(records)
            status = _status(active_days, len(records), self.config)
            for metric in METRICS:
                values = [float(row[metric]) for row in records]
                self._insert_metric("global_metric_baselines", (), window_size, metric, values, active_days, history_start, as_of_ms, revision, status)
        self._recompute_global_values(history_start, as_of_ms, revision)

    def _recompute_peer_group(self, group_id: str, as_of_ms: int, revision: int) -> None:
        self.connection.execute("DELETE FROM peer_group_metric_baselines WHERE group_id = ?", (group_id,))
        self.connection.execute("DELETE FROM peer_group_value_baselines WHERE group_id = ?", (group_id,))
        members = [user for user, group in self.peer_groups.items() if group == group_id]
        if not members:
            return
        placeholders = ",".join("?" for _ in members)
        start = as_of_ms - self.config.history_days * 86400 * 1000
        for window_size in self.config.window_sizes_seconds:
            rows = self.feature.execute(f"SELECT * FROM user_windows WHERE user_name IN ({placeholders}) AND window_size = ? AND window_start >= ? AND window_start <= ? ORDER BY window_start, user_name", (*members, window_size, start, as_of_ms)).fetchall()
            member_count = len({row["user_name"] for row in rows if row["event_count"] > 0})
            active_days = _active_days(rows)
            status = "ready" if member_count >= self.config.min_peer_members and active_days >= self.config.min_peer_active_days and len(rows) >= self.config.min_peer_samples else "warming_up"
            for metric in METRICS:
                self._insert_metric("peer_group_metric_baselines", (group_id,), window_size, metric, [float(row[metric]) for row in rows], active_days, start, as_of_ms, revision, status, member_count)
        rows = self.feature.execute(f"SELECT value_type, value_key, MAX(display_value) display_value, SUM(event_count) event_count, COUNT(*) window_count, COUNT(DISTINCT user_name) user_count, MIN(window_start) first_seen, MAX(window_start) last_seen FROM window_value_counts WHERE user_name IN ({placeholders}) AND window_size = ? AND window_start >= ? AND window_start <= ? GROUP BY value_type, value_key", (*members, self.config.value_window_size_seconds, start, as_of_ms)).fetchall()
        totals: dict[str, int] = {}
        for row in rows: totals[row["value_type"]] = totals.get(row["value_type"], 0) + int(row["event_count"])
        for row in rows:
            self.connection.execute("INSERT INTO peer_group_value_baselines(group_id,value_type,value_key,display_value,event_count,window_count,user_count,first_seen,last_seen,share,status,as_of_ms,source_revision,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (group_id,row["value_type"],row["value_key"],row["display_value"],row["event_count"],row["window_count"],row["user_count"],row["first_seen"],row["last_seen"],row["event_count"]/totals[row["value_type"]],status,as_of_ms,revision,_now_ms()))

    def _recompute_user_values(
        self, user_name: str, start: int, data_end_ms: int, snapshot_as_of_ms: int, revision: int, status: str,
    ) -> None:
        rows = self.feature.execute(
            """
            SELECT value_type, value_key, MAX(display_value) AS display_value,
                   SUM(event_count) AS event_count, COUNT(*) AS window_count,
                   MIN(window_start) AS first_seen, MAX(window_start) AS last_seen
            FROM window_value_counts
            WHERE user_name = ? AND window_size = ? AND window_start >= ? AND window_start <= ?
            GROUP BY value_type, value_key
            """,
            (user_name, self.config.value_window_size_seconds, start, data_end_ms),
        ).fetchall()
        totals: dict[str, int] = {}
        for row in rows:
            totals[row["value_type"]] = totals.get(row["value_type"], 0) + int(row["event_count"])
        for row in rows:
            self.connection.execute(
                """
                INSERT INTO user_value_baselines(
                    user_name, value_type, value_key, display_value, event_count, window_count,
                    first_seen, last_seen, share, status, as_of_ms, source_revision, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_name, row["value_type"], row["value_key"], row["display_value"], int(row["event_count"]),
                    int(row["window_count"]), int(row["first_seen"]), int(row["last_seen"]),
                    int(row["event_count"]) / totals[row["value_type"]], status, snapshot_as_of_ms, revision, _now_ms(),
                ),
            )

    def _recompute_global_values(self, start: int, as_of_ms: int, revision: int) -> None:
        rows = self.feature.execute(
            """
            SELECT value_type, value_key, MAX(display_value) AS display_value,
                   SUM(event_count) AS event_count, COUNT(*) AS window_count,
                   COUNT(DISTINCT user_name) AS user_count,
                   MIN(window_start) AS first_seen, MAX(window_start) AS last_seen
            FROM window_value_counts
            WHERE window_size = ? AND window_start >= ? AND window_start <= ?
            GROUP BY value_type, value_key
            """,
            (self.config.value_window_size_seconds, start, as_of_ms),
        ).fetchall()
        totals: dict[str, int] = {}
        for row in rows:
            totals[row["value_type"]] = totals.get(row["value_type"], 0) + int(row["event_count"])
        for row in rows:
            self.connection.execute(
                """
                INSERT INTO global_value_baselines(
                    value_type, value_key, display_value, event_count, window_count, user_count,
                    first_seen, last_seen, share, as_of_ms, source_revision, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["value_type"], row["value_key"], row["display_value"], int(row["event_count"]),
                    int(row["window_count"]), int(row["user_count"]), int(row["first_seen"]), int(row["last_seen"]),
                    int(row["event_count"]) / totals[row["value_type"]], as_of_ms, revision, _now_ms(),
                ),
            )

    def _insert_metric(
        self, table: str, user_prefix: tuple[str, ...], window_size: int, metric: str, values: list[float],
        active_days: int, start: int, as_of_ms: int, revision: int, status: str, member_count: int | None = None,
    ) -> None:
        stats = _statistics(values)
        columns = (
            "window_size, metric, sample_count, active_day_count, mean, stddev, min_value, max_value, "
            "p50, p95, p99, mad, window_start, as_of_ms, source_revision, status, updated_at"
        )
        if table == "user_metric_baselines":
            columns = "user_name, " + columns
        elif table == "peer_group_metric_baselines":
            columns = "group_id, member_count, " + columns
            user_prefix = user_prefix + (member_count or 0,)
        placeholders = ", ".join("?" for _ in (user_prefix + (
            window_size, metric, stats["sample_count"], active_days, stats["mean"], stats["stddev"], stats["min_value"],
            stats["max_value"], stats["p50"], stats["p95"], stats["p99"], stats["mad"], start, as_of_ms, revision, status, _now_ms(),
        )))
        values_to_insert = user_prefix + (
            window_size, metric, stats["sample_count"], active_days, stats["mean"], stats["stddev"], stats["min_value"],
            stats["max_value"], stats["p50"], stats["p95"], stats["p99"], stats["mad"], start, as_of_ms, revision, status, _now_ms(),
        )
        self.connection.execute(f"INSERT INTO {table}({columns}) VALUES ({placeholders})", values_to_insert)

    def _user_windows(self, user_name: str, window_size: int, start: int, as_of_ms: int) -> list[sqlite3.Row]:
        return self.feature.execute(
            """
            SELECT * FROM user_windows
            WHERE user_name = ? AND window_size = ? AND window_start >= ? AND window_start <= ?
            ORDER BY window_start
            """,
            (user_name, window_size, start, as_of_ms),
        ).fetchall()

    def _global_windows(self, window_size: int, start: int, as_of_ms: int) -> list[sqlite3.Row]:
        return self.feature.execute(
            """
            SELECT * FROM user_windows
            WHERE window_size = ? AND window_start >= ? AND window_start <= ?
            ORDER BY window_start, user_name
            """,
            (window_size, start, as_of_ms),
        ).fetchall()

    def _all_users(self) -> list[str]:
        return [row[0] for row in self.feature.execute("SELECT user_name FROM user_activity_bounds ORDER BY user_name")]

    def _changed_users(self, revision: int) -> list[str]:
        rows = self.feature.execute(
            """
            SELECT DISTINCT user_name FROM user_windows WHERE updated_revision > ?
            UNION
            SELECT DISTINCT user_name FROM window_value_counts WHERE updated_revision > ?
            ORDER BY user_name
            """,
            (revision, revision),
        ).fetchall()
        return [row[0] for row in rows]

    def _feature_revision(self) -> int:
        value = self.feature.execute("SELECT value FROM feature_meta WHERE key = 'current_revision'").fetchone()
        return int(value[0]) if value else 0

    def _feature_latest_event_time(self) -> int | None:
        row = self.feature.execute("SELECT MAX(last_event_time) FROM user_activity_bounds").fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def _verify_feature_contract(self) -> None:
        values = {row["key"]: row["value"] for row in self.feature.execute("SELECT key, value FROM feature_meta")}
        if values.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
            raise BaselineError("feature schema version changed; rebuild Feature DB")
        if not values.get("config_fingerprint"):
            raise BaselineError("feature configuration fingerprint is missing")
        self.feature_config_fingerprint = values["config_fingerprint"]

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS baseline_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS user_metric_baselines (
                user_name TEXT NOT NULL, window_size INTEGER NOT NULL, metric TEXT NOT NULL,
                sample_count INTEGER NOT NULL, active_day_count INTEGER NOT NULL,
                mean REAL, stddev REAL, min_value REAL, max_value REAL, p50 REAL, p95 REAL, p99 REAL, mad REAL,
                window_start INTEGER NOT NULL, as_of_ms INTEGER NOT NULL, source_revision INTEGER NOT NULL,
                status TEXT NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY(user_name, window_size, metric)
            );
            CREATE TABLE IF NOT EXISTS global_metric_baselines (
                window_size INTEGER NOT NULL, metric TEXT NOT NULL,
                sample_count INTEGER NOT NULL, active_day_count INTEGER NOT NULL,
                mean REAL, stddev REAL, min_value REAL, max_value REAL, p50 REAL, p95 REAL, p99 REAL, mad REAL,
                window_start INTEGER NOT NULL, as_of_ms INTEGER NOT NULL, source_revision INTEGER NOT NULL,
                status TEXT NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY(window_size, metric)
            );
            CREATE TABLE IF NOT EXISTS user_value_baselines (
                user_name TEXT NOT NULL, value_type TEXT NOT NULL, value_key TEXT NOT NULL, display_value TEXT NOT NULL,
                event_count INTEGER NOT NULL, window_count INTEGER NOT NULL, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL,
                share REAL NOT NULL, status TEXT NOT NULL, as_of_ms INTEGER NOT NULL, source_revision INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY(user_name, value_type, value_key)
            );
            CREATE TABLE IF NOT EXISTS global_value_baselines (
                value_type TEXT NOT NULL, value_key TEXT NOT NULL, display_value TEXT NOT NULL,
                event_count INTEGER NOT NULL, window_count INTEGER NOT NULL, user_count INTEGER NOT NULL,
                first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, share REAL NOT NULL,
                as_of_ms INTEGER NOT NULL, source_revision INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY(value_type, value_key)
            );
            CREATE TABLE IF NOT EXISTS peer_group_memberships (user_name TEXT PRIMARY KEY, group_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS peer_group_metric_baselines (
                group_id TEXT NOT NULL, member_count INTEGER NOT NULL, window_size INTEGER NOT NULL, metric TEXT NOT NULL,
                sample_count INTEGER NOT NULL, active_day_count INTEGER NOT NULL, mean REAL, stddev REAL, min_value REAL, max_value REAL, p50 REAL, p95 REAL, p99 REAL, mad REAL,
                window_start INTEGER NOT NULL, as_of_ms INTEGER NOT NULL, source_revision INTEGER NOT NULL, status TEXT NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY(group_id, window_size, metric)
            );
            CREATE TABLE IF NOT EXISTS peer_group_value_baselines (
                group_id TEXT NOT NULL, value_type TEXT NOT NULL, value_key TEXT NOT NULL, display_value TEXT NOT NULL,
                event_count INTEGER NOT NULL, window_count INTEGER NOT NULL, user_count INTEGER NOT NULL, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, share REAL NOT NULL, status TEXT NOT NULL, as_of_ms INTEGER NOT NULL, source_revision INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                PRIMARY KEY(group_id, value_type, value_key)
            );
            CREATE INDEX IF NOT EXISTS idx_user_metric_baselines ON user_metric_baselines(user_name, window_size);
            CREATE INDEX IF NOT EXISTS idx_user_value_baselines ON user_value_baselines(user_name, value_type, event_count);
            """
        )
        self.connection.commit()

    def _initialize_meta(self) -> None:
        fingerprint = self.config.fingerprint()
        existing = self._meta("baseline_schema_version")
        if existing is None:
            values = {
                "baseline_schema_version": BASELINE_SCHEMA_VERSION,
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
                "feature_config_fingerprint": self.feature_config_fingerprint,
                "baseline_config_fingerprint": fingerprint,
                "baseline_config_json": json.dumps(asdict(self.config), sort_keys=True, separators=(",", ":")),
                "peer_groups_fingerprint": self.peer_groups_fingerprint,
                "last_consumed_revision": "0",
            }
            self.connection.executemany("INSERT INTO baseline_meta(key, value) VALUES (?, ?)", values.items())
            self.connection.executemany("INSERT INTO peer_group_memberships(user_name, group_id) VALUES (?, ?)", self.peer_groups.items())
            self.connection.commit()
            return
        if existing != BASELINE_SCHEMA_VERSION:
            raise BaselineError("baseline schema version changed; rebuild Baseline DB")
        if self._meta("feature_config_fingerprint") != self.feature_config_fingerprint:
            raise BaselineError("feature configuration changed; rebuild Baseline DB")
        if self._meta("baseline_config_fingerprint") != fingerprint:
            raise BaselineError("baseline configuration changed; rebuild Baseline DB")
        if self._meta("baseline_config_json") is None:
            self._set_meta("baseline_config_json", json.dumps(asdict(self.config), sort_keys=True, separators=(",", ":")))
            self.connection.commit()
        if self._meta("peer_groups_fingerprint") != self.peer_groups_fingerprint:
            raise BaselineError("peer group mapping changed; rebuild Baseline DB")

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
        row = self.connection.execute("SELECT value FROM baseline_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def _meta_int(self, key: str, default: int | None = None) -> int | None:
        value = self._meta(key)
        return int(value) if value is not None else default

    def _set_meta(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO baseline_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def _personal_metric_values(records: list[sqlite3.Row], start: int, as_of_ms: int, window_size: int) -> dict[str, list[float]]:
    values_by_start = {int(row["window_start"]): row for row in records}
    width = window_size * 1000
    first = (start // width) * width
    values: dict[str, list[float]] = {metric: [] for metric in METRICS}
    window_start = first
    while window_start <= as_of_ms:
        row = values_by_start.get(window_start)
        for metric in METRICS:
            values[metric].append(float(row[metric]) if row is not None else 0.0)
        window_start += width
    return values


def _active_days(records: Iterable[sqlite3.Row]) -> int:
    return len({int(row["window_start"]) // 86_400_000 for row in records if int(row["event_count"]) > 0})


def _status(active_days: int, sample_count: int, config: BaselineConfig) -> str:
    return "ready" if active_days >= config.min_active_days and sample_count >= config.min_samples else "warming_up"


def _statistics(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {key: None for key in ("mean", "stddev", "min_value", "max_value", "p50", "p95", "p99", "mad")} | {"sample_count": 0}
    ordered = sorted(values)
    mean = sum(ordered) / len(ordered)
    variance = sum((value - mean) ** 2 for value in ordered) / len(ordered)
    centre = _percentile(ordered, 0.5)
    return {
        "sample_count": len(ordered), "mean": mean, "stddev": math.sqrt(variance),
        "min_value": ordered[0], "max_value": ordered[-1], "p50": centre,
        "p95": _percentile(ordered, 0.95), "p99": _percentile(ordered, 0.99),
        "mad": median(abs(value - centre) for value in ordered),
    }


def _percentile(values: list[float], percentile: float) -> float:
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * percentile
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def _timestamp_ms(value: str | int | datetime) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise BaselineError("invalid as_of timestamp") from exc
    else:
        raise BaselineError("invalid as_of timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _serialize(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    for field in ("window_start", "as_of_ms", "first_seen", "last_seen", "updated_at"):
        if field in result and result[field] is not None:
            result[field] = _iso(int(result[field]))
    return result


def _load_peer_groups(path: Path) -> dict[str, str]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))
    except OSError as exc:
        raise BaselineError(f"cannot read peer groups: {exc}") from exc
    if not rows or rows[0] != ["user_name", "group_id"]:
        raise BaselineError("peer groups CSV header must be user_name,group_id")
    result: dict[str, str] = {}
    for row in rows[1:]:
        if len(row) != 2 or not row[0] or not row[1] or row[0] in result:
            raise BaselineError("peer groups CSV contains an invalid or duplicate user")
        result[row[0]] = row[1]
    return result


def _peer_groups_fingerprint(path: Path | None) -> str:
    if path is None:
        return "none"
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def query_baseline_state(baseline_state: Path | str, user_name: str, window_size: int | None = None) -> dict[str, Any]:
    """Read a baseline profile without opening or modifying its Feature DB."""
    path = Path(baseline_state)
    if not path.exists():
        raise BaselineError(f"baseline state does not exist: {path}")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        meta = {row["key"]: row["value"] for row in connection.execute("SELECT key, value FROM baseline_meta")}
        if meta.get("baseline_schema_version") != BASELINE_SCHEMA_VERSION:
            raise BaselineError("baseline schema version changed; rebuild Baseline DB")
        metric_sql = "SELECT * FROM user_metric_baselines WHERE user_name = ?"
        metric_params: list[Any] = [user_name]
        global_sql = "SELECT * FROM global_metric_baselines WHERE 1 = 1"
        global_params: list[Any] = []
        if window_size is not None:
            metric_sql += " AND window_size = ?"
            metric_params.append(window_size)
            global_sql += " AND window_size = ?"
            global_params.append(window_size)
        metric_sql += " ORDER BY window_size, metric"
        global_sql += " ORDER BY window_size, metric"
        user_values = connection.execute(
            "SELECT * FROM user_value_baselines WHERE user_name = ? ORDER BY value_type, event_count DESC, value_key",
            (user_name,),
        ).fetchall()
        value_keys = {(row["value_type"], row["value_key"]) for row in user_values}
        global_values = connection.execute(
            "SELECT * FROM global_value_baselines ORDER BY value_type, event_count DESC, value_key"
        ).fetchall()
        as_of = int(meta["as_of_ms"]) if "as_of_ms" in meta else None
        return {
            "user_name": user_name,
            "as_of": _iso(as_of) if as_of is not None else None,
            "processed_revision": int(meta.get("last_consumed_revision", "0")),
            "metrics": [_serialize(row) for row in connection.execute(metric_sql, metric_params).fetchall()],
            "values": [_serialize(row) for row in user_values],
            "global_metrics": [_serialize(row) for row in connection.execute(global_sql, global_params).fetchall()],
            "global_values": [_serialize(row) for row in global_values if (row["value_type"], row["value_key"]) in value_keys],
        }
    except sqlite3.Error as exc:
        raise BaselineError(f"invalid baseline state: {exc}") from exc
    finally:
        connection.close()
