from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from logfusion.rules import load_rules, match_rule, rule_fingerprint


DETECTION_SCHEMA_VERSION = "3"
FEATURE_SCHEMA_VERSION = "2"
BASELINE_SCHEMA_VERSION = "3"
EVIDENCE_LIMIT = 20
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
    p95_score: int = 60
    p99_score: int = 80
    new_value_score: int = 75
    rare_value_score: int = 55
    source_type_overrides: dict[str, dict[str, int | float]] = field(default_factory=dict)

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
        rules_path: Path | str | None = None,
    ) -> None:
        self.feature_path = Path(feature_state)
        self.baseline_path = Path(baseline_state)
        if not self.feature_path.exists() or not self.baseline_path.exists():
            raise DetectionError("feature state and baseline state must exist")
        self.detection_path = Path(detection_state)
        self.detection_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or DetectionConfig()
        self.rules_path = Path(rules_path) if rules_path is not None else None
        self.rules = load_rules(self.rules_path) if self.rules_path is not None else {"schema_version": 1, "rules": []}
        self.rules_fingerprint = _runtime_rules_fingerprint(self.rules)
        if not 0 < self.config.rare_share_threshold <= 1 or self.config.rare_event_count_max <= 0 or any(not 0 <= value <= 100 for value in (self.config.p95_score, self.config.p99_score, self.config.new_value_score, self.config.rare_value_score)):
            raise DetectionError("invalid detection configuration")
        self.feature = _readonly(self.feature_path)
        self.baseline = _readonly(self.baseline_path)
        self.connection = sqlite3.connect(self.detection_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self._verify_inputs()
        self._reject_old_schema()
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

    def run(self, refresh_rules: bool = False) -> dict[str, Any]:
        feature_revision = self._feature_revision()
        baseline_revision = self._baseline_revision()
        if baseline_revision != feature_revision:
            raise DetectionError("Baseline DB is behind Feature DB; run baseline build first")
        previous_revision = self._meta_int("last_consumed_feature_revision", 0)
        previous_detection_revision = self._meta_int("current_detection_revision", 0)
        stored_rules_fingerprint = self._meta("rules_fingerprint") or _fingerprint({"schema_version": 1, "rules": []})
        if stored_rules_fingerprint != self.rules_fingerprint and not refresh_rules:
            raise DetectionError("rule configuration changed; rerun detect with --refresh-rules")
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
            if refresh_rules:
                superseded_count += self.connection.execute(
                    "UPDATE anomaly_candidates SET status='superseded',updated_at=? WHERE detector_family='rule' AND status='active'",
                    (_now_ms(),),
                ).rowcount
            for window in rows:
                superseded_count += self._supersede_window(window)
                candidates, reasons = self._detect_window(window, baseline_revision)
                for reason in reasons:
                    skipped[reason] = skipped.get(reason, 0) + 1
                for candidate in candidates:
                    if self._insert_candidate(candidate):
                        candidate_count += 1
                        detector_count[candidate["detector_id"]] = detector_count.get(candidate["detector_id"], 0) + 1
            rule_report = self._run_rules(0 if refresh_rules else previous_revision)
            candidate_count += rule_report["candidate_count"]
            for detector_id, count in rule_report["by_detector"].items():
                detector_count[detector_id] = detector_count.get(detector_id, 0) + count
            self._set_meta("last_consumed_feature_revision", str(feature_revision))
            self._set_meta("last_consumed_baseline_revision", str(baseline_revision))
            self._set_meta("rules_fingerprint", self.rules_fingerprint)
            detection_revision = previous_detection_revision + int(feature_revision != previous_revision or refresh_rules)
            self._set_meta("current_detection_revision", str(detection_revision))
        return {
            "processed_detection_revision": detection_revision,
            "processed_feature_revision": feature_revision,
            "processed_baseline_revision": baseline_revision,
            "scanned_windows": len(rows),
            "candidate_count": candidate_count,
            "superseded_count": superseded_count,
            "by_detector": detector_count,
            "rules": rule_report,
            "skipped": skipped,
        }

    def _run_rules(self, after_revision: int) -> dict[str, Any]:
        rules = [rule for rule in self.rules["rules"] if rule["status"] in {"shadow", "active"}]
        count = 0
        by_detector: dict[str, int] = {}
        event_rules = [rule for rule in rules if rule["scope"] == "event"]
        if event_rules:
            events = self.feature.execute(
                "SELECT * FROM event_evidence WHERE feature_revision > ? ORDER BY feature_revision,event_id",
                (after_revision,),
            ).fetchall()
            for event in events:
                document = _event_rule_document(event)
                for rule in event_rules:
                    if match_rule(rule, document):
                        candidate = self._rule_event_candidate(rule, event)
                        if self._insert_candidate(candidate):
                            count += 1
                            by_detector[rule["rule_id"]] = by_detector.get(rule["rule_id"], 0) + 1
        window_rules = [rule for rule in rules if rule["scope"] == "window"]
        if window_rules:
            windows = self.feature.execute(
                "SELECT * FROM user_windows WHERE updated_revision > ? ORDER BY updated_revision,user_name,window_size,window_start",
                (after_revision,),
            ).fetchall()
            for window in windows:
                source_types = [row[0] for row in self.feature.execute(
                    "SELECT value_key FROM window_value_counts WHERE user_name=? AND window_size=? AND window_start=? AND value_type='source_type' ORDER BY value_key",
                    (window["user_name"], window["window_size"], window["window_start"]),
                )]
                document = {**dict(window), "source_types": source_types}
                for rule in window_rules:
                    if rule["window_size"] == window["window_size"] and match_rule(rule, document):
                        candidate = self._rule_window_candidate(rule, window, source_types)
                        if self._insert_candidate(candidate):
                            count += 1
                            by_detector[rule["rule_id"]] = by_detector.get(rule["rule_id"], 0) + 1
        return {"candidate_count": count, "by_detector": by_detector, "evaluated_rule_count": len(rules)}

    def _rule_event_candidate(self, rule: dict[str, Any], event: sqlite3.Row) -> dict[str, Any]:
        fingerprint = rule_fingerprint(rule)
        candidate_key = hashlib.sha256(f"rule:event:{rule['rule_id']}:{rule['version']}:{fingerprint}:{event['event_id']}".encode()).hexdigest()
        return {
            "candidate_id": hashlib.sha256(f"{candidate_key}:{event['feature_revision']}".encode()).hexdigest(),
            "candidate_key": candidate_key,
            "user_name": event["user_name"], "entity_type": "user", "entity_id": event["user_name"],
            "window_size": 0, "window_start": int(event["event_time"]), "window_end": int(event["event_time"]) + 1,
            "detector_family": "rule", "detector_id": rule["rule_id"], "detector_version": str(rule["version"]),
            "detector_mode": rule["status"], "config_fingerprint": fingerprint,
            "metric": None, "value_type": None, "value_key": None, "baseline_group_id": None,
            "score": rule["score"], "severity": _severity(rule["score"]), "baseline_scope": None,
            "feature_revision": int(event["feature_revision"]), "baseline_revision": None,
            "explanation": _rule_explanation(rule, "event"),
            "evidence": [_evidence(event, f"rule_match:{rule['rule_id']}")],
            "evidence_query": {"kind": "event", "event_count": 1, "evidence_truncated": False},
        }

    def _rule_window_candidate(self, rule: dict[str, Any], window: sqlite3.Row, source_types: list[str]) -> dict[str, Any]:
        fingerprint = rule_fingerprint(rule)
        identity = f"rule:window:{rule['rule_id']}:{rule['version']}:{fingerprint}:{window['user_name']}:{window['window_size']}:{window['window_start']}"
        candidate_key = hashlib.sha256(identity.encode()).hexdigest()
        evidence, query = self._window_evidence(window, rule.get("source_types") or None)
        for item in evidence:
            item["reason"] = f"rule_match:{rule['rule_id']}"
        return {
            "candidate_id": hashlib.sha256(f"{candidate_key}:{window['updated_revision']}".encode()).hexdigest(),
            "candidate_key": candidate_key,
            "user_name": window["user_name"], "entity_type": "user", "entity_id": window["user_name"],
            "window_size": int(window["window_size"]), "window_start": int(window["window_start"]), "window_end": int(window["window_end"]),
            "detector_family": "rule", "detector_id": rule["rule_id"], "detector_version": str(rule["version"]),
            "detector_mode": rule["status"], "config_fingerprint": fingerprint,
            "metric": None, "value_type": None, "value_key": None, "baseline_group_id": None,
            "score": rule["score"], "severity": _severity(rule["score"]), "baseline_scope": None,
            "feature_revision": int(window["updated_revision"]), "baseline_revision": None,
            "explanation": _rule_explanation(rule, "window"), "evidence": evidence, "evidence_query": query,
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
        return [_serialize_candidate(row, self.connection) for row in rows]

    def _detect_window(self, window: sqlite3.Row, baseline_revision: int) -> tuple[list[dict[str, Any]], list[str]]:
        candidates: list[dict[str, Any]] = []
        skipped: list[str] = []
        user_name = window["user_name"]
        window_size = int(window["window_size"])
        policy, policy_id = self._window_policy(window)
        user_ready = self._baseline_status("user_metric_baselines", user_name, window_size)
        group_id = self._group_id(user_name)
        peer_ready = bool(group_id) and self._baseline_status("peer_group_metric_baselines", group_id, window_size)
        global_ready = self._baseline_status("global_metric_baselines", None, window_size)
        if user_ready:
            scope = "personal"
        elif peer_ready:
            scope = "peer_group"
        elif global_ready:
            scope = "global"
        else:
            scope = None
            skipped.append("baseline_not_ready")
        if scope is not None:
            for metric in METRICS:
                reference = self._metric_reference(scope, user_name, window_size, metric, group_id)
                if reference is None or reference["p95"] is None or reference["p99"] is None:
                    continue
                value = float(window[metric])
                if value > float(reference["p99"]):
                    candidates.append(self._numeric_candidate(window, metric, value, reference, scope, "numeric_p99", int(policy["p99_score"]), baseline_revision, group_id))
                elif value > float(reference["p95"]):
                    candidates.append(self._numeric_candidate(window, metric, value, reference, scope, "numeric_p95", int(policy["p95_score"]), baseline_revision, group_id))
        candidates.extend(self._value_candidates(window, user_ready, peer_ready, global_ready, baseline_revision, policy, group_id))
        for candidate in candidates:
            candidate["explanation"]["policy"] = {"id": policy_id, **policy}
        return candidates, skipped

    def _value_candidates(
        self, window: sqlite3.Row, user_ready: bool, peer_ready: bool, global_ready: bool, baseline_revision: int, policy: dict[str, int | float], group_id: str | None,
    ) -> list[dict[str, Any]]:
        if not user_ready and not peer_ready and not global_ready:
            return []
        scope = "personal" if user_ready else ("peer_group" if peer_ready else "global")
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
            reference = self._value_reference(scope, window["user_name"], value["value_type"], value["value_key"], group_id)
            if reference is None:
                continue
            first_seen = int(reference["first_seen"])
            if first_seen == int(window["window_start"]):
                candidates.append(self._value_candidate(window, value, reference, scope, "new_value_30d", int(policy["new_value_score"]), baseline_revision, group_id))
            if float(reference["share"]) <= float(policy["rare_share_threshold"]) and int(reference["event_count"]) <= int(policy["rare_event_count_max"]):
                candidates.append(self._value_candidate(window, value, reference, scope, "rare_value", int(policy["rare_value_score"]), baseline_revision, group_id))
        return candidates

    def _window_policy(self, window: sqlite3.Row) -> tuple[dict[str, int | float], str]:
        policy: dict[str, int | float] = {key: getattr(self.config, key) for key in ("rare_share_threshold", "rare_event_count_max", "p95_score", "p99_score", "new_value_score", "rare_value_score")}
        rows = self.feature.execute("SELECT value_key FROM window_value_counts WHERE user_name = ? AND window_size = ? AND window_start = ? AND value_type = 'source_type' ORDER BY value_key", (window["user_name"], window["window_size"], window["window_start"])).fetchall()
        for row in rows:
            override = self.config.source_type_overrides.get(row["value_key"])
            if override:
                policy.update(override)
                return policy, f"source_type:{row['value_key']}"
        return policy, "default"

    def _numeric_candidate(
        self, window: sqlite3.Row, metric: str, value: float, reference: sqlite3.Row, scope: str,
        detector_id: str, score: int, baseline_revision: int, group_id: str | None = None,
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
        return self._candidate_base(window, detector_id, metric, None, None, score, scope, explanation, baseline_revision, group_id)

    def _value_candidate(
        self, window: sqlite3.Row, value: sqlite3.Row, reference: sqlite3.Row, scope: str,
        detector_id: str, score: int, baseline_revision: int, group_id: str | None = None,
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
            window, detector_id, None, value["value_type"], value["value_key"], score, scope, explanation, baseline_revision, group_id,
        )

    def _candidate_base(
        self, window: sqlite3.Row, detector_id: str, metric: str | None, value_type: str | None,
        value_key: str | None, score: int, scope: str, explanation: dict[str, Any], baseline_revision: int, group_id: str | None = None,
    ) -> dict[str, Any]:
        identity = ":".join(str(value) for value in (
            window["user_name"], window["window_size"], window["window_start"], detector_id, metric or "", value_type or "", value_key or "",
        ))
        candidate_key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        revision = int(window["updated_revision"])
        candidate_id = hashlib.sha256(f"{candidate_key}:{revision}".encode("utf-8")).hexdigest()
        evidence, evidence_query = self._window_evidence(window)
        return {
            "candidate_id": candidate_id, "candidate_key": candidate_key, "user_name": window["user_name"],
            "entity_type": "user", "entity_id": window["user_name"],
            "window_size": int(window["window_size"]), "window_start": int(window["window_start"]), "window_end": int(window["window_end"]),
            "detector_family": "statistical", "detector_id": detector_id, "detector_version": "1", "detector_mode": "active",
            "config_fingerprint": self.config.fingerprint(), "metric": metric, "value_type": value_type, "value_key": value_key,
            "score": score, "severity": _severity(score), "baseline_scope": scope,
            "baseline_group_id": group_id if scope == "peer_group" else None,
            "feature_revision": revision, "baseline_revision": baseline_revision, "explanation": explanation,
            "evidence": evidence, "evidence_query": evidence_query,
        }

    def _window_evidence(self, window: sqlite3.Row, source_types: list[str] | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        clauses = ["user_name=?", "event_time>=?", "event_time<?"]
        values: list[Any] = [window["user_name"], window["window_start"], window["window_end"]]
        if source_types:
            clauses.append(f"source_type IN ({','.join('?' for _ in source_types)})")
            values.extend(source_types)
        where = " AND ".join(clauses)
        total = int(self.feature.execute(f"SELECT COUNT(*) FROM event_evidence WHERE {where}", values).fetchone()[0])
        rows = self.feature.execute(
            f"SELECT * FROM event_evidence WHERE {where} ORDER BY event_time,event_id LIMIT ?", [*values, EVIDENCE_LIMIT]
        ).fetchall()
        return [_evidence(row, "window_member") for row in rows], {
            "kind": "window", "user_name": window["user_name"], "window_start": int(window["window_start"]),
            "window_end": int(window["window_end"]), "event_count": total, "evidence_limit": EVIDENCE_LIMIT,
            "evidence_truncated": total > EVIDENCE_LIMIT, "source_types": source_types or [],
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
                    candidate_id, candidate_key, user_name, entity_type, entity_id, window_size, window_start, window_end,
                    detector_family, detector_id, detector_version, detector_mode, config_fingerprint,
                    metric, value_type, value_key, baseline_group_id, score, severity, baseline_scope,
                    feature_revision, baseline_revision, explanation_json, evidence_query_json,
                    status, supersedes_candidate_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
                """,
                (
                    candidate["candidate_id"], candidate["candidate_key"], candidate["user_name"], candidate["entity_type"], candidate["entity_id"],
                    candidate["window_size"], candidate["window_start"], candidate["window_end"], candidate["detector_family"],
                    candidate["detector_id"], candidate["detector_version"], candidate["detector_mode"], candidate["config_fingerprint"], candidate["metric"],
                    candidate["value_type"], candidate["value_key"], candidate["baseline_group_id"], candidate["score"], candidate["severity"],
                    candidate["baseline_scope"], candidate["feature_revision"], candidate["baseline_revision"],
                    json.dumps(candidate["explanation"], ensure_ascii=False, sort_keys=True),
                    json.dumps(candidate["evidence_query"], ensure_ascii=False, sort_keys=True), previous["candidate_id"] if previous else None,
                    _now_ms(), _now_ms(),
                ),
            )
        except sqlite3.IntegrityError:
            self.connection.execute("UPDATE anomaly_candidates SET status='active',updated_at=? WHERE candidate_id=?", (_now_ms(), candidate["candidate_id"]))
            return False
        self.connection.executemany(
            """INSERT OR IGNORE INTO candidate_evidence(
                candidate_id,evidence_order,event_id,event_time,record_id,source_id,source_type,storage_ref,checksum,reason
            ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            [(
                candidate["candidate_id"], index, item["event_id"], item["event_time"], item.get("record_id"),
                item["source_id"], item["source_type"], item.get("storage_ref"), item.get("checksum"), item["reason"],
            ) for index, item in enumerate(candidate["evidence"])],
        )
        return True

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
        elif table == "peer_group_metric_baselines":
            row = self.baseline.execute("SELECT status FROM peer_group_metric_baselines WHERE group_id = ? AND window_size = ? AND metric = 'event_count'", (user_name, window_size)).fetchone()
        else:
            row = self.baseline.execute(
                "SELECT status FROM global_metric_baselines WHERE window_size = ? AND metric = 'event_count'",
                (window_size,),
            ).fetchone()
        return row is not None and row["status"] == "ready"

    def _metric_reference(self, scope: str, user_name: str, window_size: int, metric: str, group_id: str | None = None) -> sqlite3.Row | None:
        if scope == "personal":
            return self.baseline.execute(
                "SELECT * FROM user_metric_baselines WHERE user_name = ? AND window_size = ? AND metric = ?",
                (user_name, window_size, metric),
            ).fetchone()
        if scope == "peer_group":
            return self.baseline.execute("SELECT * FROM peer_group_metric_baselines WHERE group_id = ? AND window_size = ? AND metric = ?", (group_id, window_size, metric)).fetchone()
        return self.baseline.execute(
            "SELECT * FROM global_metric_baselines WHERE window_size = ? AND metric = ?", (window_size, metric)
        ).fetchone()

    def _value_reference(self, scope: str, user_name: str, value_type: str, value_key: str, group_id: str | None = None) -> sqlite3.Row | None:
        if scope == "personal":
            return self.baseline.execute(
                "SELECT * FROM user_value_baselines WHERE user_name = ? AND value_type = ? AND value_key = ?",
                (user_name, value_type, value_key),
            ).fetchone()
        if scope == "peer_group":
            return self.baseline.execute("SELECT * FROM peer_group_value_baselines WHERE group_id = ? AND value_type = ? AND value_key = ?", (group_id, value_type, value_key)).fetchone()
        return self.baseline.execute(
            "SELECT * FROM global_value_baselines WHERE value_type = ? AND value_key = ?", (value_type, value_key)
        ).fetchone()

    def _group_id(self, user_name: str) -> str | None:
        row = self.baseline.execute("SELECT group_id FROM peer_group_memberships WHERE user_name = ?", (user_name,)).fetchone()
        return row["group_id"] if row else None

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
                entity_type TEXT NOT NULL, entity_id TEXT NOT NULL,
                window_size INTEGER NOT NULL, window_start INTEGER NOT NULL, window_end INTEGER NOT NULL,
                detector_family TEXT NOT NULL, detector_id TEXT NOT NULL, detector_version TEXT NOT NULL,
                detector_mode TEXT NOT NULL, config_fingerprint TEXT NOT NULL,
                metric TEXT, value_type TEXT, value_key TEXT, baseline_group_id TEXT,
                score INTEGER NOT NULL, severity TEXT NOT NULL, baseline_scope TEXT,
                feature_revision INTEGER NOT NULL, baseline_revision INTEGER,
                explanation_json TEXT NOT NULL, evidence_query_json TEXT NOT NULL,
                status TEXT NOT NULL, supersedes_candidate_id TEXT,
                created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                UNIQUE(candidate_key, feature_revision)
            );
            CREATE TABLE IF NOT EXISTS candidate_evidence (
                candidate_id TEXT NOT NULL, evidence_order INTEGER NOT NULL, event_id TEXT NOT NULL, event_time INTEGER NOT NULL,
                record_id TEXT, source_id TEXT NOT NULL, source_type TEXT NOT NULL, storage_ref TEXT, checksum TEXT, reason TEXT NOT NULL,
                PRIMARY KEY(candidate_id,evidence_order), FOREIGN KEY(candidate_id) REFERENCES anomaly_candidates(candidate_id)
            );
            CREATE INDEX IF NOT EXISTS idx_candidates_user_time ON anomaly_candidates(user_name, window_start, status);
            CREATE INDEX IF NOT EXISTS idx_candidates_key ON anomaly_candidates(candidate_key, feature_revision);
            CREATE INDEX IF NOT EXISTS idx_candidates_family_mode ON anomaly_candidates(detector_family,detector_mode,status);
            CREATE INDEX IF NOT EXISTS idx_candidate_evidence_event ON candidate_evidence(event_id,candidate_id);
            """
        )
        self.connection.commit()

    def _reject_old_schema(self) -> None:
        table = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='detection_meta'"
        ).fetchone()
        if table is None:
            return
        row = self.connection.execute(
            "SELECT value FROM detection_meta WHERE key='detection_schema_version'"
        ).fetchone()
        if row is not None and row[0] != DETECTION_SCHEMA_VERSION:
            raise DetectionError(
                "detection schema version changed; rebuild Detection, Incident, Risk, Case, and Evaluation DBs from Feature/Baseline"
            )

    def _initialize_meta(self) -> None:
        fingerprint = self.config.fingerprint()
        existing = self._meta("detection_schema_version")
        if existing is None:
            values = {
                "detection_schema_version": DETECTION_SCHEMA_VERSION,
                "feature_config_fingerprint": self.feature_fingerprint,
                "baseline_config_fingerprint": self.baseline_fingerprint,
                "detection_config_fingerprint": fingerprint,
                "rules_fingerprint": self.rules_fingerprint,
                "current_detection_revision": "0",
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
            "processed_detection_revision": int(meta.get("current_detection_revision", "0")),
            "candidates": [_serialize_candidate(row, connection) for row in rows],
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


def _serialize_candidate(row: sqlite3.Row, connection: sqlite3.Connection | None = None) -> dict[str, Any]:
    result = dict(row)
    for field in ("window_start", "window_end", "created_at", "updated_at"):
        result[field] = _iso(int(result[field]))
    result["explanation"] = json.loads(result.pop("explanation_json"))
    result["evidence_query"] = json.loads(result.pop("evidence_query_json"))
    if connection is not None:
        evidence = connection.execute(
            "SELECT * FROM candidate_evidence WHERE candidate_id=? ORDER BY evidence_order", (result["candidate_id"],)
        ).fetchall()
        result["evidence"] = [
            {**dict(item), "event_time": _iso(int(item["event_time"]))}
            for item in evidence
        ]
    return result


def _fingerprint(document: dict[str, Any]) -> str:
    payload = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _runtime_rules_fingerprint(document: dict[str, Any]) -> str:
    return _fingerprint({
        "schema_version": document.get("schema_version", 1),
        "rules": [rule for rule in document.get("rules", []) if rule.get("status") in {"shadow", "active"}],
    })


def _event_rule_document(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event.category": row["category"], "event.type": row["event_type"], "event.action": row["action"],
        "event.outcome": row["outcome"], "user.name": row["user_name"], "source.ip": row["source_ip"],
        "destination.ip": row["destination_ip"], "host.name": row["host_name"], "resource.type": row["resource_type"],
        "resource.name": row["resource_name"], "http.request.method": row["http_method"],
        "http.response.status_code": row["http_status"], "parser.id": row["parser_id"], "raw.source_type": row["source_type"],
    }


def _rule_explanation(rule: dict[str, Any], scope: str) -> dict[str, Any]:
    return {
        "kind": "rule_match", "scope": scope, "rule_id": rule["rule_id"], "rule_version": rule["version"],
        "rule_name": rule["name"], "match": rule["match"], "conditions": rule["conditions"], "tags": rule["tags"],
    }


def _evidence(row: sqlite3.Row, reason: str) -> dict[str, Any]:
    return {
        "event_id": row["event_id"], "event_time": int(row["event_time"]), "record_id": row["record_id"],
        "source_id": row["source_id"], "source_type": row["source_type"], "storage_ref": row["storage_ref"],
        "checksum": row["checksum"], "reason": reason,
    }
