from __future__ import annotations

from datetime import datetime, timezone
import sqlite3

import pytest

from logfusion.features import FeatureEngine, FeatureError


def _event(
    event_id: str,
    timestamp: str,
    *,
    user: str = "wangkun78",
    source_type: str = "sso",
    source_ip: str | None = "10.0.0.1",
    resource_type: str | None = "application",
    resource_name: str | None = "portal",
    resource_id: str | None = None,
    action: str = "user_login",
    outcome: str = "success",
    network_bytes: int | None = None,
) -> dict:
    event = {
        "event": {"id": event_id, "time": timestamp, "action": action, "outcome": outcome},
        "user": {"name": user},
        "raw": {"source_type": source_type, "source_id": f"{source_type}-main"},
        "source": {},
        "resource": {},
        "network": {},
    }
    if source_ip is not None:
        event["source"]["ip"] = source_ip
    if resource_type is not None:
        event["resource"]["type"] = resource_type
    if resource_name is not None:
        event["resource"]["name"] = resource_name
    if resource_id is not None:
        event["resource"]["id"] = resource_id
    if network_bytes is not None:
        event["network"]["bytes"] = network_bytes
    return event


def test_process_event_builds_all_windows_values_and_cross_source_session(tmp_path):
    engine = FeatureEngine(tmp_path / "features.db", mode="replay")
    engine.process_event(_event("sso-1", "2026-07-01T09:00:01Z", action="user_login"))
    engine.process_event(_event(
        "svn-1", "2026-07-01T09:05:00Z", source_type="svn", source_ip="10.0.0.2",
        resource_type="svn_path", resource_name="/core", action="repo_read", network_bytes=12,
    ))
    engine.flush_sessions()

    hourly = engine.query_windows("wangkun78", "2026-07-01T09:00:00Z", "2026-07-01T10:00:00Z", 3600)
    assert len(hourly) == 1
    assert hourly[0]["event_count"] == 2
    assert hourly[0]["success_count"] == 2
    assert hourly[0]["other_outcome_count"] == 0
    assert hourly[0]["source_type_distinct_count"] == 2
    assert hourly[0]["source_ip_distinct_count"] == 2
    assert hourly[0]["resource_distinct_count"] == 2
    assert hourly[0]["action_distinct_count"] == 2
    assert hourly[0]["network_bytes_total"] == 12
    assert hourly[0]["first_seen_ip_count"] == 2
    assert hourly[0]["first_seen_resource_count"] == 2
    assert hourly[0]["first_seen_action_count"] == 2

    sessions = engine.query_sessions("wangkun78", "2026-07-01T09:00:00Z", "2026-07-01T10:00:00Z")
    assert len(sessions) == 1
    assert sessions[0]["status"] == "closed"
    assert sessions[0]["event_count"] == 2
    assert sessions[0]["session_id"]


def test_first_seen_migrates_to_earlier_event_independent_of_input_order(tmp_path):
    engine = FeatureEngine(tmp_path / "features.db", mode="replay")
    engine.process_event(_event("late-first", "2026-07-01T10:00:00Z", source_ip="10.3.0.1", action="repo_read"))
    engine.process_event(_event("early-first", "2026-07-01T09:00:00Z", source_ip="10.3.0.1", action="repo_read"))

    old_window = engine.query_windows("wangkun78", "2026-07-01T10:00:00Z", "2026-07-01T11:00:00Z", 3600)[0]
    new_window = engine.query_windows("wangkun78", "2026-07-01T09:00:00Z", "2026-07-01T10:00:00Z", 3600)[0]
    assert old_window["first_seen_ip_count"] == 0
    assert new_window["first_seen_ip_count"] == 1
    stats = engine.value_stats("wangkun78", "source_ip", "10.3.0.1")
    assert stats["first_event_id"] == "early-first"
    assert stats["event_count"] == 2


def test_bridge_event_merges_provisional_sessions_and_closed_id_is_deterministic(tmp_path):
    events = [
        _event("a", "2026-07-01T10:00:00Z", action="user_login"),
        _event("b", "2026-07-01T10:31:00Z", action="repo_read"),
        _event("c", "2026-07-01T10:29:00Z", action="repo_clone"),
    ]
    first = FeatureEngine(tmp_path / "first.db", mode="replay")
    for event in events:
        first.process_event(event)
    first.flush_sessions()

    second = FeatureEngine(tmp_path / "second.db", mode="replay")
    for event in reversed(events):
        second.process_event(event)
    second.flush_sessions()

    first_session = first.query_sessions("wangkun78", "2026-07-01T09:00:00Z", "2026-07-01T12:00:00Z")
    second_session = second.query_sessions("wangkun78", "2026-07-01T09:00:00Z", "2026-07-01T12:00:00Z")
    assert [(row["event_count"], row["session_id"]) for row in first_session] == [(3, first_session[0]["session_id"])]
    assert [(row["event_count"], row["session_id"]) for row in second_session] == [(3, first_session[0]["session_id"])]


def test_dedup_rejections_revisions_and_resource_keys(tmp_path):
    engine = FeatureEngine(tmp_path / "features.db", mode="replay")
    first = engine.process_event(_event("one", "2026-07-01T09:00:00Z", resource_type="git_repo", resource_name="common"))
    duplicate = engine.process_event(_event("one", "2026-07-01T09:00:00Z", resource_type="git_repo", resource_name="common"))
    second = engine.process_event(_event("two", "2026-07-01T09:01:00Z", resource_type="svn_path", resource_name="common"))
    rejected = engine.process_event(_event("bad", "2026-07-01T09:02:00Z", user=""))
    rejected_replay = engine.process_event(_event("bad", "2026-07-01T09:02:00Z", user=""))

    assert first.status == "processed"
    assert duplicate.status == "duplicate"
    assert second.revision > first.revision
    assert rejected.status == "rejected"
    assert rejected_replay.status == "duplicate"
    windows = engine.query_windows("wangkun78", "2026-07-01T09:00:00Z", "2026-07-01T10:00:00Z", 3600)
    assert windows[0]["resource_distinct_count"] == 2
    assert engine.current_revision() == second.revision
    evidence = engine.query_event_evidence(
        "wangkun78", "2026-07-01T09:00:00Z", "2026-07-01T10:00:00Z", limit=1,
    )
    assert evidence["event_count"] == 2
    assert evidence["evidence_truncated"] is True
    assert len(evidence["events"]) == 1
    assert "raw_text" not in evidence["events"][0]


def test_network_bytes_overflow_rolls_back_entire_event(tmp_path):
    engine = FeatureEngine(tmp_path / "features.db", mode="replay")
    engine.process_event(_event("safe", "2026-07-01T09:00:00Z", network_bytes=2**63 - 2))
    with pytest.raises(FeatureError, match="int64"):
        engine.process_event(_event("overflow", "2026-07-01T09:00:01Z", network_bytes=10))

    window = engine.query_windows("wangkun78", "2026-07-01T09:00:00Z", "2026-07-01T10:00:00Z", 3600)[0]
    assert window["event_count"] == 1
    assert window["network_bytes_total"] == 2**63 - 2
    assert engine.current_revision() == 1


def test_replay_build_cannot_append_after_flush(tmp_path):
    engine = FeatureEngine(tmp_path / "features.db", mode="replay")
    engine.process_event(_event("one", "2026-07-01T09:00:00Z"))
    engine.flush_sessions()
    with pytest.raises(FeatureError, match="rebuild"):
        engine.ensure_replay_build_allowed()


def test_realtime_watermark_only_closes_session_after_gap_and_keeps_late_bridge(tmp_path):
    engine = FeatureEngine(tmp_path / "features.db", mode="realtime")
    engine.process_event(_event("a", "2026-07-01T10:00:00Z"))
    engine.process_event(_event("b", "2026-07-01T10:31:00Z", action="repo_read"))
    engine.process_event(_event("bridge", "2026-07-01T10:29:00Z", action="repo_clone"))

    assert engine.advance_watermark("2026-07-01T11:01:00Z") == 0
    assert engine.advance_watermark("2026-07-01T11:01:00.001Z") == 1
    sessions = engine.query_sessions("wangkun78", "2026-07-01T10:15:00Z", "2026-07-01T10:45:00Z")
    assert len(sessions) == 1
    assert sessions[0]["status"] == "closed"
    assert sessions[0]["event_count"] == 3


def test_queries_return_windows_and_sessions_that_overlap_range(tmp_path):
    engine = FeatureEngine(tmp_path / "features.db", mode="replay")
    engine.process_event(_event("one", "2026-07-01T09:59:00Z"))
    engine.process_event(_event("two", "2026-07-01T10:01:00Z"))
    engine.flush_sessions()

    windows = engine.query_windows("wangkun78", "2026-07-01T09:59:30Z", "2026-07-01T10:00:30Z", 3600)
    sessions = engine.query_sessions("wangkun78", "2026-07-01T09:59:30Z", "2026-07-01T10:00:30Z")
    assert [row["event_count"] for row in windows] == [1, 1]
    assert len(sessions) == 1


def test_finalized_replay_state_can_transition_to_realtime(tmp_path):
    state = tmp_path / "features.db"
    with FeatureEngine(state, mode="replay") as replay:
        replay.process_event(_event("history", "2026-07-01T09:00:00Z"))
        replay.flush_sessions()

    with FeatureEngine(state, mode="realtime") as realtime:
        result = realtime.process_event(_event("live", "2026-07-01T11:00:00Z"))
        assert result.status == "processed"


def test_feature_v1_state_is_explicitly_rejected_before_upgrade(tmp_path):
    state = tmp_path / "features.db"
    with sqlite3.connect(state) as connection:
        connection.execute("CREATE TABLE feature_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        connection.execute("INSERT INTO feature_meta VALUES ('feature_schema_version','1')")
    with pytest.raises(FeatureError, match="rebuild Feature, Baseline, Detection"):
        FeatureEngine(state)
