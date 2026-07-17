from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from logfusion.cases import get_case
from logfusion.correlation import CORRELATION_SCHEMA_VERSION
from logfusion.detection import DETECTION_SCHEMA_VERSION
from logfusion.fusion import FUSION_SCHEMA_VERSION


FORBIDDEN_RAW_KEYS = {"raw_text", "raw_event", "canonical_event", "event_document"}


def get_case_investigation(
    case_state: Path | str,
    risk_state: Path | str,
    incident_state: Path | str,
    detection_state: Path | str,
    case_id: str,
) -> dict[str, Any]:
    case = get_case(case_state, case_id)
    result: dict[str, Any] = {
        "case": case,
        "alert": None,
        "assessment": None,
        "incident": None,
        "candidates": [],
        "evidence_status": {},
    }

    risk = _open_state(Path(risk_state), "fusion_meta", "fusion_schema_version", FUSION_SCHEMA_VERSION)
    if isinstance(risk, str):
        result["evidence_status"]["risk"] = risk
    else:
        try:
            alert = risk.execute("SELECT * FROM alerts WHERE alert_id = ?", (case["current_alert_id"],)).fetchone()
            if alert is None:
                result["evidence_status"]["risk"] = "not_found"
            else:
                result["alert"] = _serialize_row(alert, ("first_seen", "last_seen", "created_at", "updated_at"))
                assessment = risk.execute("SELECT * FROM risk_assessments WHERE assessment_id = ?", (alert["assessment_id"],)).fetchone()
                if assessment is not None:
                    document = _serialize_row(assessment, ("first_seen", "last_seen", "created_at", "updated_at"))
                    document["risk_breakdown"] = json.loads(document.pop("risk_breakdown_json"))
                    result["assessment"] = _sanitize(document)
                result["evidence_status"]["risk"] = "available" if assessment is not None else "assessment_not_found"
        finally:
            risk.close()

    incidents = _open_state(Path(incident_state), "correlation_meta", "correlation_schema_version", CORRELATION_SCHEMA_VERSION)
    if isinstance(incidents, str):
        result["evidence_status"]["incident"] = incidents
        result["evidence_status"]["detection"] = "not_resolvable"
        return result
    candidate_ids: list[str] = []
    try:
        incident = incidents.execute("SELECT * FROM incidents WHERE incident_id = ?", (case["incident_id"],)).fetchone()
        if incident is None:
            result["evidence_status"]["incident"] = "not_found"
            result["evidence_status"]["detection"] = "not_resolvable"
            return result
        document = _serialize_row(incident, ("first_seen", "last_seen", "created_at", "updated_at"))
        for field in ("detector_families", "source_types", "source_ips", "resources", "timeline", "evidence"):
            document[field] = json.loads(document.pop(f"{field}_json"))
        result["incident"] = _sanitize(document)
        candidate_ids = [
            row["candidate_id"] for row in incidents.execute(
                "SELECT candidate_id FROM incident_candidates WHERE incident_id = ? ORDER BY candidate_id", (case["incident_id"],)
            )
        ]
        result["evidence_status"]["incident"] = "available"
    finally:
        incidents.close()

    detection = _open_state(Path(detection_state), "detection_meta", "detection_schema_version", DETECTION_SCHEMA_VERSION)
    if isinstance(detection, str):
        result["evidence_status"]["detection"] = detection
        return result
    try:
        candidates = []
        missing = 0
        for candidate_id in candidate_ids:
            row = detection.execute("SELECT * FROM anomaly_candidates WHERE candidate_id = ?", (candidate_id,)).fetchone()
            if row is None:
                missing += 1
                continue
            candidate = _serialize_row(row, ("window_start", "window_end", "created_at", "updated_at"))
            candidate["explanation"] = json.loads(candidate.pop("explanation_json"))
            candidate["evidence_query"] = json.loads(candidate.pop("evidence_query_json"))
            evidence = detection.execute(
                "SELECT * FROM candidate_evidence WHERE candidate_id = ? ORDER BY evidence_order", (candidate_id,)
            ).fetchall()
            candidate["evidence"] = [_serialize_row(item, ("event_time",)) for item in evidence]
            candidates.append(_sanitize(candidate))
        candidates.sort(key=lambda item: (item["window_start"], -int(item["score"]), item["candidate_id"]))
        result["candidates"] = candidates
        result["evidence_status"]["detection"] = "available" if not missing else f"partial:{missing}_missing"
    finally:
        detection.close()
    return result


def _open_state(path: Path, meta_table: str, version_key: str, expected: str) -> sqlite3.Connection | str:
    if not path.exists():
        return "missing"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        version = connection.execute(f"SELECT value FROM {meta_table} WHERE key = ?", (version_key,)).fetchone()
        if version is None or version[0] != expected:
            connection.close()
            return "incompatible"
        return connection
    except sqlite3.Error:
        if connection is not None:
            connection.close()
        return "unavailable"


def _serialize_row(row: sqlite3.Row, timestamp_fields: tuple[str, ...]) -> dict[str, Any]:
    result = dict(row)
    for field in timestamp_fields:
        if result.get(field) is not None:
            result[field] = _iso(int(result[field]))
    return result


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize(item) for key, item in value.items() if key.lower() not in FORBIDDEN_RAW_KEYS}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    return value


def _iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
