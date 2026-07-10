from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from logfusion.detect import detect_source_type
from logfusion.models import RawRecord
from logfusion.unknown import build_unknown_record


SVN_RE = re.compile(
    r'^(?P<src_ip>\S+) - (?P<user>\S+) \[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<url>\S+) (?P<protocol>[^"]+)" '
    r'(?P<status>\d+) (?P<bytes>\d+)'
    r'(?: "(?P<ua>[^"]+)")?'
    r'(?: (?P<svn_action>\S+) (?P<repo_path>\S+) r(?P<revision>\d+)(?: (?P<detail>\S+))?)?'
)


def parse_record(record: RawRecord) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    source_type = record.source_type
    if source_type == "auto":
        source_type = detect_source_type(record)

    parser = {
        "svn": _parse_svn,
        "sso": _parse_sso,
        "gitlab": _parse_gitlab,
        "hiklink": _parse_hiklink,
    }.get(source_type)

    if parser is None:
        return None, build_unknown_record(record, "no_parser", source_type)

    try:
        event = parser(record)
    except (ValueError, KeyError, json.JSONDecodeError) as exc:
        return None, build_unknown_record(record, f"parse_error:{exc}", source_type)

    event["raw"]["source_type"] = source_type
    return event, None


def _base(record: RawRecord, source_type: str, parser_id: str, confidence: float) -> dict[str, Any]:
    event = {
        "event": {
            "id": record.record_id,
            "time": None,
            "category": "unknown",
            "type": "unknown",
            "action": "unknown",
            "outcome": "unknown",
        },
        "user": {},
        "source": {},
        "destination": {},
        "host": {},
        "device": {},
        "resource": {},
        "http": {},
        "parser": {
            "id": parser_id,
            "version": "0.1.0",
            "confidence": confidence,
            "status": "success",
        },
        "raw": {
            "record_id": record.record_id,
            "source_id": record.source_id,
            "source_type": source_type,
            "file_path": record.file_path,
            "line_start": record.line_start,
            "line_end": record.line_end,
            "record_index": record.record_index,
            "checksum": record.checksum,
            "size_bytes": record.size_bytes,
            "storage_ref": record.storage_ref,
        },
        "extensions": {},
    }
    if record.include_raw_text:
        event["raw"]["text"] = record.raw_text
    if record.kafka_topic is not None:
        event["raw"]["kafka"] = {
            "topic": record.kafka_topic,
            "partition": record.kafka_partition,
            "offset": record.kafka_offset,
            "timestamp_ms": record.kafka_timestamp_ms,
        }
    return event


def _parse_svn(record: RawRecord) -> dict[str, Any]:
    match = SVN_RE.match(record.raw_text)
    if not match:
        raise ValueError("not_svn")
    data = match.groupdict()
    event = _base(record, "svn", "svn_apache_access", 0.95)
    svn_action = data.get("svn_action") or data["method"].lower()
    action = _repo_action(svn_action)
    event["event"].update({
        "time": _apache_time(data["time"]),
        "category": "repository",
        "type": "access",
        "action": action,
        "outcome": _http_outcome(int(data["status"])),
    })
    event["user"]["name"] = data["user"]
    event["source"]["ip"] = data["src_ip"]
    event["http"] = {
        "request": {"method": data["method"]},
        "response": {"status_code": int(data["status"]), "bytes": int(data["bytes"])},
        "url": {"path": data["url"]},
        "user_agent": data.get("ua"),
    }
    event["resource"] = {
        "type": "repository_path",
        "name": data.get("repo_path") or data["url"],
    }
    event["extensions"]["svn"] = {
        "action": data.get("svn_action"),
        "path": data.get("repo_path"),
        "revision": data.get("revision"),
        "detail": data.get("detail"),
    }
    return event


def _parse_sso(record: RawRecord) -> dict[str, Any]:
    data = json.loads(record.raw_text)
    event = _base(record, "sso", "sso_json", 0.98)
    action = "user_login" if data.get("eventType") == "authenticateSuccess" else "sso_redirect"
    event["event"].update({
        "time": _plain_time(data.get("ctime")),
        "category": "authentication",
        "type": "login" if action == "user_login" else "redirect",
        "action": action,
        "outcome": "success" if data.get("status") == "0" else "failure",
    })
    event["user"]["name"] = data.get("shortName")
    event["source"]["ip"] = data.get("userIp")
    event["destination"]["ip"] = data.get("casServer")
    event["resource"] = {"type": "application_url", "name": data.get("appUrl")}
    event["extensions"]["sso"] = data
    return event


def _parse_gitlab(record: RawRecord) -> dict[str, Any]:
    data = json.loads(record.raw_text)
    event = _base(record, "gitlab", "gitlab_jsonl", 0.98)
    event["event"].update({
        "time": data.get("time"),
        "category": "repository",
        "type": "api",
        "action": _gitlab_action(data),
        "outcome": _http_outcome(int(data.get("status", 0))),
    })
    event["user"] = {"id": str(data.get("user_id")), "name": data.get("username")}
    event["source"]["ip"] = str(data.get("remote_ip", "")).split(",")[0].strip()
    event["host"]["name"] = data.get("host")
    event["http"] = {
        "request": {"method": data.get("method")},
        "response": {"status_code": data.get("status")},
        "url": {"path": data.get("path")},
        "route": data.get("route"),
        "user_agent": data.get("ua"),
    }
    event["resource"] = {"type": "gitlab_route", "name": data.get("path")}
    event["extensions"]["gitlab"] = data
    return event


def _parse_hiklink(record: RawRecord) -> dict[str, Any]:
    data = _parse_json_like_object(record.raw_text)
    event = _base(record, "hiklink", "hiklink_json_like", 0.86)
    event["event"].update({
        "time": _plain_time(data.get("createTime")),
        "category": "authentication" if data.get("path") == "loginByPasswd" else "application",
        "type": "login" if data.get("path") == "loginByPasswd" else "behavior",
        "action": "user_login" if data.get("path") == "loginByPasswd" else str(data.get("path")),
        "outcome": "success" if data.get("attribute3") == "SUCCESS" else "failure",
    })
    event["user"]["name"] = data.get("uid")
    event["source"]["ip"] = data.get("attribute2")
    event["device"] = {
        "name": data.get("device"),
        "type": data.get("action"),
        "os": {"name": data.get("platformName"), "version": data.get("platformVersion")},
    }
    event["extensions"]["hiklink"] = data
    return event

def _repo_action(action: str) -> str:
    action = action.lower()
    if action in {"get-file", "get-dir", "report", "propfind"}:
        return "repo_read"
    if action in {"proppatch", "put", "post", "delete"}:
        return "repo_write"
    if action == "status":
        return "repo_status"
    return f"repo_{action}"


def _gitlab_action(data: dict[str, Any]) -> str:
    method = str(data.get("method", "")).upper()
    path = str(data.get("path", ""))
    if method == "GET" and "/projects" in path:
        return "repo_api_read"
    if method in {"POST", "PUT", "PATCH", "DELETE"}:
        return "repo_api_write"
    return "repo_api_access"


def _http_outcome(status: int) -> str:
    return "success" if 200 <= status < 400 else "failure"


def _apache_time(value: str) -> str:
    return datetime.strptime(value, "%d/%b/%Y:%H:%M:%S %z").isoformat()


def _plain_time(value: str | None) -> str | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").isoformat()


def _parse_json_like_object(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for line in text.splitlines():
        stripped = line.strip().rstrip(",")
        if not stripped or stripped in {"{", "}"} or stripped.endswith(":{"):
            continue
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        key = key.strip().strip('"')
        value = value.strip()
        if value == "null":
            parsed: Any = None
        elif value in {"true", "false"}:
            parsed = value == "true"
        elif value.startswith('"') and value.endswith('"'):
            parsed = value[1:-1]
        else:
            parsed = value
        result[key] = parsed
    return result
