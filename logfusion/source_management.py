from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from logfusion.config import load_kafka_config, load_sources_config
from logfusion.detect import detect_source_type
from logfusion.models import RawRecord
from logfusion.pipeline import iter_parse_records


SOURCE_TYPES = ("auto", "svn", "sso", "gitlab", "hiklink")
SOURCE_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
IMPORTANT_FIELDS = (
    "event.time",
    "event.action",
    "event.outcome",
    "user.name",
    "source.ip",
    "resource.name",
)


class SourceManagementError(ValueError):
    """Raised when a console-managed source is invalid or unsafe to save."""


def validate_source_settings(values: dict[str, str]) -> dict[str, Any]:
    kind = values.get("kind", "local").strip()
    source_id = values.get("source_id", "").strip()
    source_type = values.get("source_type", "auto").strip()
    if kind not in {"local", "kafka"}:
        raise SourceManagementError("数据源类型必须是本地文件或 Kafka")
    if not SOURCE_ID_RE.fullmatch(source_id):
        raise SourceManagementError("来源 ID 仅支持 1-64 位字母、数字、点、下划线和连字符")
    if source_type not in SOURCE_TYPES:
        raise SourceManagementError("不支持的日志类型")

    settings: dict[str, Any] = {
        "kind": kind,
        "source_id": source_id,
        "source_type": source_type,
        "product": _optional(values.get("product", "")),
        "format_version": _optional(values.get("format_version", "")),
        "include_raw_text": values.get("include_raw_text") == "true",
    }
    if kind == "local":
        record_mode = values.get("record_mode", "line").strip()
        path = values.get("path", "").strip()
        if record_mode not in {"line", "object"}:
            raise SourceManagementError("记录模式必须是 line 或 object")
        if not path or len(path) > 1000 or "\n" in path or "\r" in path:
            raise SourceManagementError("请输入有效的日志文件路径或 glob")
        settings.update({"record_mode": record_mode, "paths": [path]})
    else:
        bootstrap_servers = values.get("bootstrap_servers", "").strip()
        topics = [item.strip() for item in values.get("topics", "").split(",") if item.strip()]
        group_id = values.get("group_id", "").strip()
        if not bootstrap_servers or len(bootstrap_servers) > 1000 or _has_newline(bootstrap_servers):
            raise SourceManagementError("请输入 Kafka bootstrap servers")
        if not topics or any(len(topic) > 249 or _has_newline(topic) for topic in topics):
            raise SourceManagementError("请输入至少一个有效 Kafka topic")
        if not group_id or len(group_id) > 255 or _has_newline(group_id):
            raise SourceManagementError("请输入 Kafka consumer group ID")
        settings.update({
            "bootstrap_servers": bootstrap_servers,
            "topics": topics,
            "group_id": group_id,
        })
    return settings


def preview_source(settings: dict[str, Any], sample: str) -> dict[str, Any]:
    sample = sample.strip()
    if not sample:
        raise SourceManagementError("请粘贴日志样例或上传样例文件")
    if settings["kind"] == "local" and settings.get("record_mode") == "line":
        sample = next((line for line in sample.splitlines() if line.strip()), "")
    if not sample:
        raise SourceManagementError("日志样例不能是空白内容")

    digest = hashlib.sha256(sample.encode("utf-8")).hexdigest()
    record = RawRecord(
        record_id=digest,
        source_id=settings["source_id"],
        source_type=settings["source_type"],
        file_path="console-preview",
        line_start=1,
        line_end=max(1, sample.count("\n") + 1),
        record_index=1,
        raw_text=sample,
        checksum=f"sha256:{digest}",
        size_bytes=len(sample.encode("utf-8")),
        storage_ref=("kafka://console-preview/sample/0/0" if settings["kind"] == "kafka" else "file://console-preview#L1-L1"),
        include_raw_text=False,
        product=settings.get("product"),
        format_version=settings.get("format_version"),
        kafka_topic=settings.get("topics", [None])[0] if settings["kind"] == "kafka" else None,
        kafka_partition=0 if settings["kind"] == "kafka" else None,
        kafka_offset=0 if settings["kind"] == "kafka" else None,
    )
    detected = detect_source_type(record)
    outcome = next(iter_parse_records([record]))
    event = outcome.normalized
    parser = event.get("parser", {}) if event else (outcome.unknown or {}).get("parser", {})
    fields = [{"path": path, "present": _present(_get_path(event or {}, path))} for path in IMPORTANT_FIELDS]
    present_count = sum(item["present"] for item in fields)
    return {
        "ok": event is not None,
        "detected_source_type": detected,
        "effective_source_type": event.get("raw", {}).get("source_type") if event else (outcome.unknown or {}).get("suggested_source_type", detected),
        "parser_id": parser.get("id", "unknown"),
        "confidence": float(parser.get("confidence", 0.0) or 0.0),
        "field_coverage": present_count / len(fields),
        "fields": fields,
        "normalized": event,
        "unknown": outcome.unknown,
        "sample": sample,
    }


def add_local_source(path: Path, settings: dict[str, Any]) -> None:
    if settings.get("kind") != "local":
        raise SourceManagementError("不是本地文件数据源")
    if path.exists():
        try:
            existing = load_sources_config(path)
        except (OSError, ValueError) as exc:
            raise SourceManagementError(f"无法读取现有数据源配置：{exc}") from exc
        if any(item.get("source_id") == settings["source_id"] for item in existing):
            raise SourceManagementError(f"来源 ID 已存在：{settings['source_id']}")
        text = path.read_text(encoding="utf-8")
        if not any(line.strip() == "sources:" for line in text.splitlines()):
            raise SourceManagementError("现有配置缺少 sources: 段，未自动改写")
    else:
        text = "raw:\n  include_text_in_event: false\n\nsources:\n"

    lines = [
        f"  - source_id: {_yaml_scalar(settings['source_id'])}",
        f"    source_type: {_yaml_scalar(settings['source_type'])}",
        f"    record_mode: {_yaml_scalar(settings['record_mode'])}",
        f"    include_raw_text: {'true' if settings.get('include_raw_text') else 'false'}",
    ]
    if settings.get("product"):
        lines.append(f"    product: {_yaml_scalar(settings['product'])}")
    if settings.get("format_version"):
        lines.append(f"    format_version: {_yaml_scalar(settings['format_version'])}")
    lines.append("    paths:")
    lines.extend(f"      - {_yaml_scalar(item)}" for item in settings["paths"])
    updated = text.rstrip() + "\n" + "\n".join(lines) + "\n"
    _atomic_write(path, updated)


def add_kafka_source(path: Path, settings: dict[str, Any]) -> None:
    if settings.get("kind") != "kafka":
        raise SourceManagementError("不是 Kafka 数据源")
    if path.exists():
        try:
            existing = load_kafka_config(path)
        except (OSError, ValueError) as exc:
            raise SourceManagementError(f"无法读取现有 Kafka 配置：{exc}") from exc
        if existing:
            raise SourceManagementError(f"Kafka 配置已存在：{path}；当前向导不会覆盖它")
    lines = [
        "# Managed by LogFusion Console. Secrets must be provided by environment variables.",
        "kafka:",
        f"  bootstrap_servers: {_yaml_scalar(settings['bootstrap_servers'])}",
        f"  group_id: {_yaml_scalar(settings['group_id'])}",
        "  topics:",
        *(f"    - {_yaml_scalar(topic)}" for topic in settings["topics"]),
        f"  source_id: {_yaml_scalar(settings['source_id'])}",
        f"  source_type: {_yaml_scalar(settings['source_type'])}",
    ]
    if settings.get("product"):
        lines.append(f"  product: {_yaml_scalar(settings['product'])}")
    if settings.get("format_version"):
        lines.append(f"  format_version: {_yaml_scalar(settings['format_version'])}")
    lines.extend([
        f"  include_raw_text: {'true' if settings.get('include_raw_text') else 'false'}",
        "  poll_timeout_seconds: 1.0",
        "  auto_offset_reset: earliest",
    ])
    _atomic_write(path, "\n".join(lines) + "\n")


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def _yaml_scalar(value: Any) -> str:
    text = str(value)
    if _has_newline(text):
        raise SourceManagementError("配置值不能包含换行")
    return json.dumps(text, ensure_ascii=False)


def _get_path(document: dict[str, Any], path: str) -> Any:
    current: Any = document
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _present(value: Any) -> bool:
    return value is not None and value != "" and value != "unknown"


def _optional(value: str) -> str | None:
    result = value.strip()
    if len(result) > 255 or _has_newline(result):
        raise SourceManagementError("产品或格式版本字段过长或包含换行")
    return result or None


def _has_newline(value: str) -> bool:
    return "\n" in value or "\r" in value
