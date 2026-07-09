from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from logfusion.detect import detect_source_type
from logfusion.models import RawRecord


def build_unknown_record(
    record: RawRecord,
    reason: str,
    suggested_source_type: str | None = None,
    validation_errors: list[str] | None = None,
) -> dict[str, Any]:
    suggested = suggested_source_type or detect_source_type(record)
    unknown_id = hashlib.sha256(f"{record.record_id}:{reason}".encode("utf-8")).hexdigest()
    raw = {
        "record_id": record.record_id,
        "source_id": record.source_id,
        "source_type": suggested,
        "file_path": record.file_path,
        "line_start": record.line_start,
        "line_end": record.line_end,
        "record_index": record.record_index,
        "checksum": record.checksum,
        "size_bytes": record.size_bytes,
        "storage_ref": record.storage_ref,
    }
    if record.include_raw_text:
        raw["text"] = record.raw_text

    unknown = {
        "unknown_id": unknown_id,
        "reason": reason,
        "raw": raw,
        "detected_format": detect_format(record.raw_text),
        "suggested_source_type": suggested,
        "template_hint": template_hint(record.raw_text),
        "parser": {
            "id": "unknown",
            "version": "0.1.0",
            "confidence": 0.0,
            "status": "failed",
        },
    }
    if validation_errors:
        unknown["schema_errors"] = validation_errors
    return unknown


def detect_format(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return "empty"
    if stripped.startswith("{"):
        try:
            json.loads(stripped)
        except json.JSONDecodeError:
            return "json_like"
        return "json"
    if re.search(r"\b\w+=", stripped):
        return "key_value"
    if re.match(r'^\d+\.\d+\.\d+\.\d+ - \S+ \[[^\]]+\] "\S+ ', stripped):
        return "apache_access"
    return "text"


def template_hint(text: str) -> str:
    hint = re.sub(r"\b\d+\.\d+\.\d+\.\d+\b", "<IP>", text.strip())
    hint = re.sub(r"\b\d{2,}\b", "<NUM>", hint)
    hint = re.sub(r"=(?!<)([^\s]+)", "=<VAR>", hint)
    return hint[:500]

