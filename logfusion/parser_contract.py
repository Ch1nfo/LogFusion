from __future__ import annotations

import hashlib

from logfusion.unknown import detect_format, template_hint


def format_fingerprint(source_id: str, source_type: str, detected_format: str, template: str) -> str:
    identity = f"{source_id}:{source_type}:{detected_format}:{template}"
    return "fmt_" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def source_format_fingerprint(
    source_id: str,
    source_type: str,
    detected_format: str,
    product: str | None = None,
    format_version: str | None = None,
) -> str:
    identity = f"{source_id}:{source_type}:{detected_format}:{product or ''}:{format_version or ''}"
    return "fmt_" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def record_format_fingerprint(
    source_id: str,
    source_type: str,
    raw_text: str,
    product: str | None = None,
    format_version: str | None = None,
) -> str:
    return source_format_fingerprint(source_id, source_type, detect_format(raw_text), product, format_version)
