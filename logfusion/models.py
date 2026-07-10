from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RawRecord:
    record_id: str
    source_id: str
    source_type: str
    file_path: str
    line_start: int
    line_end: int
    record_index: int
    raw_text: str
    checksum: str
    size_bytes: int
    storage_ref: str
    include_raw_text: bool
    product: str | None = None
    format_version: str | None = None
    llm_enabled: bool = False
