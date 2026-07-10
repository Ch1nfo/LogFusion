from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from logfusion.file_source import iter_raw_records
from logfusion.models import RawRecord


def write_raw_store(sources: list[dict], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as handle:
        for record in iter_raw_records(sources):
            handle.write(json.dumps(raw_record_to_dict(record), ensure_ascii=False, sort_keys=True))
            handle.write("\n")
            count += 1
    return count


def write_raw_records(records: Iterable[RawRecord], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(raw_record_to_dict(record), ensure_ascii=False, sort_keys=True))
            handle.write("\n")
            count += 1
    return count


def iter_stored_raw_records(input_path: Path) -> Iterable[RawRecord]:
    with input_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            yield raw_record_from_dict(json.loads(line))


def raw_record_to_dict(record: RawRecord) -> dict:
    return {
        "record_id": record.record_id,
        "source_id": record.source_id,
        "source_type": record.source_type,
        "file_path": record.file_path,
        "line_start": record.line_start,
        "line_end": record.line_end,
        "record_index": record.record_index,
        "raw_text": record.raw_text,
        "checksum": record.checksum,
        "size_bytes": record.size_bytes,
        "storage_ref": record.storage_ref,
        "include_raw_text": record.include_raw_text,
        "product": record.product,
        "format_version": record.format_version,
        "llm_enabled": record.llm_enabled,
    }


def raw_record_from_dict(data: dict) -> RawRecord:
    return RawRecord(
        record_id=data["record_id"],
        source_id=data["source_id"],
        source_type=data["source_type"],
        file_path=data.get("file_path", ""),
        line_start=int(data.get("line_start", 0)),
        line_end=int(data.get("line_end", 0)),
        record_index=int(data.get("record_index", 0)),
        raw_text=data["raw_text"],
        checksum=data["checksum"],
        size_bytes=int(data["size_bytes"]),
        storage_ref=data["storage_ref"],
        include_raw_text=bool(data.get("include_raw_text", True)),
        product=data.get("product"),
        format_version=data.get("format_version"),
        llm_enabled=bool(data.get("llm_enabled", False)),
    )
