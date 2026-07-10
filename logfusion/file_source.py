from __future__ import annotations

import bz2
import gzip
import hashlib
import lzma
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import Iterable, Iterator

from logfusion.models import RawRecord


@dataclass(frozen=True)
class SourceFile:
    unit_id: str
    path: Path
    source: dict


def iter_raw_records(sources: list[dict]) -> Iterable[RawRecord]:
    for source_file in iter_source_files(sources):
        yield from iter_source_file_records(source_file.path, source_file.source)


def iter_source_files(sources: list[dict]) -> Iterable[SourceFile]:
    for source_index, source in enumerate(sources):
        for pattern_index, pattern in enumerate(source.get("paths", [])):
            for match_index, file_name in enumerate(sorted(glob(pattern))):
                yield SourceFile(
                    unit_id=f"local:{source_index}:{pattern_index}:{match_index}",
                    path=Path(file_name),
                    source=source,
                )


def iter_source_file_records(path: Path, source: dict) -> Iterable[RawRecord]:
    yield from _iter_file_records(path, source)


def _iter_file_records(path: Path, source: dict) -> Iterable[RawRecord]:
    mode = source.get("record_mode", "line")
    with _open_text_lines(path) as lines:
        if mode == "object":
            yield from _iter_object_records(path, source, lines)
        else:
            yield from _iter_line_records(path, source, lines)


def _iter_line_records(path: Path, source: dict, lines: Iterable[str]) -> Iterable[RawRecord]:
    record_index = 0
    for line_number, line in enumerate(lines, start=1):
        text = line.rstrip("\n")
        if not text.strip():
            continue
        record_index += 1
        yield _record(path, source, text, line_number, line_number, record_index)


def _iter_object_records(path: Path, source: dict, lines: Iterable[str]) -> Iterable[RawRecord]:
    depth = 0
    start = 0
    buffer: list[str] = []
    record_index = 0

    for line_number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not buffer and not stripped:
            continue
        if not buffer:
            start = line_number

        buffer.append(line.rstrip("\n"))
        depth += stripped.count("{")
        depth -= stripped.count("}")

        if buffer and depth == 0:
            text = "\n".join(buffer).strip()
            if text.endswith(","):
                text = text[:-1]
            record_index += 1
            yield _record(path, source, text, start, line_number, record_index)
            buffer = []


def _record(
    path: Path,
    source: dict,
    text: str,
    line_start: int,
    line_end: int,
    record_index: int,
) -> RawRecord:
    source_id = source.get("source_id", path.stem)
    source_type = source.get("source_type", "auto")
    identity = f"{source_id}:{path}:{line_start}:{line_end}:{text}"
    record_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    checksum = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return RawRecord(
        record_id=record_id,
        source_id=source_id,
        source_type=source_type,
        file_path=str(path),
        line_start=line_start,
        line_end=line_end,
        record_index=record_index,
        raw_text=text,
        checksum=f"sha256:{checksum}",
        size_bytes=len(text.encode("utf-8")),
        storage_ref=f"file://{path}#L{line_start}-L{line_end}",
        include_raw_text=bool(source.get("include_raw_text", False)),
        product=source.get("product"),
        format_version=source.get("format_version"),
        llm_enabled=bool(source.get("llm_enabled", False)),
    )


@contextmanager
def _open_text_lines(path: Path) -> Iterator[Iterable[str]]:
    suffix = path.suffix.lower()
    if suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            yield handle
        return
    if suffix == ".bz2":
        with bz2.open(path, "rt", encoding="utf-8") as handle:
            yield handle
        return
    if suffix in {".xz", ".lzma"}:
        with lzma.open(path, "rt", encoding="utf-8") as handle:
            yield handle
        return
    if suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            member = sorted(name for name in archive.namelist() if not name.endswith("/"))[0]
            with archive.open(member) as handle:
                yield (line.decode("utf-8") for line in handle)
        return
    with path.open("rt", encoding="utf-8") as handle:
        yield handle
