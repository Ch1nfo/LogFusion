from __future__ import annotations

import hashlib
import json
import os
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from logfusion.file_source import SourceFile, iter_source_file_records, iter_source_files
from logfusion.models import RawRecord
from logfusion.pipeline import ParseOutcome, iter_parse_records
from logfusion.quality import QualityAccumulator
from logfusion.raw_store import iter_stored_raw_records, raw_record_to_dict


CHECKPOINT_VERSION = 2


class CheckpointError(ValueError):
    pass


@dataclass(frozen=True)
class _InputRecord:
    record: RawRecord
    unit_id: str
    unit_record_index: int


class JsonlSink:
    def __init__(self, path: Path, resume_offset: int | None = None) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        if resume_offset is None:
            self._handle = path.open("w+b")
        else:
            if isinstance(resume_offset, bool) or not isinstance(resume_offset, int) or resume_offset < 0:
                raise CheckpointError(f"invalid checkpoint offset for output: {path}")
            if not path.exists():
                raise CheckpointError(f"resume output does not exist: {path}")
            if path.stat().st_size < resume_offset:
                raise CheckpointError(f"resume output is shorter than checkpoint offset: {path}")
            self._handle = path.open("r+b")
            self._handle.truncate(resume_offset)
            self._handle.seek(resume_offset)

    def write(self, document: dict[str, Any]) -> None:
        payload = json.dumps(document, ensure_ascii=False, sort_keys=True).encode("utf-8") + b"\n"
        self._handle.write(payload)

    def position(self) -> int:
        return self._handle.tell()

    def commit(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def close(self) -> None:
        self._handle.close()


def run_streaming_pipeline(
    sources: list[dict[str, Any]],
    normalized_output: Path,
    unknown_output: Path,
    summary_output: Path,
    raw_output: Path | None = None,
    registry_path: Path | None = None,
    checkpoint_path: Path | None = None,
    checkpoint_every: int = 10_000,
    resume: bool = False,
) -> dict[str, Any]:
    source_files = list(iter_source_files(sources))
    manifest = _source_manifest(source_files)
    config_fingerprint = _hash_json(sources)
    return _run_stream(
        records_factory=lambda completed: _iter_local_input_records(source_files, completed),
        manifest=manifest,
        config_fingerprint=config_fingerprint,
        normalized_output=normalized_output,
        unknown_output=unknown_output,
        summary_output=summary_output,
        raw_output=raw_output,
        registry_path=registry_path,
        checkpoint_path=checkpoint_path,
        checkpoint_every=checkpoint_every,
        resume=resume,
    )


def run_streaming_raw_replay(
    raw_input: Path,
    normalized_output: Path,
    unknown_output: Path,
    summary_output: Path,
    registry_path: Path | None = None,
    checkpoint_path: Path | None = None,
    checkpoint_every: int = 10_000,
    resume: bool = False,
) -> dict[str, Any]:
    unit_id = f"raw:{raw_input.resolve()}"
    metadata = _file_metadata(raw_input)
    metadata["unit_id"] = unit_id
    manifest = [metadata]
    return _run_stream(
        records_factory=lambda completed: _iter_raw_input_records(raw_input, unit_id, completed),
        manifest=manifest,
        config_fingerprint=_hash_json({"raw_input": str(raw_input)}),
        normalized_output=normalized_output,
        unknown_output=unknown_output,
        summary_output=summary_output,
        raw_output=None,
        registry_path=registry_path,
        checkpoint_path=checkpoint_path,
        checkpoint_every=checkpoint_every,
        resume=resume,
    )


def _run_stream(
    records_factory: Callable[[set[str]], Iterable[_InputRecord]],
    manifest: list[dict[str, Any]],
    config_fingerprint: str,
    normalized_output: Path,
    unknown_output: Path,
    summary_output: Path,
    raw_output: Path | None,
    registry_path: Path | None,
    checkpoint_path: Path | None,
    checkpoint_every: int,
    resume: bool,
) -> dict[str, Any]:
    if checkpoint_every <= 0:
        raise ValueError("checkpoint_every must be greater than zero")
    if resume and checkpoint_path is None:
        raise CheckpointError("--resume requires a checkpoint path")

    _validate_path_conflicts(
        manifest,
        normalized_output,
        unknown_output,
        summary_output,
        raw_output,
        registry_path,
        checkpoint_path,
    )

    expected = {
        "config_fingerprint": config_fingerprint,
        "registry_fingerprint": _file_fingerprint(registry_path),
        "input_manifest": manifest,
        "output_paths": _output_paths(normalized_output, unknown_output, summary_output, raw_output),
    }
    state = _load_resume_state(checkpoint_path, expected) if resume else None
    if state:
        try:
            accumulator = QualityAccumulator.from_snapshot(
                _expand_quality_snapshot(state["quality"])
            )
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise CheckpointError(f"invalid checkpoint quality state: {exc}") from exc
        if accumulator.finalize()["total_records"] != state["processed_records"]:
            raise CheckpointError("invalid checkpoint: processed_records does not match quality state")
    else:
        accumulator = QualityAccumulator()
    offsets = state.get("output_offsets", {}) if state else {}
    cursor = dict(state.get("cursor", {})) if state else {}
    completed_files = set(state.get("completed_files", [])) if state else set()
    processed = state.get("processed_records", 0) if state else 0

    sinks: list[JsonlSink] = []
    try:
        normalized_sink = JsonlSink(normalized_output, offsets["normalized"] if state else None)
        sinks.append(normalized_sink)
        unknown_sink = JsonlSink(unknown_output, offsets["unknown"] if state else None)
        sinks.append(unknown_sink)
        raw_sink = None
        if raw_output is not None:
            raw_sink = JsonlSink(raw_output, offsets["raw"] if state else None)
            sinks.append(raw_sink)

        if checkpoint_path is not None and state is None:
            _commit_checkpoint(
                checkpoint_path, expected, accumulator, sinks, processed, cursor, completed_files, "running",
            )
        if state and state.get("status") == "complete":
            summary = accumulator.finalize()
            _atomic_write_json(summary_output, summary)
            return summary

        pending: deque[_InputRecord] = deque()

        def records() -> Iterator[RawRecord]:
            resumable = _resume_input_records(records_factory(set(completed_files)), cursor)
            for item in resumable:
                pending.append(item)
                yield item.record

        for outcome in iter_parse_records(records(), registry_path):
            if not pending:
                raise RuntimeError("parser runtime produced an outcome without an input record")
            item = pending.popleft()
            if outcome.record.record_id != item.record.record_id:
                raise RuntimeError("parser runtime changed input record order")
            unit_id = item.unit_id
            previous_unit = cursor.get("unit_id")
            if previous_unit and previous_unit != unit_id:
                completed_files.add(previous_unit)
            previous_chain = cursor.get("record_chain", "") if previous_unit == unit_id else ""
            cursor = {
                "unit_id": unit_id,
                "file_key": _record_file_key(outcome.record),
                "source_id": outcome.record.source_id,
                "file_path": outcome.record.file_path,
                "record_index": item.unit_record_index,
                "raw_record_index": outcome.record.record_index,
                "record_id": outcome.record.record_id,
                "checksum": outcome.record.checksum,
                "record_chain": _extend_record_chain(previous_chain, outcome.record),
            }
            _write_outcome(outcome, normalized_sink, unknown_sink, raw_sink, accumulator)
            processed += 1
            if checkpoint_path is not None and processed % checkpoint_every == 0:
                _commit_checkpoint(
                    checkpoint_path, expected, accumulator, sinks, processed, cursor, completed_files, "running",
                )

        if pending:
            raise RuntimeError("parser runtime did not produce an outcome for every input record")
        if cursor.get("unit_id"):
            completed_files.add(cursor["unit_id"])
        if checkpoint_path is not None:
            _commit_checkpoint(
                checkpoint_path, expected, accumulator, sinks, processed, cursor, completed_files, "complete",
            )
        else:
            for sink in sinks:
                sink.commit()
        summary = accumulator.finalize()
        _atomic_write_json(summary_output, summary)
        return summary
    finally:
        for sink in sinks:
            sink.close()


def _write_outcome(
    outcome: ParseOutcome,
    normalized_sink: JsonlSink,
    unknown_sink: JsonlSink,
    raw_sink: JsonlSink | None,
    accumulator: QualityAccumulator,
) -> None:
    if raw_sink is not None:
        raw_sink.write(raw_record_to_dict(outcome.record))
    if outcome.normalized is not None:
        normalized_sink.write(outcome.normalized)
        accumulator.add_normalized(outcome.normalized)
    if outcome.unknown is not None:
        unknown_sink.write(outcome.unknown)
        accumulator.add_unknown(outcome.unknown)


def _iter_local_input_records(
    source_files: list[SourceFile],
    completed_files: set[str],
) -> Iterator[_InputRecord]:
    for source_file in source_files:
        if source_file.unit_id in completed_files:
            continue
        records = iter_source_file_records(source_file.path, source_file.source)
        for record_index, record in enumerate(records, start=1):
            yield _InputRecord(record, source_file.unit_id, record_index)


def _iter_raw_input_records(
    raw_input: Path,
    unit_id: str,
    completed_files: set[str],
) -> Iterator[_InputRecord]:
    if unit_id in completed_files:
        return
    for record_index, record in enumerate(iter_stored_raw_records(raw_input), start=1):
        yield _InputRecord(record, unit_id, record_index)


def _resume_input_records(
    records: Iterable[_InputRecord],
    cursor: dict[str, Any],
) -> Iterator[_InputRecord]:
    current_unit = cursor.get("unit_id")
    current_index = int(cursor.get("record_index", 0))
    cursor_validated = current_unit is None
    record_chain = ""
    for item in records:
        if item.unit_id == current_unit:
            record_chain = _extend_record_chain(record_chain, item.record)
            if item.unit_record_index < current_index:
                continue
            if item.unit_record_index == current_index:
                if (
                    item.record.checksum != cursor.get("checksum")
                    or item.record.record_id != cursor.get("record_id")
                    or record_chain != cursor.get("record_chain")
                ):
                    raise CheckpointError("input files changed at checkpoint cursor")
                cursor_validated = True
                continue
            if not cursor_validated:
                raise CheckpointError("checkpoint cursor record no longer exists")
        elif current_unit is not None and not cursor_validated:
            raise CheckpointError("checkpoint cursor file no longer matches input order")
        yield item
    if not cursor_validated:
        raise CheckpointError("checkpoint cursor record no longer exists")


def _record_file_key(record) -> str:
    return f"{record.source_id}\0{record.file_path}"


def _extend_record_chain(previous_chain: str, record) -> str:
    digest = hashlib.sha256()
    digest.update(previous_chain.encode("ascii"))
    digest.update(b"\0")
    digest.update(record.record_id.encode("utf-8"))
    digest.update(b"\0")
    digest.update(record.checksum.encode("utf-8"))
    return f"sha256:{digest.hexdigest()}"


def _commit_checkpoint(
    checkpoint_path: Path,
    expected: dict[str, Any],
    accumulator: QualityAccumulator,
    sinks: list[JsonlSink],
    processed: int,
    cursor: dict[str, Any],
    completed_files: set[str],
    status: str,
) -> None:
    for sink in sinks:
        sink.commit()
    output_offsets = {
        "normalized": sinks[0].position(),
        "unknown": sinks[1].position(),
    }
    if len(sinks) == 3:
        output_offsets["raw"] = sinks[2].position()
    document = {
        "version": CHECKPOINT_VERSION,
        **expected,
        "status": status,
        "processed_records": processed,
        "cursor": cursor,
        "completed_files": sorted(completed_files),
        "output_offsets": output_offsets,
        "quality": _compact_quality_snapshot(accumulator.snapshot()),
    }
    _atomic_write_json(checkpoint_path, document, pretty=False)


def _compact_quality_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    compact = dict(snapshot)
    counts = compact.pop("unknown_template_count")
    errors = compact.pop("unknown_template_error")
    orders = compact.pop("unknown_template_order")
    compact["unknown_template_entries"] = [
        [template, counts[template], errors[template], orders[template]]
        for template in sorted(orders, key=orders.__getitem__)
    ]
    return compact


def _expand_quality_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    if "unknown_template_entries" not in snapshot:
        return snapshot
    entries = snapshot["unknown_template_entries"]
    if not isinstance(entries, list):
        raise CheckpointError("invalid checkpoint quality state: template entries must be a list")
    expanded = dict(snapshot)
    expanded.pop("unknown_template_entries")
    counts: dict[str, Any] = {}
    errors: dict[str, Any] = {}
    orders: dict[str, Any] = {}
    for entry in entries:
        if not isinstance(entry, list) or len(entry) != 4:
            raise CheckpointError("invalid checkpoint quality state: invalid template entry")
        template, count, error, order = entry
        if not isinstance(template, str) or not template or template in counts:
            raise CheckpointError("invalid checkpoint quality state: invalid template key")
        counts[template] = count
        errors[template] = error
        orders[template] = order
    expanded["unknown_template_count"] = counts
    expanded["unknown_template_error"] = errors
    expanded["unknown_template_order"] = orders
    return expanded


def _load_resume_state(checkpoint_path: Path | None, expected: dict[str, Any]) -> dict[str, Any]:
    if checkpoint_path is None or not checkpoint_path.exists():
        raise CheckpointError("checkpoint does not exist")
    try:
        state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"invalid checkpoint: {exc}") from exc
    if not isinstance(state, dict):
        raise CheckpointError("invalid checkpoint: root must be an object")
    if state.get("version") != CHECKPOINT_VERSION:
        raise CheckpointError("unsupported checkpoint version")
    required = {
        "config_fingerprint",
        "registry_fingerprint",
        "input_manifest",
        "output_paths",
        "status",
        "processed_records",
        "cursor",
        "completed_files",
        "output_offsets",
        "quality",
    }
    missing = sorted(required.difference(state))
    if missing:
        raise CheckpointError(f"invalid checkpoint: missing fields: {', '.join(missing)}")
    if state.get("config_fingerprint") != expected["config_fingerprint"]:
        raise CheckpointError("source configuration changed")
    if state.get("registry_fingerprint") != expected["registry_fingerprint"]:
        raise CheckpointError("parser registry changed")
    if state.get("input_manifest") != expected["input_manifest"]:
        raise CheckpointError("input files changed")
    if state.get("output_paths") != expected["output_paths"]:
        raise CheckpointError("output paths changed")

    if state["status"] not in {"running", "complete"}:
        raise CheckpointError("invalid checkpoint: invalid status")
    if not _is_nonnegative_int(state["processed_records"]):
        raise CheckpointError("invalid checkpoint: processed_records must be a non-negative integer")
    if not isinstance(state["quality"], dict):
        raise CheckpointError("invalid checkpoint: quality must be an object")

    completed_files = state["completed_files"]
    if (
        not isinstance(completed_files, list)
        or any(not isinstance(unit_id, str) or not unit_id for unit_id in completed_files)
        or len(set(completed_files)) != len(completed_files)
    ):
        raise CheckpointError("invalid checkpoint: completed_files must contain unique unit identifiers")
    manifest_units = {
        item.get("unit_id")
        for item in expected["input_manifest"]
        if isinstance(item, dict)
    }
    if not set(completed_files).issubset(manifest_units):
        raise CheckpointError("invalid checkpoint: completed_files contains an unknown input unit")

    cursor = state["cursor"]
    if not isinstance(cursor, dict):
        raise CheckpointError("invalid checkpoint: cursor must be an object")
    if state["processed_records"] == 0:
        if cursor:
            raise CheckpointError("invalid checkpoint: zero-record checkpoint must have an empty cursor")
    else:
        cursor_fields = {
            "unit_id",
            "file_key",
            "source_id",
            "file_path",
            "record_index",
            "raw_record_index",
            "record_id",
            "checksum",
            "record_chain",
        }
        if cursor_fields.difference(cursor):
            raise CheckpointError("invalid checkpoint: cursor fields are incomplete")
        string_fields = cursor_fields.difference(
            {"record_index", "raw_record_index", "file_path"}
        )
        if any(not isinstance(cursor[field], str) or not cursor[field] for field in string_fields):
            raise CheckpointError("invalid checkpoint: cursor string fields are invalid")
        if not isinstance(cursor["file_path"], str):
            raise CheckpointError("invalid checkpoint: cursor file_path must be a string")
        if not _is_positive_int(cursor["record_index"]):
            raise CheckpointError("invalid checkpoint: cursor record_index must be positive")
        if not _is_nonnegative_int(cursor["raw_record_index"]):
            raise CheckpointError("invalid checkpoint: cursor raw_record_index must be non-negative")
        if not cursor["record_chain"].startswith("sha256:"):
            raise CheckpointError("invalid checkpoint: cursor record_chain is invalid")
        if cursor["unit_id"] not in manifest_units:
            raise CheckpointError("invalid checkpoint: cursor references an unknown input unit")
        if state["status"] == "running" and cursor["unit_id"] in completed_files:
            raise CheckpointError("invalid checkpoint: running cursor cannot be completed")
        if state["status"] == "complete" and cursor["unit_id"] not in completed_files:
            raise CheckpointError("invalid checkpoint: complete cursor must be completed")

    offsets = state["output_offsets"]
    if not isinstance(offsets, dict):
        raise CheckpointError("invalid checkpoint: output_offsets must be an object")
    required_offsets = {"normalized", "unknown"}
    if expected["output_paths"]["raw"] is not None:
        required_offsets.add("raw")
    if required_offsets.difference(offsets):
        raise CheckpointError("invalid checkpoint: output offset is missing")
    if any(not _is_nonnegative_int(offsets[name]) for name in required_offsets):
        raise CheckpointError("invalid checkpoint: output offsets must be non-negative integers")
    return state


def _is_nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_positive_int(value: Any) -> bool:
    return _is_nonnegative_int(value) and value > 0


def _source_manifest(source_files: list[SourceFile]) -> list[dict[str, Any]]:
    manifest = []
    for source_file in source_files:
        metadata = _file_metadata(source_file.path)
        metadata["source_id"] = source_file.source.get("source_id", source_file.path.stem)
        metadata["unit_id"] = source_file.unit_id
        manifest.append(metadata)
    return manifest


def _file_metadata(path: Path) -> dict[str, Any]:
    stat = path.stat()
    metadata = {
        "path": str(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "ctime_ns": stat.st_ctime_ns,
        "device": stat.st_dev,
        "inode": stat.st_ino,
    }
    metadata["metadata_checksum"] = f"sha256:{_hash_json(metadata)}"
    return metadata


def _file_fingerprint(path: Path | None) -> str:
    if path is None:
        return "none"
    if not path.exists():
        return "missing"
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_json(document: Any) -> str:
    encoded = json.dumps(document, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _output_paths(normalized: Path, unknown: Path, summary: Path, raw: Path | None) -> dict[str, str | None]:
    return {
        "normalized": str(normalized.resolve()),
        "unknown": str(unknown.resolve()),
        "summary": str(summary.resolve()),
        "raw": str(raw.resolve()) if raw else None,
    }


def _validate_path_conflicts(
    manifest: list[dict[str, Any]],
    normalized: Path,
    unknown: Path,
    summary: Path,
    raw: Path | None,
    registry: Path | None,
    checkpoint: Path | None,
) -> None:
    output_paths = [
        ("normalized", normalized.resolve()),
        ("unknown", unknown.resolve()),
        ("summary", summary.resolve()),
    ]
    if raw is not None:
        output_paths.append(("raw", raw.resolve()))
    if checkpoint is not None:
        output_paths.append(("checkpoint", checkpoint.resolve()))
    for index, (name, path) in enumerate(output_paths):
        for previous_name, previous_path in output_paths[:index]:
            if _same_file(path, previous_path):
                raise CheckpointError(
                    f"output paths must be distinct: {previous_name} and {name} both use {path}"
                )

    protected_paths = [Path(item["path"]).resolve() for item in manifest]
    if registry is not None:
        protected_paths.append(registry.resolve())
    for name, path in output_paths:
        if any(_same_file(path, protected) for protected in protected_paths):
            raise CheckpointError(f"{name} output path conflicts with input or registry: {path}")


def _same_file(left: Path, right: Path) -> bool:
    if left == right:
        return True
    try:
        return left.exists() and right.exists() and os.path.samefile(left, right)
    except OSError:
        return False


def _atomic_write_json(
    path: Path,
    document: dict[str, Any],
    *,
    pretty: bool = True,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(
            document,
            handle,
            ensure_ascii=False,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
            sort_keys=True,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
