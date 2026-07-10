from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from logfusion.active_runtime import load_active_parsers, parse_with_active_parsers
from logfusion.file_source import iter_raw_records
from logfusion.models import RawRecord
from logfusion.parsers import parse_record
from logfusion.quality import build_summary
from logfusion.raw_store import iter_stored_raw_records
from logfusion.schema import validate_event
from logfusion.unknown import build_unknown_record


@dataclass(frozen=True)
class PipelineResult:
    normalized: list[dict[str, Any]]
    unknown: list[dict[str, Any]]
    summary: dict[str, Any]


@dataclass(frozen=True)
class ParseOutcome:
    record: RawRecord
    normalized: dict[str, Any] | None
    unknown: dict[str, Any] | None


def parse_sources(
    sources: list[dict[str, Any]],
    registry_path: Path | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result = run_pipeline(sources, registry_path=registry_path)
    return result.normalized, result.unknown


def run_pipeline(
    sources: list[dict[str, Any]],
    registry_path: Path | None = None,
) -> PipelineResult:
    normalized: list[dict[str, Any]] = []
    unknown: list[dict[str, Any]] = []
    for outcome in iter_parse_records(iter_raw_records(sources), registry_path):
        if outcome.normalized is not None:
            normalized.append(outcome.normalized)
        if outcome.unknown is not None:
            unknown.append(outcome.unknown)
    return PipelineResult(normalized, unknown, build_summary(normalized, unknown))


def replay_raw_records(
    raw_input: Path,
    registry_path: Path | None = None,
) -> PipelineResult:
    normalized: list[dict[str, Any]] = []
    unknown: list[dict[str, Any]] = []
    for outcome in iter_parse_records(iter_stored_raw_records(raw_input), registry_path):
        if outcome.normalized is not None:
            normalized.append(outcome.normalized)
        if outcome.unknown is not None:
            unknown.append(outcome.unknown)
    return PipelineResult(normalized, unknown, build_summary(normalized, unknown))


def iter_parse_records(
    records: Iterable[RawRecord],
    registry_path: Path | None = None,
) -> Iterator[ParseOutcome]:
    active_parsers = load_active_parsers(registry_path)
    for record in records:
        yield _parse_record_outcome(record, active_parsers)


def _parse_one_record(
    record,
    active_parsers: list[dict[str, Any]],
    normalized: list[dict[str, Any]],
    unknown: list[dict[str, Any]],
) -> None:
    outcome = _parse_record_outcome(record, active_parsers)
    if outcome.normalized is not None:
        normalized.append(outcome.normalized)
    if outcome.unknown is not None:
        unknown.append(outcome.unknown)


def _parse_record_outcome(record, active_parsers: list[dict[str, Any]]) -> ParseOutcome:
    event, failed = parse_record(record)
    if failed is not None and active_parsers:
        active_event = parse_with_active_parsers(record, active_parsers)
        if active_event is not None:
            event = active_event
            failed = None
    if event is not None:
        schema_errors = validate_event(event)
        if schema_errors:
            return ParseOutcome(record, None, build_unknown_record(
                record,
                "schema_validation_failed",
                event.get("raw", {}).get("source_type"),
                schema_errors,
            ))
        return ParseOutcome(record, event, None)
    if failed is not None:
        return ParseOutcome(record, None, failed)
    return ParseOutcome(record, None, build_unknown_record(record, "no_parser_result"))
