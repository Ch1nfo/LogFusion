from __future__ import annotations

import argparse
import json
from pathlib import Path

from logfusion.config import load_sources_config
from logfusion.parser_candidates import propose_parser_candidates
from logfusion.parser_registry import ParserRegistryError, list_registry, register_candidates, set_parser_status
from logfusion.parser_test_harness import run_registry_tests
from logfusion.pipeline import run_pipeline
from logfusion.pipeline import replay_raw_records
from logfusion.quality_drift import detect_drift
from logfusion.raw_store import write_raw_store
from logfusion.replay_compare import compare_raw_replays
from logfusion.shadow_replay import run_shadow_replay


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="logfusion")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse_parser = subparsers.add_parser("parse")
    parse_parser.add_argument("--config", required=True)
    parse_parser.add_argument("--output", required=True)
    parse_parser.add_argument("--unknown-output", default="output/unknown.jsonl")
    parse_parser.add_argument("--summary-output", default="output/summary.json")
    parse_parser.add_argument("--registry")
    parse_parser.add_argument("--raw-store-output")

    propose_parser = subparsers.add_parser("propose-parsers")
    propose_parser.add_argument("--unknown-input", required=True)
    propose_parser.add_argument("--output", required=True)

    registry_parser = subparsers.add_parser("registry")
    registry_subparsers = registry_parser.add_subparsers(dest="registry_command", required=True)

    register_parser = registry_subparsers.add_parser("register-candidates")
    register_parser.add_argument("--candidates", required=True)
    register_parser.add_argument("--registry", required=True)

    list_parser = registry_subparsers.add_parser("list")
    list_parser.add_argument("--registry", required=True)
    list_parser.add_argument("--output")

    status_parser = registry_subparsers.add_parser("set-status")
    status_parser.add_argument("--registry", required=True)
    status_parser.add_argument("--parser-id", required=True)
    status_parser.add_argument("--status", required=True)

    test_parser = registry_subparsers.add_parser("test")
    test_parser.add_argument("--registry", required=True)
    test_parser.add_argument("--parser-id")
    test_parser.add_argument("--output")

    replay_parser = registry_subparsers.add_parser("replay")
    replay_parser.add_argument("--registry", required=True)
    replay_parser.add_argument("--unknown-input", required=True)
    replay_parser.add_argument("--parser-id")
    replay_parser.add_argument("--output")

    quality_parser = subparsers.add_parser("quality")
    quality_subparsers = quality_parser.add_subparsers(dest="quality_command", required=True)
    drift_parser = quality_subparsers.add_parser("drift")
    drift_parser.add_argument("--baseline", required=True)
    drift_parser.add_argument("--current", required=True)
    drift_parser.add_argument("--output", required=True)

    replay_parser_top = subparsers.add_parser("replay")
    replay_subparsers = replay_parser_top.add_subparsers(dest="replay_command", required=True)
    replay_raw_parser = replay_subparsers.add_parser("raw")
    replay_raw_parser.add_argument("--raw-input", required=True)
    replay_raw_parser.add_argument("--output", required=True)
    replay_raw_parser.add_argument("--unknown-output", default="output/replay_unknown.jsonl")
    replay_raw_parser.add_argument("--summary-output", default="output/replay_summary.json")
    replay_raw_parser.add_argument("--registry")

    replay_compare_parser = replay_subparsers.add_parser("compare")
    replay_compare_parser.add_argument("--raw-input", required=True)
    replay_compare_parser.add_argument("--baseline-registry")
    replay_compare_parser.add_argument("--current-registry")
    replay_compare_parser.add_argument("--output", required=True)

    args = parser.parse_args(argv)
    if args.command == "parse":
        sources = load_sources_config(Path(args.config))
        registry_path = Path(args.registry) if args.registry else None
        result = run_pipeline(sources, registry_path=registry_path)
        if args.raw_store_output:
            write_raw_store(sources, Path(args.raw_store_output))
        _write_jsonl(Path(args.output), result.normalized)
        _write_jsonl(Path(args.unknown_output), result.unknown)
        _write_json(Path(args.summary_output), result.summary)
        return 0
    if args.command == "propose-parsers":
        unknown = _read_jsonl(Path(args.unknown_input))
        candidates = propose_parser_candidates(unknown)
        _write_jsonl(Path(args.output), candidates)
        return 0
    if args.command == "registry":
        try:
            return _handle_registry(args)
        except ParserRegistryError as exc:
            print(str(exc))
            return 1
    if args.command == "quality":
        return _handle_quality(args)
    if args.command == "replay":
        return _handle_replay(args)
    return 2


def _handle_registry(args: argparse.Namespace) -> int:
    if args.registry_command == "register-candidates":
        candidates = _read_jsonl(Path(args.candidates))
        register_candidates(candidates, Path(args.registry))
        return 0
    if args.registry_command == "list":
        parsers = list_registry(Path(args.registry))
        if args.output:
            _write_jsonl(Path(args.output), parsers)
        else:
            for parser in parsers:
                print(json.dumps(parser, ensure_ascii=False, sort_keys=True))
        return 0
    if args.registry_command == "set-status":
        set_parser_status(Path(args.registry), args.parser_id, args.status)
        return 0
    if args.registry_command == "test":
        report = run_registry_tests(Path(args.registry), args.parser_id)
        if args.output:
            _write_json(Path(args.output), report)
        else:
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0
    if args.registry_command == "replay":
        report = run_shadow_replay(Path(args.registry), Path(args.unknown_input), args.parser_id)
        if args.output:
            _write_json(Path(args.output), report)
        else:
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0
    return 2


def _handle_quality(args: argparse.Namespace) -> int:
    if args.quality_command == "drift":
        baseline = json.loads(Path(args.baseline).read_text(encoding="utf-8"))
        current = json.loads(Path(args.current).read_text(encoding="utf-8"))
        _write_json(Path(args.output), detect_drift(baseline, current))
        return 0
    return 2


def _handle_replay(args: argparse.Namespace) -> int:
    if args.replay_command == "raw":
        registry_path = Path(args.registry) if args.registry else None
        result = replay_raw_records(Path(args.raw_input), registry_path=registry_path)
        _write_jsonl(Path(args.output), result.normalized)
        _write_jsonl(Path(args.unknown_output), result.unknown)
        _write_json(Path(args.summary_output), result.summary)
        return 0
    if args.replay_command == "compare":
        baseline_registry = Path(args.baseline_registry) if args.baseline_registry else None
        current_registry = Path(args.current_registry) if args.current_registry else None
        report = compare_raw_replays(Path(args.raw_input), baseline_registry, current_registry)
        _write_json(Path(args.output), report)
        return 0
    return 2


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def _write_json(path: Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
