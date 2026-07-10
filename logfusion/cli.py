from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from logfusion.config import load_kafka_config, load_llm_config, load_sources_config
from logfusion.kafka_source import KafkaConsumerError, run_kafka_streaming_pipeline
from logfusion.parser_candidates import propose_parser_candidates
from logfusion.llm_generation import generate_and_validate_llm_candidates, generate_llm_candidates
from logfusion.llm_provider import LLMProviderError, OpenAICompatibleProvider
from logfusion.parser_registry import ParserRegistryError, list_registry, register_candidates, set_parser_status
from logfusion.parser_test_harness import run_registry_tests
from logfusion.quality_drift import detect_drift
from logfusion.replay_compare import compare_raw_replays
from logfusion.shadow_replay import run_shadow_replay
from logfusion.streaming import CheckpointError, run_streaming_pipeline, run_streaming_raw_replay


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
    parse_parser.add_argument("--checkpoint")
    parse_parser.add_argument("--checkpoint-every", type=int, default=10_000)
    parse_parser.add_argument("--resume", action="store_true")

    consume_parser = subparsers.add_parser("consume")
    consume_subparsers = consume_parser.add_subparsers(dest="consume_command", required=True)
    kafka_consume_parser = consume_subparsers.add_parser("kafka")
    kafka_consume_parser.add_argument("--config", required=True)
    kafka_consume_parser.add_argument("--output", required=True)
    kafka_consume_parser.add_argument("--unknown-output", default="output/kafka_unknown.jsonl")
    kafka_consume_parser.add_argument("--summary-output", default="output/kafka_summary.json")
    kafka_consume_parser.add_argument("--raw-store-output")
    kafka_consume_parser.add_argument("--registry")
    kafka_consume_parser.add_argument("--checkpoint")
    kafka_consume_parser.add_argument("--checkpoint-every", type=int, default=10_000)
    kafka_consume_parser.add_argument("--resume", action="store_true")
    kafka_consume_parser.add_argument("--once", action="store_true")

    propose_parser = subparsers.add_parser("propose-parsers")
    propose_parser.add_argument("--unknown-input", required=True)
    propose_parser.add_argument("--output", required=True)
    propose_parser.add_argument("--llm", action="store_true", help="Generate source-specific draft parsers with an LLM.")
    propose_parser.add_argument("--llm-config", help="Local YAML provider configuration.")
    propose_parser.add_argument("--registry")
    propose_parser.add_argument("--auto-validate", action="store_true")
    propose_parser.add_argument("--report-output")
    propose_parser.add_argument("--llm-base-url")
    propose_parser.add_argument("--llm-model")
    propose_parser.add_argument("--llm-api-key-env")
    propose_parser.add_argument("--llm-timeout", type=float)
    propose_parser.add_argument("--llm-sample-limit", type=int)

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
    replay_raw_parser.add_argument("--checkpoint")
    replay_raw_parser.add_argument("--checkpoint-every", type=int, default=10_000)
    replay_raw_parser.add_argument("--resume", action="store_true")

    replay_compare_parser = replay_subparsers.add_parser("compare")
    replay_compare_parser.add_argument("--raw-input", required=True)
    replay_compare_parser.add_argument("--baseline-registry")
    replay_compare_parser.add_argument("--current-registry")
    replay_compare_parser.add_argument("--output", required=True)

    args = parser.parse_args(argv)
    if args.command == "parse":
        argument_error = _streaming_argument_error(args)
        if argument_error:
            print(argument_error)
            return 2
        try:
            _protect_config_path(args)
            sources = load_sources_config(Path(args.config))
            registry_path = Path(args.registry) if args.registry else None
            run_streaming_pipeline(
                sources,
                normalized_output=Path(args.output),
                unknown_output=Path(args.unknown_output),
                summary_output=Path(args.summary_output),
                raw_output=Path(args.raw_store_output) if args.raw_store_output else None,
                registry_path=registry_path,
                checkpoint_path=Path(args.checkpoint) if args.checkpoint else None,
                checkpoint_every=args.checkpoint_every,
                resume=args.resume,
            )
        except CheckpointError as exc:
            print(str(exc))
            return 1
        return 0
    if args.command == "consume":
        return _handle_consume(args)
    if args.command == "propose-parsers":
        unknown = _read_jsonl(Path(args.unknown_input))
        llm_settings = load_llm_config(Path(args.llm_config)) if args.llm_config else {}
        llm_mode = args.llm or bool(args.llm_config)
        if llm_mode and llm_settings.get("enabled") is False:
            parser.error("LLM is disabled in --llm-config")
        if args.auto_validate and (not llm_mode or not args.registry):
            parser.error("--auto-validate requires LLM mode and --registry")
        if llm_mode:
            provider_name = llm_settings.get("provider", "openai_compatible")
            if provider_name != "openai_compatible":
                parser.error(f"Unsupported LLM provider: {provider_name}")
            base_url = args.llm_base_url or llm_settings.get("base_url")
            model = args.llm_model or llm_settings.get("model")
            if not base_url or not model:
                parser.error("LLM mode requires base_url and model in flags or --llm-config")
            api_key_env = args.llm_api_key_env or llm_settings.get("api_key_env", "LOGFUSION_LLM_API_KEY")
            timeout = args.llm_timeout or llm_settings.get("timeout_seconds", 30.0)
            sample_limit = args.llm_sample_limit or llm_settings.get("sample_limit", 5)
            provider = OpenAICompatibleProvider(
                base_url,
                model,
                api_key_env,
                timeout,
            )
            try:
                if args.auto_validate:
                    report = generate_and_validate_llm_candidates(
                        unknown, Path(args.registry), provider, sample_limit,
                    )
                    candidates = report["candidates"]
                else:
                    candidates, report = generate_llm_candidates(unknown, provider, sample_limit)
            except LLMProviderError as exc:
                print(str(exc))
                return 1
            if args.report_output:
                _write_json(Path(args.report_output), report)
        else:
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
        argument_error = _streaming_argument_error(args)
        if argument_error:
            print(argument_error)
            return 2
        registry_path = Path(args.registry) if args.registry else None
        try:
            run_streaming_raw_replay(
                raw_input=Path(args.raw_input),
                normalized_output=Path(args.output),
                unknown_output=Path(args.unknown_output),
                summary_output=Path(args.summary_output),
                registry_path=registry_path,
                checkpoint_path=Path(args.checkpoint) if args.checkpoint else None,
                checkpoint_every=args.checkpoint_every,
                resume=args.resume,
            )
        except CheckpointError as exc:
            print(str(exc))
            return 1
        return 0
    if args.replay_command == "compare":
        baseline_registry = Path(args.baseline_registry) if args.baseline_registry else None
        current_registry = Path(args.current_registry) if args.current_registry else None
        report = compare_raw_replays(Path(args.raw_input), baseline_registry, current_registry)
        _write_json(Path(args.output), report)
        return 0
    return 2


def _handle_consume(args: argparse.Namespace) -> int:
    if args.consume_command != "kafka":
        return 2
    argument_error = _streaming_argument_error(args)
    if argument_error:
        print(argument_error)
        return 2
    try:
        _protect_config_path(args)
        config = load_kafka_config(Path(args.config))
        run_kafka_streaming_pipeline(
            config,
            normalized_output=Path(args.output),
            unknown_output=Path(args.unknown_output),
            summary_output=Path(args.summary_output),
            raw_output=Path(args.raw_store_output) if args.raw_store_output else None,
            registry_path=Path(args.registry) if args.registry else None,
            checkpoint_path=Path(args.checkpoint) if args.checkpoint else None,
            checkpoint_every=args.checkpoint_every,
            resume=args.resume,
            stop_when_idle=args.once,
        )
    except (CheckpointError, KafkaConsumerError, ValueError) as exc:
        print(str(exc))
        return 1
    return 0


def _streaming_argument_error(args: argparse.Namespace) -> str | None:
    if args.resume and not args.checkpoint:
        return "--resume requires --checkpoint"
    if args.checkpoint_every <= 0:
        return "--checkpoint-every must be greater than zero"
    return None


def _protect_config_path(args: argparse.Namespace) -> None:
    config_path = Path(args.config).resolve()
    writable_paths = [
        Path(args.output),
        Path(args.unknown_output),
        Path(args.summary_output),
    ]
    if args.raw_store_output:
        writable_paths.append(Path(args.raw_store_output))
    if args.checkpoint:
        writable_paths.append(Path(args.checkpoint))
    if any(_same_path(path.resolve(), config_path) for path in writable_paths):
        raise CheckpointError(f"config path conflicts with output or checkpoint: {config_path}")


def _same_path(left: Path, right: Path) -> bool:
    if left == right:
        return True
    try:
        return left.exists() and right.exists() and os.path.samefile(left, right)
    except OSError:
        return False


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
