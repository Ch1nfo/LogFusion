from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

from logfusion.config import load_detection_policy, load_fusion_policy, load_kafka_config, load_llm_config, load_sources_config
from logfusion.baseline import BaselineEngine, BaselineError, query_baseline_state
from logfusion.cases import CaseEngine, CaseError, get_case, list_cases
from logfusion.correlation import CorrelationEngine, CorrelationError, query_incident_state
from logfusion.detection import DetectionConfig, DetectionEngine, DetectionError, query_detection_state
from logfusion.evaluation import EvaluationEngine, EvaluationError, _iso
from logfusion.features import FeatureError, build_features_from_jsonl, query_feature_state
from logfusion.fusion import FusionConfig, FusionEngine, FusionError, query_risk_state
from logfusion.orchestrator import OrchestratorConfig, OrchestratorError, run_continuous_pipeline
from logfusion.kafka_source import KafkaConsumerError, run_kafka_streaming_pipeline
from logfusion.kafka_ueba import run_kafka_ueba_pipeline
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

    kafka_ueba_parser = consume_subparsers.add_parser("kafka-ueba")
    kafka_ueba_parser.add_argument("--config", required=True)
    kafka_ueba_parser.add_argument("--output", required=True)
    kafka_ueba_parser.add_argument("--unknown-output", default="output/kafka_unknown.jsonl")
    kafka_ueba_parser.add_argument("--summary-output", default="output/kafka_summary.json")
    kafka_ueba_parser.add_argument("--raw-store-output")
    kafka_ueba_parser.add_argument("--registry")
    kafka_ueba_parser.add_argument("--checkpoint")
    kafka_ueba_parser.add_argument("--checkpoint-every", type=int, default=10_000)
    kafka_ueba_parser.add_argument("--resume", action="store_true")
    kafka_ueba_parser.add_argument("--once", action="store_true")
    kafka_ueba_parser.add_argument("--feature-state", required=True)
    kafka_ueba_parser.add_argument("--baseline-state", required=True)
    kafka_ueba_parser.add_argument("--detection-state", required=True)
    kafka_ueba_parser.add_argument("--incident-state", required=True)
    kafka_ueba_parser.add_argument("--risk-state", required=True)
    kafka_ueba_parser.add_argument("--watermark-lag-seconds", type=int, default=300)
    kafka_ueba_parser.add_argument("--peer-groups")

    features_parser = subparsers.add_parser("features")
    features_subparsers = features_parser.add_subparsers(dest="features_command", required=True)
    features_build_parser = features_subparsers.add_parser("build")
    features_build_parser.add_argument("--input", required=True)
    features_build_parser.add_argument("--state", required=True)
    features_build_parser.add_argument("--batch-size", type=int, default=1000)
    features_query_parser = features_subparsers.add_parser("query")
    features_query_parser.add_argument("--state", required=True)
    features_query_parser.add_argument("--user", required=True)
    features_query_parser.add_argument("--from", dest="from_time", required=True)
    features_query_parser.add_argument("--to", dest="to_time", required=True)
    features_query_parser.add_argument("--window-size", type=int)

    baseline_parser = subparsers.add_parser("baseline")
    baseline_subparsers = baseline_parser.add_subparsers(dest="baseline_command", required=True)
    baseline_build_parser = baseline_subparsers.add_parser("build")
    baseline_build_parser.add_argument("--feature-state", required=True)
    baseline_build_parser.add_argument("--state", required=True)
    baseline_build_parser.add_argument("--as-of")
    baseline_build_parser.add_argument("--peer-groups")
    baseline_query_parser = baseline_subparsers.add_parser("query")
    baseline_query_parser.add_argument("--state", required=True)
    baseline_query_parser.add_argument("--user", required=True)
    baseline_query_parser.add_argument("--window-size", type=int)

    detect_parser = subparsers.add_parser("detect")
    detect_subparsers = detect_parser.add_subparsers(dest="detect_command", required=True)
    detect_run_parser = detect_subparsers.add_parser("run")
    detect_run_parser.add_argument("--feature-state", required=True)
    detect_run_parser.add_argument("--baseline-state", required=True)
    detect_run_parser.add_argument("--state", required=True)
    detect_run_parser.add_argument("--policy-config")
    detect_query_parser = detect_subparsers.add_parser("query")
    detect_query_parser.add_argument("--state", required=True)
    detect_query_parser.add_argument("--user", required=True)
    detect_query_parser.add_argument("--from", dest="from_time", required=True)
    detect_query_parser.add_argument("--to", dest="to_time", required=True)

    evaluate_parser = subparsers.add_parser("evaluate")
    evaluate_subparsers = evaluate_parser.add_subparsers(dest="evaluate_command", required=True)
    evaluate_run_parser = evaluate_subparsers.add_parser("run")
    evaluate_run_parser.add_argument("--feature-state", required=True)
    evaluate_run_parser.add_argument("--baseline-state", required=True)
    evaluate_run_parser.add_argument("--case-state", required=True)
    evaluate_run_parser.add_argument("--state", required=True)
    evaluate_run_parser.add_argument("--policy-config")
    evaluate_run_parser.add_argument("--name", required=True)
    evaluate_run_parser.add_argument("--from", dest="from_time", required=True)
    evaluate_run_parser.add_argument("--to", dest="to_time", required=True)
    evaluate_run_parser.add_argument("--report-output")
    evaluate_query_parser = evaluate_subparsers.add_parser("query")
    evaluate_query_parser.add_argument("--state", required=True)
    evaluate_query_parser.add_argument("--experiment-id", required=True)
    evaluate_compare_parser = evaluate_subparsers.add_parser("compare")
    evaluate_compare_parser.add_argument("--state", required=True)
    evaluate_compare_parser.add_argument("--experiment-id", action="append", required=True)

    correlate_parser = subparsers.add_parser("correlate")
    correlate_subparsers = correlate_parser.add_subparsers(dest="correlate_command", required=True)
    correlate_run_parser = correlate_subparsers.add_parser("run")
    correlate_run_parser.add_argument("--feature-state", required=True)
    correlate_run_parser.add_argument("--detection-state", required=True)
    correlate_run_parser.add_argument("--state", required=True)
    correlate_query_parser = correlate_subparsers.add_parser("query")
    correlate_query_parser.add_argument("--state", required=True)
    correlate_query_parser.add_argument("--user", required=True)
    correlate_query_parser.add_argument("--from", dest="from_time", required=True)
    correlate_query_parser.add_argument("--to", dest="to_time", required=True)

    fuse_parser = subparsers.add_parser("fuse")
    fuse_subparsers = fuse_parser.add_subparsers(dest="fuse_command", required=True)
    fuse_run_parser = fuse_subparsers.add_parser("run")
    fuse_run_parser.add_argument("--detection-state", required=True)
    fuse_run_parser.add_argument("--incident-state", required=True)
    fuse_run_parser.add_argument("--state", required=True)
    fuse_run_parser.add_argument("--policy-config")
    fuse_run_parser.add_argument("--case-state")
    fuse_run_parser.add_argument("--refresh-policy", action="store_true")
    fuse_query_parser = fuse_subparsers.add_parser("query")
    fuse_query_parser.add_argument("--state", required=True)
    fuse_query_parser.add_argument("--user", required=True)
    fuse_query_parser.add_argument("--from", dest="from_time", required=True)
    fuse_query_parser.add_argument("--to", dest="to_time", required=True)

    orchestrate_parser = subparsers.add_parser("orchestrate")
    orchestrate_subparsers = orchestrate_parser.add_subparsers(dest="orchestrate_command", required=True)
    orchestrate_run_parser = orchestrate_subparsers.add_parser("run")
    orchestrate_run_parser.add_argument("--input", required=True, help="Append-only normalized JSONL input.")
    orchestrate_run_parser.add_argument("--feature-state", required=True)
    orchestrate_run_parser.add_argument("--baseline-state", required=True)
    orchestrate_run_parser.add_argument("--detection-state", required=True)
    orchestrate_run_parser.add_argument("--incident-state", required=True)
    orchestrate_run_parser.add_argument("--risk-state", required=True)
    orchestrate_run_parser.add_argument("--checkpoint")
    orchestrate_run_parser.add_argument("--resume", action="store_true")
    orchestrate_run_parser.add_argument("--batch-size", type=int, default=1_000)
    orchestrate_run_parser.add_argument("--watermark-lag-seconds", type=int, default=300)
    orchestrate_run_parser.add_argument("--no-downstream", action="store_true")
    orchestrate_run_parser.add_argument("--peer-groups")

    cases_parser = subparsers.add_parser("cases")
    cases_subparsers = cases_parser.add_subparsers(dest="cases_command", required=True)
    cases_sync_parser = cases_subparsers.add_parser("sync")
    cases_sync_parser.add_argument("--risk-state", required=True)
    cases_sync_parser.add_argument("--state", required=True)
    cases_list_parser = cases_subparsers.add_parser("list")
    cases_list_parser.add_argument("--state", required=True)
    cases_list_parser.add_argument("--user")
    cases_list_parser.add_argument("--status")
    cases_show_parser = cases_subparsers.add_parser("show")
    cases_show_parser.add_argument("--state", required=True)
    cases_show_parser.add_argument("--case-id", required=True)
    cases_transition_parser = cases_subparsers.add_parser("transition")
    cases_transition_parser.add_argument("--risk-state", required=True)
    cases_transition_parser.add_argument("--state", required=True)
    cases_transition_parser.add_argument("--case-id", required=True)
    cases_transition_parser.add_argument("--status", required=True)
    cases_transition_parser.add_argument("--actor", required=True)
    cases_transition_parser.add_argument("--note")
    cases_transition_parser.add_argument("--suppression-until")
    cases_assign_parser = cases_subparsers.add_parser("assign")
    cases_assign_parser.add_argument("--risk-state", required=True)
    cases_assign_parser.add_argument("--state", required=True)
    cases_assign_parser.add_argument("--case-id", required=True)
    cases_assign_parser.add_argument("--owner")
    cases_assign_parser.add_argument("--actor", required=True)
    cases_assign_parser.add_argument("--note")
    cases_comment_parser = cases_subparsers.add_parser("comment")
    cases_comment_parser.add_argument("--risk-state", required=True)
    cases_comment_parser.add_argument("--state", required=True)
    cases_comment_parser.add_argument("--case-id", required=True)
    cases_comment_parser.add_argument("--actor", required=True)
    cases_comment_parser.add_argument("--note", required=True)
    cases_tags_parser = cases_subparsers.add_parser("set-tags")
    cases_tags_parser.add_argument("--risk-state", required=True)
    cases_tags_parser.add_argument("--state", required=True)
    cases_tags_parser.add_argument("--case-id", required=True)
    cases_tags_parser.add_argument("--actor", required=True)
    cases_tags_parser.add_argument("--tag", action="append", required=True)
    cases_disposition_parser = cases_subparsers.add_parser("disposition")
    cases_disposition_parser.add_argument("--risk-state", required=True)
    cases_disposition_parser.add_argument("--state", required=True)
    cases_disposition_parser.add_argument("--case-id", required=True)
    cases_disposition_parser.add_argument("--value", required=True)
    cases_disposition_parser.add_argument("--actor", required=True)
    cases_disposition_parser.add_argument("--note")

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
    if args.command == "features":
        return _handle_features(args)
    if args.command == "baseline":
        return _handle_baseline(args)
    if args.command == "detect":
        return _handle_detect(args)
    if args.command == "evaluate":
        return _handle_evaluate(args)
    if args.command == "correlate":
        return _handle_correlate(args)
    if args.command == "fuse":
        return _handle_fuse(args)
    if args.command == "orchestrate":
        return _handle_orchestrate(args)
    if args.command == "cases":
        return _handle_cases(args)
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
    if args.consume_command not in {"kafka", "kafka-ueba"}:
        return 2
    argument_error = _streaming_argument_error(args)
    if argument_error:
        print(argument_error)
        return 2
    try:
        _protect_config_path(args)
        config = load_kafka_config(Path(args.config))
        common = {
            "normalized_output": Path(args.output),
            "unknown_output": Path(args.unknown_output),
            "summary_output": Path(args.summary_output),
            "raw_output": Path(args.raw_store_output) if args.raw_store_output else None,
            "registry_path": Path(args.registry) if args.registry else None,
            "checkpoint_path": Path(args.checkpoint) if args.checkpoint else None,
            "checkpoint_every": args.checkpoint_every,
            "resume": args.resume,
            "stop_when_idle": args.once,
        }
        if args.consume_command == "kafka":
            run_kafka_streaming_pipeline(config, **common)
        else:
            run_kafka_ueba_pipeline(
                config,
                feature_state=Path(args.feature_state), baseline_state=Path(args.baseline_state),
                detection_state=Path(args.detection_state), incident_state=Path(args.incident_state),
                risk_state=Path(args.risk_state),
                orchestrator_config=OrchestratorConfig(watermark_lag_seconds=args.watermark_lag_seconds, peer_groups_path=args.peer_groups),
                **common,
            )
    except (CheckpointError, KafkaConsumerError, ValueError) as exc:
        print(str(exc))
        return 1
    return 0


def _handle_features(args: argparse.Namespace) -> int:
    try:
        if args.features_command == "build":
            if args.batch_size <= 0:
                print("--batch-size must be greater than zero")
                return 2
            report = build_features_from_jsonl(Path(args.input), Path(args.state), args.batch_size)
        elif args.features_command == "query":
            report = query_feature_state(
                Path(args.state), args.user, args.from_time, args.to_time, args.window_size,
            )
        else:
            return 2
    except FeatureError as exc:
        print(str(exc))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


def _handle_baseline(args: argparse.Namespace) -> int:
    try:
        if args.baseline_command == "build":
            with BaselineEngine(Path(args.feature_state), Path(args.state), peer_groups_path=Path(args.peer_groups) if args.peer_groups else None) as engine:
                report = engine.update(args.as_of)
        elif args.baseline_command == "query":
            report = query_baseline_state(Path(args.state), args.user, args.window_size)
        else:
            return 2
    except BaselineError as exc:
        print(str(exc))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


def _handle_detect(args: argparse.Namespace) -> int:
    try:
        if args.detect_command == "run":
            settings = load_detection_policy(Path(args.policy_config)) if args.policy_config else {}
            with DetectionEngine(Path(args.feature_state), Path(args.baseline_state), Path(args.state), DetectionConfig(**settings)) as engine:
                report = engine.run()
        elif args.detect_command == "query":
            report = query_detection_state(Path(args.state), args.user, args.from_time, args.to_time)
        else:
            return 2
    except DetectionError as exc:
        print(str(exc))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


def _handle_evaluate(args: argparse.Namespace) -> int:
    try:
        if args.evaluate_command == "run":
            settings = load_detection_policy(Path(args.policy_config)) if args.policy_config else {}
            with EvaluationEngine(Path(args.feature_state), Path(args.baseline_state), Path(args.case_state), Path(args.state), DetectionConfig(**settings)) as engine:
                report = engine.run(args.name, args.from_time, args.to_time)
            if args.report_output:
                _write_json(Path(args.report_output), report)
        elif args.evaluate_command in {"query", "compare"}:
            # Query and compare only need the Evaluation DB, but the engine constructor
            # deliberately validates production inputs. Use its persisted schema directly.
            connection = sqlite3.connect(Path(args.state))
            connection.row_factory = sqlite3.Row
            try:
                if args.evaluate_command == "query":
                    row = connection.execute("SELECT * FROM experiments WHERE experiment_id = ?", (args.experiment_id,)).fetchone()
                    metrics = connection.execute("SELECT * FROM experiment_metrics WHERE experiment_id = ?", (args.experiment_id,)).fetchone()
                    if row is None:
                        raise EvaluationError(f"experiment does not exist: {args.experiment_id}")
                    report = dict(row)
                    report["from"] = _iso(report.pop("start_ms"))
                    report["to"] = _iso(report.pop("end_ms"))
                    report["metrics"] = dict(metrics) if metrics else {}
                else:
                    if len(args.experiment_id) != 2:
                        raise EvaluationError("evaluate compare requires exactly two --experiment-id values")
                    def read(experiment_id: str) -> dict:
                        row = connection.execute("SELECT * FROM experiments WHERE experiment_id = ?", (experiment_id,)).fetchone()
                        metric = connection.execute("SELECT * FROM experiment_metrics WHERE experiment_id = ?", (experiment_id,)).fetchone()
                        if row is None:
                            raise EvaluationError(f"experiment does not exist: {experiment_id}")
                        return {"experiment_id": experiment_id, "name": row["name"], "metrics": dict(metric) if metric else {}}
                    left, right = read(args.experiment_id[0]), read(args.experiment_id[1])
                    fields = ("true_positive", "false_positive", "false_negative", "precision", "recall", "f1", "unlabeled_predictions")
                    report = {"left": left, "right": right, "delta": {field: right["metrics"].get(field, 0) - left["metrics"].get(field, 0) for field in fields}}
            finally:
                connection.close()
        else:
            return 2
    except (EvaluationError, sqlite3.Error) as exc:
        print(str(exc))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


def _handle_correlate(args: argparse.Namespace) -> int:
    try:
        if args.correlate_command == "run":
            with CorrelationEngine(Path(args.feature_state), Path(args.detection_state), Path(args.state)) as engine:
                report = engine.run()
        elif args.correlate_command == "query":
            report = query_incident_state(Path(args.state), args.user, args.from_time, args.to_time)
        else:
            return 2
    except CorrelationError as exc:
        print(str(exc))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


def _handle_fuse(args: argparse.Namespace) -> int:
    try:
        if args.fuse_command == "run":
            settings = load_fusion_policy(Path(args.policy_config)) if args.policy_config else {}
            with FusionEngine(
                Path(args.detection_state), Path(args.incident_state), Path(args.state), FusionConfig(**settings),
                Path(args.case_state) if args.case_state else None,
            ) as engine:
                report = engine.run(refresh_policy=args.refresh_policy)
        elif args.fuse_command == "query":
            report = query_risk_state(Path(args.state), args.user, args.from_time, args.to_time)
        else:
            return 2
    except FusionError as exc:
        print(str(exc))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


def _handle_orchestrate(args: argparse.Namespace) -> int:
    if args.orchestrate_command != "run":
        return 2
    try:
        report = run_continuous_pipeline(
            args.input, args.feature_state, args.baseline_state, args.detection_state,
            args.incident_state, args.risk_state,
            checkpoint_path=args.checkpoint,
            resume=args.resume,
            config=OrchestratorConfig(args.batch_size, args.watermark_lag_seconds, args.peer_groups),
            run_downstream=not args.no_downstream,
        )
    except (OrchestratorError, FeatureError) as exc:
        print(str(exc))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


def _handle_cases(args: argparse.Namespace) -> int:
    try:
        if args.cases_command == "sync":
            with CaseEngine(Path(args.risk_state), Path(args.state)) as engine:
                report = engine.sync()
        elif args.cases_command == "list":
            report = list_cases(Path(args.state), args.user, args.status)
        elif args.cases_command == "show":
            report = get_case(Path(args.state), args.case_id)
        elif args.cases_command in {"transition", "assign", "comment", "set-tags", "disposition"}:
            with CaseEngine(Path(args.risk_state), Path(args.state)) as engine:
                if args.cases_command == "transition":
                    report = engine.transition(args.case_id, args.status, args.actor, args.note, args.suppression_until)
                elif args.cases_command == "assign":
                    report = engine.assign(args.case_id, args.owner, args.actor, args.note)
                elif args.cases_command == "comment":
                    report = engine.comment(args.case_id, args.actor, args.note)
                else:
                    report = engine.set_disposition(args.case_id, args.value, args.actor, args.note) if args.cases_command == "disposition" else engine.set_tags(args.case_id, args.tag, args.actor)
        else:
            return 2
    except CaseError as exc:
        print(str(exc))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
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
