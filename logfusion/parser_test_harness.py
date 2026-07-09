from __future__ import annotations

from pathlib import Path
from typing import Any

from logfusion.parser_registry import ParserRegistryError, _find_parser, _now, load_registry, save_registry
from logfusion.parser_runtime import extract_fields


def run_registry_tests(registry_path: Path, parser_id: str | None = None) -> dict[str, Any]:
    registry = load_registry(registry_path)
    parsers = [_find_parser(registry, parser_id)] if parser_id else registry["parsers"]
    reports = [_run_parser_tests(parser) for parser in parsers]

    for parser, report in zip(parsers, reports, strict=True):
        parser["test_case_count"] = report["test_case_count"]
        parser["success_rate"] = report["success_rate"]
        parser["schema_mapping_rate"] = report["schema_mapping_rate"]
        parser["last_test_report"] = report
        parser["updated_at"] = _now()

    save_registry(registry, registry_path)
    return reports[0] if parser_id else {"reports": reports}


def _run_parser_tests(parser: dict[str, Any]) -> dict[str, Any]:
    test_cases = parser.get("test_cases", [])
    results = [_run_test_case(parser, test_case) for test_case in test_cases]
    passed_count = sum(1 for result in results if result["passed"])
    test_case_count = len(results)
    mapping_rate = _schema_mapping_rate(parser)
    return {
        "parser_id": parser["parser_id"],
        "test_case_count": test_case_count,
        "passed_count": passed_count,
        "failed_count": test_case_count - passed_count,
        "success_rate": round(passed_count / test_case_count, 6) if test_case_count else 0.0,
        "schema_mapping_rate": mapping_rate,
        "results": results,
    }


def _run_test_case(parser: dict[str, Any], test_case: dict[str, Any]) -> dict[str, Any]:
    raw_text = test_case.get("raw_text", "")
    expected = test_case.get("expected_fields", {})
    extracted = _extract_fields(parser, raw_text)
    missing_or_mismatched = {
        key: {"expected": value, "actual": extracted.get(key)}
        for key, value in expected.items()
        if extracted.get(key) != value
    }
    return {
        "record_id": test_case.get("record_id"),
        "passed": not missing_or_mismatched,
        "extracted_fields": extracted,
        "missing_or_mismatched": missing_or_mismatched,
    }


def _extract_fields(parser: dict[str, Any], raw_text: str) -> dict[str, str]:
    return extract_fields(parser, raw_text)


def _schema_mapping_rate(parser: dict[str, Any]) -> float:
    fields = parser.get("field_candidates", {})
    mapping = parser.get("schema_mapping", {})
    return round(len(mapping) / len(fields), 6) if fields else 0.0
