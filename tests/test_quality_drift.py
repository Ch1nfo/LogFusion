import json

from logfusion.cli import main
from logfusion.quality import build_summary
from logfusion.quality_drift import detect_drift


def _normalized_event(parser_id="p1", source_type="sso", confidence=0.9):
    return {
        "event": {"category": "authentication"},
        "user": {"name": "alice"},
        "source": {"ip": "10.1.2.3"},
        "parser": {"id": parser_id, "confidence": confidence},
        "raw": {"source_type": source_type},
    }


def _unknown(template="user=<VAR>", reason="no_parser", source_type="unknown"):
    return {
        "reason": reason,
        "template_hint": template,
        "suggested_source_type": source_type,
        "raw": {"source_type": source_type},
        "parser": {"id": "unknown"},
    }


def test_quality_summary_includes_field_coverage_and_unknown_template_metrics():
    summary = build_summary(
        [_normalized_event(), _normalized_event(parser_id="p2", source_type="gitlab", confidence=0.8)],
        [_unknown(), _unknown(template="ip=<IP> user=<VAR>")],
    )

    assert summary["required_field_coverage"]["user.name"] == 1.0
    assert summary["required_field_coverage"]["source.ip"] == 1.0
    assert summary["unknown_template_count"] == 2
    assert summary["unknown_reason_count"]["no_parser"] == 2
    assert summary["parser_confidence_min"] == 0.8
    assert summary["parser_confidence_p50"] == 0.9
    assert summary["source_type_count"]["unknown"] == 2


def test_detect_drift_flags_parse_success_drop_unknown_template_spike_and_confidence_drop():
    baseline = {
        "parse_success_rate": 0.95,
        "unknown_template_count": 2,
        "parser_confidence_avg": 0.9,
        "required_field_coverage": {"user.name": 0.95},
    }
    current = {
        "parse_success_rate": 0.70,
        "unknown_template_count": 8,
        "parser_confidence_avg": 0.60,
        "required_field_coverage": {"user.name": 0.50},
    }

    report = detect_drift(baseline, current)

    assert report["drift_detected"] is True
    assert report["flags"]["parse_success_rate_drop"]["triggered"] is True
    assert report["flags"]["unknown_template_count_spike"]["triggered"] is True
    assert report["flags"]["parser_confidence_avg_drop"]["triggered"] is True
    assert report["flags"]["required_field_coverage_drop:user.name"]["triggered"] is True


def test_cli_quality_drift_writes_report(tmp_path):
    baseline = tmp_path / "baseline.json"
    current = tmp_path / "current.json"
    output = tmp_path / "drift.json"
    baseline.write_text(json.dumps({
        "parse_success_rate": 1.0,
        "unknown_template_count": 1,
        "parser_confidence_avg": 0.95,
        "required_field_coverage": {"user.name": 1.0},
    }), encoding="utf-8")
    current.write_text(json.dumps({
        "parse_success_rate": 0.7,
        "unknown_template_count": 5,
        "parser_confidence_avg": 0.7,
        "required_field_coverage": {"user.name": 0.6},
    }), encoding="utf-8")

    exit_code = main([
        "quality",
        "drift",
        "--baseline",
        str(baseline),
        "--current",
        str(current),
        "--output",
        str(output),
    ])

    assert exit_code == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["drift_detected"] is True
