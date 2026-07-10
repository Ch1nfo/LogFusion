import json

import pytest

from logfusion import quality
from logfusion.schema import validate_event


def _normalized_event(
    *,
    parser_id="p1",
    source_type="sso",
    confidence=0.9,
    missing=(),
):
    event = {
        "event": {
            "category": "authentication",
            "action": "login",
            "outcome": "success",
        },
        "user": {"name": "alice"},
        "source": {"ip": "10.1.2.3"},
        "parser": {"id": parser_id, "confidence": confidence},
        "raw": {
            "source_type": source_type,
            "checksum": "sha256:abc",
            "storage_ref": "file://events.log#L1-L1",
        },
    }
    for field_path in missing:
        current = event
        *parents, leaf = field_path.split(".")
        for part in parents:
            current = current[part]
        current[leaf] = ""
    return event


def _unknown_event(
    *,
    template="user=<VAR>",
    reason="no_parser",
    source_type="unknown",
    schema_errors=(),
):
    return {
        "reason": reason,
        "template_hint": template,
        "schema_errors": list(schema_errors),
        "raw": {"source_type": source_type},
    }


def test_quality_accumulator_computes_summary_incrementally():
    accumulator = quality.QualityAccumulator()
    accumulator.add_normalized(_normalized_event(confidence=0.2))
    accumulator.add_unknown(_unknown_event(template="user=<VAR>"))
    accumulator.add_normalized(
        _normalized_event(
            parser_id="p2",
            confidence=0.8,
            missing=("event.action", "user.name", "raw.storage_ref"),
        )
    )
    accumulator.add_unknown(
        _unknown_event(
            template="ip=<IP>",
            reason="schema_validation_failed",
            source_type="gitlab",
            schema_errors=("event.time is required", "event.id is required"),
        )
    )
    accumulator.add_unknown(
        _unknown_event(template="user=<VAR>", schema_errors=("parser.id is required",))
    )

    assert accumulator.finalize() == {
        "total_records": 5,
        "normalized_records": 2,
        "unknown_records": 3,
        "parse_success_rate": 0.4,
        "parser_confidence_avg": 0.5,
        "parser_confidence_min": 0.2,
        "parser_confidence_p50": 0.8,
        "source_type_count": {"sso": 2, "unknown": 2, "gitlab": 1},
        "parser_count": {"p1": 1, "p2": 1},
        "missing_required_fields_count": 3,
        "required_field_coverage": {
            "event.category": 1.0,
            "event.action": 0.5,
            "event.outcome": 1.0,
            "user.name": 0.5,
            "source.ip": 1.0,
            "raw.checksum": 1.0,
            "raw.storage_ref": 0.5,
        },
        "unknown_template_count": 2,
        "unknown_template_top": {"user=<VAR>": 2, "ip=<IP>": 1},
        "unknown_reason_count": {"no_parser": 2, "schema_validation_failed": 1},
    }


def test_confidence_p50_uses_a_fixed_size_histogram():
    accumulator = quality.QualityAccumulator()
    histogram_size = len(accumulator._confidence_histogram)

    for index in range(20_000):
        accumulator.add_normalized(
            _normalized_event(confidence=(index % 1_001) / 1_000)
        )

    assert histogram_size == quality.CONFIDENCE_HISTOGRAM_BINS
    assert len(accumulator._confidence_histogram) == histogram_size
    assert accumulator.finalize()["parser_confidence_p50"] == 0.5


def test_large_confidence_stream_uses_compensated_sum_and_snapshot_round_trips():
    accumulator = quality.QualityAccumulator()
    record_count = 4_000_000
    for _ in range(record_count):
        accumulator._add_confidence(0.1)
    accumulator._normalized_records = record_count
    accumulator._source_type_count["sso"] = record_count
    accumulator._parser_count["p1"] = record_count
    for field in quality.TRACKED_FIELD_PATHS:
        accumulator._field_present_count[field] = record_count

    restored = quality.QualityAccumulator.from_snapshot(
        json.loads(json.dumps(accumulator.snapshot()))
    )

    assert restored.finalize()["parser_confidence_avg"] == 0.1
    assert restored.snapshot() == accumulator.snapshot()


def test_boolean_confidence_is_rejected_by_schema_and_not_accumulated():
    event = _normalized_event(confidence=True)
    assert "parser.confidence must be numeric" in validate_event(event)

    accumulator = quality.QualityAccumulator()
    accumulator.add_normalized(event)
    restored = quality.QualityAccumulator.from_snapshot(
        json.loads(json.dumps(accumulator.snapshot()))
    )

    assert restored.finalize()["parser_confidence_avg"] == 0.0


def test_unknown_template_tracking_is_bounded_and_tracks_late_heavy_hitters():
    accumulator = quality.QualityAccumulator()

    for index in range(quality.UNKNOWN_TEMPLATE_LIMIT):
        accumulator.add_unknown(_unknown_event(template=f"template-{index:05d}"))
    for _ in range(100):
        accumulator.add_unknown(_unknown_event(template="late-hot-template"))

    summary = accumulator.finalize()

    assert summary["unknown_template_count"] == 10_000
    assert summary["unknown_template_count_capped"] is True
    assert summary["unknown_template_top_approximate"] is True
    assert summary["unknown_template_top"]["late-hot-template"] >= 100
    assert len(accumulator._unknown_template_count) == quality.UNKNOWN_TEMPLATE_LIMIT


def test_snapshot_round_trip_restores_state_and_can_continue():
    before_checkpoint = [
        _normalized_event(confidence=0.2),
        _normalized_event(parser_id="p2", source_type="gitlab", confidence=0.8),
    ]
    before_checkpoint_unknown = [
        _unknown_event(template=f"template-{index:05d}") for index in range(10_001)
    ]
    after_checkpoint = [
        _normalized_event(confidence=0.6, missing=("source.ip",)),
    ]
    after_checkpoint_unknown = [
        _unknown_event(
            template="template-00000",
            reason="schema_validation_failed",
            schema_errors=("event.time is required",),
        )
    ]

    uninterrupted = quality.QualityAccumulator()
    checkpointed = quality.QualityAccumulator()
    for accumulator in (uninterrupted, checkpointed):
        for event in before_checkpoint:
            accumulator.add_normalized(event)
        for record in before_checkpoint_unknown:
            accumulator.add_unknown(record)

    serialized = json.loads(json.dumps(checkpointed.snapshot()))
    restored = quality.QualityAccumulator.from_snapshot(serialized)

    for accumulator in (uninterrupted, restored):
        for event in after_checkpoint:
            accumulator.add_normalized(event)
        for record in after_checkpoint_unknown:
            accumulator.add_unknown(record)

    assert restored.finalize() == uninterrupted.finalize()
    assert restored.finalize()["unknown_template_count_capped"] is True
    assert restored.finalize()["unknown_template_top_approximate"] is True
    assert restored.snapshot() == uninterrupted.snapshot()


def _valid_snapshot():
    accumulator = quality.QualityAccumulator()
    accumulator.add_normalized(_normalized_event(confidence=0.2))
    accumulator.add_normalized(_normalized_event(confidence=0.8))
    accumulator.add_unknown(_unknown_event())
    return json.loads(json.dumps(accumulator.snapshot()))


def test_snapshot_contains_all_heavy_hitter_checkpoint_state():
    snapshot = _valid_snapshot()

    assert {
        "unknown_template_error",
        "unknown_template_order",
        "unknown_template_next_order",
    } <= snapshot.keys()


@pytest.mark.parametrize("malformed", [None, [], "snapshot", 1, True])
def test_from_snapshot_rejects_non_mapping_values_with_value_error(malformed):
    with pytest.raises(ValueError, match="invalid quality snapshot"):
        quality.QualityAccumulator.from_snapshot(malformed)


def test_from_snapshot_rejects_every_missing_required_field():
    snapshot = _valid_snapshot()

    for field in tuple(snapshot):
        malformed = json.loads(json.dumps(snapshot))
        malformed.pop(field)
        with pytest.raises(ValueError, match=field):
            quality.QualityAccumulator.from_snapshot(malformed)


def _set_value(field, value):
    def mutate(snapshot):
        snapshot[field] = value

    return mutate


def _set_histogram_value(index, value):
    def mutate(snapshot):
        snapshot["confidence_histogram"][index] = value

    return mutate


def _increment_histogram(index):
    def mutate(snapshot):
        snapshot["confidence_histogram"][index] += 1

    return mutate


def _set_counter_value(field, key, value):
    def mutate(snapshot):
        snapshot[field][key] = value

    return mutate


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(_set_value("normalized_records", True), id="bool-count"),
        pytest.param(_set_value("unknown_records", -1), id="negative-count"),
        pytest.param(_set_value("confidence_count", 3), id="confidence-count-too-large"),
        pytest.param(_set_value("confidence_sum", float("inf")), id="non-finite-sum"),
        pytest.param(
            _set_value("confidence_sum_compensation", float("nan")),
            id="non-finite-sum-compensation",
        ),
        pytest.param(
            _set_value("confidence_sum_compensation", 2.0),
            id="out-of-range-sum-compensation",
        ),
        pytest.param(_set_value("confidence_sum", 1.9), id="sum-histogram-mismatch"),
        pytest.param(_set_value("confidence_min", None), id="missing-positive-min"),
        pytest.param(_set_value("confidence_min", 0.9), id="min-histogram-mismatch"),
        pytest.param(_set_histogram_value(2_000, True), id="bool-histogram-count"),
        pytest.param(_set_histogram_value(0, -1), id="negative-histogram-count"),
        pytest.param(_increment_histogram(0), id="histogram-sum-mismatch"),
        pytest.param(
            _set_counter_value("source_type_count", "sso", -1),
            id="negative-source-count",
        ),
        pytest.param(
            _set_counter_value("source_type_count", "sso", 99),
            id="source-total-mismatch",
        ),
        pytest.param(
            _set_counter_value("parser_count", "p1", 1),
            id="parser-total-mismatch",
        ),
        pytest.param(
            _set_counter_value("field_present_count", "event.category", 3),
            id="field-count-too-large",
        ),
        pytest.param(
            _set_counter_value("field_present_count", "not.a.tracked.field", 1),
            id="unknown-field-count",
        ),
        pytest.param(
            _set_counter_value("unknown_reason_count", "no_parser", -1),
            id="negative-reason-count",
        ),
        pytest.param(
            _set_value("missing_required_fields_count", False),
            id="bool-missing-required-count",
        ),
        pytest.param(
            _set_counter_value("unknown_template_count", "user=<VAR>", -1),
            id="negative-template-count",
        ),
        pytest.param(
            _set_value("unknown_template_count_capped", True),
            id="invalid-template-cap",
        ),
        pytest.param(
            _set_value("unknown_template_error", {"user=<VAR>": -1}),
            id="negative-template-error",
        ),
        pytest.param(
            _set_value("unknown_template_order", {"user=<VAR>": True}),
            id="bool-template-order",
        ),
        pytest.param(
            _set_value("unknown_template_next_order", 0),
            id="invalid-next-template-order",
        ),
    ],
)
def test_from_snapshot_rejects_invalid_types_ranges_and_invariants(mutate):
    malformed = _valid_snapshot()
    mutate(malformed)

    with pytest.raises(ValueError, match="invalid quality snapshot"):
        quality.QualityAccumulator.from_snapshot(malformed)


def test_from_snapshot_rejects_impossible_capped_template_order():
    accumulator = quality.QualityAccumulator()
    for index in range(quality.UNKNOWN_TEMPLATE_LIMIT + 1):
        accumulator.add_unknown(_unknown_event(template=f"template-{index:05d}"))
    malformed = json.loads(json.dumps(accumulator.snapshot()))
    malformed["unknown_template_next_order"] = malformed["unknown_records"] + 1

    with pytest.raises(ValueError, match="invalid quality snapshot"):
        quality.QualityAccumulator.from_snapshot(malformed)


def test_build_summary_feeds_the_accumulator(monkeypatch):
    normalized = [_normalized_event()]
    unknown = [_unknown_event()]
    calls = []

    class SpyAccumulator:
        def add_normalized(self, event):
            calls.append(("normalized", event))

        def add_unknown(self, record):
            calls.append(("unknown", record))

        def finalize(self):
            calls.append(("finalize", None))
            return {"sentinel": True}

    monkeypatch.setattr(quality, "QualityAccumulator", SpyAccumulator)

    assert quality.build_summary(normalized, unknown) == {"sentinel": True}
    assert calls == [
        ("normalized", normalized[0]),
        ("unknown", unknown[0]),
        ("finalize", None),
    ]
