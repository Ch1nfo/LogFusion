from __future__ import annotations

import builtins
import random

import pytest

from logfusion.model_detection import (
    MODEL_FEATURES,
    ModelDetectionConfig,
    ModelDetectionError,
    feature_vector,
    fit_model_detector,
    model_versions,
)


def _row(index: int, *, outlier: bool = False) -> dict:
    generator = random.Random(index)
    events = 1_000 if outlier else max(1, int(100 + generator.gauss(0, 8)))
    distinct = 100 if outlier else max(1, int(10 + generator.gauss(0, 2)))
    failures = 200 if outlier else max(0, int(events * (0.02 + generator.random() * 0.01)))
    other = 50 if outlier else int(generator.random() * 2)
    return {
        "event_count": events,
        "success_count": events - failures - other,
        "failure_count": failures,
        "other_outcome_count": other,
        "source_type_distinct_count": distinct,
        "source_ip_distinct_count": distinct,
        "resource_distinct_count": distinct,
        "action_distinct_count": distinct,
        "read_action_count": events // 2,
        "write_action_count": events // 3,
        "network_bytes_total": 100_000_000 if outlier else int(events * (100 + generator.random() * 30)),
        "first_seen_ip_count": distinct if outlier else int(generator.random() < 0.1),
        "first_seen_resource_count": distinct if outlier else int(generator.random() < 0.1),
        "first_seen_action_count": distinct if outlier else int(generator.random() < 0.1),
    }


def test_feature_vector_has_stable_derived_features():
    vector = feature_vector(_row(1))
    assert tuple(vector) == MODEL_FEATURES
    assert vector["event_count_log"] > 0
    assert 0 <= vector["failure_rate"] <= 1
    assert 0 <= vector["first_seen_ip_rate"] <= 1


def test_hbos_is_deterministic_and_explains_outlier():
    rows = [_row(index) for index in range(120)]
    config = ModelDetectionConfig()
    left = fit_model_detector("hbos", rows, config)
    right = fit_model_detector("hbos", rows, config)
    score = left.score(_row(999, outlier=True))
    assert left.threshold == right.threshold
    assert score.raw_score > left.threshold
    assert score.percentile == 1.0
    assert len(score.explanation["top_feature_contributions"]) == 3


def test_isolation_forest_is_deterministic_and_uses_supporting_evidence():
    rows = [_row(index) for index in range(120)]
    config = ModelDetectionConfig()
    left = fit_model_detector("isolation_forest", rows, config)
    right = fit_model_detector("isolation_forest", rows, config)
    left_score, right_score = left.score(_row(999, outlier=True)), right.score(_row(999, outlier=True))
    assert left_score.raw_score == pytest.approx(right_score.raw_score)
    assert left_score.raw_score >= left.threshold
    assert len(left_score.explanation["supporting_feature_deviations"]) == 3
    assert "not Isolation Forest feature attribution" in left_score.explanation["attribution_note"]


def test_isolation_forest_missing_dependency_has_install_hint(monkeypatch):
    original = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name in {"numpy", "sklearn"} or name.startswith("sklearn."):
            raise ImportError(name)
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    with pytest.raises(ModelDetectionError, match="optional ML dependencies"):
        model_versions("isolation_forest")


def test_model_rejects_insufficient_training_samples():
    with pytest.raises(ModelDetectionError, match="model_not_ready"):
        fit_model_detector("hbos", [_row(index) for index in range(99)], ModelDetectionConfig())
