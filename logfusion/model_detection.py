from __future__ import annotations

import bisect
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from statistics import median
from typing import Any, Protocol, Sequence


MODEL_DETECTORS = ("hbos", "isolation_forest")
MODEL_FEATURES = (
    "event_count_log",
    "failure_rate",
    "other_outcome_rate",
    "source_type_distinct_log",
    "source_ip_distinct_log",
    "resource_distinct_log",
    "action_distinct_log",
    "read_rate",
    "write_rate",
    "network_bytes_log",
    "first_seen_ip_rate",
    "first_seen_resource_rate",
    "first_seen_action_rate",
)


class ModelDetectionError(ValueError):
    """Raised when a model detector cannot be configured or trained."""


@dataclass(frozen=True)
class ModelDetectionConfig:
    threshold_quantile: float = 0.995
    min_training_samples: int = 100
    min_peer_members: int = 5
    top_feature_count: int = 3
    hbos_min_bins: int = 5
    hbos_max_bins: int = 20
    isolation_forest_estimators: int = 200
    isolation_forest_max_samples: int = 256
    random_state: int = 42

    def validate(self) -> None:
        if not 0.5 < self.threshold_quantile < 1:
            raise ModelDetectionError("threshold_quantile must be between 0.5 and 1")
        if self.min_training_samples < 2 or self.min_peer_members < 1 or self.top_feature_count < 1:
            raise ModelDetectionError("model sample and explanation limits must be positive")
        if self.hbos_min_bins < 2 or self.hbos_max_bins < self.hbos_min_bins:
            raise ModelDetectionError("invalid HBOS bin limits")
        if self.isolation_forest_estimators < 1 or self.isolation_forest_max_samples < 2:
            raise ModelDetectionError("invalid Isolation Forest configuration")

    def fingerprint(self, detector_id: str, versions: dict[str, str] | None = None) -> str:
        payload = {"detector": detector_id, "config": asdict(self), "versions": versions or {}}
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class ModelScore:
    raw_score: float
    percentile: float
    score: int
    explanation: dict[str, Any]


class FittedEvaluationDetector(Protocol):
    detector_id: str
    threshold: float
    training_scores: list[float]
    versions: dict[str, str]

    def score(self, row: Any) -> ModelScore: ...


def feature_vector(row: Any) -> dict[str, float]:
    events = max(float(row["event_count"]), 0.0)
    denominator = max(events, 1.0)
    source_ips = max(float(row["source_ip_distinct_count"]), 0.0)
    resources = max(float(row["resource_distinct_count"]), 0.0)
    actions = max(float(row["action_distinct_count"]), 0.0)
    return {
        "event_count_log": math.log1p(events),
        "failure_rate": float(row["failure_count"]) / denominator,
        "other_outcome_rate": float(row["other_outcome_count"]) / denominator,
        "source_type_distinct_log": math.log1p(max(float(row["source_type_distinct_count"]), 0.0)),
        "source_ip_distinct_log": math.log1p(source_ips),
        "resource_distinct_log": math.log1p(resources),
        "action_distinct_log": math.log1p(actions),
        "read_rate": float(row["read_action_count"]) / denominator,
        "write_rate": float(row["write_action_count"]) / denominator,
        "network_bytes_log": math.log1p(max(float(row["network_bytes_total"]), 0.0)),
        "first_seen_ip_rate": float(row["first_seen_ip_count"]) / max(source_ips, 1.0),
        "first_seen_resource_rate": float(row["first_seen_resource_count"]) / max(resources, 1.0),
        "first_seen_action_rate": float(row["first_seen_action_count"]) / max(actions, 1.0),
    }


def fit_model_detector(detector_id: str, rows: Sequence[Any], config: ModelDetectionConfig) -> FittedEvaluationDetector:
    config.validate()
    if len(rows) < config.min_training_samples:
        raise ModelDetectionError("model_not_ready")
    if detector_id == "hbos":
        return _HBOSDetector(rows, config)
    if detector_id == "isolation_forest":
        return _IsolationForestDetector(rows, config)
    raise ModelDetectionError(f"unsupported model detector: {detector_id}")


def model_versions(detector_id: str) -> dict[str, str]:
    if detector_id == "hbos":
        return dict(_HBOSDetector.versions)
    if detector_id == "isolation_forest":
        try:
            import numpy
            import sklearn
        except ImportError as exc:
            raise ModelDetectionError('Isolation Forest requires the optional ML dependencies; install with pip install -e ".[ml]"') from exc
        return {"numpy": numpy.__version__, "scikit_learn": sklearn.__version__}
    if detector_id == "statistical":
        return {"implementation": "logfusion-statistical-v1"}
    raise ModelDetectionError(f"unsupported evaluation detector: {detector_id}")


class _HBOSDetector:
    detector_id = "hbos"
    versions = {"implementation": "logfusion-stdlib-v1"}

    def __init__(self, rows: Sequence[Any], config: ModelDetectionConfig) -> None:
        self.config = config
        vectors = [feature_vector(row) for row in rows]
        bin_count = max(config.hbos_min_bins, min(config.hbos_max_bins, math.ceil(math.sqrt(len(vectors)))))
        self.histograms: dict[str, dict[str, Any]] = {}
        for feature in MODEL_FEATURES:
            values = sorted(vector[feature] for vector in vectors)
            edges = sorted(set(_percentile(values, index / bin_count) for index in range(1, bin_count)))
            counts = [0] * (len(edges) + 1)
            for value in values:
                counts[bisect.bisect_right(edges, value)] += 1
            self.histograms[feature] = {
                "edges": edges,
                "counts": counts,
                "minimum": values[0],
                "maximum": values[-1],
                "sample_count": len(values),
            }
        raw = [self._raw_score(vector)[0] for vector in vectors]
        self.training_scores = sorted(raw)
        self.threshold = _percentile(self.training_scores, config.threshold_quantile)

    def _raw_score(self, vector: dict[str, float]) -> tuple[float, list[tuple[str, float]]]:
        contributions: list[tuple[str, float]] = []
        for feature in MODEL_FEATURES:
            histogram = self.histograms[feature]
            value = vector[feature]
            bin_count = len(histogram["counts"])
            if value < histogram["minimum"] or value > histogram["maximum"]:
                probability = 1 / (histogram["sample_count"] + bin_count)
            else:
                count = histogram["counts"][bisect.bisect_right(histogram["edges"], value)]
                probability = (count + 1) / (histogram["sample_count"] + bin_count)
            contributions.append((feature, -math.log(probability)))
        return sum(value for _, value in contributions), contributions

    def score(self, row: Any) -> ModelScore:
        vector = feature_vector(row)
        raw_score, contributions = self._raw_score(vector)
        percentile = _empirical_percentile(self.training_scores, raw_score)
        top = sorted(contributions, key=lambda item: (-item[1], item[0]))[: self.config.top_feature_count]
        explanation = {
            "kind": "model_anomaly",
            "model": self.detector_id,
            "threshold": self.threshold,
            "threshold_quantile": self.config.threshold_quantile,
            "top_feature_contributions": [
                {"feature": feature, "contribution": contribution, "value": vector[feature]}
                for feature, contribution in top
            ],
        }
        return ModelScore(raw_score, percentile, _candidate_score(percentile, self.config.threshold_quantile), explanation)


class _IsolationForestDetector:
    detector_id = "isolation_forest"

    def __init__(self, rows: Sequence[Any], config: ModelDetectionConfig) -> None:
        try:
            import numpy
            import sklearn
            from sklearn.ensemble import IsolationForest
        except ImportError as exc:  # pragma: no cover - exercised by import blocking test
            raise ModelDetectionError('Isolation Forest requires the optional ML dependencies; install with pip install -e ".[ml]"') from exc
        self.config = config
        self.versions = {"numpy": numpy.__version__, "scikit_learn": sklearn.__version__}
        vectors = [feature_vector(row) for row in rows]
        matrix = [[vector[name] for name in MODEL_FEATURES] for vector in vectors]
        self.model = IsolationForest(
            n_estimators=config.isolation_forest_estimators,
            max_samples=min(config.isolation_forest_max_samples, len(matrix)),
            contamination="auto",
            random_state=config.random_state,
            n_jobs=1,
        )
        self.model.fit(matrix)
        self.training_scores = sorted(float(-score) for score in self.model.score_samples(matrix))
        self.threshold = _percentile(self.training_scores, config.threshold_quantile)
        self.centres: dict[str, tuple[float, float]] = {}
        for name in MODEL_FEATURES:
            values = [vector[name] for vector in vectors]
            centre = median(values)
            mad = median(abs(value - centre) for value in values)
            self.centres[name] = (centre, mad)

    def score(self, row: Any) -> ModelScore:
        vector = feature_vector(row)
        matrix = [[vector[name] for name in MODEL_FEATURES]]
        raw_score = float(-self.model.score_samples(matrix)[0])
        percentile = _empirical_percentile(self.training_scores, raw_score)
        deviations = []
        for name in MODEL_FEATURES:
            centre, mad = self.centres[name]
            deviation = abs(vector[name] - centre) / max(mad, 1e-9)
            deviations.append((name, deviation, centre))
        top = sorted(deviations, key=lambda item: (-item[1], item[0]))[: self.config.top_feature_count]
        explanation = {
            "kind": "model_anomaly",
            "model": self.detector_id,
            "threshold": self.threshold,
            "threshold_quantile": self.config.threshold_quantile,
            "supporting_feature_deviations": [
                {"feature": name, "robust_deviation": deviation, "value": vector[name], "training_median": centre}
                for name, deviation, centre in top
            ],
            "attribution_note": "Supporting deviations are contextual evidence, not Isolation Forest feature attribution.",
        }
        return ModelScore(raw_score, percentile, _candidate_score(percentile, self.config.threshold_quantile), explanation)


def _candidate_score(percentile: float, threshold_quantile: float) -> int:
    tail = max(0.0, min(1.0, (percentile - threshold_quantile) / (1 - threshold_quantile)))
    return int(round(60 + 40 * tail))


def _empirical_percentile(ordered: Sequence[float], value: float) -> float:
    return bisect.bisect_right(ordered, value) / len(ordered)


def _percentile(ordered: Sequence[float], quantile: float) -> float:
    if not ordered:
        raise ModelDetectionError("cannot calculate a percentile without samples")
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * quantile
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    return float(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower))
