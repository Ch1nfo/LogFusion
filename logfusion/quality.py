from __future__ import annotations

import heapq
import math
from array import array
from collections import Counter
from typing import Any

TRACKED_FIELD_PATHS = (
    "event.category",
    "event.action",
    "event.outcome",
    "user.name",
    "source.ip",
    "raw.checksum",
    "raw.storage_ref",
)

CONFIDENCE_HISTOGRAM_BINS = 10_001
UNKNOWN_TEMPLATE_LIMIT = 10_000
UNKNOWN_TEMPLATE_TOP_K = 10
QUALITY_SNAPSHOT_VERSION = 2
_UNKNOWN_TEMPLATE_HEAP_LIMIT = UNKNOWN_TEMPLATE_LIMIT * 2
_HISTOGRAM_COUNT_MAX = (1 << 64) - 1
_SNAPSHOT_REQUIRED_FIELDS = (
    "version",
    "normalized_records",
    "unknown_records",
    "confidence_count",
    "confidence_sum",
    "confidence_sum_compensation",
    "confidence_min",
    "confidence_histogram",
    "source_type_count",
    "parser_count",
    "field_present_count",
    "missing_required_fields_count",
    "unknown_template_count",
    "unknown_template_error",
    "unknown_template_order",
    "unknown_template_next_order",
    "unknown_template_count_capped",
    "unknown_reason_count",
)


class QualityAccumulator:
    """Incrementally compute the quality summary with bounded per-record state."""

    def __init__(self) -> None:
        self._normalized_records = 0
        self._unknown_records = 0
        self._confidence_count = 0
        self._confidence_sum = 0.0
        self._confidence_sum_compensation = 0.0
        self._confidence_min: int | float | None = None
        self._confidence_histogram = array("Q", [0]) * CONFIDENCE_HISTOGRAM_BINS
        self._source_type_count: Counter[Any] = Counter()
        self._parser_count: Counter[Any] = Counter()
        self._field_present_count: Counter[str] = Counter()
        self._missing_required_fields_count = 0
        self._unknown_template_count: Counter[str] = Counter()
        self._unknown_template_error: dict[str, int] = {}
        self._unknown_template_order: dict[str, int] = {}
        self._unknown_template_next_order = 0
        self._unknown_template_heap: list[tuple[int, int, str]] = []
        self._unknown_template_count_capped = False
        self._unknown_reason_count: Counter[Any] = Counter()

    def add_normalized(self, event: dict[str, Any]) -> None:
        parser_id = event["parser"]["id"]
        confidence = event.get("parser", {}).get("confidence")
        source_type = event.get("raw", {}).get("source_type", "unknown")
        present_fields = [
            field
            for field in TRACKED_FIELD_PATHS
            if _get_path(event, field) not in (None, "")
        ]

        self._normalized_records += 1
        self._source_type_count[source_type] += 1
        self._parser_count[parser_id] += 1
        self._field_present_count.update(present_fields)
        if (
            type(confidence) in (int, float)
            and math.isfinite(confidence)
            and 0 <= confidence <= 1
        ):
            self._add_confidence(confidence)

    def add_unknown(self, record: dict[str, Any]) -> None:
        source_type = record.get("raw", {}).get("source_type", "unknown")
        reason = record.get("reason", "unknown")
        missing_required = len(record.get("schema_errors", []))
        template = record.get("template_hint", "")

        self._unknown_records += 1
        self._source_type_count[source_type] += 1
        self._unknown_reason_count[reason] += 1
        self._missing_required_fields_count += missing_required
        if template:
            self._add_unknown_template(template)

    def finalize(self) -> dict[str, Any]:
        total = self._normalized_records + self._unknown_records
        confidence_avg = (
            round(self._confidence_sum / self._confidence_count, 6)
            if self._confidence_count
            else 0.0
        )
        summary = {
            "total_records": total,
            "normalized_records": self._normalized_records,
            "unknown_records": self._unknown_records,
            "parse_success_rate": (
                round(self._normalized_records / total, 6) if total else 0.0
            ),
            "parser_confidence_avg": confidence_avg,
            "parser_confidence_min": (
                self._confidence_min if self._confidence_min is not None else 0.0
            ),
            "parser_confidence_p50": self._confidence_p50(),
            "source_type_count": dict(self._source_type_count),
            "parser_count": dict(self._parser_count),
            "missing_required_fields_count": self._missing_required_fields_count,
            "required_field_coverage": {
                field: (
                    round(self._field_present_count[field] / self._normalized_records, 6)
                    if self._normalized_records
                    else 0.0
                )
                for field in TRACKED_FIELD_PATHS
            },
            "unknown_template_count": len(self._unknown_template_count),
            "unknown_template_top": dict(
                self._unknown_template_count.most_common(UNKNOWN_TEMPLATE_TOP_K)
            ),
            "unknown_reason_count": dict(self._unknown_reason_count),
        }
        if self._unknown_template_count_capped:
            summary["unknown_template_count_capped"] = True
            summary["unknown_template_top_approximate"] = True
        return summary

    def snapshot(self) -> dict[str, Any]:
        """Return all accumulator state in a JSON-serializable representation."""

        return {
            "version": QUALITY_SNAPSHOT_VERSION,
            "normalized_records": self._normalized_records,
            "unknown_records": self._unknown_records,
            "confidence_count": self._confidence_count,
            "confidence_sum": self._confidence_sum,
            "confidence_sum_compensation": self._confidence_sum_compensation,
            "confidence_min": self._confidence_min,
            "confidence_histogram": list(self._confidence_histogram),
            "source_type_count": dict(self._source_type_count),
            "parser_count": dict(self._parser_count),
            "field_present_count": dict(self._field_present_count),
            "missing_required_fields_count": self._missing_required_fields_count,
            "unknown_template_count": dict(self._unknown_template_count),
            "unknown_template_error": dict(self._unknown_template_error),
            "unknown_template_order": dict(self._unknown_template_order),
            "unknown_template_next_order": self._unknown_template_next_order,
            "unknown_template_count_capped": self._unknown_template_count_capped,
            "unknown_reason_count": dict(self._unknown_reason_count),
        }

    @classmethod
    def from_snapshot(cls, data: dict[str, Any]) -> QualityAccumulator:
        """Restore an accumulator snapshot produced by :meth:`snapshot`."""

        _validate_snapshot(data)
        try:
            accumulator = cls()
            accumulator._normalized_records = data["normalized_records"]
            accumulator._unknown_records = data["unknown_records"]
            accumulator._confidence_count = data["confidence_count"]
            accumulator._confidence_sum = data["confidence_sum"]
            accumulator._confidence_sum_compensation = data[
                "confidence_sum_compensation"
            ]
            accumulator._confidence_min = data["confidence_min"]
            accumulator._confidence_histogram = array(
                "Q", data["confidence_histogram"]
            )
            accumulator._source_type_count = Counter(data["source_type_count"])
            accumulator._parser_count = Counter(data["parser_count"])
            accumulator._field_present_count = Counter(data["field_present_count"])
            accumulator._missing_required_fields_count = data[
                "missing_required_fields_count"
            ]

            template_order = data["unknown_template_order"]
            ordered_templates = sorted(template_order, key=template_order.__getitem__)
            accumulator._unknown_template_count = Counter({
                template: data["unknown_template_count"][template]
                for template in ordered_templates
            })
            accumulator._unknown_template_error = {
                template: data["unknown_template_error"][template]
                for template in ordered_templates
            }
            accumulator._unknown_template_order = {
                template: template_order[template]
                for template in ordered_templates
            }
            accumulator._unknown_template_next_order = data[
                "unknown_template_next_order"
            ]
            accumulator._unknown_template_count_capped = data[
                "unknown_template_count_capped"
            ]
            accumulator._unknown_reason_count = Counter(data["unknown_reason_count"])
            accumulator._rebuild_unknown_template_heap()
            return accumulator
        except (KeyError, TypeError, OverflowError) as exc:
            raise ValueError(
                f"invalid quality snapshot: could not restore state ({exc})"
            ) from exc

    def _add_confidence(self, confidence: int | float) -> None:
        self._confidence_count += 1
        adjusted = float(confidence) - self._confidence_sum_compensation
        updated = self._confidence_sum + adjusted
        self._confidence_sum_compensation = (
            updated - self._confidence_sum
        ) - adjusted
        self._confidence_sum = updated
        if self._confidence_min is None or confidence < self._confidence_min:
            self._confidence_min = confidence
        self._confidence_histogram[_confidence_bin(confidence)] += 1

    def _confidence_p50(self) -> float:
        if not self._confidence_count:
            return 0.0
        target_index = int(self._confidence_count * 0.5)
        cumulative = 0
        for bin_index, count in enumerate(self._confidence_histogram):
            cumulative += count
            if cumulative > target_index:
                return round(bin_index / (CONFIDENCE_HISTOGRAM_BINS - 1), 4)
        return 1.0

    def _add_unknown_template(self, template: str) -> None:
        if template in self._unknown_template_count:
            self._unknown_template_count[template] += 1
            self._push_unknown_template(template)
        elif len(self._unknown_template_count) < UNKNOWN_TEMPLATE_LIMIT:
            self._unknown_template_count[template] = 1
            self._unknown_template_error[template] = 0
            self._unknown_template_order[template] = self._next_unknown_template_order()
            self._push_unknown_template(template)
        else:
            self._unknown_template_count_capped = True
            victim_count, _, victim = self._pop_min_unknown_template()
            del self._unknown_template_count[victim]
            del self._unknown_template_error[victim]
            del self._unknown_template_order[victim]

            self._unknown_template_count[template] = victim_count + 1
            self._unknown_template_error[template] = victim_count
            self._unknown_template_order[template] = self._next_unknown_template_order()
            self._push_unknown_template(template)

    def _next_unknown_template_order(self) -> int:
        order = self._unknown_template_next_order
        self._unknown_template_next_order += 1
        return order

    def _push_unknown_template(self, template: str) -> None:
        heapq.heappush(
            self._unknown_template_heap,
            (
                self._unknown_template_count[template],
                self._unknown_template_order[template],
                template,
            ),
        )
        if len(self._unknown_template_heap) > _UNKNOWN_TEMPLATE_HEAP_LIMIT:
            self._rebuild_unknown_template_heap()

    def _pop_min_unknown_template(self) -> tuple[int, int, str]:
        while self._unknown_template_heap:
            count, order, template = heapq.heappop(self._unknown_template_heap)
            if (
                self._unknown_template_count.get(template) == count
                and self._unknown_template_order.get(template) == order
            ):
                return count, order, template
        raise RuntimeError("unknown template heap is empty")

    def _rebuild_unknown_template_heap(self) -> None:
        self._unknown_template_heap = [
            (count, self._unknown_template_order[template], template)
            for template, count in self._unknown_template_count.items()
        ]
        heapq.heapify(self._unknown_template_heap)


def build_summary(normalized: list[dict[str, Any]], unknown: list[dict[str, Any]]) -> dict[str, Any]:
    accumulator = QualityAccumulator()
    for event in normalized:
        accumulator.add_normalized(event)
    for record in unknown:
        accumulator.add_unknown(record)
    return accumulator.finalize()


def _confidence_bin(confidence: int | float) -> int:
    if confidence <= 0:
        return 0
    if confidence >= 1:
        return CONFIDENCE_HISTOGRAM_BINS - 1
    return int(confidence * (CONFIDENCE_HISTOGRAM_BINS - 1) + 0.5)


def _validate_snapshot(data: Any) -> None:
    if not isinstance(data, dict):
        _invalid_snapshot("snapshot must be a JSON object")

    missing = [field for field in _SNAPSHOT_REQUIRED_FIELDS if field not in data]
    if missing:
        _invalid_snapshot(f"missing required field(s): {', '.join(missing)}")

    _require_nonnegative_int(data["version"], "version")
    if data["version"] != QUALITY_SNAPSHOT_VERSION:
        _invalid_snapshot(f"unsupported version {data['version']}")

    normalized_records = _require_nonnegative_int(
        data["normalized_records"], "normalized_records"
    )
    unknown_records = _require_nonnegative_int(
        data["unknown_records"], "unknown_records"
    )
    missing_required = _require_nonnegative_int(
        data["missing_required_fields_count"],
        "missing_required_fields_count",
    )
    if unknown_records == 0 and missing_required != 0:
        _invalid_snapshot(
            "missing_required_fields_count must be zero when unknown_records is zero"
        )

    _validate_confidence_snapshot(data, normalized_records)

    source_total = _validate_count_mapping(
        data["source_type_count"], "source_type_count"
    )
    if source_total != normalized_records + unknown_records:
        _invalid_snapshot("source_type_count total does not match record counts")

    parser_total = _validate_count_mapping(data["parser_count"], "parser_count")
    if parser_total != normalized_records:
        _invalid_snapshot("parser_count total does not match normalized_records")

    field_counts = data["field_present_count"]
    _validate_count_mapping(
        field_counts,
        "field_present_count",
        allowed_keys=frozenset(TRACKED_FIELD_PATHS),
    )
    for field, count in field_counts.items():
        if count > normalized_records:
            _invalid_snapshot(
                f"field_present_count.{field} exceeds normalized_records"
            )

    reason_total = _validate_count_mapping(
        data["unknown_reason_count"], "unknown_reason_count"
    )
    if reason_total != unknown_records:
        _invalid_snapshot("unknown_reason_count total does not match unknown_records")

    _validate_unknown_template_snapshot(data, unknown_records)


def _validate_confidence_snapshot(
    data: dict[str, Any], normalized_records: int
) -> None:
    confidence_count = _require_nonnegative_int(
        data["confidence_count"], "confidence_count"
    )
    if confidence_count > normalized_records:
        _invalid_snapshot("confidence_count exceeds normalized_records")

    confidence_sum = _require_finite_number(
        data["confidence_sum"], "confidence_sum"
    )
    confidence_sum_compensation = _require_finite_number(
        data["confidence_sum_compensation"], "confidence_sum_compensation"
    )
    histogram = data["confidence_histogram"]
    if not isinstance(histogram, list):
        _invalid_snapshot("confidence_histogram must be a list")
    if len(histogram) != CONFIDENCE_HISTOGRAM_BINS:
        _invalid_snapshot(
            f"confidence_histogram must contain {CONFIDENCE_HISTOGRAM_BINS} bins"
        )

    histogram_total = 0
    histogram_lower_sum = 0.0
    histogram_upper_sum = 0.0
    first_occupied_bin: int | None = None
    scale = CONFIDENCE_HISTOGRAM_BINS - 1
    for index, count in enumerate(histogram):
        count = _require_nonnegative_int(
            count, f"confidence_histogram[{index}]"
        )
        if count > _HISTOGRAM_COUNT_MAX:
            _invalid_snapshot(
                f"confidence_histogram[{index}] exceeds storage range"
            )
        if not count:
            continue
        if first_occupied_bin is None:
            first_occupied_bin = index
        histogram_total += count
        histogram_lower_sum += count * max(0.0, (index - 0.5) / scale)
        histogram_upper_sum += count * min(1.0, (index + 0.5) / scale)

    if histogram_total != confidence_count:
        _invalid_snapshot("confidence_histogram total does not match confidence_count")

    confidence_min = data["confidence_min"]
    if confidence_count == 0:
        if (
            confidence_sum != 0
            or confidence_sum_compensation != 0
            or confidence_min is not None
        ):
            _invalid_snapshot(
                "zero confidence_count requires zero confidence sum state and null confidence_min"
            )
        return

    if abs(confidence_sum_compensation) > 1:
        _invalid_snapshot("confidence_sum_compensation is outside the valid range")

    confidence_min = _require_finite_number(confidence_min, "confidence_min")
    if not 0 <= confidence_min <= 1:
        _invalid_snapshot("confidence_min must be between 0 and 1")

    tolerance = max(1e-9, confidence_count * 1e-12)
    if not 0 <= confidence_sum <= confidence_count:
        _invalid_snapshot("confidence_sum is outside the valid confidence range")
    if confidence_sum + tolerance < confidence_min * confidence_count:
        _invalid_snapshot("confidence_sum is inconsistent with confidence_min")
    if confidence_sum - tolerance > confidence_min + confidence_count - 1:
        _invalid_snapshot("confidence_sum is inconsistent with confidence_min")
    if first_occupied_bin != _confidence_bin(confidence_min):
        _invalid_snapshot("confidence_min does not match confidence_histogram")
    if not (
        histogram_lower_sum - tolerance
        <= confidence_sum
        <= histogram_upper_sum + tolerance
    ):
        _invalid_snapshot("confidence_sum does not match confidence_histogram")


def _validate_unknown_template_snapshot(
    data: dict[str, Any], unknown_records: int
) -> None:
    template_counts = data["unknown_template_count"]
    template_total = _validate_count_mapping(
        template_counts, "unknown_template_count"
    )
    if len(template_counts) > UNKNOWN_TEMPLATE_LIMIT:
        _invalid_snapshot(
            f"unknown_template_count exceeds {UNKNOWN_TEMPLATE_LIMIT} entries"
        )
    if template_total > unknown_records:
        _invalid_snapshot("unknown_template_count total exceeds unknown_records")

    template_errors = data["unknown_template_error"]
    _validate_count_mapping(
        template_errors,
        "unknown_template_error",
        positive=False,
    )
    template_orders = data["unknown_template_order"]
    _validate_count_mapping(
        template_orders,
        "unknown_template_order",
        positive=False,
    )
    count_keys = set(template_counts)
    if set(template_errors) != count_keys:
        _invalid_snapshot("unknown_template_error keys do not match template counts")
    if set(template_orders) != count_keys:
        _invalid_snapshot("unknown_template_order keys do not match template counts")

    orders = list(template_orders.values())
    if len(set(orders)) != len(orders):
        _invalid_snapshot("unknown_template_order values must be unique")
    next_order = _require_nonnegative_int(
        data["unknown_template_next_order"], "unknown_template_next_order"
    )
    if orders and next_order <= max(orders):
        _invalid_snapshot(
            "unknown_template_next_order must exceed all template orders"
        )
    if not orders and next_order != 0:
        _invalid_snapshot(
            "unknown_template_next_order must be zero for an empty template table"
        )
    if next_order > template_total:
        _invalid_snapshot(
            "unknown_template_next_order exceeds observed template records"
        )

    for template, count in template_counts.items():
        if template_errors[template] >= count:
            _invalid_snapshot(
                f"unknown_template_error.{template} must be smaller than its count"
            )

    capped = data["unknown_template_count_capped"]
    if type(capped) is not bool:
        _invalid_snapshot("unknown_template_count_capped must be a boolean")
    if capped:
        if len(template_counts) != UNKNOWN_TEMPLATE_LIMIT:
            _invalid_snapshot(
                "capped template state must contain the full bounded table"
            )
        if not any(template_errors.values()):
            _invalid_snapshot("capped template state must contain approximation errors")
        if next_order <= UNKNOWN_TEMPLATE_LIMIT:
            _invalid_snapshot("capped template state has an invalid next order")
    else:
        if any(template_errors.values()):
            _invalid_snapshot("uncapped template state cannot contain approximation errors")
        expected_orders = set(range(len(template_counts)))
        if set(orders) != expected_orders or next_order != len(template_counts):
            _invalid_snapshot("uncapped template order state is inconsistent")


def _validate_count_mapping(
    value: Any,
    field: str,
    *,
    positive: bool = True,
    allowed_keys: frozenset[str] | None = None,
) -> int:
    if not isinstance(value, dict):
        _invalid_snapshot(f"{field} must be a JSON object")
    total = 0
    for key, count in value.items():
        if type(key) is not str or not key:
            _invalid_snapshot(f"{field} keys must be non-empty strings")
        if allowed_keys is not None and key not in allowed_keys:
            _invalid_snapshot(f"{field} contains unsupported key {key}")
        count = _require_nonnegative_int(count, f"{field}.{key}")
        if positive and count == 0:
            _invalid_snapshot(f"{field}.{key} must be positive")
        total += count
    return total


def _require_nonnegative_int(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        _invalid_snapshot(f"{field} must be a non-negative integer")
    return value


def _require_finite_number(value: Any, field: str) -> int | float:
    if type(value) not in (int, float):
        _invalid_snapshot(f"{field} must be a finite number")
    if type(value) is float and not math.isfinite(value):
        _invalid_snapshot(f"{field} must be a finite number")
    return value


def _invalid_snapshot(message: str) -> None:
    raise ValueError(f"invalid quality snapshot: {message}")


def _field_coverage(events: list[dict[str, Any]]) -> dict[str, float]:
    if not events:
        return {field: 0.0 for field in TRACKED_FIELD_PATHS}
    coverage = {}
    for field in TRACKED_FIELD_PATHS:
        present = sum(1 for event in events if _get_path(event, field) not in (None, ""))
        coverage[field] = round(present / len(events), 6)
    return coverage


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, int(len(sorted_values) * q))
    return sorted_values[index]


def _get_path(document: dict[str, Any], field_path: str) -> Any:
    current: Any = document
    for part in field_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current
