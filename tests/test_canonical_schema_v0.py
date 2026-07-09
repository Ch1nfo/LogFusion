from copy import deepcopy

from logfusion.schema import validate_event


def _valid_event():
    return {
        "event": {
            "id": "evt-1",
            "time": "2026-07-09T09:10:00Z",
            "category": "authentication",
            "type": "login",
            "action": "user_login",
            "outcome": "success",
        },
        "user": {"name": "alice"},
        "source": {"ip": "10.1.2.3"},
        "destination": {"ip": "10.9.8.7"},
        "network": {"bytes": 123},
        "parser": {
            "id": "test_parser",
            "version": "0.1.0",
            "status": "success",
            "confidence": 0.9,
        },
        "raw": {
            "record_id": "raw-1",
            "source_id": "source-1",
            "source_type": "sso",
            "checksum": "sha256:" + "a" * 64,
            "storage_ref": "file://data/sso/sample.txt#L1-L12",
            "size_bytes": 128,
        },
        "extensions": {},
    }


def test_canonical_schema_accepts_valid_event():
    assert validate_event(_valid_event()) == []


def test_canonical_schema_rejects_invalid_event_category_type_and_outcome():
    event = _valid_event()
    event["event"]["category"] = "random_category"
    event["event"]["type"] = "random_type"
    event["event"]["outcome"] = "maybe"

    errors = validate_event(event)

    assert "event.category must be one of: application, authentication, endpoint, network, repository, unknown" in errors
    assert "event.type must be one of: access, activity, api, behavior, login, redirect, unknown" in errors
    assert "event.outcome must be one of: failure, success, unknown" in errors


def test_canonical_schema_rejects_invalid_time_ip_checksum_and_storage_ref():
    event = _valid_event()
    event["event"]["time"] = "not-a-time"
    event["source"]["ip"] = "999.999.999.999"
    event["raw"]["checksum"] = "sha256:abc"
    event["raw"]["storage_ref"] = "data/sso/sample.txt"

    errors = validate_event(event)

    assert "event.time must be ISO-8601" in errors
    assert "source.ip must be a valid IP address" in errors
    assert "raw.checksum must match sha256:<64 hex chars>" in errors
    assert "raw.storage_ref must start with file:// or kafka://" in errors


def test_canonical_schema_rejects_invalid_numeric_fields():
    event = _valid_event()
    event["raw"]["size_bytes"] = -1
    event["network"]["bytes"] = "123"
    event["http"] = {"response": {"status_code": "200"}}

    errors = validate_event(event)

    assert "raw.size_bytes must be a non-negative integer" in errors
    assert "network.bytes must be a non-negative integer" in errors
    assert "http.response.status_code must be an integer" in errors


def test_canonical_schema_reports_multiple_errors_stably():
    event = deepcopy(_valid_event())
    del event["event"]["time"]
    event["parser"]["confidence"] = 1.5

    errors = validate_event(event)

    assert errors == [
        "event.time is required",
        "parser.confidence must be between 0 and 1",
    ]
