import json
from pathlib import Path

import logfusion.raw_store as raw_store


def _raw_record_dict(index: int) -> dict:
    return {
        "record_id": f"record-{index}",
        "source_id": "source-1",
        "source_type": "test",
        "file_path": "input.log",
        "line_start": index,
        "line_end": index,
        "record_index": index,
        "raw_text": f"line {index}",
        "checksum": f"sha256:{index}",
        "size_bytes": len(f"line {index}"),
        "storage_ref": f"file://input.log#L{index}-L{index}",
        "include_raw_text": True,
        "product": None,
        "format_version": None,
        "llm_enabled": False,
    }


def _write_jsonl(path: Path, rows: list[dict | None]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write("\n" if row is None else f"{json.dumps(row)}\n")


def test_iter_stored_raw_records_streams_without_path_read_text(tmp_path, monkeypatch):
    input_path = tmp_path / "raw.jsonl"
    _write_jsonl(input_path, [_raw_record_dict(1), None, _raw_record_dict(2)])

    def fail_read_text(*args, **kwargs):
        raise AssertionError("iter_stored_raw_records must not call Path.read_text")

    monkeypatch.setattr(Path, "read_text", fail_read_text)

    records = list(raw_store.iter_stored_raw_records(input_path))

    assert [record.record_index for record in records] == [1, 2]
    assert [record.raw_text for record in records] == ["line 1", "line 2"]


def test_iter_stored_raw_records_parses_one_record_per_next(tmp_path, monkeypatch):
    input_path = tmp_path / "raw.jsonl"
    _write_jsonl(input_path, [_raw_record_dict(1), _raw_record_dict(2)])
    parsed_record_ids = []
    original_from_dict = raw_store.raw_record_from_dict

    def tracking_from_dict(data):
        parsed_record_ids.append(data["record_id"])
        return original_from_dict(data)

    monkeypatch.setattr(raw_store, "raw_record_from_dict", tracking_from_dict)
    records = iter(raw_store.iter_stored_raw_records(input_path))

    assert parsed_record_ids == []
    assert next(records).record_id == "record-1"
    assert parsed_record_ids == ["record-1"]
    assert next(records).record_id == "record-2"
    assert parsed_record_ids == ["record-1", "record-2"]
