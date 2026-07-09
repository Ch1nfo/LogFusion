import json
from pathlib import Path

from logfusion.cli import main
from logfusion.config import load_sources_config
from logfusion.pipeline import replay_raw_records, run_pipeline
from logfusion.raw_store import iter_stored_raw_records, write_raw_store


def test_write_raw_store_and_iter_stored_records(tmp_path):
    sources = load_sources_config(Path("config/sources.yaml"))
    raw_store = tmp_path / "raw_records.jsonl"

    count = write_raw_store(sources, raw_store)
    records = list(iter_stored_raw_records(raw_store))

    assert count == 8
    assert len(records) == 8
    assert records[0].raw_text.startswith("10.1.75.130 - luyuantong")
    assert records[0].storage_ref == "file://data/svn/sample.log#L1-L1"
    assert records[0].checksum.startswith("sha256:")
    assert records[0].include_raw_text is True


def test_replay_raw_records_matches_file_parse_counts(tmp_path):
    sources = load_sources_config(Path("config/sources.yaml"))
    raw_store = tmp_path / "raw_records.jsonl"
    write_raw_store(sources, raw_store)

    original = run_pipeline(sources)
    replayed = replay_raw_records(raw_store)

    assert len(replayed.normalized) == len(original.normalized) == 8
    assert replayed.unknown == []
    assert replayed.summary["parse_success_rate"] == 1.0
    assert replayed.normalized[0]["raw"]["storage_ref"] == "file://data/svn/sample.log#L1-L1"


def test_cli_parse_can_write_raw_store_and_replay_raw(tmp_path):
    raw_store = tmp_path / "raw_records.jsonl"
    normalized = tmp_path / "normalized.jsonl"
    unknown = tmp_path / "unknown.jsonl"
    summary = tmp_path / "summary.json"
    replayed = tmp_path / "replayed.jsonl"
    replay_unknown = tmp_path / "replay_unknown.jsonl"
    replay_summary = tmp_path / "replay_summary.json"

    assert main([
        "parse",
        "--config", "config/sources.yaml",
        "--output", str(normalized),
        "--unknown-output", str(unknown),
        "--summary-output", str(summary),
        "--raw-store-output", str(raw_store),
    ]) == 0
    assert raw_store.exists()
    assert len(raw_store.read_text(encoding="utf-8").splitlines()) == 8

    assert main([
        "replay",
        "raw",
        "--raw-input", str(raw_store),
        "--output", str(replayed),
        "--unknown-output", str(replay_unknown),
        "--summary-output", str(replay_summary),
    ]) == 0

    replay_rows = [json.loads(line) for line in replayed.read_text(encoding="utf-8").splitlines()]
    assert len(replay_rows) == 8
    assert replay_unknown.read_text(encoding="utf-8") == ""
    assert json.loads(replay_summary.read_text(encoding="utf-8"))["parse_success_rate"] == 1.0
