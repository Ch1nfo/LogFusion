import json
import bz2
import gzip
import lzma
import os
import zipfile
from pathlib import Path

import pytest

from logfusion.config import load_sources_config
from logfusion.file_source import iter_raw_records
from logfusion.models import RawRecord
from logfusion.pipeline import ParseOutcome, iter_parse_records, run_pipeline
from logfusion.raw_store import raw_record_to_dict
from logfusion.streaming import CheckpointError, run_streaming_pipeline, run_streaming_raw_replay


def _read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_iter_parse_records_yields_one_outcome_per_record():
    sources = load_sources_config(Path("config/sources.yaml"))
    records = iter_raw_records(sources)

    outcomes = list(iter_parse_records(records))

    assert len(outcomes) == 8
    assert all(isinstance(outcome, ParseOutcome) for outcome in outcomes)
    assert all(outcome.normalized is not None and outcome.unknown is None for outcome in outcomes)


def test_streaming_pipeline_matches_collecting_pipeline_and_writes_raw_once(tmp_path, monkeypatch):
    sources = load_sources_config(Path("config/sources.yaml"))
    normalized_path = tmp_path / "normalized.jsonl"
    unknown_path = tmp_path / "unknown.jsonl"
    summary_path = tmp_path / "summary.json"
    raw_path = tmp_path / "raw.jsonl"
    expected = run_pipeline(sources)

    from logfusion import file_source

    opened = []
    original_open = file_source._open_text_lines

    def counted_open(path):
        opened.append(path)
        return original_open(path)

    monkeypatch.setattr(file_source, "_open_text_lines", counted_open)

    summary = run_streaming_pipeline(
        sources,
        normalized_path,
        unknown_path,
        summary_path,
        raw_output=raw_path,
    )

    assert len(opened) == 4
    assert len(set(opened)) == 4
    assert _read_jsonl(normalized_path) == expected.normalized
    assert _read_jsonl(unknown_path) == expected.unknown
    assert summary == expected.summary
    assert json.loads(summary_path.read_text(encoding="utf-8")) == expected.summary
    assert len(_read_jsonl(raw_path)) == 8


def test_checkpoint_resume_truncates_uncommitted_output_and_avoids_duplicates(tmp_path, monkeypatch):
    source_path = tmp_path / "unknown.log"
    source_path.write_text(
        "user=alice action=download\n"
        "user=bob action=upload\n"
        "user=carol action=delete\n"
        "user=dave action=read\n",
        encoding="utf-8",
    )
    sources = [{
        "source_id": "checkpoint_source",
        "source_type": "new_source",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(source_path)],
    }]
    normalized_path = tmp_path / "normalized.jsonl"
    unknown_path = tmp_path / "unknown.jsonl"
    summary_path = tmp_path / "summary.json"
    checkpoint_path = tmp_path / "checkpoint.json"

    from logfusion import streaming

    original = streaming.iter_parse_records

    def interrupted(records, registry_path=None):
        iterator = original(records, registry_path)
        yield next(iterator)
        yield next(iterator)
        yield next(iterator)
        raise RuntimeError("simulated interruption")

    monkeypatch.setattr(streaming, "iter_parse_records", interrupted)
    with pytest.raises(RuntimeError, match="simulated interruption"):
        run_streaming_pipeline(
            sources,
            normalized_path,
            unknown_path,
            summary_path,
            checkpoint_path=checkpoint_path,
            checkpoint_every=2,
        )

    # Record three reached the sink but was not part of the record-two checkpoint.
    assert len(_read_jsonl(unknown_path)) == 3

    monkeypatch.setattr(streaming, "iter_parse_records", original)
    summary = run_streaming_pipeline(
        sources,
        normalized_path,
        unknown_path,
        summary_path,
        checkpoint_path=checkpoint_path,
        checkpoint_every=2,
        resume=True,
    )

    rows = _read_jsonl(unknown_path)
    assert len(rows) == 4
    assert len({row["raw"]["record_id"] for row in rows}) == 4
    assert summary["total_records"] == 4


def test_resume_rejects_changed_input_file(tmp_path):
    source_path = tmp_path / "source.log"
    source_path.write_text("user=alice action=download\n", encoding="utf-8")
    sources = [{
        "source_id": "changed_source",
        "source_type": "new_source",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(source_path)],
    }]
    normalized_path = tmp_path / "normalized.jsonl"
    unknown_path = tmp_path / "unknown.jsonl"
    summary_path = tmp_path / "summary.json"
    checkpoint_path = tmp_path / "checkpoint.json"
    run_streaming_pipeline(
        sources,
        normalized_path,
        unknown_path,
        summary_path,
        checkpoint_path=checkpoint_path,
        checkpoint_every=1,
    )
    source_path.write_text("user=alice action=download\nuser=bob action=upload\n", encoding="utf-8")

    with pytest.raises(CheckpointError, match="input files changed"):
        run_streaming_pipeline(
            sources,
            normalized_path,
            unknown_path,
            summary_path,
            checkpoint_path=checkpoint_path,
            checkpoint_every=1,
            resume=True,
        )


def _write_compressed(path: Path, content: str) -> None:
    if path.suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            handle.write(content)
    elif path.suffix == ".bz2":
        with bz2.open(path, "wt", encoding="utf-8") as handle:
            handle.write(content)
    elif path.suffix == ".xz":
        with lzma.open(path, "wt", encoding="utf-8") as handle:
            handle.write(content)
    else:
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("events.log", content)


@pytest.mark.parametrize("suffix", [".gz", ".bz2", ".xz", ".zip"])
def test_compressed_source_resume_rescans_without_duplicates(tmp_path, monkeypatch, suffix):
    source_path = tmp_path / f"events{suffix}"
    _write_compressed(source_path, "one\ntwo\nthree\nfour\n")
    sources = [{
        "source_id": f"compressed-{suffix}",
        "source_type": "new_source",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(source_path)],
    }]
    normalized_path = tmp_path / "normalized.jsonl"
    unknown_path = tmp_path / "unknown.jsonl"
    summary_path = tmp_path / "summary.json"
    raw_path = tmp_path / "raw.jsonl"
    checkpoint_path = tmp_path / "checkpoint.json"

    from logfusion import streaming

    original = streaming.iter_parse_records

    def interrupted(records, registry_path=None):
        for index, outcome in enumerate(original(records, registry_path), start=1):
            yield outcome
            if index == 3:
                raise RuntimeError("simulated compressed interruption")

    monkeypatch.setattr(streaming, "iter_parse_records", interrupted)
    with pytest.raises(RuntimeError, match="compressed interruption"):
        run_streaming_pipeline(
            sources,
            normalized_path,
            unknown_path,
            summary_path,
            raw_output=raw_path,
            checkpoint_path=checkpoint_path,
            checkpoint_every=2,
        )

    monkeypatch.setattr(streaming, "iter_parse_records", original)
    summary = run_streaming_pipeline(
        sources,
        normalized_path,
        unknown_path,
        summary_path,
        raw_output=raw_path,
        checkpoint_path=checkpoint_path,
        checkpoint_every=2,
        resume=True,
    )

    assert summary["total_records"] == 4
    raw_rows = _read_jsonl(raw_path)
    assert [row["raw_text"] for row in raw_rows] == ["one", "two", "three", "four"]
    assert len({row["record_id"] for row in raw_rows}) == 4


def test_resume_rejects_cursor_record_changed_with_same_size_and_mtime(tmp_path, monkeypatch):
    source_path = tmp_path / "source.log"
    source_path.write_text("alpha\nbeta\nthird\n", encoding="utf-8")
    original_stat = source_path.stat()
    sources = [{
        "source_id": "cursor-checksum-source",
        "source_type": "new_source",
        "record_mode": "line",
        "include_raw_text": True,
        "paths": [str(source_path)],
    }]
    normalized_path = tmp_path / "normalized.jsonl"
    unknown_path = tmp_path / "unknown.jsonl"
    summary_path = tmp_path / "summary.json"
    checkpoint_path = tmp_path / "checkpoint.json"

    from logfusion import streaming

    original = streaming.iter_parse_records

    def interrupted(records, registry_path=None):
        iterator = original(records, registry_path)
        yield next(iterator)
        yield next(iterator)
        raise RuntimeError("simulated interruption")

    monkeypatch.setattr(streaming, "iter_parse_records", interrupted)
    with pytest.raises(RuntimeError):
        run_streaming_pipeline(
            sources,
            normalized_path,
            unknown_path,
            summary_path,
            checkpoint_path=checkpoint_path,
            checkpoint_every=2,
        )
    original_manifest = json.loads(checkpoint_path.read_text(encoding="utf-8"))["input_manifest"][0]

    # The checkpoint cursor record itself ("beta") is unchanged. The rolling
    # record checksum must still detect a same-size mutation earlier in the file.
    source_path.write_text("gamma\nbeta\nthird\n", encoding="utf-8")
    os.utime(source_path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    monkeypatch.setattr(streaming, "iter_parse_records", original)
    saved_metadata = {
        key: value
        for key, value in original_manifest.items()
        if key not in {"source_id", "unit_id"}
    }
    monkeypatch.setattr(streaming, "_file_metadata", lambda path: dict(saved_metadata))

    with pytest.raises(CheckpointError, match="checkpoint cursor"):
        run_streaming_pipeline(
            sources,
            normalized_path,
            unknown_path,
            summary_path,
            checkpoint_path=checkpoint_path,
            checkpoint_every=2,
            resume=True,
        )


def test_resume_rejects_output_truncated_below_checkpoint_offset(tmp_path):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\ntwo\n", encoding="utf-8")
    sources = [{
        "source_id": "output-validation-source",
        "source_type": "new_source",
        "record_mode": "line",
        "paths": [str(source_path)],
    }]
    normalized_path = tmp_path / "normalized.jsonl"
    unknown_path = tmp_path / "unknown.jsonl"
    summary_path = tmp_path / "summary.json"
    checkpoint_path = tmp_path / "checkpoint.json"
    run_streaming_pipeline(
        sources,
        normalized_path,
        unknown_path,
        summary_path,
        checkpoint_path=checkpoint_path,
        checkpoint_every=1,
    )
    unknown_path.write_bytes(unknown_path.read_bytes()[:-1])

    with pytest.raises(CheckpointError, match="shorter than checkpoint offset"):
        run_streaming_pipeline(
            sources,
            normalized_path,
            unknown_path,
            summary_path,
            checkpoint_path=checkpoint_path,
            checkpoint_every=1,
            resume=True,
        )


def test_checkpoint_manifest_contains_metadata_checksum(tmp_path):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\n", encoding="utf-8")
    checkpoint_path = tmp_path / "checkpoint.json"
    run_streaming_pipeline(
        [{"source_id": "manifest-source", "source_type": "new_source", "paths": [str(source_path)]}],
        tmp_path / "normalized.jsonl",
        tmp_path / "unknown.jsonl",
        tmp_path / "summary.json",
        checkpoint_path=checkpoint_path,
        checkpoint_every=1,
    )

    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["input_manifest"][0]["metadata_checksum"].startswith("sha256:")


def test_resume_rejects_changed_source_configuration(tmp_path):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\n", encoding="utf-8")
    sources = [{"source_id": "configured-source", "source_type": "new_source", "paths": [str(source_path)]}]
    paths = {
        "normalized_output": tmp_path / "normalized.jsonl",
        "unknown_output": tmp_path / "unknown.jsonl",
        "summary_output": tmp_path / "summary.json",
        "checkpoint_path": tmp_path / "checkpoint.json",
    }
    run_streaming_pipeline(sources, checkpoint_every=1, **paths)

    changed_sources = [{**sources[0], "source_type": "changed_source_type"}]
    with pytest.raises(CheckpointError, match="source configuration changed"):
        run_streaming_pipeline(changed_sources, checkpoint_every=1, resume=True, **paths)


def test_resume_rejects_changed_registry(tmp_path):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\n", encoding="utf-8")
    registry_path = tmp_path / "registry.json"
    registry_path.write_text('{"version": 1, "parsers": []}\n', encoding="utf-8")
    sources = [{"source_id": "registry-source", "source_type": "new_source", "paths": [str(source_path)]}]
    paths = {
        "normalized_output": tmp_path / "normalized.jsonl",
        "unknown_output": tmp_path / "unknown.jsonl",
        "summary_output": tmp_path / "summary.json",
        "checkpoint_path": tmp_path / "checkpoint.json",
        "registry_path": registry_path,
    }
    run_streaming_pipeline(sources, checkpoint_every=1, **paths)
    registry_path.write_text('{"version": 2, "parsers": []}\n', encoding="utf-8")

    with pytest.raises(CheckpointError, match="parser registry changed"):
        run_streaming_pipeline(sources, checkpoint_every=1, resume=True, **paths)


def test_resume_rejects_changed_output_path(tmp_path):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\n", encoding="utf-8")
    sources = [{"source_id": "output-source", "source_type": "new_source", "paths": [str(source_path)]}]
    checkpoint_path = tmp_path / "checkpoint.json"
    normalized_path = tmp_path / "normalized.jsonl"
    unknown_path = tmp_path / "unknown.jsonl"
    summary_path = tmp_path / "summary.json"
    run_streaming_pipeline(
        sources,
        normalized_path,
        unknown_path,
        summary_path,
        checkpoint_path=checkpoint_path,
        checkpoint_every=1,
    )

    with pytest.raises(CheckpointError, match="output paths changed"):
        run_streaming_pipeline(
            sources,
            tmp_path / "different-normalized.jsonl",
            unknown_path,
            summary_path,
            checkpoint_path=checkpoint_path,
            checkpoint_every=1,
            resume=True,
        )


def test_raw_replay_rejects_input_output_path_collision_without_truncating_input(tmp_path):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\n", encoding="utf-8")
    raw_path = tmp_path / "raw.jsonl"
    run_streaming_pipeline(
        [{"source_id": "raw-collision-source", "source_type": "new_source", "paths": [str(source_path)]}],
        tmp_path / "initial-normalized.jsonl",
        tmp_path / "initial-unknown.jsonl",
        tmp_path / "initial-summary.json",
        raw_output=raw_path,
    )
    original_raw = raw_path.read_bytes()

    with pytest.raises(CheckpointError, match="conflicts with input"):
        run_streaming_raw_replay(
            raw_path,
            raw_path,
            tmp_path / "replay-unknown.jsonl",
            tmp_path / "replay-summary.json",
        )

    assert raw_path.read_bytes() == original_raw


def test_streaming_pipeline_rejects_duplicate_sink_paths(tmp_path):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\n", encoding="utf-8")
    duplicate_output = tmp_path / "events.jsonl"

    with pytest.raises(CheckpointError, match="output paths must be distinct"):
        run_streaming_pipeline(
            [{"source_id": "sink-collision-source", "source_type": "new_source", "paths": [str(source_path)]}],
            duplicate_output,
            duplicate_output,
            tmp_path / "summary.json",
        )


@pytest.mark.parametrize("collision_kind", ["raw", "checkpoint", "registry"])
def test_streaming_pipeline_rejects_source_or_registry_output_collision(tmp_path, collision_kind):
    source_path = tmp_path / "source.log"
    source_path.write_text("one\n", encoding="utf-8")
    normalized = tmp_path / "normalized.jsonl"
    kwargs = {}
    if collision_kind == "raw":
        kwargs["raw_output"] = source_path
    elif collision_kind == "checkpoint":
        kwargs["checkpoint_path"] = source_path
    else:
        registry_path = tmp_path / "registry.json"
        registry_path.write_text('{"version": 1, "parsers": []}\n', encoding="utf-8")
        normalized = registry_path
        kwargs["registry_path"] = registry_path

    protected = source_path.read_bytes() if collision_kind != "registry" else kwargs["registry_path"].read_bytes()
    with pytest.raises(CheckpointError, match="conflicts with input"):
        run_streaming_pipeline(
            [{"source_id": "protected-source", "source_type": "new_source", "paths": [str(source_path)]}],
            normalized,
            tmp_path / "unknown.jsonl",
            tmp_path / "summary.json",
            **kwargs,
        )
    path = source_path if collision_kind != "registry" else kwargs["registry_path"]
    assert path.read_bytes() == protected


def _raw_record(record_id: str, file_path: str, record_index: int, text: str) -> RawRecord:
    return RawRecord(
        record_id=record_id,
        source_id="raw-replay-source",
        source_type="new_source",
        file_path=file_path,
        line_start=record_index,
        line_end=record_index,
        record_index=record_index,
        raw_text=text,
        checksum=f"sha256:{record_id}",
        size_bytes=len(text),
        storage_ref=f"file://{file_path}#L{record_index}-L{record_index}",
        include_raw_text=True,
    )


def test_raw_replay_resume_preserves_non_contiguous_original_file_keys(tmp_path, monkeypatch):
    raw_input = tmp_path / "raw.jsonl"
    rows = [
        _raw_record("a1", "a.log", 1, "A-one"),
        _raw_record("b1", "b.log", 1, "B-one"),
        _raw_record("a2", "a.log", 2, "A-two"),
    ]
    raw_input.write_text(
        "".join(f"{json.dumps(raw_record_to_dict(row))}\n" for row in rows),
        encoding="utf-8",
    )
    normalized = tmp_path / "normalized.jsonl"
    unknown = tmp_path / "unknown.jsonl"
    summary = tmp_path / "summary.json"
    checkpoint = tmp_path / "checkpoint.json"

    from logfusion import streaming

    original = streaming.iter_parse_records

    def interrupted(records, registry_path=None):
        for index, outcome in enumerate(original(records, registry_path), start=1):
            yield outcome
            if index == 3:
                raise RuntimeError("raw replay interrupted")

    monkeypatch.setattr(streaming, "iter_parse_records", interrupted)
    with pytest.raises(RuntimeError):
        run_streaming_raw_replay(
            raw_input,
            normalized,
            unknown,
            summary,
            checkpoint_path=checkpoint,
            checkpoint_every=2,
        )

    monkeypatch.setattr(streaming, "iter_parse_records", original)
    result = run_streaming_raw_replay(
        raw_input,
        normalized,
        unknown,
        summary,
        checkpoint_path=checkpoint,
        checkpoint_every=2,
        resume=True,
    )

    assert result["total_records"] == 3
    assert [row["raw"]["record_id"] for row in _read_jsonl(unknown)] == ["a1", "b1", "a2"]


def test_resume_does_not_open_completed_local_files(tmp_path, monkeypatch):
    source_paths = []
    for name in ("a.log", "b.log", "c.log"):
        path = tmp_path / name
        path.write_text(f"event-{name}\n", encoding="utf-8")
        source_paths.append(path)
    sources = [{
        "source_id": "multi-file-source",
        "source_type": "new_source",
        "paths": [str(path) for path in source_paths],
    }]
    normalized = tmp_path / "normalized.jsonl"
    unknown = tmp_path / "unknown.jsonl"
    summary = tmp_path / "summary.json"
    checkpoint = tmp_path / "checkpoint.json"

    from logfusion import file_source, streaming

    original_parser = streaming.iter_parse_records

    def interrupted(records, registry_path=None):
        iterator = original_parser(records, registry_path)
        yield next(iterator)
        yield next(iterator)
        raise RuntimeError("multi-file interruption")

    monkeypatch.setattr(streaming, "iter_parse_records", interrupted)
    with pytest.raises(RuntimeError):
        run_streaming_pipeline(
            sources,
            normalized,
            unknown,
            summary,
            checkpoint_path=checkpoint,
            checkpoint_every=2,
        )

    opened = []
    original_open = file_source._open_text_lines

    def tracking_open(path):
        opened.append(path)
        return original_open(path)

    monkeypatch.setattr(streaming, "iter_parse_records", original_parser)
    monkeypatch.setattr(file_source, "_open_text_lines", tracking_open)
    run_streaming_pipeline(
        sources,
        normalized,
        unknown,
        summary,
        checkpoint_path=checkpoint,
        checkpoint_every=2,
        resume=True,
    )

    assert source_paths[0] not in opened
    assert source_paths[1:] == opened


def test_resume_rejects_same_size_mtime_change_in_completed_file(tmp_path, monkeypatch):
    first = tmp_path / "first.log"
    second = tmp_path / "second.log"
    third = tmp_path / "third.log"
    first.write_text("alpha\n", encoding="utf-8")
    second.write_text("beta!\n", encoding="utf-8")
    third.write_text("third\n", encoding="utf-8")
    first_stat = first.stat()
    sources = [{
        "source_id": "completed-change-source",
        "source_type": "new_source",
        "paths": [str(first), str(second), str(third)],
    }]
    paths = {
        "normalized_output": tmp_path / "normalized.jsonl",
        "unknown_output": tmp_path / "unknown.jsonl",
        "summary_output": tmp_path / "summary.json",
        "checkpoint_path": tmp_path / "checkpoint.json",
    }

    from logfusion import streaming

    original = streaming.iter_parse_records

    def interrupted(records, registry_path=None):
        iterator = original(records, registry_path)
        yield next(iterator)
        yield next(iterator)
        raise RuntimeError("completed-file interruption")

    monkeypatch.setattr(streaming, "iter_parse_records", interrupted)
    with pytest.raises(RuntimeError):
        run_streaming_pipeline(sources, checkpoint_every=2, **paths)

    first.write_text("gamma\n", encoding="utf-8")
    os.utime(first, ns=(first_stat.st_atime_ns, first_stat.st_mtime_ns))
    monkeypatch.setattr(streaming, "iter_parse_records", original)

    with pytest.raises(CheckpointError, match="input files changed"):
        run_streaming_pipeline(sources, checkpoint_every=2, resume=True, **paths)


def test_resume_rejects_checkpoint_missing_output_offset_before_mutating_outputs(tmp_path):
    source = tmp_path / "source.log"
    source.write_text("one\ntwo\n", encoding="utf-8")
    normalized = tmp_path / "normalized.jsonl"
    unknown = tmp_path / "unknown.jsonl"
    summary = tmp_path / "summary.json"
    checkpoint = tmp_path / "checkpoint.json"
    sources = [{"source_id": "corrupt-checkpoint-source", "source_type": "new_source", "paths": [str(source)]}]
    run_streaming_pipeline(
        sources,
        normalized,
        unknown,
        summary,
        checkpoint_path=checkpoint,
        checkpoint_every=1,
    )
    checkpoint_doc = json.loads(checkpoint.read_text(encoding="utf-8"))
    del checkpoint_doc["output_offsets"]["unknown"]
    checkpoint.write_text(json.dumps(checkpoint_doc), encoding="utf-8")
    before = {path: path.read_bytes() for path in (normalized, unknown)}

    with pytest.raises(CheckpointError, match="invalid checkpoint"):
        run_streaming_pipeline(
            sources,
            normalized,
            unknown,
            summary,
            checkpoint_path=checkpoint,
            checkpoint_every=1,
            resume=True,
        )

    assert {path: path.read_bytes() for path in (normalized, unknown)} == before


def test_checkpoint_uses_compact_unknown_template_state(tmp_path):
    source = tmp_path / "source.log"
    source.write_text("alpha-template\nbeta-template\n", encoding="utf-8")
    checkpoint = tmp_path / "checkpoint.json"
    paths = {
        "normalized_output": tmp_path / "normalized.jsonl",
        "unknown_output": tmp_path / "unknown.jsonl",
        "summary_output": tmp_path / "summary.json",
        "checkpoint_path": checkpoint,
    }
    sources = [{"source_id": "compact-quality-source", "source_type": "new_source", "paths": [str(source)]}]
    run_streaming_pipeline(sources, checkpoint_every=1, **paths)

    quality_state = json.loads(checkpoint.read_text(encoding="utf-8"))["quality"]
    assert "unknown_template_entries" in quality_state
    assert "unknown_template_count" not in quality_state
    assert "unknown_template_error" not in quality_state
    assert "unknown_template_order" not in quality_state
    assert run_streaming_pipeline(sources, checkpoint_every=1, resume=True, **paths)["total_records"] == 2


def test_resume_rejects_processed_quality_count_mismatch_before_mutating_outputs(tmp_path):
    source = tmp_path / "source.log"
    source.write_text("one\n", encoding="utf-8")
    normalized = tmp_path / "normalized.jsonl"
    unknown = tmp_path / "unknown.jsonl"
    summary = tmp_path / "summary.json"
    checkpoint = tmp_path / "checkpoint.json"
    sources = [{"source_id": "count-mismatch-source", "source_type": "new_source", "paths": [str(source)]}]
    run_streaming_pipeline(
        sources,
        normalized,
        unknown,
        summary,
        checkpoint_path=checkpoint,
        checkpoint_every=1,
    )
    checkpoint_doc = json.loads(checkpoint.read_text(encoding="utf-8"))
    checkpoint_doc["processed_records"] += 1
    checkpoint.write_text(json.dumps(checkpoint_doc), encoding="utf-8")
    before = {path: path.read_bytes() for path in (normalized, unknown)}

    with pytest.raises(CheckpointError, match="processed_records does not match quality state"):
        run_streaming_pipeline(
            sources,
            normalized,
            unknown,
            summary,
            checkpoint_path=checkpoint,
            checkpoint_every=1,
            resume=True,
        )

    assert {path: path.read_bytes() for path in (normalized, unknown)} == before


def test_streaming_pipeline_rejects_hardlinked_input_output_collision(tmp_path):
    source = tmp_path / "source.log"
    source.write_text("protected\n", encoding="utf-8")
    output_alias = tmp_path / "output-hardlink.jsonl"
    os.link(source, output_alias)
    original = source.read_bytes()

    with pytest.raises(CheckpointError, match="conflicts with input"):
        run_streaming_pipeline(
            [{"source_id": "hardlink-source", "source_type": "new_source", "paths": [str(source)]}],
            output_alias,
            tmp_path / "unknown.jsonl",
            tmp_path / "summary.json",
        )

    assert source.read_bytes() == original


def test_raw_replay_checkpoint_supports_legacy_empty_file_path(tmp_path, monkeypatch):
    raw_input = tmp_path / "raw.jsonl"
    records = [
        _raw_record("legacy-1", "", 0, "one"),
        _raw_record("legacy-2", "", 0, "two"),
    ]
    raw_input.write_text(
        "".join(f"{json.dumps(raw_record_to_dict(record))}\n" for record in records),
        encoding="utf-8",
    )
    normalized = tmp_path / "normalized.jsonl"
    unknown = tmp_path / "unknown.jsonl"
    summary = tmp_path / "summary.json"
    checkpoint = tmp_path / "checkpoint.json"

    from logfusion import streaming

    original = streaming.iter_parse_records

    def interrupted(records, registry_path=None):
        iterator = original(records, registry_path)
        yield next(iterator)
        raise RuntimeError("legacy replay interruption")

    monkeypatch.setattr(streaming, "iter_parse_records", interrupted)
    with pytest.raises(RuntimeError):
        run_streaming_raw_replay(
            raw_input,
            normalized,
            unknown,
            summary,
            checkpoint_path=checkpoint,
            checkpoint_every=1,
        )

    monkeypatch.setattr(streaming, "iter_parse_records", original)
    result = run_streaming_raw_replay(
        raw_input,
        normalized,
        unknown,
        summary,
        checkpoint_path=checkpoint,
        checkpoint_every=1,
        resume=True,
    )

    assert result["total_records"] == 2
