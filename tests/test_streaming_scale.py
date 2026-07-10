import tracemalloc

from logfusion.streaming import run_streaming_pipeline


def test_streaming_pipeline_100k_records_stays_below_64_mib_extra_peak(tmp_path):
    source_path = tmp_path / "large.log"
    with source_path.open("w", encoding="utf-8") as handle:
        for index in range(100_000):
            handle.write(f"unrecognized-event-{index:06d}\n")

    tracemalloc.start()
    try:
        summary = run_streaming_pipeline(
            [{
                "source_id": "large-source",
                "source_type": "new_source",
                "record_mode": "line",
                "paths": [str(source_path)],
            }],
            tmp_path / "normalized.jsonl",
            tmp_path / "unknown.jsonl",
            tmp_path / "summary.json",
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert summary["total_records"] == 100_000
    assert peak < 64 * 1024 * 1024, f"extra traced peak was {peak / 1024 / 1024:.2f} MiB"
