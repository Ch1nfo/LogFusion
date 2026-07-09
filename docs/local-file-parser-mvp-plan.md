# Local File Parser MVP Implementation Plan

**Goal:** Build a local-file-only parser pipeline that reads sample files, emits Canonical Event JSONL, and routes failed records to unknown JSONL.

**Architecture:** Use a small Python package with focused modules for raw records, file reading, parser routing, source parsers, and CLI output. Explicit source configuration is preferred, with automatic detection as fallback.

**Runtime:** `conda run -n agent python ...`

## Tasks

1. Create split sample data under `data/svn`, `data/sso`, `data/gitlab`, and `data/hiklink`.
2. Add tests for file record framing, parser routing, source-specific mappings, unknown handling, and CLI output.
3. Implement the minimal Python package under `src/logfusion`.
4. Run tests with `conda run -n agent python -m pytest`.
5. Run the CLI against `config/sources.yaml` and inspect JSONL output.
6. Add raw integrity metadata and optional raw text control.
7. Add Canonical Event v0 validation, structured unknown output, and parse quality summary.
8. Add parser candidate proposal interface for unknown records without calling a real LLM.
9. Add Parser Registry v0 backed by a local JSON file, including candidate registration, listing, and guarded status transitions.
10. Add Parser Test Harness for registered candidates and require passing tests before `shadow`.
11. Add Shadow Parser Replay and require successful replay before `active`.
12. Add Active Parser Runtime as a fallback after handwritten parsers and before unknown routing.
13. Add Generic Parser Runtime for `key_value`, `json`, and `regex` registry parsers.
14. Add active parser conflict resolution and conflict metadata.
15. Enhance Canonical Schema v0 with enums, type checks, and raw reference validation.
16. Enhance parse quality summary and add parser drift detection from baseline/current summaries.
17. Add local raw JSONL storage and replay re-normalization.
18. Add parser version replay compare using the same raw store and two registry states.

## Constraints

- No Kafka implementation in this phase.
- Tests are local validation assets and are not assumed to be committed.
- Parser confidence and raw references must be present on every normalized event.
- Unknown records must not be dropped.
- `raw.text` is configurable and should be disabled for large-scale production output.
- Parser output must pass Canonical Event v0 validation before it is written as normalized output.
- LLM-assisted parser generation must produce draft candidates only; active parsers require review and tests.
- Parser Registry v0 uses a local JSON document and enforces allowed status transitions.
- Parser Registry must block `testing -> shadow` until parser tests have run and passed.
- Parser Registry must block `shadow -> active` until shadow replay has run and passed.
- Active registry parsers must not override handwritten source-specific parsers.
- Parser Test Harness, Shadow Replay, and Active Runtime must use the same generic parser runtime.
- Active parser conflicts must resolve deterministically and record conflict metadata.
- Canonical Schema v0 must reject invalid enum values, malformed timestamps, invalid IPs, malformed raw checksums, invalid storage refs, and invalid numeric field types.
- Drift detection must compare parse summaries and flag parse success drops, unknown template spikes, confidence drops, and required field coverage drops.
- Raw replay must parse stored raw records with the current parser/runtime/registry and produce fresh normalized, unknown, and summary outputs.
- Replay compare must align records by `raw.record_id` and report summary deltas plus record-level parser/output changes.
