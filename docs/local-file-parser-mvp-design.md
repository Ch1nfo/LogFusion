# Logfusion Local File Parser MVP Design

## Scope

The first implementation focuses only on local file ingestion. Kafka is intentionally out of scope until the local parsing pipeline is stable.

## Pipeline

```text
local file / compressed file
  -> RawRecord
  -> optional source detection
  -> ParserRouter
  -> source parser / unknown parser
  -> CanonicalEvent v0
  -> normalized.jsonl / unknown.jsonl
```

## Source selection

The MVP supports both explicit configuration and automatic fallback:

1. `source_type` from config has priority.
2. `source_type: auto` uses content-based detection.
3. Records that cannot be identified or parsed are written as unknown records.

## Raw record strategy

Every normalized event carries raw reference metadata:

- `raw.record_id`
- `raw.source_id`
- `raw.source_type`
- `raw.file_path`
- `raw.line_start`
- `raw.line_end`
- `raw.record_index`
- `raw.checksum`
- `raw.size_bytes`
- `raw.storage_ref`

`raw.text` is controlled by configuration:

```yaml
raw:
  include_text_in_event: true
```

Development and small-sample debugging can enable `raw.text`. Large-scale production output should disable it and rely on `raw.storage_ref` plus checksum to retrieve and verify the original record.

## Raw Storage and Replay

The MVP supports local JSONL raw storage. A parse run can write every `RawRecord` to a raw store:

```bash
python -m logfusion parse \
  --config config/sources.yaml \
  --output output/normalized.jsonl \
  --unknown-output output/unknown.jsonl \
  --summary-output output/summary.json \
  --raw-store-output output/raw_records.jsonl
```

Stored raw records include:

- `record_id`
- `source_id`
- `source_type`
- `file_path`
- `line_start`
- `line_end`
- `record_index`
- `raw_text`
- `checksum`
- `size_bytes`
- `storage_ref`
- `include_raw_text`

Historical records can be replayed with the current parser and registry state:

```bash
python -m logfusion replay raw \
  --raw-input output/raw_records.jsonl \
  --registry output/parser_registry.json \
  --output output/replayed_normalized.jsonl \
  --unknown-output output/replayed_unknown.jsonl \
  --summary-output output/replayed_summary.json
```

This enables parser version replay and re-normalization without re-reading the original source files.

## Parser Version Replay Compare

The same raw store can be replayed with two parser/registry states and compared:

```bash
python -m logfusion replay compare \
  --raw-input output/raw_records.jsonl \
  --baseline-registry output/baseline_registry.json \
  --current-registry output/current_registry.json \
  --output output/replay_compare_report.json
```

The compare report includes:

- `baseline_summary`
- `current_summary`
- `summary_delta`
- record-level changes:
  - `unknown_resolved`
  - `new_unknown`
  - `parser_changed`
  - `event_changed`
  - `new_record`
  - `removed_record`
  - `unchanged`

This allows a parser update to be evaluated before being used operationally.

## Canonical schema strategy

Logfusion uses an internal Canonical Event Schema. Field naming borrows from ECS where practical, and category/action mapping borrows from OCSF concepts. ECS/OCSF export adapters will be added later rather than binding the internal storage schema to either standard.

Canonical Event v0 requires:

- `event.id`
- `event.time`
- `event.category`
- `event.type`
- `event.action`
- `event.outcome`
- `parser.id`
- `parser.version`
- `parser.status`
- `parser.confidence`
- `raw.record_id`
- `raw.source_id`
- `raw.source_type`
- `raw.checksum`
- `raw.storage_ref`

Parser output is validated before it is accepted as normalized output. Schema validation failures are routed to the unknown pool with `reason: schema_validation_failed` and explicit `schema_errors`.

Canonical Event v0 also validates:

- `event.category`: `application`, `authentication`, `endpoint`, `network`, `repository`, `unknown`
- `event.type`: `access`, `activity`, `api`, `behavior`, `login`, `redirect`, `unknown`
- `event.outcome`: `success`, `failure`, `unknown`
- `parser.status`: `success`, `failed`
- `event.time`: ISO-8601 timestamp
- `source.ip` / `destination.ip`: valid IP address when present
- `raw.checksum`: `sha256:<64 hex chars>`
- `raw.storage_ref`: starts with `file://` or `kafka://`
- `raw.size_bytes` and `network.bytes`: non-negative integer when present
- `http.response.status_code`: integer when present

## Unknown pool

Unknown records are structured JSONL objects with:

- `unknown_id`
- `reason`
- `raw`
- `detected_format`
- `suggested_source_type`
- `template_hint`
- `parser`
- optional `schema_errors`

This creates a stable input surface for future LLM-assisted parser generation.

## Quality summary

Each CLI parse run writes a summary document with:

- `total_records`
- `normalized_records`
- `unknown_records`
- `parse_success_rate`
- `parser_confidence_avg`
- `source_type_count`
- `parser_count`
- `missing_required_fields_count`
- `required_field_coverage`
- `parser_confidence_min`
- `parser_confidence_p50`
- `unknown_template_count`
- `unknown_template_top`
- `unknown_reason_count`

## Parser Drift Detection

Drift detection compares a baseline parse summary with a current parse summary. It flags:

- parse success rate drop
- unknown template count spike
- parser confidence average drop
- required field coverage drop

CLI command:

```bash
python -m logfusion quality drift \
  --baseline output/baseline_summary.json \
  --current output/current_summary.json \
  --output output/drift_report.json
```

The detector is intentionally summary-based in v0. It does not require a database and can run in CI, cron, or an operator workflow.

## LLM-assisted parser generation

LLM assistance is not part of the hot path. It will consume unknown or low-confidence samples, propose parser rules and schema mappings, and save them as draft parser candidates. Draft parsers must pass tests before becoming active.

The MVP implements the interface layer without calling a real model:

```text
unknown.jsonl
  -> template grouping
  -> heuristic field hints
  -> LLM prompt payload
  -> parser candidate JSONL
```

Parser candidates are always `draft` and include:

- `candidate_id`
- `status`
- `source`
- `suggested_source_type`
- `detected_format`
- `template_hint`
- `sample_count`
- `samples`
- `suggested_parser`
- `llm_prompt`

The CLI command is:

```bash
python -m logfusion propose-parsers \
  --unknown-input output/unknown.jsonl \
  --output output/parser_candidates.jsonl
```

## Parser Registry

Parser Registry v0 is a local JSON document. It records draft parser candidates and parser lifecycle metadata before a parser is allowed into the active parsing path.

Parser records include:

- `parser_id`
- `source_type`
- `format`
- `version`
- `status`
- `priority`
- `owner`
- `created_at`
- `updated_at`
- `candidate_id`
- `template_hint`
- `parser_type`
- `field_candidates`
- `schema_mapping`
- `test_case_count`
- `success_rate`
- `schema_mapping_rate`
- `sample_count`

Supported statuses:

```text
draft -> testing -> shadow -> active
draft/testing/shadow/active -> deprecated
draft/testing -> blocked
```

The MVP intentionally does not auto-activate parser candidates. A candidate must be registered, tested, and explicitly promoted.

CLI commands:

```bash
python -m logfusion registry register-candidates \
  --candidates output/parser_candidates.jsonl \
  --registry output/parser_registry.json

python -m logfusion registry set-status \
  --registry output/parser_registry.json \
  --parser-id candidate_xxx \
  --status testing

python -m logfusion registry list \
  --registry output/parser_registry.json \
  --output output/parser_registry_list.jsonl
```

## Parser Test Harness

Parser Test Harness validates registry records before they can enter `shadow`.

For v0 it supports `key_value` parser candidates. It runs stored candidate samples as test cases, extracts fields, compares them with expected field candidates, and updates:

- `test_case_count`
- `success_rate`
- `schema_mapping_rate`
- `last_test_report`

The registry blocks `testing -> shadow` unless:

- `test_case_count > 0`
- `success_rate == 1.0`

CLI command:

```bash
python -m logfusion registry test \
  --registry output/parser_registry.json \
  --parser-id candidate_xxx \
  --output output/parser_test_report.json
```

## Shadow Parser Replay

Shadow replay runs `status: shadow` parsers on unknown or historical samples without writing normalized output and without affecting the main parser path.

Replay updates parser registry metadata:

- `shadow_replay_record_count`
- `shadow_replay_success_rate`
- `shadow_replay_schema_mapping_rate`
- `last_shadow_replay_report`

The registry blocks `shadow -> active` unless:

- `shadow_replay_record_count > 0`
- `shadow_replay_success_rate == 1.0`
- `shadow_replay_schema_mapping_rate == 1.0`

CLI command:

```bash
python -m logfusion registry replay \
  --registry output/parser_registry.json \
  --unknown-input output/unknown.jsonl \
  --parser-id candidate_xxx \
  --output output/shadow_replay_report.json
```

## Active Parser Runtime

Active Parser Runtime safely connects reviewed registry parsers to the main parsing pipeline.

Parser priority:

```text
handwritten source parser
  -> active registry parser
  -> unknown pool
```

This means generated parsers never override high-value handwritten parsers such as SVN, SSO, GitLab, or Hiklink parsers. They only run as a fallback when a record would otherwise enter the unknown pool.

The parse CLI can load active parsers:

```bash
python -m logfusion parse \
  --config config/sources.yaml \
  --registry output/parser_registry.json \
  --output output/normalized.jsonl \
  --unknown-output output/unknown.jsonl \
  --summary-output output/summary.json
```

Active registry parser events include:

- `parser.origin: registry_active`
- `extensions.active_parser`
- original `raw` references
- Canonical Event v0 fields

## Generic Parser Runtime

The registry-backed runtime supports these parser types in v0:

- `key_value`: extracts `key=value` fields.
- `json`: extracts fields using simple dot-path `field_extractors`.
- `regex`: extracts named capture groups from `field_extractors.pattern`.

The same runtime is used by:

- Parser Test Harness
- Shadow Parser Replay
- Active Parser Runtime

This keeps test, replay, and production parsing behavior aligned. Grok is intentionally deferred until a reusable pattern library is introduced.

## Active Parser Conflict Resolution

Handwritten source-specific parsers always run before active registry parsers and cannot be overridden by generated parsers.

When more than one active registry parser matches the same record, Logfusion selects one parser using this stable order:

1. Lower `priority`.
2. Higher `success_rate`.
3. Higher `schema_mapping_rate`.
4. Higher `shadow_replay_success_rate`.
5. Lower lexical `parser_id`.

If multiple registry parsers match, the selected normalized event records conflict metadata under `parser.conflict`:

```json
{
  "candidate_count": 2,
  "selected_parser_id": "candidate_a",
  "competing_parser_ids": ["candidate_b"],
  "resolution": "priority"
}
```

If only one active parser matches, no conflict metadata is emitted.
