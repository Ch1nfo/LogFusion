# LogFusion

本地日志解析 MVP：把本地文件日志归一成 **Canonical Event v0**，无法识别的记录进入 unknown 池，并提供 parser 候选 → 注册 → 测试 → shadow → active 的完整生命周期，以及 raw 回放与版本对比。

当前阶段**只做本地文件**，不做 Kafka。未知日志的 parser 候选生成可选调用真实 LLM，但不会出现在逐条日志解析路径中。

## 环境

推荐使用已有 conda 环境 `agent`：

```bash
cd /path/to/LogFusion
conda run -n agent pip install -e ".[dev]"
conda run -n agent python -m logfusion --help
conda run -n agent python -m pytest -q
```

也可在仓库根目录直接 `python -m logfusion`（包在路径上即可）。

依赖：Python 3.10+（运行时仅标准库；开发/测试见 `pyproject.toml` 的 `dev` 可选依赖）。

**隐私：** `output/`、`data/`、根目录原始日志 dump、`.env` / 密钥类文件均在 `.gitignore` 中，默认不会进版本库。提交前请确认样例已脱敏。

## 10 分钟最小闭环

### 1. 解析已知样例

```bash
conda run -n agent python -m logfusion parse \
  --config config/sources.yaml \
  --output output/normalized.jsonl \
  --unknown-output output/unknown.jsonl \
  --summary-output output/summary.json \
  --raw-store-output output/raw_records.jsonl
```

样例覆盖：`svn` / `sso` / `gitlab` / `hiklink`（见 `data/`）。

### 2. 看结果

| 文件 | 含义 |
|------|------|
| `output/normalized.jsonl` | 通过 schema 校验的规范化事件 |
| `output/unknown.jsonl` | 未识别 / 解析失败 / schema 失败 |
| `output/summary.json` | 成功率、置信度、字段覆盖率等质量摘要 |
| `output/raw_records.jsonl` | 原始记录落盘，供后续 replay / compare |

`parse` 默认使用固定内存的流式链路：每条记录只经过一次读取，随后直接写入 normalized、unknown 和可选 raw store，不会把整批事件保留在内存。`.gz`、`.bz2`、`.xz` / `.lzma` 和 `.zip` 历史文件使用同一条链路。

百 GB 级文件建议开启 checkpoint：

```bash
conda run -n agent python -m logfusion parse \
  --config config/sources.yaml \
  --output output/normalized.jsonl \
  --unknown-output output/unknown.jsonl \
  --summary-output output/summary.json \
  --raw-store-output output/raw_records.jsonl \
  --checkpoint output/parse.checkpoint.json \
  --checkpoint-every 10000

# 中断后使用完全相同的配置、registry、输入和输出路径恢复
conda run -n agent python -m logfusion parse \
  --config config/sources.yaml \
  --output output/normalized.jsonl \
  --unknown-output output/unknown.jsonl \
  --summary-output output/summary.json \
  --raw-store-output output/raw_records.jsonl \
  --checkpoint output/parse.checkpoint.json \
  --checkpoint-every 10000 \
  --resume
```

恢复时会校验数据源配置、Registry、输入文件元数据和输出路径，并把输出截断到最后一次成功 checkpoint 的字节位置。输入发生变化时会拒绝继续；压缩文件会重新扫描当前文件并跳过已提交记录。

## Parser 生命周期（unknown → active）

用 unknown 样例走一遍完整路径：

```bash
# 1) 解析 unknown 源（会进入 unknown 池）
conda run -n agent python -m logfusion parse \
  --config config/unknown-source.yaml \
  --output output/lifecycle_normalized.jsonl \
  --unknown-output output/lifecycle_unknown.jsonl \
  --summary-output output/lifecycle_summary.json \
  --raw-store-output output/lifecycle_raw.jsonl

# 2) 从 unknown 生成 draft 候选（启发式 + LLM prompt 载荷，不调模型）
conda run -n agent python -m logfusion propose-parsers \
  --unknown-input output/lifecycle_unknown.jsonl \
  --output output/parser_candidates.jsonl

# 3) 注册到本地 registry
conda run -n agent python -m logfusion registry register-candidates \
  --candidates output/parser_candidates.jsonl \
  --registry output/parser_registry.json

# 4) draft → testing
conda run -n agent python -m logfusion registry set-status \
  --registry output/parser_registry.json \
  --parser-id <candidate_id> \
  --status testing

# 5) 跑测试 harness（通过后才能进 shadow）
conda run -n agent python -m logfusion registry test \
  --registry output/parser_registry.json \
  --parser-id <candidate_id> \
  --output output/parser_test_report.json

# 6) testing → shadow
conda run -n agent python -m logfusion registry set-status \
  --registry output/parser_registry.json \
  --parser-id <candidate_id> \
  --status shadow

# 7) shadow replay（通过后才能进 active）
conda run -n agent python -m logfusion registry replay \
  --registry output/parser_registry.json \
  --unknown-input output/lifecycle_unknown.jsonl \
  --parser-id <candidate_id> \
  --output output/shadow_replay_report.json

# 8) shadow → active
conda run -n agent python -m logfusion registry set-status \
  --registry output/parser_registry.json \
  --parser-id <candidate_id> \
  --status active

# 9) 带 registry 再 parse：active parser 作为手写 parser 之后的兜底
conda run -n agent python -m logfusion parse \
  --config config/unknown-source.yaml \
  --registry output/parser_registry.json \
  --output output/active_normalized.jsonl \
  --unknown-output output/active_unknown.jsonl \
  --summary-output output/active_summary.json
```

`<candidate_id>` 可从 `output/parser_candidates.jsonl` 或：

```bash
conda run -n agent python -m logfusion registry list \
  --registry output/parser_registry.json
```

状态流转门禁：

```text
draft → testing → shadow → active
              ↘ blocked / deprecated
```

- `testing → shadow`：必须测试通过（`success_rate == 1.0`）
- `shadow → active`：必须 shadow replay 通过
- **手写 parser 优先**；active registry parser 只处理否则会进 unknown 的记录

## Raw 回放与版本对比

同一份 raw store，用不同 registry 重放并对比，评估 parser 更新收益/风险：

```bash
# 用当前 registry 重放
conda run -n agent python -m logfusion replay raw \
  --raw-input output/raw_records.jsonl \
  --registry output/parser_registry.json \
  --output output/replayed_normalized.jsonl \
  --unknown-output output/replayed_unknown.jsonl \
  --summary-output output/replayed_summary.json \
  --checkpoint output/replay.checkpoint.json

# baseline vs current 对比
conda run -n agent python -m logfusion replay compare \
  --raw-input output/raw_records.jsonl \
  --baseline-registry output/baseline_registry.json \
  --current-registry output/current_registry.json \
  --output output/replay_compare_report.json
```

对比报告包含：`baseline_summary`、`current_summary`、`summary_delta`、`record_changes`。

Record 级变化类型：`unknown_resolved` / `new_unknown` / `parser_changed` / `event_changed` / `new_record` / `removed_record` / `unchanged`。

## 新日志源的 LLM Parser 生成

新接入源可声明自己的格式契约，并显式开启 LLM 候选生成：

```yaml
- source_id: edr_acme
  source_type: edr
  product: acme-edr
  format_version: v1
  llm_enabled: true
  record_mode: line
  paths:
    - data/edr/*.log
```

Unknown Pool 会按同一来源与格式指纹聚类。先复制 `config/llm.example.yaml` 为 `config/llm.local.yaml`，填写企业网关或自托管模型地址；该本地文件已被 Git 忽略。以下命令生成独立 `draft` parser，并自动执行测试和 shadow replay：

```bash
export DEEPSEEK_API_KEY='...'
cp config/llm.example.yaml config/llm.local.yaml

conda run -n agent python -m logfusion propose-parsers \
  --unknown-input output/edr_unknown.jsonl \
  --output output/edr_candidates.jsonl \
  --report-output output/edr_llm_report.json \
  --llm-config config/llm.local.yaml \
  --registry output/parser_registry.json --auto-validate
```

API Key 只从环境变量读取；常见凭据字段会在调用模型前脱敏。验证成功的 LLM 候选进入 `pending_approval`，仍需人工切换为 `active` 才能参与正式解析。

## 质量漂移

对比两次 parse 的 summary：

```bash
conda run -n agent python -m logfusion quality drift \
  --baseline output/quality_baseline_summary.json \
  --current output/quality_current_summary.json \
  --output output/quality_drift_report.json
```

会标记：成功率下降、unknown 模板激增、置信度下降、必填字段覆盖率下降。

## CLI 一览

| 命令 | 作用 |
|------|------|
| `parse` | 流式读取本地/压缩文件 → normalized / unknown / summary（可选 raw store、registry、checkpoint） |
| `propose-parsers` | unknown → 启发式或 LLM draft parser 候选 |
| `registry register-candidates` | 候选写入 registry |
| `registry list` | 列出 registry 中的 parser |
| `registry set-status` | 受控状态流转 |
| `registry test` | Parser Test Harness |
| `registry replay` | Shadow replay |
| `replay raw` | 流式从 raw store 重放（可选 checkpoint） |
| `replay compare` | 两套 registry 的 replay 对比 |
| `quality drift` | summary 级漂移检测 |

## 目录结构

```text
LogFusion/
├── config/                 # 数据源 YAML
├── data/                   # 样例日志（svn / sso / gitlab / hiklink / unknown）
├── docs/                   # 设计与实现计划
├── logfusion/              # Python 包
├── output/                 # CLI 运行产物（本地生成）
├── tests/                  # pytest
└── README.md
```

### 模块地图

| 模块 | 职责 |
|------|------|
| `cli.py` | 命令行入口 |
| `config.py` | 轻量 YAML 配置加载 |
| `file_source.py` | 本地/压缩文件读取与切分 |
| `models.py` | `RawRecord` |
| `detect.py` | `source_type: auto` 内容识别 |
| `parsers.py` | 手写源解析器（svn/sso/gitlab/hiklink） |
| `schema.py` | Canonical Event v0 校验 |
| `unknown.py` | unknown 记录构造 |
| `pipeline.py` | 单记录解析核心与小数据兼容包装器 |
| `streaming.py` | JSONL sink、增量质量统计编排与 checkpoint / resume |
| `raw_store.py` | raw JSONL 读写 |
| `quality.py` / `quality_drift.py` | 质量摘要与漂移 |
| `parser_candidates.py` | 从 unknown 提出 draft 候选 |
| `parser_registry.py` | 本地 registry 与状态机 |
| `parser_runtime.py` | 通用 runtime（key_value / json / regex） |
| `parser_test_harness.py` | 候选测试 |
| `shadow_replay.py` | shadow 重放 |
| `active_runtime.py` | active parser 兜底与冲突择优 |
| `replay_compare.py` | 双 registry replay 对比 |

## 配置要点

`config/sources.yaml` 示例：

```yaml
raw:
  include_text_in_event: true   # 生产大数据量建议 false，靠 storage_ref + checksum 回溯

sources:
  - source_id: svn_sample
    source_type: svn            # 或 auto
    record_mode: line           # line | object
    paths:
      - data/svn/*.log
```

解析优先级：

```text
手写 source parser → active registry parser → unknown 池
```

## 文档

- [设计说明](docs/local-file-parser-mvp-design.md) — 流水线、schema、registry、replay 细节
- [实现计划](docs/local-file-parser-mvp-plan.md) — MVP 任务清单与约束

## 明确不做（本阶段）

- Kafka / 远程采集
- ECS / OCSF 导出适配器
- 数据库持久化（产物均为本地 JSON / JSONL）

## License

Apache License 2.0. See [LICENSE](LICENSE).
