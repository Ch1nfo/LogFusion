# LogFusion

本地日志解析 MVP：把本地文件日志归一成 **Canonical Event v0**，无法识别的记录进入 unknown 池，并提供 parser 候选 → 注册 → 测试 → shadow → active 的完整生命周期，以及 raw 回放与版本对比。

当前阶段支持本地历史文件与 Kafka 实时消费，以及基于 `user.name` 的 Session + Feature v1 行为聚合。未知日志的 parser 候选生成可选调用真实 LLM，但不会出现在逐条日志解析路径中。

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

Kafka 连接器是可选依赖：`conda run -n agent pip install -e ".[kafka]"`。没有 Kafka 或该依赖时，本地文件解析和所有 fake-consumer 测试仍可使用。

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

## Kafka 实时消费

复制 `config/kafka.example.yaml` 为本地配置并设置凭据环境变量。每条 Kafka message 对应一条完整日志；运行时先将输出与 checkpoint 持久化，再同步提交 offset，因此采用至少一次语义。

```bash
export LOGFUSION_KAFKA_USERNAME='...'
export LOGFUSION_KAFKA_PASSWORD='...'

conda run -n agent python -m logfusion consume kafka \
  --config config/kafka.local.yaml \
  --output output/kafka_normalized.jsonl \
  --unknown-output output/kafka_unknown.jsonl \
  --summary-output output/kafka_summary.json \
  --raw-store-output output/kafka_raw.jsonl \
  --checkpoint output/kafka.checkpoint.json \
  --checkpoint-every 10000
```

加 `--once` 可在一次空 poll 后退出，适合批量排空与调试；默认持续消费。Kafka 原始引用格式为 `kafka://<topic>/<partition>/<offset>`，并保留 topic、partition、offset 与 broker 时间戳。

如需直接运行完整实时 UEBA 链路，而不是只写入 normalized JSONL，可使用 `kafka-ueba`：

```bash
conda run -n agent python -m logfusion consume kafka-ueba \
  --config config/kafka.local.yaml \
  --output output/kafka_normalized.jsonl \
  --unknown-output output/kafka_unknown.jsonl \
  --summary-output output/kafka_summary.json \
  --raw-store-output output/kafka_raw.jsonl \
  --feature-state output/features.db \
  --baseline-state output/baseline.db \
  --detection-state output/detection.db \
  --incident-state output/incidents.db \
  --risk-state output/risk.db \
  --checkpoint output/kafka_ueba.checkpoint.json \
  --checkpoint-every 10000
```

每个提交批次会先持久化 raw/normalized 输出、Feature 状态、watermark、Baseline、Detection、Correlation 与 Fusion，再写本地 checkpoint，最后才同步提交 Kafka offset。若崩溃发生在 checkpoint 与 broker commit 之间，恢复时会从本地 checkpoint seek 并补交 offset；如果崩溃更早则允许 Kafka 重放，依靠稳定的 `event.id` 保持聚合和告警幂等。

## Session + Feature v1

规范化事件可进一步按完全相同的 `user.name` 聚合为固定时间窗口和跨系统行为会话，为下一阶段行为基线提供确定性状态。状态保存到本地 SQLite；它不做账号别名合并或异常判定。

先用一份完整的 normalized JSONL 历史初始化新的状态库：

```bash
conda run -n agent python -m logfusion features build \
  --input output/normalized.jsonl \
  --state output/features.db
```

`build` 流式读取 JSONL，生成 1 分钟、5 分钟、1 小时与 1 天窗口，并以 30 分钟不活跃间隔构建用户会话。重复 `event.id` 不会重复累计；历史初始化在 EOF 后关闭会话，已完成的状态库不能再追加历史文件，补录历史必须用完整输入重建新库。

查询用户窗口与会话：

```bash
conda run -n agent python -m logfusion features query \
  --state output/features.db \
  --user wangkun78 \
  --from 2026-07-01T00:00:00Z \
  --to 2026-07-02T00:00:00Z \
  --window-size 3600
```

返回范围内相交的窗口和会话。窗口包含事件/结果计数、来源/IP/资源/动作 distinct、读写动作、网络字节数及首次 IP/资源/动作计数；会话保留有界的调查样本和动作首尾序列。精确频次、首次/最后时间和 revision 保存在 SQLite，供后续 Baseline Engine 增量读取。

## Baseline Engine v1

基线引擎从 Feature DB 生成独立的 Baseline DB，不改写特征状态。它按完全相同的 `user.name` 建立个人基线，并从已观察到的用户窗口建立全局参照；默认使用全局最新事件时间截止的滚动 30 天历史。

```bash
conda run -n agent python -m logfusion baseline build \
  --feature-state output/features.db \
  --state output/baseline.db

conda run -n agent python -m logfusion baseline query \
  --state output/baseline.db \
  --user wangkun78 \
  --window-size 3600
```

首版构建 1 小时和 1 天的事件量、结果、distinct、读写、字节与首次出现统计，保存均值、标准差、P50/P95/P99、MAD 和常用 IP/资源/动作频次。个人基线只在用户首末行为之间补齐零窗口；全局基线只使用真实存在的窗口。基线至少有 14 个活跃日和 20 个窗口样本时标为 `ready`，否则标为 `warming_up`。再次运行 `baseline build` 会按 Feature revision 增量更新；Feature 或 Baseline 配置变化时需重建 Baseline DB。

可选的 Peer Group v1 在个人基线未就绪时提供比全局基线更精确的参照。映射 CSV 固定为 `user_name,group_id`，一个用户只属于一个群体：

```csv
user_name,group_id
wangkun78,engineering
zhangsan12,operations
```

```bash
logfusion baseline build --feature-state output/features.db --state output/baseline.db \
  --peer-groups config/peer-groups.csv
```

检测回退顺序为个人、同群体、全局。同群体至少需要 5 名活跃成员、14 个活跃日和 100 个真实窗口才会标记为 `ready`；群体映射改变时必须重建 Baseline DB。持续运行时可将相同参数传给 `orchestrate run` 或 `consume kafka-ueba`。

## Statistical Anomaly Detection v1

检测引擎消费已同步的 Feature DB 与 Baseline DB，输出独立 SQLite 中可解释的异常候选；不会触发告警或自动处置。先确保 `baseline build` 已运行到当前 Feature revision：

```bash
conda run -n agent python -m logfusion detect run \
  --feature-state output/features.db \
  --baseline-state output/baseline.db \
  --state output/detection.db

conda run -n agent python -m logfusion detect query \
  --state output/detection.db \
  --user wangkun78 \
  --from 2026-07-01T00:00:00Z \
  --to 2026-07-02T00:00:00Z
```

首版检测 1 小时和 1 天窗口的个人 P95/P99 偏离；个人基线未就绪时可回退到 ready 的全局基线。它还检测 30 天首次值和占比不超过 1%、累计不超过 3 次的低频 IP、资源、动作与来源类型。候选分数固定为 P95=60、P99=80、首次值=75、低频值=55；80+ 为 `high`、60–79 为 `medium`、其余为 `low`。同一窗口重跑不会重复写入；迟到事件修订窗口时新候选会替代旧候选。

## Cross-System Correlation & Incident Aggregation v1

关联引擎把同一用户的多个 active anomaly candidates 聚合为版本化 Incident。运行前 Detection DB 必须已经消费到当前 Feature revision：

```bash
conda run -n agent python -m logfusion correlate run \
  --feature-state output/features.db \
  --detection-state output/detection.db \
  --state output/incidents.db

conda run -n agent python -m logfusion correlate query \
  --state output/incidents.db \
  --user wangkun78 \
  --from 2026-07-01T00:00:00Z \
  --to 2026-07-02T00:00:00Z
```

1 小时候选优先关联到相交的 Feature session；没有 session 时按用户和 1 小时时间桶关联。日窗口候选按日窗口独立聚合。Incident 保存候选时间线、来源系统、IP、资源、证据 ID 和 supersedes 链。聚合分采用“最高候选分 + 每个附加候选 5 分、最多奖励 20 分”，只用于排序，不代表最终告警风险。

## Risk Fusion & Detection Policy v1

风险融合引擎消费已同步的 Detection DB 与 Incident DB，把 active Incident 转为独立、版本化的风险评估和策略控制后的告警。先确保 `correlate run` 已消费到当前 Detection revision：

```bash
conda run -n agent python -m logfusion fuse run \
  --detection-state output/detection.db \
  --incident-state output/incidents.db \
  --state output/risk.db

conda run -n agent python -m logfusion fuse query \
  --state output/risk.db \
  --user wangkun78 \
  --from 2026-07-01T00:00:00Z \
  --to 2026-07-02T00:00:00Z
```

首版风险分为 Incident 聚合分，加上跨来源系统奖励 10 分，以及每多一种检测器 3 分（总计最多 10 分），最高 100 分。`80+` 进入告警策略、`90+` 为 `critical`；每个用户每天默认最多产生 3 条告警，超过预算的高风险评估保留为 `suppressed_budget`，低于门槛的保留为 `below_threshold`。输入 revision 更新时，旧风险评估和告警标记为 `superseded`，新版本保留明确的替代链。该模块尚不引入资产、部门、身份图谱或外部 SOC 推送。

风险策略可通过本地 YAML 调整；策略指纹受 Risk DB 保护，改变后需重建 Risk DB，避免静默改变历史告警语义：

```yaml
# config/fusion.local.yaml
fusion:
  alert_threshold: 80
  critical_threshold: 90
  per_user_daily_alert_limit: 3
  case_suppression_enabled: true
```

Case 被标记为仍有效的 `suppressed` 后，执行一次策略刷新即可保留风险评估、撤销对应 active alert，并标记 `suppressed_by_case_policy`：

```bash
logfusion fuse run --detection-state output/detection.db --incident-state output/incidents.db \
  --state output/risk.db --case-state output/cases.db \
  --policy-config config/fusion.local.yaml --refresh-policy
```

## Continuous UEBA Orchestrator v1

编排器将已经解析完成的追加式 normalized JSONL 接入 Feature、Baseline、Detection、Correlation 和 Risk Fusion。它以 `event.id` 幂等处理重放，使用 checkpoint 保存已提交字节位置和已提交内容的 SHA-256；只有下游整条链路成功后才推进 checkpoint。

```bash
conda run -n agent python -m logfusion orchestrate run \
  --input output/normalized.jsonl \
  --feature-state output/features.db \
  --baseline-state output/baseline.db \
  --detection-state output/detection.db \
  --incident-state output/incidents.db \
  --risk-state output/risk.db \
  --checkpoint output/orchestrator.checkpoint.json

# 进程重启后，只处理 checkpoint 之后追加的完整行
conda run -n agent python -m logfusion orchestrate run \
  --input output/normalized.jsonl \
  --feature-state output/features.db \
  --baseline-state output/baseline.db \
  --detection-state output/detection.db \
  --incident-state output/incidents.db \
  --risk-state output/risk.db \
  --checkpoint output/orchestrator.checkpoint.json --resume
```

默认 watermark 是已观察最大 `event.time` 减 5 分钟，可用 `--watermark-lag-seconds` 调整；它用于关闭稳定的实时会话。末尾尚未写完的一行不会提交，会留给下次追加后处理。输入必须是 append-only：恢复时如已提交前缀被改写、文件被替换、配置或任一 State DB 路径改变，编排器会拒绝继续。`--no-downstream` 仅更新 Feature DB，适合排障或分阶段运行。

## Alert Lifecycle & Analyst Feedback v1

Risk DB 只保存机器产生的版本化告警；Case DB 以稳定的 `alert_key` 保存人工处理状态，因此 Fusion 更新和旧告警 `superseded` 不会覆盖分析结论。先同步新的 active alert：

```bash
conda run -n agent python -m logfusion cases sync \
  --risk-state output/risk.db \
  --state output/cases.db

conda run -n agent python -m logfusion cases list --state output/cases.db --status new

conda run -n agent python -m logfusion cases transition \
  --risk-state output/risk.db --state output/cases.db \
  --case-id <case_id> --status investigating --actor alice \
  --note '已开始核查会话与仓库访问'
```

支持 `new`、`acknowledged`、`investigating`、`resolved`、`false_positive`、`suppressed` 状态，以及 `assign`、`comment`、`set-tags`。`suppressed` 必须指定未来的 `--suppression-until`；到期后下一次 `cases sync` 自动恢复为 `new`。所有操作都写入 Case DB 的审计事件。终态 Case 收到新的风险告警版本时保持原结论，但标记 `requires_review: true`，由分析员决定是否重新打开。

Case 的处置结论（`disposition`）与生命周期状态独立：`confirmed_threat` 是回测正样本、`benign` 是负样本、默认 `unknown` 不参与准确率。它不会改变 `status`，可通过以下命令记录并保留审计轨迹：

```bash
conda run -n agent python -m logfusion cases disposition \
  --risk-state output/risk.db --state output/cases.db \
  --case-id <case_id> --value confirmed_threat --actor alice
```

## Detection Backtest & Evaluation v1

Evaluation 使用 Feature、Baseline 和 Case DB 的只读快照，不会写入生产 Baseline、Detection、Incident 或 Risk DB。它按 UTC 自然日评估：对每个被评估日，仅使用此前 30 天的 Feature 数据构建 point-in-time 基线，因此当天及未来行为不会泄露到参照分布中。检测语义复用同一份 Detection Policy，包括 P95/P99、低频/首次值规则、source type 覆盖，以及 personal → peer group → global 回退。

```bash
conda run -n agent python -m logfusion evaluate run \
  --feature-state output/features.db --baseline-state output/baseline.db \
  --case-state output/cases.db --state output/evaluation.db \
  --policy-config config/detection.local.yaml --name detection-policy-v2 \
  --from 2026-06-01T00:00:00Z --to 2026-07-01T00:00:00Z \
  --report-output output/evaluation-report.json

conda run -n agent python -m logfusion evaluate query \
  --state output/evaluation.db --experiment-id <experiment_id>

conda run -n agent python -m logfusion evaluate compare \
  --state output/evaluation.db --experiment-id <id-a> --experiment-id <id-b>
```

评估单位是 `user.name + UTC day`：当天所有 candidate 合并为最大分数、candidate 数量、detector、严重度和基线范围。与该用户日相交的 Case 作为标签；同日多个标签以 `confirmed_threat` 优先，其次是 `benign`，仅有 `unknown` 时不计标签。指标只在已标注样本上计算 TP、FP、FN、precision、recall 和 F1；不计算 TN 或 accuracy。未标注但有预测的用户日单独计数，**不等于误报**。

应先使用 Evaluation DB 比较统计策略及不同 policy，再引入 Isolation Forest 等模型；后续模型仍应输出可解释的 candidate evidence，并使用同一套评估契约。

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
| `consume kafka` | Kafka → normalized / unknown / summary（可选 raw store、registry、checkpoint） |
| `consume kafka-ueba` | Kafka → 解析输出 + 增量 Feature / Baseline / Detection / Correlation / Fusion，并在本地持久化后提交 offset |
| `features build` | 完整 normalized JSONL → SQLite 用户窗口与会话状态 |
| `features query` | 查询用户在时间范围内相交的窗口与会话 |
| `baseline build` | Feature DB → 独立的个人/全局 Baseline DB（支持 revision 增量更新） |
| `baseline query` | 查询用户基线画像及对应全局参照 |
| `detect run` | Feature DB + Baseline DB → 可解释的统计异常候选 DB |
| `detect query` | 查询用户时间范围内相交的 active 异常候选 |
| `correlate run` | Active anomaly candidates → 版本化跨系统 Incident DB |
| `correlate query` | 查询用户时间范围内相交的 active Incident |
| `fuse run` | Detection DB + Incident DB → 风险评估与策略控制后的告警 DB |
| `fuse query` | 查询用户时间范围内相交的 active 风险评估与告警 |
| `orchestrate run` | 追加式 normalized JSONL → 增量 Feature / Baseline / Detection / Correlation / Fusion 链路 |
| `cases sync` | Risk DB active alert → 稳定逻辑 Case 与审计状态 |
| `cases list/show` | 查询 Case 列表或完整证据/操作时间线 |
| `cases transition/assign/comment/set-tags` | 人工处置、负责人、评论和标签操作 |
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
| `features.py` | SQLite FeatureEngine、窗口聚合、会话构建与 JSONL build/query |
| `baseline.py` | SQLite BaselineEngine、个人/全局统计与值频次基线 |
| `detection.py` | SQLite DetectionEngine、统计异常候选与 revision 替代 |
| `correlation.py` | SQLite CorrelationEngine、session/时间桶关联与 Incident 聚合 |
| `fusion.py` | SQLite FusionEngine、风险评分、告警门槛、预算抑制与版本替代 |
| `orchestrator.py` | 追加 JSONL 编排、event-time watermark、全链路推进与断点恢复 |
| `kafka_ueba.py` | Kafka 解析、UEBA 链路与 offset/checkpoint 提交顺序适配 |
| `cases.py` | SQLite CaseEngine、人工生命周期、审计事件与版本更新复核 |

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

## 明确不做（当前阶段）

- ECS / OCSF 导出适配器
- 账号/资产实体图谱与组织上下文增强
- 风险融合、场景化权重与告警抑制

## License

Apache License 2.0. See [LICENSE](LICENSE).
