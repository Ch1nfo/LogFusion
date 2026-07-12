# Logfusion Session + Feature v1 设计

## 目标

在日志标准化之后，为 UEBA 建立可回放、可解释、可增量更新的行为构建层。首版以 Canonical Event 的 `user.name` 精确字符串作为用户主键，将历史 normalized JSONL 和未来 Kafka 标准事件统一处理为：

- 用户固定时间窗口特征；
- 用户跨系统行为会话；
- 首次出现的 IP、资源与动作统计；
- 可供后续 Baseline Engine 使用的 SQLite 状态。

本模块不做账号图谱、组织/资产上下文、异常判定、机器学习、告警和自动处置。

## 已确认约束

- 跨系统用户归类规则为：相同 `user.name` 字符串即同一用户；不做大小写、域前缀、邮箱或账号别名归一。
- 状态存储首版使用本地 SQLite。
- 事件按照 `event.time`（UTC）聚合。
- 会话采用 30 分钟不活跃超时。
- 允许 5 分钟乱序/迟到事件。超过该范围的事件仍进入所属固定窗口，但不修改已关闭会话。
- 初次运行允许没有历史数据，随着新事件持续建立状态。
- `features build` 仅用于向全新的 SQLite 状态库执行一次完整历史初始化；状态库完成 `flush_sessions()` 后不接受后续历史回填。需要补充更早历史时必须使用全部历史重建新库。
- 初始化完成后由实时 Kafka 持续增量更新；若首次运行没有历史数据，可直接以空状态库进入实时模式。

## 数据流

```text
normalized event JSONL / future Kafka normalized event
    -> event validation and deduplication
    -> fixed-window aggregation
    -> sessionization
    -> SQLite feature state
    -> future baseline and detection modules
```

输入必须是已经通过 Canonical Schema 校验的标准事件。模块不读取 raw log，也不重新执行 parser；raw、normalized JSONL 与回放继续由现有解析链路负责。

### Canonical 字段契约

FeatureEngine 只读取下列固定字段，不在多个候选路径中猜测：

| 含义 | Canonical 路径 | 必填 |
| --- | --- | --- |
| 事件 ID | `event.id` | 是 |
| 事件时间 | `event.time` | 是 |
| 用户名 | `user.name` | 是 |
| 日志来源类型 | `raw.source_type` | 是 |
| 日志来源实例 | `raw.source_id` | 是 |
| 来源 IP | `source.ip` | 否 |
| 资源 ID | `resource.id` | 否 |
| 资源类型 | `resource.type` | 否 |
| 资源名称 | `resource.name` | 否 |
| 动作 | `event.action` | 是 |
| 结果 | `event.outcome` | 是 |
| 网络字节数 | `network.bytes` | 否 |

当前 Canonical Schema v0 没有 `event.provider`、`network.bytes_in` 或 `network.bytes_out`，Feature v1 不创建或推断这些字段。字段契约、动作分类与结果分类共同纳入配置指纹。

## 核心组件

### FeatureEngine

`FeatureEngine` 是唯一的事件写入入口：

```python
engine = FeatureEngine(state_db, config)
result = engine.process_event(event)
```

`process_event()` 在一个 SQLite 事务中完成事件去重、revision 分配、窗口更新、值频次/历史统计更新和会话更新。返回结果说明该事件是否重复、更新了哪些窗口、出现了哪些首次项、归属哪个会话，以及被跳过时的原因。

事务逻辑拆成不自行开启事务的 `_process_event_in_transaction()`：`process_event()` 在单事件事务中调用它；`process_events()` 在批事务中为每条事件创建 SAVEPOINT 后调用它，避免 SQLite 嵌套 `BEGIN`，同时兼顾吞吐和单事件失败隔离。

同一接口供本地 JSONL 初始化与未来 Kafka 消费适配器调用，避免离线/在线特征定义漂移。

### 固定窗口

每个 `user.name × window_size × window_start` 建立自然时间边界窗口，首版窗口大小为：1 分钟、5 分钟、1 小时、1 天。窗口统一采用左闭右开区间 `[window_start, window_end)`；恰好等于窗口结束时间的事件进入下一个窗口。

每个窗口保存：

- `event_count`、`success_count`、`failure_count`、`other_outcome_count`；
- `source_type_distinct_count`、`source_ip_distinct_count`、`resource_distinct_count`、`action_distinct_count`；
- `read_action_count`、`write_action_count`；
- `network_bytes_total`；
- `first_seen_ip_count`、`first_seen_resource_count`、`first_seen_action_count`；
- `window_start`、`window_end`、`window_size`。

每个具体 IP、资源、动作和来源系统的出现次数写入 `window_value_counts`，窗口行只保存对应的 distinct 数量。

读写动作分类来自版本化配置而不是 FeatureEngine 的硬编码：配置明确大小写处理、空 action 行为，以及 `repo_checkout`、`file_create`、`rename` 等动作的分类。结果分类同样版本化：当前 `success` 计入成功、`failure` 计入失败、`unknown` 计入其他，始终满足 `event_count = success_count + failure_count + other_outcome_count`。

首版只累计 Canonical `network.bytes` 到 `network_bytes_total`。字段必须是非负 int64；窗口累计使用受检查的 int64 加法，单值或聚合溢出时整个事件事务回滚，不产生部分特征更新。未来 Canonical Schema 增加方向字节字段后再通过新 Feature Schema 版本扩展，避免重复累计。

### 历史首次项

首次项按事件时间而不是处理顺序定义：同一 `user.name` 的某个值以最小 `(event.time, event.id)` 作为首次出现。比较的值包括：

- `source.ip`；
- 复合 `resource_key`；
- `event.action`。

资源统计键按固定优先级生成：存在 `resource.id` 时使用 `resource.type + ":" + resource.id`，否则使用 `resource.type + ":" + resource.name`。资源类型或 ID 缺失时使用明确的空值占位，避免不同来源的同名资源错误合并；`resource.name` 仅作为展示值。

`user_value_stats` 保存 `value_key`、展示值、首次/最后事件时间与 ID及总次数。若更早事件迟到，事务会将原首次事件所属四个窗口的对应 `first_seen_*_count` 减一，并将新首次事件所属窗口加一，再更新首次键。因此同一组输入无论处理顺序如何，完成回放后的首次统计一致。首版不根据异常结果排除基线数据；该污染控制属于后续 Baseline Engine 的职责。

### 会话

会话按 `user.name` 建立，允许跨 SSO、SVN、GitLab 等所有来源，并采用真正的事件时间 session window。会话可在允许乱序期内保持多个 `provisional` 状态：新事件作为单事件会话，与同一用户所有相邻 provisional 会话按 30 分钟间隔规则合并；桥接事件可以将两个会话合并。合并后，动作按 `(event_time, event_id)` 重排。

SQLite 内部使用稳定的 `session_row_id INTEGER PRIMARY KEY` 管理 provisional 会话。provisional 阶段对外只返回可变的临时标识；closed 会话的稳定 `session_id` 由 `SHA256(user.name + session_start_time + first_event_id)` 生成，其中首事件按 `(event.time, event.id)` 确定。会话合并时保留确定性的 survivor，并在被合并行记录 `merged_into_row_id`。

会话摘要包含：

- `session_row_id`、稳定或临时 `session_id`、`user_name`、开始/结束时间、精确事件数；
- 来源系统、IP、资源的有界展示样本 `*_samples_json` 与 `*_samples_capped`；
- 前 N 个与后 N 个动作、有界标记及精确 `action_count`；
- `closed_at` 与关闭原因。

会话样本只用于调查解释，不代表精确 distinct，也不参与基线计算。精确频次以 `window_value_counts` 和 `user_value_stats` 为准；两个已 capped 会话合并时只合并展示样本并保持 capped，不推导精确集合数量。

FeatureEngine 不自行猜测上游水位线，公开 `advance_watermark(watermark)` 与 `flush_sessions()`。实时 Kafka Adapter 根据分区状态计算安全水位线；仅当 `watermark > session.last_event_time + 30 minutes` 时，会话才能从 `provisional` 转为 `closed`。超过允许迟到范围的事件仍可更新固定窗口及值统计，但不修改关闭会话。

历史 JSONL 的 `features build` 使用 replay 模式：在 EOF 前不推进关闭水位线，因此同一批历史事件不受文件排列顺序影响，EOF 调用 `flush_sessions()` 后统一以 `end_of_input` 原因关闭会话。完成 flush 的历史状态库拒绝再次执行 build；v1 不重新打开 closed 会话，也不支持历史增量回填。实时模式使用 5 分钟允许迟到。两种语义明确区分：历史初始化优先确定性，实时消费优先有界状态与延迟。

## SQLite 状态

| 表 | 用途 |
| --- | --- |
| `processed_events` | 以全局稳定 `event.id` 为主键实现幂等，记录 `processed`/`rejected`、拒绝原因及 Canonical/Feature Schema 版本。 |
| `user_windows` | 用户窗口数值特征，包含 `updated_revision` 与 `updated_at`。 |
| `window_value_counts` | 窗口内 IP、资源、动作、来源系统的精确频次，包含 `updated_revision`。 |
| `user_value_stats` | 用户历史值的首次/最后事件时间与总频次，包含 `value_key`、展示值与 `updated_revision`。 |
| `user_sessions` | `provisional` 与 `closed` 会话摘要、合并关系和 `updated_revision`。 |
| `user_activity_bounds` | 用户首次与最后事件时间及 `updated_revision`，供 Baseline 查询时补齐空窗口。 |
| `feature_meta` | Feature/Canonical Schema 版本、配置指纹、运行模式、会话参数、统计计数、水位线和 `current_revision`。 |

`window_value_counts` 与 `user_value_stats` 保持精确，不设置简单截断；否则无法同时保证准确 distinct、首次判断和稀有度。只对会话展示摘要集合及动作序列设置上限：会话保留前 N 个和后 N 个动作、完整 `action_count` 与 `sequence_capped` 标记。IP、资源、来源集合也保留展示上限及各自 `capped` 标记。数据库容量由监控和后续的保留/近似统计策略治理，而非静默牺牲 v1 正确性。

每个成功修改特征状态的事件事务将 `feature_meta.current_revision` 加一，并把该 revision 写入所有被修改行的 `updated_revision`。Baseline Engine 保存 `last_consumed_revision`，通过 `updated_revision > last_consumed_revision` 获取新增和被迟到事件修订的历史状态。重复或 rejected 事件不产生特征 revision。

## 错误处理与一致性

- 缺失 `event.id` 的事件仅计入当次运行摘要，无法持久化幂等；有 `event.id` 但缺失有效 `event.time` 或非空 `user.name` 的事件以 `rejected` 状态持久化，重放时不重复计数。
- 任一写入失败时，整个单事件事务回滚；不会留下半更新窗口或半更新会话。批量构建可在外层批次事务中为每条事件使用 SAVEPOINT，兼顾单事件失败隔离与吞吐。
- 已处理或已拒绝的 `event.id` 直接跳过，保证 JSONL 重跑与 Kafka 至少一次重放不会重复累计。输入契约要求 Normalize 层生成的 `event.id` 在所有来源全局唯一且重放稳定；现有本地和 Kafka 路径均以来源上下文构造 SHA-256 标识。
- 状态中的 feature schema 版本或配置指纹与当前运行不兼容时，拒绝继续写入；用户须显式重建或执行未来迁移工具。
- rejected 幂等只在当前 Canonical/Feature Schema 版本内有效。parser 或字段映射修复后，v1 通过重建 Feature DB 重新处理，不提供 `retry-rejected`。
- SQLite 仅是 v1 单机状态实现，并采用单写者、多个只读查询者模型；启用 WAL、`busy_timeout` 与必要索引。存储接口保持隔离，以便未来替换为分布式在线特征存储。

## CLI

```bash
logfusion features build \
  --input output/normalized.jsonl \
  --state output/features.db

logfusion features query \
  --state output/features.db \
  --user wangkun78 \
  --from 2026-07-01T00:00:00Z \
  --to 2026-07-02T00:00:00Z
```

`build` 流式读取 JSONL，不将完整输入加载到内存；目标数据库必须是全新状态库。它输出处理摘要，包括处理数、去重数、跳过数、更新窗口数、会话创建/关闭数和迟到事件数。

`query` 使用左闭右开范围 `[from, to)`：窗口按 `window_start < to AND window_end > from` 返回，会话按 `start_time < to AND last_event_time >= from` 返回，因此包括跨越查询边界的窗口和会话。

## 非目标

- 完整身份、资产、组织和威胁情报上下文增强；
- 同群体/全局基线；
- 阈值、异常分、告警、风险融合；
- 图特征、序列模型和机器学习；
- Kafka Source Adapter 直接写入特征库（后续复用 `FeatureEngine.process_event()` 接入）；
- Redis、ClickHouse、Parquet 或对象存储。

## 验收与测试

- 同一 `user.name` 的 SSO 与 SVN 事件进入同一用户窗口和会话；
- 固定窗口左闭右开边界、成功/失败、来源/IP/资源/动作 distinct 与具体值频次正确；
- IP、资源和动作首次出现按 `(event.time, event.id)` 确定；不同输入顺序回放得到一致最终状态；
- 同名但不同类型/ID 的资源生成不同 `resource_key`；
- 重复 `event.id`、整份 JSONL 重跑和模拟 Kafka 重放不会重复累计；
- 历史 replay 下，不同输入顺序得到相同会话；`10:00`、`10:31`、迟到的 `10:29` 可合并为一个会话；桥接两个 provisional 会话、30 分钟边界与动作序列中间插入均正确；
- 5 分钟内乱序可更新 provisional 会话，超过范围只更新窗口和值统计；EOF flush 后没有 provisional 会话；
- 无用户名或必填事件字段的事件被安全跳过并写入摘要；
- 会话摘要集合/动作序列上限生效且状态有 `capped` 标记，精确频次状态不被截断；
- closed `session_id` 在不同输入顺序下保持一致；两个 capped provisional 会话合并后样本语义保持正确；
- 已 flush 状态库拒绝再次历史 build，补录历史需要完整重建；
- 迟到事件修改旧窗口时，对应状态行获得新的 `updated_revision`；
- `unknown` 结果进入 `other_outcome_count`，并保持结果计数恒等式；
- `network.bytes` 聚合不重复累计，int64 溢出时完整事务回滚；
- query 返回与范围相交而非仅在范围内开始的窗口和会话；
- SQLite 写入失败回滚；
- 大批量 JSONL 流式处理保持固定内存；
- 全部既有解析、Registry、replay、streaming 与 Kafka 单元测试保持通过。

## 后续顺序

本模块完成后，下一模块是 Baseline Engine v1：基于这些窗口构建用户个人历史分布、全局分布，以及 IP/资源/动作稀有度。稳定基线完成后，才增加可解释的统计异常候选。
