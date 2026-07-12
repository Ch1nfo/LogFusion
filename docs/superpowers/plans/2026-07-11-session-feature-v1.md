# Session + Feature v1 实施计划

1. 新增 FeatureEngine 与 SQLite schema：状态元数据、事件幂等、窗口、值频次/统计、会话和 revision。
2. 以测试驱动实现固定窗口、资源键、首次出现、重复/拒绝事件、int64 检查与批处理事务。
3. 实现事件时间 session window、provisional 合并、历史 EOF 收尾及确定性 closed session ID。
4. 增加 JSONL 流式 build/query 服务和 `logfusion features` CLI。
5. 覆盖历史状态库限制、查询相交语义、版本/配置保护、回放顺序确定性与全量回归。
