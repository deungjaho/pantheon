# ADR-0004: SQLite 为 canonical event 事实源

- **Status:** Accepted
- **Decided:** 2026-07-28
- **Decision maker:** master
- **Supersedes:** 无
- **Superseded by:** 无

---

## Context

Pantheon 需要选择状态事实源（回答“现在是什么”）。blueprint §9.2 和 §10.1 明确区分四类数据：

- Current State（当前状态）→ Conductor SQLite；
- Event Journal（状态变迁）→ append-only JSONL；
- Artifact（证据）→ 文件存储；
- Semantic Memory（长期经验）→ Mnemos。

blueprint §6.1 论证 tmux 不能作为状态事实源。master 决策：tmux 非状态事实源，SQLite event + projection。

## Decision

1. **SQLite 为 canonical event 事实源**——workspace / task / run / agent / lease / approval / artifact metadata 保存在 SQLite；
2. **append-only JSONL 为 event journal**——结构化事件为主，便于故障恢复和审计；
3. **projection 从 event 重建当前状态**——SQLite 既是 event 存储也是 projection；
4. **tmux 非状态事实源**（见 ADR-0006）；
5. **失败 transcript 保留 7 天**——不长期保存完整 transcript；
6. **结构化事件为主**——不依赖从自由文本日志解析状态。

## Consequences

**正面：**
- 单机可靠，事务性保证（blueprint §8.4 推荐 SQLite）；
- event journal append-only，故障后可恢复；
- projection 与 event 分离，查询快；
- 不依赖 tmux screen scraping（blueprint §19）。

**负面：**
- SQLite 单机，跨机器时需迁移策略（Phase 5）；
- event journal 需要轮转和容量管理；
- projection 重建策略需设计；
- event spool 需要容量 / lease / ack / dead-letter（blueprint §10.2）。

**待验证：**
- SQLite schema（见 `docs/contracts/README.md` §2.5）；
- event schema_version 与字段；
- projection 重建策略；
- event spool 容量与 dead-letter。

## Cross-ref

- ADR-0006（tmux 非状态事实源）
- `docs/contracts/README.md` §2.5（状态契约待定项）
- `docs/PRINCIPLES.md` §3（状态真实性）、§6（证据优先）
- blueprint §6.1（tmux 不是 Agent 状态）、§9.2（registry vs event journal）、§10.1（四类数据分离）、§10.2（canonical event）
