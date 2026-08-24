# ADR-0006: tmux 非状态事实源

- **Status:** Accepted
- **Decided:** 2026-07-28
- **Decision maker:** master
- **Supersedes:** 无
- **Superseded by:** 无

---

## Context

Pantheon 使用 tmux 作为 Agent 运行容器。一个自然倾向是从 tmux pane 文本推导 Agent 状态（是否完成、是否在运行、角色是什么）。

blueprint §6.1 明确论证 tmux 不能作为状态事实源：
- tmux 只能证明 server/session/pane 在某时存在；
- 不能证明 Agent 是否有可用模型连接、是否在等待工具、任务是否完成、修改是否通过验收、pane 中的 PID 是否仍是原 Agent、runtime 是否已重启并失上下文。

master 决策：tmux 非状态事实源，SQLite event + projection（ADR-0004）为事实源。

## Decision

1. **tmux 是运行和观察容器，不是状态事实源**——tmux 用于启动、attach、观察 Agent 进程，但不作为状态查询的权威来源；
2. **状态查询走 SQLite projection**（ADR-0004）——status 命令返回来自 SQLite，非 tmux；
3. **不依赖 pane 文本推导角色或完成状态**——blueprint §7.4“布局只是默认呈现，不是状态协议”；
4. **Reconciler 对比 tmux 与 registry**——tmux 用于发现 orphan pane / stale session，但修正状态需写入 SQLite，不反向；
5. **一个 Task 一个 worktree/session**——tmux session 与 task 一一对应，但状态仍以 SQLite 为准。

## Consequences

**正面：**
- 状态真实，不受 tmux UI 混淆影响；
- Agent 自报完成、pane alive、日志 success 都不单独证明完成（blueprint §0.1）；
- tmux 可重启/销毁而不丢状态（状态在 SQLite）；
- 符合 blueprint §19（禁止 tmux screen scraping 作为正式协议）。

**负面：**
- 需要维护 SQLite 与 tmux 的 reconciliation（blueprint §8.2 Reconciler）；
- tmux orphan pane / stale session 需要清理策略；
- 多 tmux server 发现需要 registry（blueprint §7.2）；
- 用户不能只靠 `tmux ls` 判断状态，需用 `conductor status` 类命令。

**待验证：**
- Reconciler 对比 tmux 与 SQLite 的具体策略；
- orphan pane 清理规则；
- 多 tmux server 发现机制。

## Cross-ref

- ADR-0004（SQLite 为 canonical event 事实源）
- `docs/PRINCIPLES.md` §3（状态真实性）
- `docs/contracts/README.md` §2.5（状态契约）
- blueprint §6.1（tmux 不是 Agent 状态）、§7.2（一任务一 tmux server）、§7.4（布局非状态协议）、§8.2 Reconciler、§19（禁止 tmux screen scraping）
