# ADR-0018: run、agent 和后续 run 的终态一致性

- **Status:** Accepted
- **Decided:** 2026-08-12
- **Decision maker:** Portfolio Master
- **Supersedes:** 无
- **Superseded by:** 无

---

## Context

**问题（已证实，来自真实 Argus run）：**

1. `run_77b243950d58dee9781f26531e39d4c7`（旧 Argus）在其后续 run（`run_8a07625e0b7bd1f8391c463d4c7bb079`，P2-UX）fulfilled 后仍保持 `running` 状态。没有操作能在不重写旧 run 历史的情况下将旧 run 链接到其后续 run。旧 run 永远停留在瞬态。

2. `run_8a07625e0b7bd1f8391c463d4c7bb079`（已完成的 P2-UX）的 `result_state` 为空。`VerifyRun` 转换了 run 状态（running → verifying → completed）但没有设置 `result_state`。投影不一致：run 是 `completed` 但其结果是 `not_started`。

3. 已完成 run 的 worker/verifier agent 仍保持 `registered`/`running` 状态且 PID 已死。没有终态化操作关闭它们。agent 投影泄漏了僵尸 agent。

4. 已完成的 P2-UX run 没有显式的下一步行动记录。portfolio 在无告警的情况下进入空闲——没有附加到终态 run 的"none | continuation | blocked"持久化决策。

**根因：** Pantheon 的终态路径不完整。`VerifyRun` 投影了 run *状态*但没有投影*结果*。Agent 终态化是 agent 自身的责任（自报告），没有 run 驱动的终态化。旧 run 与后续 run 之间没有链接原语（只有续接记录，它是一种*需求*，不是*链接*）。完成时没有记录显式的下一步行动决策，因此 reconcile tick 无法呈现"已完成但无决策"的 run。

## Decision

实现四个原语来弥合终态缺口，全部原子且 append-only：

### 1. 原子 verify 投影（C1）

`VerifyRun` 现在在同一事务中原子地设置 `result_state` 与状态转换：
- PASS → `result_state = 'accepted'`
- FAIL → `result_state = 'failed'`

run 状态和结果状态在 verify 后永远不会不一致。

### 2. Agent 终态化（C2）

当 run 转换到终态（completed/failed/canceled）时，所有非终态 agent（registered/starting/running）被关闭：

- `state = 'exited'`
- `exited_at = now`
- `exit_code = NULL`（真实退出码未知）
- 追加 `agent.terminalized` event，内容为 `{agent_id, run_id, reason: "run_terminalized", run_state, evidence_ref}`

实现为 `Store.TerminalizeAgents(ctx, runID, reason, evidenceRef)`，从 `VerifyRun`（同一事务）和 `handleRunCancel`（后续事务）调用。

### 3. 显式 supersede 链接（C3）

一个 `supersedes` 表（migration v9）将旧 run 链接到其后续 run。这是显式 PM/操作者操作——一种*链接*，不是状态转换。旧 run 的状态不会被 supersede 改变（可能被单独终态化）。

```sql
CREATE TABLE IF NOT EXISTS supersedes (
    supersede_id    TEXT PRIMARY KEY,
    old_run_id      TEXT NOT NULL UNIQUE,
    successor_run_id TEXT NOT NULL,
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL
)
```

- 每个旧 run 一个后续 run（`old_run_id` 上 UNIQUE）。
- `SupersedeRun(ctx, oldRunID, successorRunID, reason)` 验证两个 run 都存在、`oldRunID != successorRunID`、且 `oldRunID` 无现有 supersede。追加 `run.superseded` event。全部在一个事务中。
- RPC：`run.supersede`，参数 `{old_run_id, successor_run_id, reason}`。

### 4. 完成时的下一步行动决策（C4）

`runs` 上的 `next_action` 列（migration v9）记录 PM 在 run 完成时的决策：`none | continuation | blocked`。空字符串表示"未决策"——reconcile tick 呈现这些。

- `run.verify` 接受可选 `next_action` 参数。如果未提供，PASS 默认为 `none`，FAIL 默认为 `blocked`。
- `run.set_next_action` 允许 PM 在 verify 后设置/更改决策（如后续创建续接时）。幂等——调用两次更新值。
- Reconcile 呈现 `next_action` 为空的终态 run（"缺失决策"情况）。

## Consequences

- **原子：** verify 在一个事务中投影状态 + 结果 + next_action + agent 终态化。无部分投影。
- **显式：** supersede 是链接，不是状态重写。旧 run 的历史被保留。
- **可呈现：** 无下一步行动决策的终态 run 被 reconcile 呈现，因此 portfolio 不会静默空闲。
- **向后兼容：** migration v9 添加带空默认值的列；现有 run 获得 `next_action = ''`（未决策），reconcile 会呈现。不重写现有数据。
- **可测试：** 原子性、幂等性、并发和崩溃回滚可单元测试。

## Implementation scope

- `internal/domain/supersede.go` — `SupersedeRecord` 类型
- `internal/domain/types.go` — `NextAction` 类型 + 常量，`Run` 上的 `NextAction` 字段
- `internal/domain/runstate_v2.go` — 终态 helper
- `internal/store/store.go` — migration v9（`next_action` 列 + `supersedes` 表）
- `internal/store/supersede.go` — `SupersedeRun`、`GetSupersede`
- `internal/store/crud.go` — `VerifyRun` 原子投影 + `TerminalizeAgents` + `SetNextAction` + `scanRun`/`CreateRun` 更新
- `internal/store/terminal_state.go` — reconcile 呈现
- `internal/rpc/service.go` — `run.supersede`、`run.set_next_action`、`reconcile.terminal_state`、`run.verify` next_action 参数
- 以上所有的测试（原子性、幂等性、并发、崩溃）
- Argus `run_77b...` → `run_8a07...` 作为首个真实 fixture
