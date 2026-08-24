# ADR-0017: 已完成/阻塞 run 的持久化续接切片

- **Status:** Accepted
- **Decided:** 2026-08-05
- **Decision maker:** Portfolio Master
- **Supersedes:** 无
- **Superseded by:** 无

---

## Context

**问题（已证实）：** Argus P2-B 完成了代码但没有活跃 owner、没有交接/状态更新、也没有后续 P2-UX run。该 run 静默停留在 `completed` 状态。没有 daemon tick 检测到它。没有唤醒通知发送给 PM。项目停滞数天，直到手动 portfolio review 发现它。

**根因：** Pantheon 没有"此已完成 run 需要后续 run"的持久化表示，也没有检测此类 run 并唤醒 PM 的 daemon tick。现有 `wake.Loop`（ADR-0013）处理*新事件*但不检测*需要关注的 stale 已完成 run*。`ReconcileAfterCrash` 只处理崩溃恢复（将运行中的事物标记为失败）——它不处理"已完成但需要后续"的情况。

**约束：** 不要构建 LLM 调度器或自动启动任意业务 worker。PM/操作者必须显式创建后续 run。系统只*检测*和*通知*——不*自主行动*。

## Decision

实现最小的持久化切片，包含四个原语：

### 1. 续接记录（持久化，在 SQLite 中）

一个 `continuations` 表，将已完成/阻塞的 run 链接到所需的后续 run。由 PM/操作者在注册续接需求时显式创建。状态机：

```
pending → notified → fulfilled
                    ↘ cancelled
pending → cancelled
```

- `pending`：已注册，PM 尚未被通知（或通知已过期）
- `notified`：唤醒通知已发送到 PM 队列（去重标记）
- `fulfilled`：后续 run 已创建（显式 PM 操作）
- `cancelled`：PM 决定不需要后续 run

### 2. Reconcile tick（幂等，daemon 侧）

`ReconcileContinuations(ctx)` — 由 daemon tick 或 wake loop handler 调用。列出所有 `pending` 和 `notified` 的续接记录。对每条：

- 如果 `wake_sent_at` 为零或早于 `wake_interval`：向 PM 消息队列发送唤醒通知并设置 `wake_sent_at = now`。
- 如果 `wake_sent_at` 在 `wake_interval` 内：跳过（去重）。

这是幂等的：在去重窗口内多次运行不会产生重复通知。daemon 重启后运行也能正确恢复，因为状态在 SQLite 中，不在内存中。

### 3. 唤醒通知（去重，到 PM 队列）

使用现有 `PublishMessage` 基础设施。通知是 topic 为 `wake.continuation` 的消息，收件人为 run 的 owner。去重键是 `(continuation_id, wake_sent_at)`——如果 `wake_sent_at` 非零且在间隔内，tick 跳过。

### 4. 显式后续创建（无隐式）

`CreateSuccessorRun(ctx, continuationID, ...)` — 显式 PM/操作者操作。创建链接到续接记录的新 run，将续接记录标记为 `fulfilled`。永远不会发生隐式后续创建。系统从不自动启动 worker。

## Consequences

- **持久化：** 续接状态在 daemon 重启后存活（SQLite）。
- **幂等：** reconcile tick 可被调用任意次数。
- **去重：** 唤醒通知通过 `wake_sent_at` 去重。
- **显式：** 后续创建始终是 PM/操作者操作。
- **无 LLM 调度器：** 系统检测和通知，从不自主行动。
- **可测试：** 重启/stale/无重复行为可单元测试。

## Implementation scope

- `internal/domain/continuation.go` — 类型和状态机
- `internal/store/continuation.go` — SQLite CRUD（migration v8）
- `internal/wake/reconcile.go` — reconcile tick + 通知
- `internal/rpc/service.go` — RPC 方法（registerContinuation, createSuccessor, listPending）
- 以上所有的测试
- Argus P2-B → P2-UX 作为首个真实 fixture
