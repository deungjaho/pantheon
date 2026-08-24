# ADR-0016: Message Bus + Notification Adapter — 集成到 Pantheon

- **Status:** Accepted
- **Decided:** 2026-07-29
- **Accepted:** 2026-07-29 (D-pantheon-015, message bus + TmuxNotifier implemented + tested)
- **Decision maker:** 用户决策
- **Related:** ADR-0013 (wake-loop), ADR-0015 (subagent independent devin CLI)

---

## Context

当前 agent 间通信依赖文件系统 (inbox/outbox) + mtime 轮询 (wake-loop.sh) + 手动 tmux send-keys。
问题：
1. wake-loop 轮询 mtime — 有延迟，不能区分内容变化
2. tmux send-keys 是 leaky abstraction — 手动拼 session:window.pane
3. inbox/outbox 是无结构文本 — 无 history，无 replay，无 audit
4. agent 间不能直接通信 — 必须经过文件系统中转
5. 无消息路由 — 不能按 topic 订阅

用户提议：建立消息队列推送机制，集成 tmux 命令包装，便于 agent 间同步状态 + 下达指令。
问题：单独建组件 vs 集成到 Pantheon？

## Decision

**集成到 Pantheon。** Pantheon 已有 event journal (SQLite append-only) + EventsSince(cursor) + SendNotification — 80% 基础设施已就绪。单独建消息队列会与 event journal 重复。

### 架构

```
Pantheon daemon
├── event journal (已有 — events 表)
│   └── seq, event_id, run_id, agent_id, event_type, severity, payload, timestamp
│
├── agent registry (已有 — agents 表)
│   └── agent_id, run_id, pid, state, session_id, tmux_session
│
├── message bus (新增 — 薄层，复用 events 表)
│   ├── message.publish {topic, payload, target_agent?}
│   │   → 写入 events 表 (event_type="message", payload={topic, body, from, to})
│   │   → 如果 target_agent 有活跃 session → 触发 notification
│   │
│   ├── message.subscribe {topic, cursor}
│   │   → 返回 EventsSince(cursor) filtered by topic
│   │
│   └── message.history {topic, limit}
│       → 查询历史消息
│
└── notification adapter (新增 — 替代 tmux send-keys)
    ├── TmuxNotifier
    │   ├── notify(agent_id, message) → 查 agent registry → tmux session → send-keys
    │   ├── 封装为原子语义命令 (不是 leaky send-keys)
    │   └── session_for_agent(agent_id) → agents.tmux_session
    │
    ├── InboxNotifier (fallback / persistent)
    │   └── write(agent_name, message) → 追加到 inbox/{name}.md
    │
    └── (未来) WebhookNotifier / EmailNotifier
```

### 消息类型

```
directive:  Portfolio Master → PM     (命令，替代 outbox/*.md)
report:     PM → Portfolio Master     (报告，替代 inbox/*.md)
event:      系统 → 所有 agent          (run.started, run.completed, agent.registered)
query:      agent → agent              (信息查询)
```

### 消息 topic 命名

```
directive.{project}    — 指令 (e.g. directive.hydra)
report.{project}       — 报告 (e.g. report.mnemos)
event.{type}           — 系统事件 (e.g. event.run_completed)
agent.{agent_id}       — agent 私有消息
```

### 工作流变化

**之前（当前）**：
```
Portfolio Master 写 outbox/hydra.md
  → tmux send-keys "检查 outbox" (手动，leaky)
  → Hydra PM 手动读 outbox/hydra.md
  → Hydra PM 写 inbox/hydra.md
  → wake-loop 检测 inbox mtime 变化
  → Portfolio Master 读 inbox/hydra.md
```

**之后（集成 Pantheon message bus）**：
```
Portfolio Master: pantheon message.publish {topic: "directive.hydra", payload: {...}}
  → Pantheon event journal 记录 (seq, timestamp, payload)
  → TmuxNotifier 推送到 hydra-remote-pm session (语义命令)
  → Hydra PM 收到通知 + 消息内容
  → Hydra PM: pantheon message.publish {topic: "report.portfolio", payload: {...}}
  → Pantheon event journal 记录
  → Portfolio Master subscribe "report.*" → 自动收到
```

### Agent registry 扩展

当前 Agent struct 需要增加 `TmuxSession` 字段：
```go
type Agent struct {
    // ... existing fields ...
    TmuxSession string `json:"tmux_session,omitempty"` // 新增: tmux session name for notification
}
```

`TmuxNotifier.notify(agent_id, msg)` 流程：
1. `Store.GetAgent(agent_id)` → 获取 tmux_session
2. `tmux send-keys -t {tmux_session} "{msg}" Enter`
3. 封装在 TmuxNotifier 内部，调用方不接触 tmux

### RPC 方法

```
message.publish   {topic, payload, target_agent_id?}  → {seq}
message.subscribe {topic, cursor}                     → {messages[], next_cursor}
message.history   {topic, limit}                      → {messages[]}
```

### 与 wake-loop 的关系

wake-loop.sh (interim) 升级为 message bus subscriber：
- 不再轮询 mtime
- subscribe `report.*` + `event.*`
- 收到消息后通知 Portfolio Master (tmux send-keys)

最终 wake-loop 被 Pantheon reconciler + message bus 替代。

### 与 inbox/outbox 文件的关系

**过渡期**：inbox/outbox 文件继续用，message bus 并行运行。
**最终**：inbox/outbox 被 message bus 替代，文件只作为 human-readable fallback。

inbox/outbox 文件可以由 InboxNotifier 自动生成（从 message bus 投影），保持人类可读性。

## Implementation

### Phase 2 Priority 1.6 (新增)

| Priority | 内容 | 依赖 |
|---|---|---|
| 1.5 | Daemon 长驻 + Unix socket (ADR-0015) | — |
| **1.6** | **Message bus + TmuxNotifier** | **1.5** |
| 2 | Verifier (独立 Run) | 1.6 (verifier 通过 message bus 报告) |
| 3 | Reconciler automation | 1.6 (reconciler 通过 message bus 通知) |
| 4 | Wake-loop integration | 1.6 (wake-loop 变为 subscriber) |

### 实施步骤

1. **Agent struct 加 TmuxSession 字段** — 小改动
2. **message.publish RPC** — 写入 events 表 + 触发 notification
3. **message.subscribe RPC** — EventsSince filtered by topic
4. **TmuxNotifier** — 封装 tmux send-keys 为语义命令
5. **InboxNotifier** — 从 message 投影到 inbox/*.md (保持人类可读)
6. **wake-loop.sh 升级** — 从 mtime 轮询改为 message.subscribe

### 短期过渡（Pantheon daemon 部署前）

在 Pantheon daemon 长驻模式完成前，先封装 tmux 语义命令脚本：

```bash
# scripts/notify-pm.sh
#!/bin/bash
# 语义命令: notify-pm <project> <message>
# 封装 tmux send-keys，调用方不接触 tmux 细节
PROJECT="$1"
MSG="$2"
SESSION="${PROJECT}-remote-pm"
tmux send-keys -t "$SESSION" "$MSG" Enter
```

这解决 H5 (tmux send-keys leaky abstraction) 和 E2 (tmux 语义命令层)。

## Consequences

**正面:**
- 不再依赖文件 mtime 轮询 — event journal 是 push
- tmux send-keys 封装为语义命令 — 不再 leaky
- 所有消息有 history — 可以 replay + audit
- agent 间可以直接通信 — 不需要文件系统中转
- wake-loop 升级为 subscriber — 不再轮询
- 与 ADR-0015 subagent 独立化一致 — 独立 agent 通过 message bus 通信

**负面:**
- Pantheon Phase 2 scope 再扩展 — 开发量增加
- 短期内 inbox/outbox + message bus 并行 — 有重复

**风险:**
- message bus 的可靠性依赖 SQLite — 单点故障（但 Pantheon daemon 本身就是单点，可接受）
- tmux session name 需要稳定命名约定 — 当前 {project}-remote-pm 已有约定
