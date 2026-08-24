# ADR-0015: Subagent 独立化 — 从 Devin 内部 run_subagent 到独立 devin CLI + Pantheon 注册

- **Status:** Accepted
- **Decided:** 2026-07-29
- **Accepted:** 2026-07-29 (D-pantheon-014, daemon deployed + --prompt-file fixed + socket mode verified)
- **Decision maker:** 用户决策（一步到位）
- **Supersedes:** Devin 内部 run_subagent 模式
- **Superseded by:** 无

---

## Context

当前 PM 的 implementer/verifier subagent 是 Devin 内部 `run_subagent`，不是独立 OS 进程。问题：
1. verifier 独立性不可外部审计
2. subagent 共享 PM 进程上下文（Devin 黑盒）
3. 无独立日志 — subagent 输出经过 PM
4. PM 既是执行者又是报告者

用户决策：**一步到位 — 要求 subagent 起独立 devin CLI 进程，并接入 Pantheon 已有的 agent 注册系统。**

## 现有基础设施

Pantheon 已实现：
- `RuntimeAdapter` 接口 (Start/Stop/Inspect)
- `DevinAdapter` — spawn `devin CLI` as `exec.CommandContext`, 获得真实 PID, 写 pidfile
- `Store.RegisterAgent` — SQLite INSERT + event journal (`agent.registered`)
- `Store.UpdateAgentState` — 状态机 (registered → running → exited/lost)
- `Store.GetAgentByRun` — agent 发现
- `run.submit` RPC — 创建 workspace + spawn devin + 注册 agent
- `run.list` RPC — 列出所有 run + agent
- `run.status` RPC — 查询 run + agent 状态
- `run.reconcile` RPC — 对比 SQLite vs PID liveness

**关键：Pantheon 的 agent 注册系统已经完整实现并 E2E verified。问题不是"没有系统"，而是"PM 体系没有用它"。**

## Decision

### 目标架构

```
Portfolio Master (devin CLI, tmux)
  ↓ tmux send-keys / SSH RPC
Pantheon daemon (pantheond, systemd service)
  ↓ run.submit RPC
  ├── Run A: implementer
  │   ↓ WorkspaceManager.PrepareWorktree (独立 git worktree)
  │   ↓ DevinAdapter.Start (spawn devin CLI, 独立 OS 进程)
  │   ↓ Store.RegisterAgent (SQLite + event journal)
  │   └─ devin CLI (PID 可追踪, 独立上下文)
  │
  └── Run B: verifier
      ↓ WorkspaceManager.PrepareWorktree (独立 git worktree)
      ↓ DevinAdapter.Start (spawn devin CLI, 独立 OS 进程)
      ↓ Store.RegisterAgent (SQLite + event journal)
      └─ devin CLI (PID 可追踪, 独立上下文)
```

### 实施步骤

1. **Pantheon daemon 部署为 systemd service**（决策3）
   - `pantheond` binary 安装到 `~/.local/bin/`
   - systemd user service: `~/.config/systemd/user/pantheond.service`
   - 长驻运行，stdin/stdout JSON-RPC over Unix socket（不是 per-request SSH）
   - 改动：当前 pantheond 是 per-request（SSH spawn），需要改为长驻 + Unix socket

2. **PM 调用 Pantheon daemon 而非内部 run_subagent**
   - PM 需要起 implementer 时：调用 `pantheon run.submit` RPC
   - Pantheon daemon spawn 独立 devin CLI 进程
   - PM 通过 `pantheon run.status` 查询 implementer 状态
   - PM 不再使用 Devin 内部 `run_subagent`

3. **Verifier 作为独立 Run**
   - PM 起 verifier 时：调用 `pantheon run.submit` RPC (role=verifier)
   - Verifier 在独立 worktree 中运行，独立 devin CLI 进程
   - Verifier 完成后 commit verification artifact 到独立 branch
   - PM 通过 `pantheon run.status` 查询 verifier 状态

4. **Agent 注册 + 发现**
   - 每个 implementer/verifier 在 SQLite 注册: agent_id, run_id, pid, state
   - Portfolio Master 可以 `pantheon run.list` 查看所有 agent
   - Reconciler 自动检测 dead agent (PID liveness)

## 阻塞点 / 设计问题 / 效率问题

### 🔴 阻塞点 1: pantheond 当前是 per-request 模型，不是长驻 daemon

**问题**: pantheond 设计为 `ssh omarchy pantheond` per-request — 每次 RPC 调用 spawn 一个新 daemon进程，处理完就退出。这适合 Mac CLI 远程调用，但不适合 PM 本地频繁调用。

**解决**: 改为长驻 systemd service + Unix socket
- pantheond 启动时监听 Unix socket (`~/.local/share/pantheon/pantheond.sock`)
- PM 通过 `curl --unix-socket` 或 Go client 调用
- 改动量：中等 — 需要加 socket listener + connection handling

**影响**: Pantheon Phase 2 scope 需扩展（当前 Phase 2 是 multi-worker + verifier + reconciler + wake-loop，需要加 "daemon 长驻模式"）

### 🔴 阻塞点 2: devin CLI 不支持 `--prompt` 参数

**问题**: Pantheon 的 `DevinAdapter.buildArgs` 使用 `--prompt` 参数，但实际 devin CLI 用的是 `--prompt-file`（从 takeover prompt 看到）。

**解决**: 修改 DevinAdapter，写 prompt 到临时文件，用 `--prompt-file` 传参
- 改动量：小 — 修改 `buildArgs` + 加临时文件管理

### 🟡 设计问题 1: devin CLI 进程没有 JSON-RPC 输出接口

**问题**: Pantheon 的 agent 注册系统追踪 PID + exit code，但无法追踪 agent 的工作进展。devin CLI 是交互式的，不是 JSON-RPC server — 它的输出是人类可读的，不是结构化的。

**影响**: PM 无法通过 Pantheon 查询 "implementer 做到哪一步了" — 只能知道 "running" 或 "exited"。

**解决（短期）**: PM 通过 git commit 追踪进展（implementer 的 worktree 有 commit history）
**解决（长期）**: devin CLI 加 `--json-output` 模式，输出结构化事件到 stdout，Pantheon daemon 解析

### 🟡 设计问题 2: Verifier 如何获取 implementer 的产出

**问题**: Verifier 需要审查 implementer 的 diff + 跑测试。当前 Pantheon 的 checkpoint 机制是 `run.pause` → git ref，但 verifier 需要的是 implementer 完成后的完整 commit history。

**解决**: 
- Implementer 完成后 `run.pause` → 产生 checkpoint (git ref)
- Verifier 的 worktree 从 implementer 的 checkpoint commit 创建
- Verifier 跑测试 + review diff + commit verification artifact
- 这已经在 Pantheon 的 `run.takeover` 中实现（从 candidate commit 创建 worktree）

### 🟡 设计问题 3: PM 如何知道 implementer/verifier 完成

**问题**: PM 调用 `pantheon run.submit` 后，devin CLI 异步运行。PM 如何知道它完成了？

**解决**:
- 短期: PM 轮询 `pantheon run.status`（每 60s）
- 中期: Pantheon daemon 发 notification（`run.completed` event）通过 tmux send-keys 通知 PM
- 长期: wake-loop 集成 Pantheon event journal，自动通知

### 🟢 效率问题 1: 独立 devin CLI 进程启动开销

**问题**: 每个 subagent 是独立 devin CLI 进程 — 启动需要几秒（npm 加载 + model 连接）。Devin 内部 run_subagent 可能更快（共享进程）。

**影响**: 轻微 — 启动开销 ~5s，相比任务执行时间可忽略

### 🟢 效率问题 2: 独立 worktree 的 git 操作开销

**问题**: 每个 subagent 有独立 git worktree — `git worktree add` 需要几秒。

**影响**: 轻微 — 一次性开销

### 🟢 效率问题 3: PM context 消耗

**问题**: PM 不再自己做实现，而是 delegate 给独立 devin CLI。PM 的 context 消耗降低（不需要读代码/写代码），但需要管理多个 run 的状态。

**影响**: 正面 — PM context 消耗降低，可以管理更多并发任务

## 实施优先级

1. **P0: 修复 DevinAdapter --prompt-file**（阻塞点 2，小改动）
2. **P0: pantheond 长驻模式 + Unix socket**（阻塞点 1，中等改动）
3. **P1: pantheond systemd service 部署**（决策3）
4. **P1: PM 调用 pantheon run.submit 起 implementer**（核心流程）
5. **P2: Verifier 作为独立 Run**（设计问题 2）
6. **P2: Pantheon notification → PM**（设计问题 3）

## 与 Phase 2 scope 的关系

当前 Phase 2 scope (ADR-0014):
1. Multi-Worker (Priority 1) ✅ Slice 1 complete
2. Verifier (Priority 2)
3. Reconciler automation (Priority 3)
4. Wake-loop integration (Priority 4)

**扩展**: 
- 新增 Priority 1.5: **Daemon 长驻模式 + Unix socket**（阻塞点 1）
- Verifier (Priority 2) 升级为 **独立 devin CLI Run**（不是 PM 内部 subagent）
- 这与 ADR-0014 的 Verifier 一致，但实现方式不同

## Consequences

**正面:**
- Verifier 真正独立（独立进程 + 独立 worktree + PID 可追踪 + SQLite 注册）
- Portfolio Master 可审计（`pantheon run.list` 查看所有 agent）
- PM context 消耗降低（delegate 而非自己做）
- 符合 ADR-0006（tmux 非状态源，PID + SQLite 是状态源）

**负面:**
- pantheond 需要改为长驻模式（中等开发量）
- subagent 启动开销略增（~5s）
- PM 需要学习 Pantheon RPC 接口（而非用 Devin 内部 run_subagent）

**待验证:**
- pantheond Unix socket 的稳定性
- 多个并发 devin CLI 进程的资源消耗
- PM 从 run_subagent 迁移到 Pantheon RPC 的工作量
