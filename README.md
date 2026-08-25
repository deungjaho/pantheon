# Pantheon

Agent run 注册与事件追踪系统。Pantheon 注册工作区、任务、运行和 agent；驱动运行时适配器；维护只追加事件日志加 SQLite 注册表投影作为唯一事实源。

## Pantheon 是什么

Pantheon 是 *Conductor* 控制平面：注册工作区、任务、运行和 agent；驱动运行时适配器（Devin、Claude 或 Codex，接口可替换）；记录只追加事件加 SQLite 注册表投影作为唯一事实源。

它**不是** agent 运行时、模型网关、浏览器工具、记忆服务、状态渲染器或远程入口。这些是独立的可组合组件（Devin/Claude/Codex、Hydra、Argus、Mnemos、Beacon、Iris），Pantheon 不替代它们。

## 当前能力

- 两个 Go 二进制：`pantheon` CLI + `pantheond` 守护进程
- 传输层：本地 Unix socket 优先，SSH stdio 兜底（ADR-0019）
- SQLite（modernc.org/sqlite，无 CGO）只追加事件 + 注册表投影，同一事务写入
- 领域对象：Project、Workspace、Run、Task、Agent、Event、Artifact、Candidate、Message、Continuation、Finding
- V2 类型化 RPC 方法：`initialize`、`project.register`、`project.list`、`project.status`、`run.create`、`run.start`、`run.status`、`run.verify`、`run.approve`、`run.block`、`run.unblock`、`run.terminate`、`run.list`、`agent.register`、`agent.heartbeat`、`agent.complete`、`agent.block`、`agent.discover`、`run.supersede`、`run.set_next_action`、`reconcile.crash`
- 消息总线：`message.publish.envelope`、`messages.by_run`、`message.ack`、`message.nack`、`messages.deadline_check`、`messages.status`（ADR-0016）
- 消息总线推送层（方案 B）：Unix socket 推送服务器实时流式推送消息发布通知；未启用时默认 `NoopPusher`
- 续接：`continuation.register`、`continuation.list`、`continuation.fulfill`、`continuation.cancel`、`reconcile.continuations`（ADR-0017）
- 终态一致性：`reconcile.terminal_state`（ADR-0018）
- WorkspaceManager：显式 base commit、脏仓库返回 `SNAPSHOT_REQUIRED`、每任务独立 git worktree、有界清理
- RuntimeAdapter 接口 + Devin / Claude / Codex 适配器；agent 存活扫描器带自动续接和自动验证
- 预算强制：运行超出预算（默认 8h）自动停止，`result_state=budget_exceeded`
- 同根因熔断器：同一根因在运行链中出现 3 次触发熔断 → `blocked` 状态
- 风险分级验证（R0-R3）：`RiskLevel` 驱动验证门控 — R0/R1 验证 PASS 自动接受；R2/R3 需 `run.approve`
- TaskSpec：`AcceptanceCriteria`、`Constraints`、`Deliverables`、`RiskLevel`
- Beacon agent 发现：`agent.discover` 查询 Beacon 获取活跃 agent 会话（可选，`-beacon` 标志）
- Hydra 模型路由：`hydra.models` / `hydra.health` 查询 Hydra LLM 网关（可选，`-hydra-url` 标志）
- 全局审计器：`auditor.audit`、`auditor.findings`、`auditor.review` 产出结构化发现供人工审查（可选，`-auditor` 标志）
- 检查点管理器：不可变 ref、候选提交、轻量交接
- 事件驱动唤醒循环（ADR-0013）
- 通知适配器：TmuxNotifier（tmux send-keys）、FileInboxProjector（inbox/outbox markdown 投影）
- `doctor` 子命令：检查 git/tmux/devin/ssh/disk
- 默认任务预算：8h

## 构建

```bash
go build ./cmd/pantheon
go build ./cmd/pantheond
```

## 测试

```bash
gofmt -l .
go vet ./...
go test -count=1 -timeout 120s ./...
go test -race -count=1 -timeout 180s ./...
```

## 目录结构

```
cmd/pantheon/       CLI 客户端（本地 socket 或 SSH）
cmd/pantheond/      守护进程（JSON-RPC over Unix socket 或 stdio）
internal/domain/    稳定领域类型、状态机、消息信封、发现
internal/store/     SQLite 事件日志 + 注册表投影 + 迁移
internal/rpc/       JSON-RPC 2.0 服务器 + 服务处理器
internal/workspace/ WorkspaceManager（git worktree 生命周期）
internal/runtime/   RuntimeAdapter 接口 + Devin/Claude/Codex 适配器 + 扫描器 + 验证器
internal/checkpoint/不可变 ref + 候选 + 交接
internal/notify/    TmuxNotifier + FileInboxProjector
internal/wake/      事件驱动唤醒循环 + 协调器
internal/beacon/    Beacon agent 发现客户端（shell out 到 beacon CLI）
internal/hydra/     Hydra 模型路由 HTTP 客户端
internal/push/      消息总线推送层（方案 B，Unix socket 推送服务器）
internal/auditor/   全局审计器（运行历史分析 + 发现）
internal/integration/ 端到端集成测试
docs/               架构、ADR、教程、威胁模型
deploy/             安装脚本、manifest、systemd 单元
examples/           示例脚本
```

## 文档

- [架构说明](docs/ARCHITECTURE.md)
- [快速上手教程](docs/TUTORIAL.md)
- [威胁模型](docs/THREAT_MODEL.md)
- [开发规范](AGENTS.md)
- [架构决策记录](docs/adr/)
