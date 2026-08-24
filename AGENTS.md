# Pantheon 开发规范

## Pantheon 是什么

Pantheon 是 Conductor 控制平面：注册工作区、任务、运行和 agent；驱动运行时适配器；维护只追加事件日志加 SQLite 注册表投影作为唯一事实源。

## Pantheon 不是什么

- 不是 agent 运行时（Devin/Claude Code/Codex 是运行时）
- 不是模型网关（Hydra 是）
- 不是浏览器工具（Argus 是）
- 不是记忆服务（Mnemos 是）
- 不是状态/通知渲染器（Beacon 是）
- 不是远程入口（Iris 是）
- 不是 Kubernetes/Temporal/Airflow 式通用工作流引擎

## 设计原则

1. **权威主机是部署决策。** 守护进程和 SQLite 存储可运行在任何主机（omarchy、Mac、其他）。客户端优先本地 Unix socket；SSH 是兜底。不硬编码主机名、用户路径或平台。见 ADR-0001（修订版）和 ADR-0019。
2. **tmux 是容器，不是事实源。** 进程状态从真实 PID/退出码推导，绝不从 tmux pane 文本获取。
3. **一个 Run = 一个干净的 Controller。一个 Task = 一个独立的 worktree/session。** 不静默复用租约、worktree 或会话。
4. **事件只追加；注册表是投影。** 两者在同一 SQLite 事务中写入。日志回答"怎么到这里的"；注册表回答"当前状态是什么"。
5. **agent 自报告是声明，不是事实。** `completed` 只由验收设置，绝不由 Worker 的 `report_result` 设置。
6. **可替换接缝，不是插件 SDK。** RuntimeAdapter、CommandRunner、Pusher、Transport 是消费端定义的小接口。
7. **一切有界。** 事件载荷大小、worktree 数量、清理窗口、SSH 请求大小、spool 容量都有显式限制。
8. **不假设默认分支或公司远程。** Pantheon 绝不假设名为 `origin` 的远程、名为 `main` 的分支或任何公司 git 主机。

## 构建与测试

```bash
gofmt -l .
go vet ./...
go test -count=1 -timeout 120s ./...
go test -race -count=1 -timeout 180s ./...
```

测试必须收集真实退出码。不要通过掩盖非零退出状态的构造管道命令输出。测试中失败的命令必须让测试失败。

## Go 约定

- `internal/` 布局；不用 `pkg/`
- 接口在消费端定义
- 每个 goroutine 有所有者、context 和有界 join
- `%w` 包装错误；`internal/domain` 中定义类型化错误
- DTO 不泄漏数据库行形状
- 表驱动测试、稳定的 golden fixture
- 包边界外不 `panic`；仅在边界 recover
- commit 不加 AI co-author trailer
- 一个 commit 解决一个问题；测试和实现同一 commit

## Git 约定

- commit message：`component: what changed`
- 不用 `feat:`/`fix:`/`chore:` 前缀
- 不加 AI co-author trailer
- 小 commit
- 不明确要求不 push
- 不创建 tag
- 不假设默认分支或公司远程

## 安全

- SSH 传输使用系统 `ssh` 配置和 `ProxyJump`。Pantheon 不维护长连接、不存储 SSH 密钥
- 事件、日志、artifact 中不放密钥
- `request_id` 跨 SSH 边界幂等
- worktree 隔离；用户工作树绝不修改
- 脏 base 仓库返回 `SNAPSHOT_REQUIRED` 而非静默 fork

## 当前边界（未经用户批准不扩展）

- 运行时适配器：Devin、Claude、Codex 已实现。`RuntimeAdapter` 接口可替换；新适配器只需新实现，无需改核心
- 可选集成（默认全部禁用，通过 `pantheond` 标志启用）：
  - **Beacon**（`-beacon`）：通过 `beacon` CLI 发现 agent。禁用时 `agent.discover` 返回 `beacon not configured`
  - **Hydra**（`-hydra-url`）：通过 Hydra HTTP API 模型路由。禁用时 `hydra.models`/`hydra.health` 返回 `hydra not configured`
  - **Argus**：workspace 级浏览器能力 — 尚未集成
  - **Auditor**（`-auditor`）：全局审计器分析运行历史。禁用时 `auditor.*` RPC 返回 `auditor not configured`
  - **Mnemos**（`-mnemos-url`）：语义记忆 auto-ingest。run complete 后异步将 run 摘要 ingest 到 Mnemos。禁用时跳过（run complete 不 ingest）
- 预算强制（默认 8h 自动停止）、同根因熔断器（3 次）、风险分级验证（R0-R3）已实现
- 消息总线推送层（方案 B）已实现；拉取式 cursor RPC 仍是事实源和兜底
- 无 TUI、Web UI 或生产部署
- 无多主机调度
- 实现中遇到的产品决策记录在 ADR 中，不静默扩展
