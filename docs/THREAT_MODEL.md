# Pantheon 威胁模型

> 状态：活文档。信任边界或缓解措施变更时更新。

本文档描述 Pantheon 部署中的信任边界、考虑的威胁、已实施的缓解措施和残余风险。对局限性诚实：Pantheon 是单用户、本地优先的控制平面，不是加固的多租户服务。

## 1. 系统描述

Pantheon 是 Olympus agent 工作区系统的 Conductor 控制平面。以用户级守护进程（`pantheond`）运行，后端 SQLite 存储，通过 Unix socket 暴露 JSON-RPC API，由 CLI（`pantheon`）驱动。可选推送服务器通过第二个 Unix socket 流式推送消息总线通知。无本地 socket 时 SSH 是兜底传输。

## 2. 信任边界

| 边界 | 机制 | 谁可跨越 |
|------|------|---------|
| 本地 RPC socket | Unix socket `${PANTHEON_HOME}/pantheond.sock` | 同一 OS 用户的任何进程 |
| 推送 socket | Unix socket `${PANTHEON_HOME}/pantheond-push.sock` | 同一 OS 用户的任何进程 |
| SSH 传输 | 每请求 `ssh <host> pantheond`，用系统 `ssh` 配置和 `ProxyJump` | 用户（通过其 SSH agent/密钥） |
| SQLite 存储 | 文件 `${PANTHEON_HOME}/pantheon.db` | 同一 OS 用户的任何进程 |
| Worktree 文件系统 | `${PANTHEON_HOME}/worktrees/<task_id>` | 运行时适配器（Devin/Claude/Codex）和用户 |
| 可选集成 | Beacon（CLI）、Hydra（HTTP）、Argus（浏览器）、Auditor（进程内） | 同一用户；集成可选，默认禁用 |

Pantheon 假设单一可信 OS 用户。无每用户认证、无 TLS、无网络监听。能连到 socket 或读数据库文件的，按定义就是可信用户。

## 3. 考虑的威胁

### T1. 未授权 RPC 访问

同主机上非特权进程连到 RPC socket 并下发命令（创建运行、阻塞 agent、标记运行已验证）。

### T2. SQLite 注入

恶意输入（项目名、运行目标、消息体）通过字符串拼接进入 SQL 语句并篡改查询。

### T3. Worktree 路径穿越

构造的 task ID 或仓库路径逃逸 worktree 基目录，写到 `${PANTHEON_HOME}/worktrees` 之外。

### T4. SSH 密钥泄露

Pantheon 不当处理 SSH 凭据、存储密钥或维护可能泄露它们的长连接。

### T5. 推送 socket 窃听

进程读取本给其他消费者的消息总线通知，获知运行目标、指令或 agent 状态。

### T6. 事件/日志/artifact 中的密钥

密钥（API key、token、密码）被写入只追加事件日志、SQLite 投影或检查点提交，后续暴露。

### T7. 脏 base 仓库静默 fork

Pantheon 从脏 base commit fork worktree，静默将用户未提交改动带入 agent worktree。

### T8. agent 自报告被当作事实

Worker 报告 `completed` 而 Pantheon 无独立 verifier 即标记任务完成。

## 4. 缓解措施

| 威胁 | 缓解 |
|------|------|
| T1 | socket 目录以 mode `0700` 创建（`cmd/pantheond/main.go` 中 `os.MkdirAll(dir, 0o700)`）。Unix socket 权限继承目录，仅属主用户可连接。不开网络监听。 |
| T2 | 所有 SQL 语句用参数化查询（`?` 占位符）经 `database/sql`。见 `internal/store/crud.go` 和迁移文件。无用户输入拼入 SQL。 |
| T3 | worktree 路径从 crypto-random task ID 派生，绝不来自用户输入（`internal/workspace/manager.go` 中 `filepath.Join(m.baseDir, taskID)`）。基目录守护进程启动时固定。脏 base 仓库返回 `SNAPSHOT_REQUIRED` 而非静默 fork。 |
| T4 | SSH 传输 shell out 到系统 `ssh` 二进制，依赖用户 `~/.ssh/config` 和 `ProxyJump`。Pantheon 绝不存储、读取或转发 SSH 密钥，绝不持有长连接 — 每请求 spawn 新 `ssh` 进程。 |
| T5 | 推送 socket 与 RPC socket 同在 `0700` 目录，仅属主用户可连接。通知不含密钥 — 见 T6。 |
| T6 | 设计原则"事件、日志、artifact 中不放密钥"由惯例和审查强制（见 `AGENTS.md` 安全章节）。事件载荷有界。检查点提交到私有 agent 远程，非公开。 |
| T7 | `PrepareWorktree` 运行 `git status --porcelain`，base 仓库脏时返回 `SNAPSHOT_REQUIRED`。Pantheon 绝不自动提交用户工作树。 |
| T8 | `completed` 只由 `run.verify` 带 verifier `PASS` 判定设置。Worker 的 `agent.complete` 设 agent 为 `exited`，绝不设 run 为 `completed`。见 `internal/rpc/service.go`。 |

## 5. 残余风险

这些是单用户、本地优先控制平面可接受的已知限制，多用户或网络部署前需解决。

- **本地 RPC 无认证。** 同一用户的任何进程可下发任何 RPC。可接受因为 OS 用户是信任主体，但被攻破的进程有 Pantheon 完全控制权。
- **socket 无 TLS。** Unix socket 仅本地、由目录权限保护，但无完整性或机密性层。`/tmp` 语义宽松或 `XDG_DATA_HOME` 配错会削弱此保护。
- **单用户系统。** 无多 Pantheon 用户或每运行授权概念。所有运行属于同一 OS 用户。
- **SQLite 文件未加密。** 数据库文件属主用户（和 root）可读。无静态加密。
- **可选集成增加攻击面。** Beacon shell out 到 PATH 上的二进制；Hydra 发起出站 HTTP。默认禁用，仅显式标志（`-beacon`、`-hydra-url`）启用。被攻破的 Beacon 二进制或恶意 Hydra 端点可向 Pantheon 投毒，但无法超越用户权限。
- **SSH 兜底信任远程主机。** 用 `-host` 时 Pantheon 信任远程 `pantheond` 正确行为。SSH 配置（含 `ProxyJump`）是用户责任。
- **无速率限制。** RPC socket 无每客户端速率限制。行为异常的本地进程可淹没守护进程。唤醒循环和扫描器有界，但 RPC 面无界。

## 6. 范围外

以下明确不在本威胁模型覆盖范围，后续阶段处理：

- 多租户隔离（非 Pantheon 目标；见 `AGENTS.md`）
- 网络监听（Iris / 远程入口有独立安全审查）
- 生产部署加固
- Go 工具链或第三方模块的供应链完整性
