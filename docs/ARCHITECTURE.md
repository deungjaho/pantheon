# Pantheon 架构说明

## 系统概览

Pantheon 是 Olympus agent 工作区系统的 Conductor 控制平面。它以用户级守护进程（`pantheond`）运行，后端为 SQLite 存储，通过 Unix socket 暴露 JSON-RPC API，由 CLI（`pantheon`）驱动。可选的推送服务器通过第二个 Unix socket 流式推送消息总线通知。无本地 socket 时 SSH 是兜底传输。

## 核心组件

### 守护进程（pantheond）

- 监听 Unix socket 接受 JSON-RPC 2.0 请求
- 可选 stdin/stdout 模式用于 SSH 逐请求调用
- 事件驱动唤醒循环处理续接和孤儿运行
- agent 存活扫描器 10s 轮询检测退出进程
- 可选推送服务器实时流式推送消息通知

### CLI 客户端（pantheon）

- 语义子命令：`project`、`run`、`agent`、`message`、`doctor`、`wake-poll`
- 本地 socket 优先，SSH 兜底
- 直接 RPC 模式（调试后门）

### 存储（SQLite）

- 只追加事件日志：所有状态变更记录为事件
- 注册表投影：当前状态物化视图，与事件同一事务写入
- 迁移：v1 至 v12，幂等
- 无 CGO 依赖（modernc.org/sqlite）

## 数据模型

### 领域对象

| 对象 | 说明 |
|------|------|
| Project | 注册的 git 仓库，含名称、路径、base ref |
| Workspace | 项目下的工作区配置 |
| Run | 一次任务执行实例，对应一个 Controller |
| Task | Run 的工作单元，对应一个独立 worktree |
| Agent | 运行中的 agent 进程（Worker/Verifier 角色） |
| Event | 只追加日志条目，记录状态变更 |
| Artifact | 运行产物引用 |
| Candidate | 检查点候选提交 |
| Message | 消息总线信封（v1.1 类型化） |
| Continuation | 续接记录（孤儿运行 → 新运行） |
| Finding | 审计器发现（建议/记忆候选/策略提案/风险发现） |

### Run 状态机（V2）

```
requested → planning → ready → running → verifying → completed
                                      ↓           ↓
                                   blocked      failed
                                      ↓           ↓
                                   running     canceled
                                                  ↓
                                              blocked
```

- `requested`：已创建，未规划
- `planning`：规划中
- `ready`：就绪，可启动
- `running`：运行中
- `verifying`：验证中
- `blocked`：已阻塞（可恢复）
- `completed`：已完成（验收通过）
- `failed`：已失败
- `canceled`：已取消

### 风险分级验证

| 风险级别 | 验证 PASS 行为 |
|---------|---------------|
| R0 | 自动接受 → completed |
| R1 | 自动接受 → completed |
| R2 | 进入 verifying，需 `run.approve` 人工批准 |
| R3 | 进入 verifying，需 `run.approve` 人工批准 |

### 预算强制

- 默认预算 8h
- 扫描器每轮检查运行中 run 的已用时间
- 超预算 → `failed`，`result_state=budget_exceeded`

### 同根因熔断器

- 续接链中追踪根因标签
- 同一根因出现 3 次 → `blocked`，通知 PM
- 不替代进度门控（独立机制）

## 传输层

### 本地 Unix socket（优先）

- 默认路径：`$PANTHEON_HOME/pantheond.sock`
- 目录权限 0700
- 无网络监听

### SSH stdio（兜底）

- 每请求 `ssh <host> pantheond` 进程
- 使用系统 `ssh` 配置和 `ProxyJump`
- 不存储 SSH 密钥、不维护长连接

## 消息总线

### 拉取式（事实源）

- `message.publish.envelope`：发布类型化消息信封
- `messages.by_run`：按 run ID 和 cursor 拉取消息
- `message.ack`/`message.nack`：确认/拒绝
- `messages.deadline_check`：截止检查
- `messages.status`：投递状态查询

### 推送式（方案 B，可选）

- Unix socket 推送服务器
- 订阅者发送 JSON 订阅请求（指定 run ID 或空=全部）
- 消息发布时触发推送通知
- 每订阅者有界缓冲（64 条）
- 溢出时记录日志，可通过 cursor 恢复
- 断线依赖 cursor 兜底

## 可选集成

所有集成默认禁用，通过 `pantheond` 标志启用。禁用时返回明确的降级错误。

| 集成 | 标志 | 说明 |
|------|------|------|
| Beacon | `-beacon` | shell out 到 `beacon agents --json` 发现活跃 agent 会话 |
| Hydra | `-hydra-url` | HTTP 客户端查询 Hydra `/v1/models` 和 `/healthz` |
| Argus | — | workspace 级浏览器能力，尚未集成 |
| Auditor | `-auditor` | 全局审计器分析运行历史，产出结构化发现 |
| Mnemos | — | 语义记忆，尚未集成；记忆候选暂存本地 |
| Push | `-push-socket` | 消息总线推送服务器 |

## 运行时适配器

`RuntimeAdapter` 接口在消费端定义，支持：

- **Devin**（默认）：spawn `devin` CLI
- **Claude**：spawn `claude` CLI
- **Codex**：spawn `codex` CLI

所有适配器共享生命周期：
- 启动外部 CLI 进程
- 创建 PID/exit/prompt/log 文件
- 支持进程组
- 检查存活
- SIGTERM 停止，宽限期后 SIGKILL

## 检查点管理

- 不可变 ref（`refs/pantheon/checkpoint/<run_id>`）
- 候选提交（agent 工作成果）
- 轻量交接（takeover 准备）

## 唤醒循环

- 事件驱动（非轮询）：SQLite 事件触发唤醒
- 唤醒处理器分发到协调器
- 协调器查询存储当前状态（待续接、孤儿运行）并行动
- systemd timer 60s 兜底轮询
- `pantheon wake-poll` CLI 手动触发

## 全局审计器

- 分析运行历史：失败模式、预算超支、熔断频次、验证失败、stale run
- 产出四种发现：`recommendation`、`memory_candidate`、`policy_proposal`、`risk_finding`
- 所有发现初始 `pending`，需人工 `auditor.review` 接受/拒绝
- 审计器绝不自动修改任何东西
- 幂等：同 type+title 的 pending 发现不重复创建

## 通知

- TmuxNotifier：通过 tmux send-keys 投递
- FileInboxProjector：投影到 inbox/outbox markdown 文件
- 最佳努力：错误不阻塞发布
