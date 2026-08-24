# ADR-0013: Wake-loop 事件驱动调度器

- **Status:** Proposed
- **Decided:** 2026-07-29
- **Decision maker:** Portfolio Master (D0019)
- **Supersedes:** 无（手动轮询为过渡方案）
- **Superseded by:** 无

---

## Context

Portfolio Master 当前每 15-20 分钟手动轮询所有 Project Master inbox（D0016，OPERATING_CONTRACT §14）。这不可扩展：

- **浪费周期**：即使没有 PM 提交新证据，空轮询也会消耗 Portfolio Master 的上下文和注意力。
- **延迟**：在 T+1 完成工作的 PM 最多需要等待 19 分钟才能被下一次轮询覆盖。时间敏感的阻塞（熔断、死锁）可能恶化。
- **无 lease/heartbeat**：没有对"谁是当前活跃 Portfolio Master"槽位的正式所有权，没有证明 Master 存活的 heartbeat，没有卡死 Master 的 deadline，也没有从死亡 Master 恢复的 reconciler。
- **tmux 混淆**：tmux session 的存在被当作"Master 在工作"的代理——这正是 ADR-0006 和 COLD_START §6 警告的假活性陷阱。tmux 是运行时容器，不是权威。

D0019 指示：目标是 Pantheon daemon 事件驱动调度器，具备 append-only event journal + lease + heartbeat + deadline + reconciler。tmux 仅作运行时，不作权威。过渡方案：手动轮询 + tmux pane 通知。

本 ADR 定义目标架构和替代纯手动轮询的最小过渡实现，同时不阻塞 camtOS onboarding 或其他 PM 工作。

## REFERENCE_REVIEW（§13 证据门）

| 字段 | 内容 |
|---|---|
| **具体问题** | 每 15-20 分钟手动轮询 N 个 PM inbox 不可扩展：空轮询浪费周期、时间敏感事件最多 19 分钟延迟、Portfolio Master 槽位无正式 lease/heartbeat/deadline/reconciler、tmux session 存在与进度混淆。`[fact]` 本 session 中观察到：4 个 stale-checkpoint 差异被发现，因为 10:48 轮询数据在 12:10 外部探测时已过期 1.5 小时。 |
| **真实先例** | 1. **Kubernetes controller-runtime** — watch + work queue + reconcile loop + leader election lease。主要来源：`k8s.io/client-go/tools/leaderelection` + `sigs.k8s.io/controller-runtime/pkg/manager`（v0.18，2024 年锁定）。Lease = Kubernetes Lease object + renew deadline + acquire timeout。Reconcile 是 level-triggered，不是 edge。2. **systemd** — notify socket（READY=1, WATCHDOG=1, RELOADING=1）+ Restart=on-failure + watchdog timeout。主要来源：`sd_notify(3)` man page + `org.freedesktop.systemd1` D-Bus spec（systemd v256）。Watchdog = 硬件或软件定时器，服务必须在 NotifyAccess 窗口内 ping，否则 unit 被 kill。3. **Temporal durable execution** — event-sourced workflow history + sticky worker + heartbeat + activity timeout。主要来源：Temporal docs "Reliability" + `temporal-sdk-go`（v1.29，2024）。Heartbeat = Activity heartbeat；timeout = ScheduleToClose + StartToClose；恢复 = 从 event log replay history。4. **GitHub Actions runners** — 注册 + lease + job queue + heartbeat + offline 检测。主要来源：`actions/runner` repo（v2.317，2024）— `Runner.Listener` 注册、轮询 job、发送 keep-alive；服务器在错过 keep-alive 后标记 offline。 |
| **生产约束** | K8s：lease renew interval 默认 2s，需要 jitter 防止惊群效应；需要 etcd（我们只有 SQLite）。systemd：需要 systemd 作为 PID 1 + notify socket 管道；我们在 tmux 中运行 Devin agent，不是 systemd unit（camtos-wayback 是 user unit 但 PM 不是）。Temporal：需要 Temporal server 集群（Cassandra/Postgres + matching）；对单主机个人系统来说太重。GitHub Actions：需要中央服务器；我们没有这样的服务器，Pantheon daemon 是最接近的类比。 |
| **采纳的想法** | 1. **Lease + leader election**（来自 K8s）：同一时间只有一个活跃 Portfolio Master；lease 有 renew deadline + acquire timeout；第二个 Master 等待，不重复执行。2. **Watchdog heartbeat**（来自 systemd）：活跃 Master 必须在有界窗口内 heartbeat，否则视为死亡 → 触发继任者。3. **Append-only event journal 作为恢复源**（来自 Temporal）：状态通过 replay 事件重建，不是读取可变 snapshot；崩溃恢复 = 从最后 checkpoint replay。4. **Poll-for-jobs with keep-alive**（来自 GitHub Actions）：PM 轮询轻量级队列（或通过 tmux send-keys 被通知），而不是被轮询；keep-alive 证明 PM 存活。5. **Level-triggered reconcile**（来自 K8s controller-runtime）：reconciler 比较期望状态与观察状态并基于差异行动，不是基于 edge 事件；幂等。 |
| **明确拒绝的想法** | 1. **etcd / Consul 做 lease** — 太重，单主机，SQLite WAL 足够。2. **systemd notify 给 PM** — PM 在 tmux 中作为 Devin 进程运行，不是 systemd unit；包装成 systemd unit 超出 Phase 1 范围。3. **Temporal server** — 需要集群基础设施，违反"无分布式基础设施"的 Phase 1 边界。4. **PM 的 webhook/callback** — PM 是 Devin agent，没有入站 HTTP server；只能写文件 + 通过 tmux send-keys 被通知。5. **PM 的长连接 WebSocket** — 同样约束；PM 不能持有入站连接。6. **永远每 15-20 分钟轮询每个 PM inbox** — 本 ADR 替代的现状。 |
| **为什么本地约束不同** | 单主机（omarchy），SQLite 而非 etcd，PM 是写文件的 agent 没有入站 server，tmux 是唯一通知 PM 的通道，Portfolio Master 是 Devin agent（不是 daemon 进程）所以也不能持有长连接 socket。D0019 中的"daemon"是未来的 Pantheon daemon；过渡方案是 shell 脚本 + Devin Master 本身。 |
| **最小可证伪 spike** | 过渡 wake-loop shell 脚本（本 ADR §Interim）：一个 bash 脚本，每 60s 检查 inbox mtime + git HEAD，写 `state.json` tick 文件，仅在检测到真实变化（新 inbox mtime 或新 git HEAD 或缺失 heartbeat）时通过 `tmux send-keys` 通知 Portfolio Master pane。如果此脚本相比手动 15-20 分钟轮询降低了 Master 轮询开销和延迟，则方案得到验证。终止条件：脚本误报率 >10%，或 Master 仍手动轮询因为脚本漏检变化，或脚本本身成为维护负担。 |
| **成功/终止标准** | **成功**：(1) Master 在 PM inbox 更新后 60s 内被通知；(2) Master 零空轮询上下文消耗；(3) lease/heartbeat 在 1 个 heartbeat 窗口内检测到死亡 Master；(4) 无双 Master 执行。**终止**：(1) 误报率 >10%；(2) 漏检率 >5%；(3) 脚本维护 > 1h/周；(4) PM 无法通过 tmux send-keys 可靠通知。 |
| **操作/测试证据** | `[proposal]` — spike 尚未执行。测试计划：(a) touch inbox/test.md，验证脚本在 60s 内通知；(b) commit 到 stage repo，验证脚本检测到 HEAD 变化；(c) kill Master pane，验证 heartbeat timeout 触发；(d) 启动两个 Master pane，验证只有一个获取 lease。 |

### 来源优先级合规

- 主要来源：K8s controller-runtime 源码 + leaderelection 包、systemd sd_notify(3) man page、Temporal SDK 源码 + 文档、GitHub Actions runner 源码。`[fact]`
- 未使用仅博客或仅 AI 摘要来源作为唯一参考。`[fact]`
- fact/inference/proposal 内联标注。

## Decision

### 目标架构（Pantheon daemon，Phase 1 之后）

1. **Append-only event journal**（已是 ADR-0004 SQLite events）：每个 PM 报告、指令、heartbeat、blocker 和状态转换都是一个 event。状态是 event 的投影，不是可变 blob。
2. **Lease**：同一时间只有一个活跃 Portfolio Master 持有 lease。
   - Lease 记录：`{holder_id, acquired_at, renew_deadline, tick_id}`。
   - Renew deadline = acquired_at + renew_interval（如 60s）。
   - 第二个 Master 看到 lease 被持有则等待；不执行。
   - 如果持有者错过 renew_deadline，lease 视为过期，等待中的 Master（或新继任者）获取它。
3. **Heartbeat**：活跃 Master 每 `heartbeat_interval`（如 30s）写一个 heartbeat event。如果 `heartbeat_timeout`（如 90s = 3 次漏 beat）内无 heartbeat，lease 被撤销并触发继任者。
4. **Deadline**：每个 PM task/run 有 deadline。如果 deadline 前无进度 event，reconciler 标记 run 为 `deadline_exceeded` 并升级处理。
5. **Reconciler**（level-triggered，幂等，来自 K8s controller-runtime）：
   比较期望状态（如"所有 PM 在 30 分钟内已报告"）与观察状态（event journal 投影）。基于差异行动：通知 stale PM、升级阻塞 PM、为死亡 Master 触发继任者。通过 `tick_id` 幂等——相同 tick_id 产生相同行动。
6. **事件驱动唤醒**：Master 仅在以下情况被唤醒：(a) journal 中有新 event，(b) timeout（heartbeat/lease/deadline），(c) blocker。空轮询不唤醒 Master，不调用 LLM。
7. **tmux 仅作运行时**：tmux send-keys 是通知 PM 的通道（PM 没有入站 server）。tmux session 存在不是权威——lease + heartbeat 是权威（ADR-0006）。

### 过渡实现（现在，daemon 之前）

一个最小 shell 脚本，替代手动 15-20 分钟轮询，不需要完整 Pantheon daemon。这是 REFERENCE_REVIEW 中的"最小可证伪 spike"。

**脚本**：Pantheon（控制仓库）中的 `scripts/wake-loop.sh`。

**行为**：
1. 每 `POLL_INTERVAL`（默认 60s）检查：
   - 每个 `inbox/*.md` 的 mtime 与 `state.json` 中存储的上次已知 mtime 对比
   - 每个 stage repo 的 `git rev-parse --short HEAD` 与上次已知 HEAD 对比
2. 如果检测到任何变化：
   - 用新 mtime/HEAD + `last_tick` 时间戳更新 `state.json`
   - `tmux send-keys` 到 `portfolio-successor` pane：一行变化摘要（哪个 inbox、哪个 repo、旧→新 HEAD）
3. 如果无变化：仅更新 `state.json` 中的 `last_tick`（不调用 LLM，不通知）
4. Heartbeat：每次循环迭代写 `state.json.heartbeat`，内容为 `{holder: portfolio-successor, ts: <ISO8601>}`。如果脚本死亡，heartbeat 变 stale，未来的 Master/继任者可以检测到。

**Lease（过渡，轻量）**：
- `state.json.lease` = `{holder: portfolio-successor, acquired_at, renew_deadline}`
- 脚本启动时：如果 lease 存在且未过期 → 退出（另一个 Master 在运行）。如果过期或不存在 → 获取。
- 这防止两个 wake-loop 脚本重复通知。

**安全**：
- 不用 `pkill -f` 配宽泛模式（D0019 禁止）。
- 脚本对 inbox/repos 只读；仅在 Pantheon（控制仓库）中写 `state.json` + `state.json.heartbeat`。
- `tmux send-keys` 目标明确为 `portfolio-successor`，绝不使用通配符。
- 脚本不调用任何 LLM。仅通知。Master（Devin agent）决定是否行动。

**过渡方案不做的事**（推迟到 daemon）：
- 完整 event journal replay（使用文件 mtime + git HEAD 作为代理）
- 按任务 deadline（PM 目前自行管理 deadline）
- 跨多主机正式 leader election（单主机，单脚本）
- 幂等 tick_id reconciliation（Master 目前手动处理去重）

### 迁移路径

1. **现在**：过渡 `wake-loop.sh` + 手动 Master 决策。
2. **Pantheon Phase 1 稳定**：将 wake-loop 接入 Pantheon daemon 作为 reconciler，读取 SQLite event journal 而非文件 mtime。
3. **Pantheon Phase 2**：完整 lease/heartbeat/deadline 支持多 Master + event-sourced 恢复。

## Consequences

**正面：**
- Master 在 PM 进展后 60s 内被通知，而非 15-20 分钟。
- 空轮询消耗零 Master 上下文（脚本处理，不调用 LLM）。
- Lease 防止双 Master 执行。
- Heartbeat 在 `heartbeat_timeout` 内检测到死亡 Master。
- 在构建完整 daemon 前验证事件驱动方案。
- tmux 保持仅运行时；lease + heartbeat 是权威（符合 ADR-0006）。

**负面：**
- 多维护一个脚本（通过 <100 行 bash 缓解，有明确终止标准）。
- `state.json` 是新状态文件（必须在备份/snapshot 路径中，D0017 pending）。
- `tmux send-keys` 通知可能被错过，如果 Master pane 不在 prompt-accepting 状态（缓解：Master 在 resume 时检查 `state.json.last_tick`）。
- 文件 mtime 是"新报告"的代理——PM 可能 touch 文件但无真实进展（缓解：Master 读内容 + 交叉检查 git HEAD）。

**待验证：**
- mtime/HEAD 变化检测的误报率。
- `tmux send-keys` 到达 Devin prompt 的可靠性。
- 脚本被 kill 时的 heartbeat timeout 行为（如系统重启）。
- 60s 轮询间隔是否是正确默认值或应自适应。

## Cross-ref

- D0019（wake-loop 事件驱动调度器指令）
- ADR-0004（SQLite canonical events — 目标 journal）
- ADR-0006（tmux 非权威 — lease/heartbeat 替代 tmux 作为权威）
- OPERATING_CONTRACT §6（D0018 v2 条件交接 — heartbeat 启用自审计触发）
- OPERATING_CONTRACT §14（轮询契约 — 本 ADR 替代手动轮询）
- COLD_START §6（tmux 假活性陷阱 — heartbeat 是解决方案）
- AGENTS.md §2（tmux 是容器，不是事实源）
