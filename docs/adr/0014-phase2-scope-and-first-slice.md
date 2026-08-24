# ADR-0014: Phase 2 范围 — 优先级与首个垂直切片

- **Status:** Accepted
- **Decided:** 2026-07-29
- **Accepted:** 2026-07-29（D-pantheon-013，用户批准全部范围）
- **Decision maker:** pantheon-remote-pm（omarchy，GLM-5.2 High）
- **Supersedes:** 无
- **Superseded by:** 无
- **Depends on:** ADR-0003（仅 Devin adapter）、ADR-0004（SQLite events）、ADR-0006（tmux 非权威）、ADR-0013（wake-loop 调度器）、ROADMAP Phase 2
- **Authority:** D-pantheon-012（Phase 1 EXIT CONFIRMED → Phase 2 规划）

---

## Context

Phase 1 已完成（7/7 退出条件验证通过，HEAD `0d23a65`，10 个 commit）。
D-pantheon-012 指示 PM 评估 Phase 2 的 6 个候选方向，提出优先级排序，定义首个垂直切片，并编写本 ADR。**在用户批准范围前不开始实现。**

### 当前状态（Phase 1 基线）

| 组件 | 状态 | 覆盖率 |
|-----------|--------|----------|
| domain（类型、状态机、ID） | verified | 58.2% |
| store（SQLite event journal + projection） | verified | 53.7% |
| rpc（JSON-RPC 2.0 server + service） | verified | 65.6% |
| workspace（manager + containment check） | verified | 47.1% |
| runtime（Devin adapter） | verified | 77.7% |
| checkpoint（manager + GitPusher） | verified | 88.1% |
| cmd/pantheond（daemon 入口） | verified | 0.0%（集成测试） |
| cmd/pantheon（Mac CLI） | verified | 0.0%（mock SSH 测试） |

Phase 2 必须解决的 Phase 1 关键限制：
- **仅单 Worker**：`run.submit` 只 spawn 一个 Devin agent，无并发。
- **无 Verifier**：`candidate_ready` 是终态（无 `accepted`/`rejected`）。
- **无 Concierge/Controller**：用户直接调用 `run.submit`，无编排层。
- **无自动 reconcile**：`reconcile` 是显式 RPC 调用，非周期性。
- **无预算强制**：`BUDGET_EXCEEDED` 不自动检查。
- **无熔断**：同根因 3 次自动停止未实现。
- **每请求 daemon**：pantheond 每次 SSH 调用 spawn，非长驻。

### 6 个候选方向（来自 D-pantheon-012）

1. **多 Worker 支持** — 从 1 Worker 扩展到 N Worker（并发任务执行）
2. **Reconciler 自动化** — 周期性 reconciler daemon（替代每请求 reconcile）
3. **D0019 wake-loop 集成** — 将过渡 wake-loop.sh 升级为 Pantheon daemon reconciler（ADR-0013 目标）
4. **Runtime adapter 扩展** — 在 Devin 之外支持 Claude Code / Codex（ADR-0003 接口保持可替换）
5. **Iris 集成** — Iris 作为 Pantheon 远程入口（ADR-0011 可选交互面）
6. **生产部署** — Pantheon daemon 作为 omarchy 上的 systemd 服务

---

## REFERENCE_REVIEW（§13 证据门）

| 字段 | 内容 |
|---|---|
| **具体问题** | Phase 1 交付了单 Worker 垂直切片。ROADMAP Phase 2 目标是"受限多 Agent: 2 Worker + 1 Verifier + 风险分级验证"。但提出了 6 个候选方向，其中一些按 ROADMAP 是 Phase 3+ 项。需要确定哪些候选属于 Phase 2、以什么顺序、首个垂直切片应是什么。`[fact]` |
| **真实先例** | 1. **Kubernetes 分阶段推出** — 核心 API（Pod/Service）先于 controller（Deployment/ReplicaSet）先于 autoscaling。每阶段在稳定原语上添加编排。主要来源：k8s sig-architecture roadmap。2. **GitHub Actions** — 单 runner 先于 self-hosted runner 先于 matrix/parallel。主要来源：actions/runner repo history。3. **Temporal** — 单 worker 先于 activity retry 先于 child workflow。主要来源：temporal-sdk-go docs。`[fact]` |
| **采纳的想法** | 遵循 K8s 模式：稳定原语（Phase 1 完成），添加编排（多 Worker + Verifier），再添加自动化（reconciler），再添加集成（Iris、其他 runtime），最后生产。不跳过编排直接做自动化——没有多 Worker 的自动化价值低。`[inference]` |
| **明确拒绝的想法** | 1. **Runtime adapter 扩展作为 Phase 2** — ADR-0003 说"第二个 adapter 稳定接口"；目前不需要第二个 adapter，在多 Worker 之前加一个是过早抽象。2. **Iris 作为 Phase 2** — ROADMAP 说 Phase 3+；Iris 是远程入口点，不是核心编排；在多 Worker + Verifier 之前加它是风险无价值。3. **生产部署作为 Phase 2** — ROADMAP 说 Phase 5+；将单 Worker 系统部署到 systemd 增加运维负担无功能收益。`[inference]` |
| **为什么本地约束不同** | Pantheon 是单主机（omarchy），SQLite 而非 etcd，PM 是写文件的 agent。ROADMAP 已定义 Phase 2 为"2 Worker + 1 Verifier"——6 个候选包含按 ROADMAP 是 Phase 3+ 的项。PM 应将 Phase 2 范围与 ROADMAP 对齐，不扩大。`[fact]` |
| **最小可证伪 spike** | 首个垂直切片："2 个 Worker 在独立任务上并发运行，各自产生 checkpoint，两者状态均可查询"。这验证 store + workspace + runtime 层能处理并发无竞态。如果可行，添加 Verifier 是自然扩展。`[proposal]` |

---

## 6 个候选方向评估

### 1. 多 Worker 支持

| 维度 | 评估 |
|-----------|------------|
| **ROADMAP 对齐** | 直接匹配 Phase 2："2 Worker + 1 Verifier" |
| **依赖** | Phase 1 store + workspace + runtime（均已验证）。需要：并发 worktree 分配、按 run agent 隔离、store 并发（已用 -race 测试）。 |
| **风险** | 中。Store 已通过 -race。主要风险：worktree 路径冲突、多 PID 的 devin 进程管理、SSH session 隔离。 |
| **工作量** | 中。基础设施已存在（WorkspaceMgr 准备独立 worktree，store 处理多个 run）。主要工作：移除 service.go 中的单 Worker 假设，添加并发 run.submit 支持，测试 2 个并发 run。 |
| **价值** | 高。解锁并行任务执行——Phase 2 核心价值主张。没有它，Pantheon 是单任务队列。 |
| **结论** | **Phase 2，优先级 1** |

### 2. Reconciler 自动化

| 维度 | 评估 |
|-----------|------------|
| **ROADMAP 对齐** | 隐含在 Phase 2 中（"Kill Worker/tmux/restart → reconcile"是 Phase 1 条件 5，但自动 reconcile 是 Phase 2） |
| **依赖** | 长驻 daemon（非每请求）。当前 pantheond 每次 SSH 调用 spawn。周期性 reconciler 需要长驻进程。 |
| **风险** | 中。需要从每请求到长驻 daemon 的架构变更。这是重大转变。 |
| **工作量** | 中高。需要 (a) 将 pantheond 改为长驻 + reconcile ticker，或 (b) 添加独立 reconciler 进程。选项 (a) 与每请求 SSH 模型冲突。 |
| **价值** | 中。自动 reconcile 能在无用户干预下捕获崩溃 worker。但单 Worker 时用户可直接调用 `reconcile`。价值随多 Worker 增加。 |
| **结论** | **Phase 2，优先级 3**（在多 Worker + Verifier 之后，自动 reconcile 价值更大时） |

### 3. D0019 wake-loop 集成

| 维度 | 评估 |
|-----------|------------|
| **ROADMAP 对齐** | ADR-0013 目标架构是"Phase 1 之后"。过渡 wake-loop.sh 已在 Pantheon（控制仓库）中运行。 |
| **依赖** | 需要长驻 daemon（同 #2）。还需要 SQLite event journal 作为输入（已存在）。 |
| **风险** | 中。wake-loop.sh 已作为过渡运行。将其升级为读取 SQLite events 而非文件 mtime 是增量改动。 |
| **工作量** | 低中。过渡脚本已存在。升级路径：用 SQLite event 查询替代文件 mtime 检查，添加 lease/heartbeat。 |
| **价值** | 中。降低 Portfolio Master 轮询开销。但这是 Portfolio Master 关注点，不是 Pantheon 用户关注点。 |
| **结论** | **Phase 2，优先级 4**（可与 #2 并行，但不阻塞 Phase 2 退出） |

### 4. Runtime adapter 扩展（Claude Code / Codex）

| 维度 | 评估 |
|-----------|------------|
| **ROADMAP 对齐** | 不在 Phase 2。ADR-0003："第二个 adapter 稳定接口"。不需要第二个 adapter。 |
| **依赖** | 第二个 runtime 必须存在并测试。Claude Code 和 Codex 目前在 omarchy 上不可用。 |
| **风险** | 低（不更改现有代码）。但按 ADR-0003 有过早抽象风险。 |
| **工作量** | 每个 adapter 中等。每个 adapter 需要进程管理、session 处理、prompt 作用域。 |
| **价值** | 对 Phase 2 低。Devin 是主要 runtime。在多 Worker + Verifier 稳定前加第二个 runtime 是过早的。 |
| **结论** | **Phase 3+，不是 Phase 2** |

### 5. Iris 集成

| 维度 | 评估 |
|-----------|------------|
| **ROADMAP 对齐** | ROADMAP 说 Phase 3+。ADR-0011 将 Iris 定位为可选交互面。 |
| **依赖** | Iris 项目本身、WeChat plugin、远程入口安全模型。 |
| **风险** | 高。远程入口是最高风险面（ROADMAP："安全闭环优先"）。 |
| **工作量** | 高。需要安全模型、认证、隧道、消息映射。 |
| **价值** | 对 Phase 2 低。Iris 是便利层，不是核心编排。 |
| **结论** | **Phase 3+，不是 Phase 2** |

### 6. 生产部署（systemd）

| 维度 | 评估 |
|-----------|------------|
| **ROADMAP 对齐** | ROADMAP 说 Phase 5+。 |
| **依赖** | 长驻 daemon（#2）、稳定多 Worker、稳定 Verifier、监控。 |
| **风险** | 中。systemd unit 简单，但部署不稳定系统是运维负担。 |
| **工作量** | unit 文件工作量低，但需要系统先达到生产就绪。 |
| **价值** | 对 Phase 2 低。每请求 daemon 对 Phase 1-2 足够。 |
| **结论** | **Phase 5+，不是 Phase 2** |

---

## Decision

### Phase 2 范围（与 ROADMAP 对齐）

**在范围内：**
1. **多 Worker 支持**（优先级 1）— 2 个并发 Worker 处理独立任务
2. **Verifier**（优先级 2）— 独立 Verifier agent，candidate_ready → verifying → accepted/rejected
3. **Reconciler 自动化**（优先级 3）— 周期性 reconcile 处理崩溃/stale worker
4. **Wake-loop 集成**（优先级 4）— 将 wake-loop.sh 升级为 SQLite 事件驱动（ADR-0013）

**不在范围内（Phase 3+）：**
5. Runtime adapter 扩展（Phase 3+，依据 ADR-0003）
6. Iris 集成（Phase 3+，依据 ROADMAP + ADR-0011）
7. 生产部署（Phase 5+，依据 ROADMAP）

### Phase 2 优先级排序

```
优先级 1: 多 Worker（2 个并发 Worker）
    ↓
优先级 2: Verifier（candidate_ready → verifying → accepted/rejected）
    ↓
优先级 3: Reconciler 自动化（周期性，长驻）
    ↓
优先级 4: Wake-loop 集成（SQLite 事件驱动，ADR-0013）
```

理由：多 Worker 是基础——没有并发，Verifier 和 Reconciler 价值较低。Verifier 在多 Worker 之上添加质量控制。Reconciler 在两者之上添加自动化。Wake-loop 是 Portfolio Master 关注点，可最后做。

### 首个垂直切片

**切片："2 个并发 Worker，各自产生 checkpoint，均可查询"**

范围：
1. 移除 `service.go` 中的单 Worker 假设 — `run.submit` 已创建独立 run/task/worktree。主要变更是确保并发 `run.submit` 调用不冲突（store 已用事务 + -race 测试处理）。
2. 添加 `run.list` RPC 方法 — 返回所有 run 及其当前状态。用于监控多个并发 run。
3. 测试：并发提交 2 个 run，验证两者都到达 `running` 状态，都在 `run.pause` 时产生 checkpoint，都可通过 `run.status` 和 `run.list` 查询。
4. 测试：验证无 worktree 路径冲突（WorkspaceMgr 已使用 task_id 路径，但需用 2 个并发任务验证）。

**首个切片不包含：**
- Verifier（优先级 2，下一个切片）
- Concierge/Controller 编排（后续）
- TaskSpec 契约（后续）
- 风险分级验证（后续）
- 熔断（后续）
- 预算强制（后续）

**首个切片退出标准：**
1. `run.list` 返回所有 run 及正确状态
2. 2 个并发 `run.submit` 调用都成功
3. 两个 run 都到达 `running` 状态并有独立 devin 进程
4. 每个 `run.pause` 产生独立 checkpoint
5. 每个 `run.status` 返回正确的独立状态
6. 无竞态条件（go test -race 通过）
7. 无 worktree 路径冲突

### Phase 2 完整退出标准（来自 ROADMAP，供参考）

1. Verifier 能捕获故意注入的错误实现
2. Worker 不能自我批准或完成任务
3. 两个独立任务可安全并行运行
4. 冲突进入 Integrator / changes_requested
5. stop/resume 不丢失 task/artifact
6. 同根因 3 次熔断激活
7. Run 超过 8h 自动停止

---

## Consequences

**正面：**
- Phase 2 范围与 ROADMAP 紧密对齐，无范围蔓延。
- 首个切片小且可证伪——在不添加 Verifier/Reconciler 复杂度的情况下验证并发。
- 明确的优先级排序防止在编排稳定前过早做自动化/集成。
- 候选 4/5/6 明确推迟并附 ADR 引用，防止歧义。

**负面：**
- Reconciler 自动化（优先级 3）需要从每请求到长驻 daemon 的架构变更。这是重大转变，可能需要自己的 ADR。
- Wake-loop 集成（优先级 4）依赖 Reconciler，形成依赖链。
- Phase 2 无 Iris 意味着用户必须继续使用 SSH CLI 进行所有交互。

**风险：**
- 多 Worker 可能暴露 -race 测试未捕获的 store/workspace 并发 bug（如真实负载下的 SQLite WAL 竞争）。
- Verifier 需要第二个 Devin agent 用不同 prompt——这是 Pantheon 首次为同一任务管理 2 个 agent，可能暴露 prompt 隔离问题。

---

## 用户批准

**ACCEPTED 2026-07-29（D-pantheon-013）。** 用户批准全部 Phase 2 范围：
1. ✅ 范围 = 多 Worker + Verifier + Reconciler + Wake-loop
2. ✅ 优先级 1→2→3→4
3. ✅ 首个垂直切片 = "2 个并发 Worker，各自产生 checkpoint"
4. ✅ 候选 4/5/6 推迟到 Phase 3+

首个垂直切片的实现已开始。
