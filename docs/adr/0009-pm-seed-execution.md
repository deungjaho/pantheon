# ADR-0009: Project Master 种子执行（proposed）

> **Status**: PROPOSED — 试点草案，尚未强制执行。
> **Date**: 2026-07-29
> **Decision ID**: D0014 (pending)
> **Pilot**: 下一个 Argus P2-B 或 Mnemos Round 2 有界里程碑。

## Context

当前工作流赋予 Project Master 开放式代码执行权限。Worker 收到任务描述后独立探索代码库。这导致探索 token 成本高、质量不稳定、范围漂移，以及多个 Worker 偏离共享理解时产生合并冲突。

### 已观察到的问题

1. **Worker 重新设计而非实现。** 没有具体的代码级契约，Worker 花费 token 重新推导 PM 已经理解的接口。
2. **范围膨胀。** Worker 修复遇到的相邻问题，导致 diff 膨胀和审查负担增加。
3. **接口不一致。** 多个 Worker 为同一领域对象生成不兼容的函数签名。
4. **没有可证伪的检查点。** Worker 可以报告"完成"而无需证明端到端路径可用，因为验收标准从未在代码中具体化。

### 先例（依据 D0013 证据门）

| 先例 | 主要来源 | 锁定版本 | 采纳的想法 | 拒绝的想法 |
|---|---|---|---|---|
| **Cockburn walking skeleton** | Cockburn, "Just Enough" (2004); IEEE Software 2008 | — | 先做薄端到端切片，一个 commit，验证路径可行 | 不是完整实现；不是代码行数指标 |
| **Kubernetes KEP** | [kubernetes/enhancements](https://github.com/kubernetes/enhancements) KEPR-0001 | KEP-0001 (2018)，流程自 2020 年稳定 | 实现前设计门：问题、备选方案、设计、测试计划 | 小任务不需要完整 KEP 流程；机械修复可跳过 |
| **SWE-agent ACI** | Yang et al., "SWE-agent: Agent-Computer Interfaces Enable Software Engineering Language Models" (2024) | arXiv:2405.15793, commit a0f2b3c | 任务特定接口（ACI）影响 agent 性能；受限接口优于开放 shell | 不声称 ACI 普遍最优；本地约束不同（Devin，而非 SWE-agent runtime） |

**为什么本地约束不同**：我们的 agent 是 Devin（GLM-5.2 High），不是 GPT-4。我们的接口是 git worktree + tmux，不是自定义 ACI。但核心洞见——受限、明确的接口能降低 agent 探索成本和错误率——是可迁移的。

## Decision（proposed）

### PM 种子执行协议

对于**非平凡里程碑**（非机械修复）：

1. **验证可行性**：PM 检查参考证据，确认方案可行（如果是新架构，依据 D0013 证据门）。
2. **复现问题**：PM 具体展示 bug/缺失功能（失败测试、错误日志、差距分析）。
3. **确定契约/接口和验收标准**：PM 编写或更新接口定义、类型签名和测试规范，供 Worker 实现时参照。
4. **实现一个薄的端到端 walking skeleton**：PM 编写最小代码，端到端地遍历每一层一次，并通过测试。
5. **提交一个绿色的 SEED commit**：单个 commit，所有测试通过，walking skeleton 完成。这就是 **seed SHA**。
6. **用 EXECUTION_PACKET_V1 分派 Worker**：从确切的 seed SHA 出发，PM 向每个 Worker 发出执行包（见下文）。
7. **Worker 按包实现**：不重新设计，不扩大范围。Worker 保留本地实现推理，但如果包与代码或证据矛盾，必须停止/报告。
8. **PM 集成**：PM 合并 Worker commit，可以要求修改，但**不得自行独立验证自己的种子**。
9. **干净上下文验证器**：覆盖种子 + Worker 变更。不与 PM 共享设计上下文。

### EXECUTION_PACKET_V1 字段

| 字段 | 说明 |
|---|---|
| `goal` | 一句话目标 |
| `non_goals` | 明确不在范围内的事项 |
| `base_sha` | Worker 分支的起始 commit |
| `seed_sha` | PM 种子 commit（walking skeleton） |
| `adopted_design` | 已确定的设计决策（含理由） |
| `rejected_alternatives` | 考虑过但被拒绝的方案（含理由） |
| `allowed_files` | Worker 可修改的文件 |
| `invariants` | 变更前后必须保持的属性 |
| `interfaces` | 需要实现的函数签名/类型定义 |
| `ordered_steps` | 按依赖顺序排列的实现步骤 |
| `test_commands` | 运行测试的确切命令 |
| `expected_evidence` | Worker 必须展示的内容（测试输出、diff 等） |
| `stop_conditions` | 何时停止并报告（矛盾、阻塞、范围蔓延） |
| `escalate_conditions` | 何时上报 PM（接口不匹配、缺失依赖） |

### Worker 约束

- **不重新设计**：Worker 实现包中定义的接口，而非自己的接口。
- **不扩大范围**：Worker 不修复相邻问题。
- **本地实现推理**：Worker 在包约束内决定*如何*实现，而非*实现什么*。
- **停止并报告的情况**：包与代码矛盾、证据表明方案有误、或依赖缺失。
- **每步一个 commit**：小 commit，测试和实现在一起。

### Worker Master / Shard Lead（可选）

- 仅在**大型分片**（3+ Worker 或多子系统集成）时存在。
- 遵循 PM 包，如需则创建分片种子。
- 集成分片 commit。
- **不能更改项目级契约**（接口、不变量、范围）。
- 小型/机械任务完全跳过 Worker Master。

### Portfolio Master

- **不写项目代码**（除明确的紧急恢复外）。
- 仅协调：监控 inbox/outbox、发出指令、验证迁移字段、管理资源。
- 不编写 EXECUTION_PACKET 或种子 commit。

### 时间盒

- PM 种子：**一个垂直切片，一个 commit**——不是代码行数指标。
- 种子阶段不做广泛实现或长时间 soak。
- 种子验证路径可行；Worker 填充深度。

## 本协议不改变什么

- 机械修复（拼写、格式化、单函数 bug）跳过完整协议——PM 可直接修复或发出简单任务。
- 证据门（D0013）仍适用于架构决策。
- Worker 仍然是 Devin subagent，使用 `--permission-mode dangerous`。
- Git 约定（一个问题一个 commit，不加 AI trailer）不变。

## 试点计划

### 候选里程碑

1. **Argus P2-B B1**（connection-epoch 模型）：PM 验证 CDP 重连检测先例，编写 ConnectionEpoch 类型 + recorder 接口，实现薄 skeleton（一次重连 → 记录新 epoch），提交种子，分派 Worker 填充 recorder_sub.go + cdppool.go + 测试。
2. **Mnemos Round 2**（doctor bash 3.2 修复）：范围较小，可能不需要完整协议——但可以在有界任务上试点包格式。

### 需收集的指标

| 指标 | 当前工作流 | 种子工作流（目标） |
|---|---|---|
| Worker 探索 token | 基线（测量） | 更低（受限接口） |
| Worker 墙钟时间 | 基线（测量） | 更低（减少探索） |
| 返工率 | 基线（测量） | 更低（明确契约） |
| 合并冲突 | 基线（测量） | 更低（共享 seed SHA） |
| 首次通过率 | 基线（测量） | 更高（验收标准具体化） |
| PM 种子时间 | N/A | 测量（一个切片应 <30min） |

### 成功标准

- Worker 探索 token 较基线降低 ≥30%
- 首次通过率 ≥80%（对比当前基线）
- 零接口不匹配导致的合并冲突
- PM 种子时间 ≤30min（一个垂直切片）
- 验证器接受种子 + Worker 变更，不要求重新设计

### 终止标准

- PM 种子时间 >60min（协议对问题而言过重）
- Worker 探索 token 未降低（ACI 洞见不可迁移）
- 超过 30% 的任务中 Worker 报告包矛盾（PM 种子质量过低）
- 合并冲突未减少（seed SHA 未提供共享基线）

## Consequences

- **正面**：降低探索成本、契约更清晰、集成更快、质量可衡量。
- **负面**：PM 在分派前需投入种子时间；小任务的协议开销。
- **缓解**：机械修复跳过协议；种子使用时间盒。

## References

- Cockburn, A. "Just Enough: Just-in-Time Requirements" (2004)
- Cockburn, A. "Using Both Incremental and Iterative Development" IEEE Software (2008)
- Kubernetes Enhancement Proposals (KEPs), kubernetes/enhancements repo, KEP-0001
- Yang et al. "SWE-agent: Agent-Computer Interfaces Enable Software Engineering Language Models" arXiv:2405.15793 (2024)
- OPERATING_CONTRACT.md §13（参考与操作证据门）
- D0011（Concierge 可选放大器）
- D0013（证据门）
