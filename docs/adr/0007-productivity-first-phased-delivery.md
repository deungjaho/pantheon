# ADR-0007: 个人生产力优先、分阶段交付

- **Status:** Accepted
- **Decided:** 2026-07-28
- **Decision maker:** master
- **Supersedes:** 无
- **Superseded by:** 无

---

## Context

Pantheon 是新独立 Go 仓库，可选择的交付策略：

1. 一次建成完整系统（Conductor + 多 runtime + 全组件 + 跨机器 + 生产部署）；
2. 分阶段交付，每阶段最小可用，前一阶段未验证不进入下一阶段；
3. 只做设计文档，不实现。

blueprint §17 已提出 Phase 0–5 分阶段实施。blueprint §2.4 提出“前瞻设计预算”，`planned` 级别不进入代码。blueprint §19 禁止“为本文立即重构所有现有项目”。作者需求是个人生产力优先、长期迭代。

## Decision

1. **个人生产力优先**——Pantheon 首先服务作者真实开发环境，不预先建设假想规模基础设施；
2. **长期迭代**——按阶段交付，不一次建成；
3. **Phase 1 先做最小闭环**：Mac submit → omarchy → 单 Worker → checkpoint / status / takeover；
4. **后置项明确**：TUI / Web / Iris / Mnemos / Auditor / 生产部署 在 Phase 1–2 不做；
5. **前一阶段未验证不进入下一阶段**——每阶段有退出条件，全部 `verified` 后才进入下一阶段；
6. **blueprint Phase 0–5 裁剪为 Pantheon 自己的阶段**（见 `docs/ROADMAP.md`）。

## Consequences

**正面：**
- 聚焦最小闭环，快速验证真实可用；
- 避免过度设计（blueprint §2.4）；
- 每阶段退出条件明确，进度可判断；
- 后置项不分散 Phase 1–2 精力。

**负面：**
- Phase 1 功能有限，早期用户体验不完整；
- 后置组件（Iris / Mnemos / Auditor）用户需等待；
- 阶段切换可能暴露前一阶段的设计缺陷，需回溯；
- “个人生产力优先”可能与通用化目标冲突，需显式取舍。

**待验证：**
- Phase 1 退出条件（见 `docs/ROADMAP.md`）全部满足后进入 Phase 2；
- 后置项的最早进入阶段是否需要调整。

## Cross-ref

- `docs/VISION.md` §3（优先级决策、Phase 1、后置项）
- `docs/ROADMAP.md`（Phase 1–5 完整计划与退出条件）
- `docs/PRINCIPLES.md` §1（个人生产力优先）
- `docs/STATUS.md` §1（当前阶段：Phase 1 in progress）
- blueprint §2.4（前瞻设计预算）、§17（实施阶段）、§19（禁止立即重构所有项目）
