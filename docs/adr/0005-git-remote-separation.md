# ADR-0005: Git remote 分离（私人 GitHub vs 公司 Codeup）

- **Status:** Accepted
- **Decided:** 2026-07-28
- **Decision maker:** master
- **Supersedes:** 无
- **Superseded by:** 无

---

## Context

Pantheon 在工作过程中会产生 checkpoint、candidate commit、integration commit，需要保存到 Git remote。作者环境有两类 Git remote：

1. 私人 GitHub：个人仓库，适合保存 agent 工作产物；
2. 公司 Codeup：公司内部 Git 托管，有访问限制和合规要求。

blueprint §14.1 将 repository content 列为 untrusted，§14.3 要求跨项目写入需 approval。Agent 在工作过程中可能产生不应自动推送到公司 remote 的内容。

## Decision

1. **私人 GitHub agent remote 保存 checkpoint / candidate / integration**——agent 工作产物默认推送到私人 GitHub；
2. **公司 Codeup remote 对普通 agent 隐藏**——agent 不感知 Codeup remote 的存在，不自动写；
3. **不自动写公司 remote**——任何写入公司 Codeup 的操作需显式 approval（blueprint §14.3）；
4. **默认不合并主分支**——candidate 在独立分支/worktree 验收，不自动合并到 main/master。

## Consequences

**正面：**
- agent 工作产物与公司代码隔离，降低误推风险；
- 公司 remote 不被 agent 自动污染；
- candidate 验收独立，不破坏主分支稳定性；
- 符合 blueprint §14.3（跨项目写入需 approval）。

**负面：**
- 需要机制确保 Codeup remote 对 agent 隐藏（git config / 权限 / 环境变量）；
- 从 candidate 到公司 remote 的合并需显式流程，增加摩擦；
- remote 命名约定需明确。

**待验证：**
- remote 命名约定（见 `docs/contracts/README.md` §2.6）；
- checkpoint/candidate/integration 分支命名约定；
- Codeup remote 隐藏的具体机制；
- 合并审批流程。

## Cross-ref

- `docs/VISION.md` §3（Git remote 决策）、§5（不变量 9–10）
- `docs/PRINCIPLES.md` §7（安全默认拒绝：Codeup 隐藏）
- `docs/contracts/README.md` §2.6（Git remote 契约待定项）
- blueprint §14.1（信任边界）、§14.3（必须审批：跨项目写入）
