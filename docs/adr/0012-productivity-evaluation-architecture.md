# ADR-0012: 生产力评估架构（proposed）

> **Status**: PROPOSED — 仅设计和 schema，不执行 benchmark。
> **Date**: 2026-07-29
> **Decision ID**: D0015 (proposed)

## Context

系统已有工程验证（测试、竞态、验证器），但没有结构化的产品与生产力评估。没有它，我们无法回答：
- 多 agent 系统是否比替代方案更快/更便宜/更好？
- 哪些组件值得其成本？
- 瓶颈在哪里？

## Decision

采纳
[docs/PRODUCTIVITY_EVALUATION_ARCHITECTURE.md](../PRODUCTIVITY_EVALUATION_ARCHITECTURE.md)
中定义的架构作为 **proposed** 设计。关键要素：

1. **双轨**：工程验证 + 产品与生产力评估（不可互换）
2. **三层证据**：Capability、Uplift、Efficiency——每层标注 `observed`/`inferred`/`self-reported`
3. **受控实验**：相同任务/snapshot/model、重复运行、基线（人类、单 agent、Claude Code、Codex、OpenClaw、Pantheon）、组件消融、一切版本化
4. **系统核心指标**：accepted-task rate、value-adjusted success、human-minutes/accepted-task、wall time、interventions、autonomous span、rework、defect escape、recovery、token/cost、parallel speedup、orchestration overhead——没有单一聚合分数（防止 Goodhart 效应）
5. **子项目指标**：按组件（Pantheon、Argus、Hydra、Mnemos、Iris、Beacon）
6. **统一数据契约**：Experiment、EvaluationRun、TaskCase、Treatment、Baseline、TraceRef、Outcome、HumanReview、MetricSample、ArtifactRef——SQLite + JSON，不引入新基础设施
7. **自迭代闭环**：观察 → 瓶颈 → 假设 → 预注册 → A/B → 验证器 + 人工 → 采纳/回滚 → Decision。无自我修改。
8. **分阶段激活**：Phase 0（现在，设计）→ Phase 1（影子遥测，<3% 开销）→ Phase 2（20-30 次运行）→ Phase 3（A/B + 消融）→ Phase 4（季度市场对比）
9. **模板**：benchmark case + report 模板，包含 task value、rubric、hidden tests、budget、failure classification、confidence interval
10. **暂不做**：不做大规模 benchmark、不做公开排名、不为指标修改产品、不建时间序列 DB、不保存完整 transcript、不把 session 存活视为生产力

## Consequences

- **正面**：为衡量真实生产力提供结构化框架；防止 Goodhart 定律；将工程质量与产品价值分离。
- **负面**：Phase 1 激活时的插桩开销（必须 <5%）。
- **中性**：不阻塞主线工作；Phase 0 仅为设计。

## References

- SWE-bench Verified (Jimenez et al., 2024)
- SWE-Lancer (OpenAI, 2025)
- METR time-horizon (METR, 2024)
- Goodhart's Law
- PRODUCTIVITY_EVALUATION_ARCHITECTURE.md（完整设计）
