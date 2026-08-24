# ADR-0003: 仅 Devin 的 v1 RuntimeAdapter

- **Status:** Accepted
- **Decided:** 2026-07-28
- **Decision maker:** master
- **Supersedes:** 无
- **Superseded by:** 无

---

## Context

Pantheon 需要选择 Agent runtime（实际执行编码任务的 Agent）。blueprint §11.6 提出 RuntimeAdapter 概念，可适配 Claude Code / Codex / Devin 等不同 runtime。

选项：
1. v1 同时支持多个 runtime；
2. v1 只支持一个，但接口可扩展；
3. 不建 adapter，直接硬编码 runtime 调用。

blueprint §11.6 明确：“只有第二个真实 adapter 验证后才稳定接口”“不为假想 runtime 建完整插件 SDK”（blueprint §19）。作者当前主力使用 Devin。

## Decision

1. **v1 只实现 Devin adapter**——Pantheon Phase 1–2 的 Agent runtime 为 Devin；
2. **RuntimeAdapter 接口可扩展**——接口设计参照 blueprint §11.6 公共能力（start / send / interrupt / stop / inspect / resume / events），但 v1 只有一个实现；
3. **不预先建多 runtime SDK**——第二个 adapter 出现时才稳定接口；
4. **不伪造 Devin 不支持的能力**——如 resume / permission 语义以 Devin 实际能力为准。

## Consequences

**正面：**
- v1 复杂度低，聚焦 Devin 单路径；
- 接口预留扩展点，未来加第二个 runtime 不需大改；
- 避免为假想 runtime 过早抽象（blueprint §19）。

**负面：**
- Devin runtime 更新可能破坏 adapter（blueprint §11.6 难点）；
- Devin 特有能力（如 session 恢复）需在 adapter 内吸收，不能泄漏到核心；
- 第二个 runtime 接入时接口可能需要调整。

**待验证：**
- Devin 的 hook / permission / session 语义（blueprint §16.2）；
- Devin session 恢复能力；
- stream-json 格式稳定性。

## Cross-ref

- `docs/VISION.md` §3（Runtime 决策）、§4（blueprint 映射：RuntimeAdapter）
- `docs/ROADMAP.md` Phase 1（单 Worker = Devin）
- blueprint §11.6（Agent Runtime Adapters）、§16.2（runtime hook 能力需逐一验证）
