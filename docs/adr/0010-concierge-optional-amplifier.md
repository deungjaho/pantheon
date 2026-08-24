# ADR-0010: Concierge 是可选放大器，非强制控制路径

> **Status**: Accepted
> **Date**: 2026-07-29
> **Decision ID**: D0011 (Portfolio Master, omarchy)
> **Authority**: 用户 ARCHITECTURE CORRECTION 2026-07-29

## Context

初始设计（VISION §1, blueprint 映射）将 Concierge 定位为"持久入口"，隐含所有用户请求经 Concierge 路由到 Project Master。用户在 2026-07-29 发出权威架构纠正：

> Pantheon/Concierge 是 OPTIONAL capability amplifier 和 assistant/housekeeper，永远不是强制控制路径或上级权威。

## Decision

Concierge 是 **optional capability amplifier**。正确路由不是严格的 `User→Concierge→Project Master`，而是：

```
User → {direct Project Master OR optional Concierge}
```

Concierge 职责：`→ {create Run/project/task/workspace when asked, route/aggregate/coordinate}`

### 5 个必须实现的契约

| 契约 | 定义 |
|---|---|
| **Bypassability** | 用户可绕过 Concierge 直接操作任何 PM/worker/workspace；Concierge 不得拦截或要求经过自己 |
| **Transparent provenance** | 每个 PM/Run/Task/workspace 记录创建者（user-manual vs concierge-created）；provenance 不可被 Concierge 静默改写 |
| **Idempotent create/adopt** | 创建已存在的 PM = adopt（接管），不是 fail 或 duplicate；adopt 记录 provenance 变更 |
| **Discovery of manually-created PMs** | Concierge 能发现它未创建的 PM（手动创建的 PM 可被 Concierge 检测并纳入协调视图，但不被强制控制） |
| **Reversible handoff** | 用户可随时收回交给 Concierge 的协调权；handoff to Concierge 是可逆的，不是永久授权 |

### 禁止

- Concierge 不得成为单点故障（SPOF）：Concierge 宕机不影响用户直连 PM
- Concierge 不得静默拦截用户与 PM 之间的通信
- Concierge 不得重新解释用户指令（只转发/聚合/协调）
- Concierge 不得声称对 PM 有上级权威

## Consequences

- Concierge 进程可选启动；不启动 Concierge 时用户直连 PM 的路径必须完整可用
- PM/Run/Task/workspace 记录需增加 provenance 字段（user-manual vs concierge-created）
- Concierge 的 create 操作需幂等（已存在则 adopt）
- Concierge 需能发现未由自己创建的 PM（discovery 机制）
- handoff to Concierge 需可逆（用户可收回协调权）
- Concierge 不得成为 SPOF：架构中不得存在"经 Concierge 才能到达"的路径

## Alternatives Considered

- **Concierge 为强制入口**（初始设计）：被用户明确否决。强制入口引入 SPOF，限制用户直连 PM 的能力，与"用户是 root authority"原则冲突。
- **Concierge 为上级权威**：被用户明确否决。Concierge 是 assistant/housekeeper，不是 controller。

## References

- OPERATING_CONTRACT.md §9.0（跨项目契约层）
- DECISIONS.md D0011（Portfolio Master 决策记录）
- PRINCIPLES.md §12（项目原则层）
- VISION.md §4 架构纠正段落
- AGENT_FRAMEWORK.md §2 Control Paths + §7 Contracts
