# ADR-0011: Iris 是可选用户交互面

> **Status**: Accepted
> **Date**: 2026-07-29
> **Decision ID**: D0012 (Portfolio Master, omarchy)
> **Authority**: 用户 PORTFOLIO ARCHITECTURE ADDENDUM 2026-07-29

## Context

ADR-0010 确立 Concierge 为可选放大器后，用户追加架构增补：Iris 是 optional user interaction surface，与 optional-Concierge 设计紧密耦合。初始设计将 Iris 列为"后置"（Phase 1 不做），但用户纠正：Iris 与 Concierge 的可选性紧密耦合，需在架构层面明确定位，虽 V1 实现仍可后置。

> Iris 是 optional user interaction surface，与 optional-Concierge 紧密耦合。Iris 不是 Concierge。

## Decision

Iris 是 **optional user interaction surface**。Iris 提供 channel/session presentation 和 acknowledgements，不是 task registry、不是 event truth、不是 mandatory control path、不是 Project Master。

### Iris 提供的能力

1. **persistent sandboxed general-assistant session**：碎片化聊天 + 有界临时任务
2. **explicit user-selected routing/switching**：用户选择路由到具体 Project Master 提交需求/获取状态，可选由 Concierge 转发
3. **notification delivery**：完成/blocker/决策/checkpoint 通知送达

### 角色边界（不混淆）

| 角色 | 职责 |
|---|---|
| Pantheon registry/events | 权威 task registry + event truth |
| Concierge | 仅当被选择时编排（create when asked / route / aggregate / coordinate） |
| Portfolio Master | 聚合跨项目状态 |
| Project Master | 项目本地分解/执行 |
| Beacon | 可能拥有 notification policy |
| **Iris** | **channel/session presentation 和 acknowledgements** |

### 7 个必须实现的契约

| 契约 | 定义 |
|---|---|
| Visible active route/project | 当前路由到的 PM/project 可见 |
| Source and correlation IDs | 每条消息/通知有 source 和 correlation ID 用于追踪 |
| Idempotent dedup/ack | 重复消息去重 + 确认机制幂等 |
| Reversible switching to general chat | 用户可随时切回 general-assistant session |
| Bounded histories/queues | 历史和队列有界，溢出可见 |
| Restart recovery | 重启后恢复 session/route 状态 |
| Direct/manual bypass | 用户可绕过 Iris 直连 PM（与 ADR-0010 Bypassability 一致） |

### V1 实现约束

- 使用 OpenClaw 引用的官方 WeChat plugin 方式，在可替换的 transport adapter 之后
- 自定义 mini-program 是未来需求驱动范围，V1 不做

## Consequences

- Iris 是可选的：不启动 Iris 时用户直连 PM 的路径必须完整可用
- Iris 不拥有 task registry 或 event truth（Pantheon 是权威）
- Iris 不分解/执行项目工作（PM 负责）
- Iris 的 notification delivery 需与 Beacon notification policy 协调
- Iris 需实现 source + correlation ID 追踪
- Iris 需实现 idempotent dedup/ack
- Iris 需实现 restart recovery（session/route 状态持久化）
- V1 用 OpenClaw 官方 WeChat plugin，transport adapter 可替换

## Alternatives Considered

- **Iris 为 mandatory control path**：被用户明确否决。与 ADR-0010 Bypassability 一致，用户可绕过 Iris 直连 PM。
- **Iris = Concierge**：被用户明确否决。Iris 是交互面，Concierge 是编排器，职责不同不可混淆。
- **Iris 拥有 task registry/event truth**：被用户明确否决。Pantheon registry/events 是权威，Iris 只做 presentation。
- **Iris 为 Phase 1 后置不做**：部分修正。Iris V1 实现可后置，但架构定位需现在明确（与 Concierge 可选性紧密耦合）。

## References

- OPERATING_CONTRACT.md §9.0.3（跨项目契约层）
- DECISIONS.md D0012（Portfolio Master 决策记录）
- PRINCIPLES.md §13（项目原则层）
- VISION.md §4 架构纠正段落
- AGENT_FRAMEWORK.md §2 Control Paths + §7 Contracts
- ADR-0010（Concierge 可选放大器，Iris 与其紧密耦合）
