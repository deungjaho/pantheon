# ADR-0001: 权威宿主是部署决策，不绑定特定机器

- **Status:** Accepted（2026-08-21 修订）
- **Decided:** 2026-07-28（原决策），2026-08-21（修订）
- **Decision maker:** master
- **Supersedes:** 无
- **Superseded by:** 无
- **Amended by:** ADR-0019（transport 抽象）

---

## Context

Pantheon 需要选择权威运行环境（Agent 实际执行代码、持有 worktree、运行 tmux server 的主机）和用户核心入口（用户提交任务、查看状态、takeover 的主机）。

原决策（2026-07-28）将 omarchy 硬编码为权威宿主，Mac 为瘦客户端。实际运行发现：
- omarchy 离线时 Mac 完全无法编排，这是单点故障；
- Mac 上也能跑 pantheond + SQLite + tmux，没有技术理由禁止；
- "工作区不绑定机器"是系统核心设计原则，原 ADR 违反了它。

## Decision（修订）

1. **权威宿主是部署决策，不是代码假设**：pantheond 和 SQLite store 可以运行在任何一台机器上（omarchy / Mac / 其他），由部署者决定；
2. **客户端优先本地 daemon**：CLI 先探测本地 Unix socket，有就直连；没有才走 SSH 到 `-host` 指定的远程；
3. **不硬编码 hostname**：`-host` 默认不写死 `omarchy`，必须显式指定；
4. **不硬编码用户路径**：不假设 `/home/camt/`，路径用环境变量或 `~` 展开；
5. **不硬编码平台**：服务定义同时支持 systemd 和 LaunchAgent。

## 原决策（保留作历史记录）

原决策（2026-07-28）：
1. omarchy 为权威运行环境；
2. Mac 为核心入口；
3. 传输经 SSH ProxyJump `dengzihao`。

修订原因：原决策把"作者当前选择 omarchy"写成了"代码必须假设 omarchy"，混淆了部署决策和代码假设。

## Consequences

**正面：**
- omarchy 离线时 Mac 可以独立编排（本地 daemon）；
- 双机协作时 Mac CLI 自动走 SSH，单机时走本地 socket；
- 符合"工作区不绑定机器"原则；
- 其他用户可以部署在自己的机器上。

**负面：**
- 需要维护两套服务定义（systemd + LaunchAgent）；
- 本地 daemon 和远程 daemon 的 SQLite 数据不自动同步（部署者自行处理）；
- 需要明确"哪台是权威"的运维约定（文档说明，不是代码强制）。

## Cross-ref

- ADR-0002（SSH stdio 传输，仍然有效，作为远程 transport）
- ADR-0019（transport 抽象：本地 socket 优先，SSH 兜底）
- `docs/ROADMAP.md` Phase 1（Mac submit → omarchy，改为：本地优先 → SSH 兜底）
