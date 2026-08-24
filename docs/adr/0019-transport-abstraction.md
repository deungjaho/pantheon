# ADR-0019: Transport 抽象 — 本地 socket 优先，SSH 兜底

- **Status:** Accepted
- **Decided:** 2026-08-21
- **Decision maker:** master
- **Supersedes:** 无
- **Superseded by:** 无
- **Amends:** ADR-0001

---

## Context

ADR-0001 原决策将 SSH 作为唯一 transport，Mac CLI 默认 `-host omarchy`。实际运行发现：

- omarchy 离线时 Mac 完全无法编排；
- Mac 上也能跑 pantheond，但 CLI 不会连本地 socket；
- Transport 接口已存在（ADR-0006 "Replaceable seams"），但本地 transport 未接。

## Decision

1. **Transport 接口抽象**：定义 `Transport` 接口，有 `LocalTransport`（Unix socket）和 `SSHTransport`（SSH stdio）两个实现；
2. **连接策略**：
   - CLI 启动时先探测本地 Unix socket（默认 `~/.local/share/pantheon/pantheon.sock` 或 `$PANTHEON_SOCKET`）；
   - 本地 socket 存在 → 直连 `LocalTransport`；
   - 不存在 → 走 `SSHTransport` 到 `-host` 指定的远程；
   - `-host` 为空且无本地 socket → 明确报错，不默认任何 hostname；
3. **`-host` 默认值改为空**：不写死 `omarchy`，必须显式指定；
4. **`-socket` 参数**：可覆盖本地 socket 路径；
5. **daemon 端不变**：pantheond 仍然监听 Unix socket，不关心客户端怎么连。

## Consequences

**正面：**
- Mac 上有 daemon 时自动直连，无 SSH 开销；
- Mac 上无 daemon 时走 SSH，行为与原决策一致；
- omarchy 离线 + Mac 有 daemon → Mac 独立编排；
- 其他部署场景（单机 Linux）也支持。

**负面：**
- 本地和远程的 SQLite 数据不自动同步（部署者自行处理）；
- 需要文档说明"哪台是权威"的运维约定。

## Implementation notes

- `Transport` 接口已有，只需加 `LocalTransport` 实现；
- CLI flag 解析改为：`-host` 默认空，`-socket` 默认 `~/.local/share/pantheon/pantheon.sock`；
- 探测逻辑：`os.Stat(socketPath)` → 存在且可写 → LocalTransport；否则 → SSHTransport（需要 `-host` 非空）。

## Cross-ref

- ADR-0001（修订：权威宿主是部署决策）
- ADR-0002（SSH stdio 传输，作为 SSHTransport 实现）
- ADR-0006（tmux 非权威，Replaceable seams）
