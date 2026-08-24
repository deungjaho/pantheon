# ADR-0002: SSH stdio + line-delimited JSON-RPC 2.0 传输

- **Status:** Accepted
- **Decided:** 2026-07-28
- **Decision maker:** master
- **Supersedes:** 无
- **Superseded by:** 无

---

## Context

Pantheon 需要在 Mac（入口）和 omarchy（权威运行环境，见 ADR-0001）之间建立传输契约。选项：

1. 自建 HTTP/gRPC 服务 + 暴露端口；
2. 复用现有 SSH 通路 + stdio 消息；
3. 建专用消息中间件。

blueprint §8.4 明确不推荐初期引入 gRPC / Redis / Kafka。作者已有 SSH ProxyJump `dengzihao` 通路。

## Decision

1. **传输层：SSH stdio**——Mac 通过 SSH 连接 omarchy，消息经 stdin/stdout 传递，不暴露额外端口；
2. **消息格式：line-delimited JSON-RPC 2.0**——每行一条 JSON-RPC 2.0 消息，`\n` 分隔；
3. **连接管理：复用现有 SSH ProxyJump/ControlMaster**——v1 不建自定义长连接，依赖 SSH ControlMaster 的连接复用和多路复用；
4. **不建 HTTP/gRPC 服务**（Phase 1）；
5. **不引入消息中间件**。

## Consequences

**正面：**
- 复用 SSH 现有鉴权（key 认证），不额外建 token 体系；
- 不暴露公网端口，攻击面小；
- stdio 简单可靠，调试方便（可直接管道）；
- line-delimited JSON 易于流式处理，无需 framing 协议；
- ControlMaster 复用现有连接，减少握手开销，无需自建连接池。

**负面：**
- 单连接吞吐受 SSH 通道限制；
- 断线重连需应用层处理（blueprint §16.1.8）；
- JSON-RPC 2.0 的 streaming/notification 语义需明确（stdio 是双向流）；
- 大 payload 不适合走 stdio，需走 artifact（blueprint §5.5）；
- 依赖 ControlMaster 配置正确性（ControlPath / ControlPersist），配置缺失时退化为每次新连接。

**待验证：**
- stdio 流控与背压策略；
- SSH 断线后 in-flight 请求的 `result_state`（可能 `result_unknown`）；
- JSON-RPC method 列表与 schema（见 `docs/contracts/README.md` §2.1）。

## Cross-ref

- ADR-0001（omarchy 权威运行环境）
- `docs/contracts/README.md` §2.1（传输契约待定项）
- `docs/VISION.md` §3（传输决策）
- blueprint §8.4（不推荐 gRPC/Redis/Kafka）、§13.2（错误语义）
