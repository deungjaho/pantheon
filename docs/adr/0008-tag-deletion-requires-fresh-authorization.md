# ADR-0008: Tag 删除/移动和破坏性 metadata 变更需 fresh explicit authorization

- **Status:** Accepted
- **Decided:** 2026-07-28
- **Decision maker:** master
- **Supersedes:** 无
- **Superseded by:** 无

---

## Context

2026-07-28 发生 Argus P2-A stage tag 事件：

1. Argus commit `dfafd60` 通过验证（10/10 targeted TwoHopRedirect pass + 5/5 full integration soak pass + resources at baseline every round）；
2. Master 明确授权创建 tag `argus-p2a-stage` 指向 `dfafd60`；
3. Worker 创建了 tag（tag object `c8097f955796a158a77519683b0331cf0d0ccd74`）；
4. 随后在 commit `20fee81` 中，Worker **移除了该 tag**，原因是误读过时的条件指令（该指令仅适用于首次 10x run 失败后，但后续证据已 10/10 + 5/5 PASS，且 Master 已明确授权）；
5. Tag ref 当前不存在于 Argus 仓库，但 dangling tag object 经 `git fsck` 和 `git cat-file -p` 确认存活；
6. Worker 已 frozen，independent verifier 正在独立验证 `dfafd60`。

**根本问题：** Worker 用旧的条件指令覆盖了最新的 Master 决策和证据，且 tag 删除（破坏性 metadata 变更）未要求 fresh authorization。

## Decision

确立两条持久治理规则：

### 规则 1：最新事实证据 + 明确 Master 决策 supersedes 旧条件指令

- 当最新验证证据（如 10/10 + 5/5 PASS）与旧条件指令冲突时，以最新证据 + Master 明确决策为准；
- 旧条件指令若已被新证据和新决策 supersede，Worker 不得继续执行旧指令；
- Worker 在执行破坏性操作前必须检查：是否存在更新的证据或 Master 决策改变了前提条件。

### 规则 2：Tag 删除/移动和破坏性 metadata 变更始终需要 fresh explicit authorization

- **无论 prior instructions 是否授权过创建**，tag 的删除、移动、重写始终需要 Master 的 **fresh explicit authorization**；
- "Fresh" 意指：针对该次具体破坏性操作的明确授权，不是对之前创建授权的推定延伸；
- 此规则适用于所有 git metadata 破坏性操作：tag delete/move、branch force-push、commit rewrite、history rewrite；
- 此规则不限制 tag 创建——tag 创建遵循项目正常流程（证据 + 授权）；
- 此规则限制的是**撤销已创建的 metadata**，因为撤销可能丢失不可恢复的引用。

## Consequences

**正面：**
- 防止 Worker 用过时指令覆盖最新 Master 决策；
- 破坏性 metadata 变更有了明确的 authorization gate；
- Dangling tag object 可恢复（需 Master fresh 授权），不会因误删而永久丢失（只要 gc 未清理）；
- 规则持久化在 ADR 中，不依赖 session 记忆。

**负面：**
- 增加了破坏性操作的摩擦（需等待 Master 授权）；
- Worker 需要判断什么是"破坏性 metadata 变更"，可能有边界模糊情况；
- Dangling object 可能被后续 `git gc` 清理，恢复窗口有限。

**待验证：**
- Argus `dfafd60` independent verifier 结果（verifier in progress）；
- Argus tag `argus-p2a-stage` 恢复决策（需 Master fresh explicit authorization）；
- Dangling tag object `c8097f9` 是否在下次 `git gc` 前被恢复。

## Cross-ref

- `docs/STATUS.md` §3.2（Argus P2-A stage tag 事件完整记录）
- `docs/HANDOFF.md` Approval boundaries（tag 操作需 Master explicit approval）
- `docs/PRINCIPLES.md` §7（安全默认拒绝）
- `docs/DOCUMENTATION_POLICY.md` §4（ADR 不可静默改写）
- blueprint §14.3（必须审批：destructive Git/filesystem 操作）
