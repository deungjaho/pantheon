# Pantheon 快速上手

从全新 checkout 到完成一次验证运行的最小 walkthrough。所有命令可直接复制粘贴。在 Pantheon 仓库根目录执行。

## 前置条件

- Go >= 1.21
- SQLite >= 3.35
- git
- 带 systemd 的 Linux（用于守护进程服务）— 或在 macOS / 非 systemd 主机上手动运行 `pantheond`
- `~/.local/bin` 在 `PATH` 中（安装脚本会把二进制放这里）

## 1. 安装 Pantheon

在仓库根目录执行：

```sh
./deploy/install.sh
```

构建 `pantheond` 和 `pantheon`，安装到 `~/.local/bin`，创建 `~/.local/share/pantheon/`，安装 systemd 用户单元，启用 `pantheond.service`（不启动）。

不用 systemd 时手动构建：

```sh
go build -o ~/.local/bin/pantheond ./cmd/pantheond
go build -o ~/.local/bin/pantheon ./cmd/pantheon
mkdir -p ~/.local/share/pantheon
```

## 2. 启动守护进程

用 systemd：

```sh
systemctl --user start pantheond
```

不用 systemd，在终端运行：

```sh
pantheond -socket ~/.local/share/pantheon/pantheond.sock -wake
```

保持终端打开。守护进程日志输出到 stderr。

## 3. 健康检查

```sh
pantheon doctor
```

应看到 CLI、socket、守护进程的 `ok` 行。机器可读输出：

```sh
pantheon doctor --json
```

## 4. 注册项目

需要一个至少一次提交的 git 仓库。没有的话：

```sh
mkdir -p ~/demo-repo && cd ~/demo-repo
git init && echo "# demo" > README.md
git add -A && git commit -m "initial"
```

注册到 Pantheon（按需替换路径和分支）：

```sh
pantheon project register \
  --name demo \
  --repo-path ~/demo-repo \
  --base-ref main
```

响应包含 `project_id`（`prj_...`）。列出项目确认：

```sh
pantheon project list
```

## 5. 创建并启动运行

```sh
pantheon run create \
  --project-id prj_... \
  --objective "在 README.md 中添加 hello-world 函数" \
  --risk-level R1
```

响应包含 `run_id`（`run_...`）。启动：

```sh
pantheon run start --run-id run_...
```

> 无真实运行时适配器连接时，运行转到 `running` 但无 agent 做事。学习控制平面够用。要接真实 agent 见 `pantheon agent register`。

## 6. 查看运行状态

```sh
pantheon run status --run-id run_...
```

`state` 字段显示运行生命周期位置（`running` / `blocked` / `completed` 等）。

## 7. 停止和恢复运行

停止将 running 转为 `blocked`（可恢复）：

```sh
pantheon run stop --run-id run_...
pantheon run status --run-id run_...   # state: blocked
```

恢复：

```sh
pantheon run resume --run-id run_...
pantheon run status --run-id run_...   # state: running
```

## 8. 验证运行

验证需要注册的 verifier agent 和真实证据引用（运行日志中的 `event_id`）。先注册 verifier：

```sh
pantheon agent register \
  --run-id run_... \
  --role verifier \
  --runtime devin \
  --pid 0
```

从日志取 event ID（直接 RPC 模式）：

```sh
pantheon run.events '{"run_id":"run_..."}'
```

选一个 `event_id`，记录 PASS 判定：

```sh
pantheon run verify \
  --run-id run_... \
  --verifier agt_... \
  --verdict PASS \
  --evidence evt_...
```

运行现在 `completed`。FAIL 判定会转为 `rejected`。

## 9. 使用消息总线

发布指令到运行：

```sh
pantheon run message \
  --run-id run_... \
  --body "请保持改动最小化。"
```

拉取运行消息：

```sh
pantheon message receive --run-id run_...
```

守护进程启动了推送 socket 时（`-push-socket ~/.local/share/pantheon/pantheond-push.sock`），可在另一终端订阅实时通知：

```sh
pantheon message subscribe --run-id run_...
```

每条发布消息打印为一行 JSON。断开时 CLI 打印 cursor 兜底提示，可用 `pantheon message receive --cursor N` 恢复错过的消息。

## 10. 运行审计器

全局审计器分析运行历史产出发现（策略提案、风险观察）。可选。用 `-auditor` 启动守护进程：

```sh
systemctl --user stop pantheond
pantheond -socket ~/.local/share/pantheon/pantheond.sock -wake -auditor
```

触发一次审计：

```sh
pantheon auditor.audit
```

列出发现：

```sh
pantheon auditor.findings
```

审查（接受或拒绝）发现：

```sh
pantheon auditor.review '{"finding_id":"fnd_...","status":"accepted","reviewed_by":"你"}'
```

## 11. 卸载

完成后：

```sh
./deploy/uninstall.sh
```

移除二进制和 systemd 单元，保留 `~/.local/share/pantheon/` 数据。

## 下一步

- `examples/quick-start.sh` — 上述步骤的脚本
- `examples/message-bus.sh` — 两终端发布 + 订阅
- `examples/multi-worker.sh` — 同项目两个并发运行
- [威胁模型](THREAT_MODEL.md) — 信任边界和残余风险
