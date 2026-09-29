# spde AGENTS.md

> 本文件是 AI 代理进入 spde 仓库时的首读指南。
> 生态级全局约束请参考 [根目录 AGENTS.md](../AGENTS.md)。

## 仓库定位

spde（Super Download Engine）是 PandaNetOS 生态的**下载执行 Agent**，Agent 级独立进程。多协议下载（HTTP/HTTPS/SSH/SFTP/本地文件，特性开关扩充 FTP、BT/磁力）、做种、上传；支持本地独立运行与 PK 主控集中调度双模式。当前版本 v1.4.1。

## 目录结构

```
spde/
├── src/
│   ├── bin/main.rs    # CLI 入口（serve / agent / config / stats）
│   ├── lib.rs
│   ├── cli/           # 命令实现（含 p2p/bt、manifest、ws_client）
│   ├── domain/        # 领域模型
│   └── infra/         # 基础设施（磁盘写入、pk_client、文件源）
├── .github/workflows/ # release.yml（五平台构建 + 自动发布）
└── Cargo.toml
```

运行期工作目录固定于二进制同级 `spde-node/`：`config/config.yaml`、`data/node-id.json`、`data/run-history.jsonl`。

## 构建与测试

| 命令 | 说明 |
|---|---|
| `cargo build --release` | Release 构建 |
| `cargo build --release --target x86_64-unknown-linux-musl` | Linux musl 静态链接（vendored OpenSSL） |
| `cargo test --all` | 运行所有测试 |
| `cargo fmt --all -- --check` | 格式检查 |
| `cargo clippy --all-targets -- -D warnings` | 静态分析 |

## 关键事实

| 项 | 值 |
|---|---|
| 当前版本 | v1.4.1（Cargo.toml） |
| 依赖 | `pandanetos`（path，../PandaNetOS/crates/pandanetos）——**旧标准库，迁移待办** |
| 特性开关 | `ftp`（suppaftp）、`torrent`（librqbit），默认全开 |
| 发布 | 推送 `v*` tag 触发 GitHub Actions 五平台构建并自动 Release |

## 注意事项

1. **旧依赖阻塞本地编译**：工作区 `PandaNetOS/` 目录已删除，本地 `cargo build` 会因找不到 path 依赖失败；需先克隆 `PandaNetOS/PandaNetOS` 到同级目录，或完成向 `pnos-spec`（pnos）的迁移（P0 待办）
2. Agent 模式通过 HTTP REST + WebSocket 与 PK 主控通信（`/api/v1/agent/register`、`/api/v1/agent/ws` 等），未指定 master 时扫描局域网常见端口
3. 合规检查使用 `pandanetos-meta/check-compliance.ps1`（26 项 + 行为冒烟），提交/推送前必过

## 变更历史

| 日期 | 版本 | 变更内容 |
|---|---|---|
| 2026-09-29 | v1.0 | 补建本文件（此前缺失）；记录旧依赖阻塞本地编译的现状 |
