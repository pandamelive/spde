# SPDE (Super-Download-Engine)

PandaNetPL 生态统一下载中心，单点 CLI 实现。

> 当前版本：v0.2.0
> 支持平台：Windows x86_64 / Linux x86_64 musl / Linux aarch64 musl / macOS x86_64 / macOS aarch64

## 特性

- 单二进制，复制即可运行，无外部依赖
- 启动自动目录自检，缺失目录/配置自动生成最小模板
- `node-id`：节点永久唯一标识
- **单文件多连接分片下载**：可配置连接数，跑满带宽
- **分片失败自动重试**：可配置重试次数，失败正确标记 error
- **断点续传**：支持从已下载位置继续
- **实时进度显示**：百分比、已下载/总大小、当前速度
- **下载汇总统计**：本次下载量、历史总下载量、总耗时、平均速度
- **run-history.jsonl 永久持久化**：每次下载任务流量信息追加写入，只追加不修改
- **dry-run 模式**：数据直接丢弃不落盘，仅统计速度和流量（适合测速）
- 支持 HTTP/HTTPS 代理
- 支持跳过 TLS 证书验证
- SIGINT/SIGTERM 捕获，优雅退出
- GitHub Actions 自动多平台编译 + Tag 触发自动 Release

## 快速开始

```bash
# 直接运行（首次启动自动生成 spde-node/ 目录和配置模板）
./spde serve

# 编辑配置
vim spde-node/config/config.yaml

# 再次运行开始下载
./spde serve
```

## 目录结构

工作目录固定在二进制同级目录下的 `spde-node/`（避免与二进制文件名 `spde` 冲突）：

```
spde-node/
├── bin/                          # 二进制目录
├── config/
│   └── config.yaml               # 主配置文件
└── data/
    ├── node-id.json              # 节点永久唯一标识
    └── run-history.jsonl         # 下载历史永久记录（追加写入）
```

## 配置项说明（config.yaml）

### global — 全局配置

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `work_dir` | string/null | `null` | 数据目录路径，相对路径基于 `spde-node/` 根目录；为 null 时使用 `spde-node/data/` |
| `max_concurrent` | int | `4` | 最大并发下载任务数 |
| `resume` | bool | `true` | 是否启用断点续传 |
| `retry_times` | int | `3` | 单个分片下载失败后的重试次数 |
| `timeout` | int | `1800` | 单个下载任务超时时间（秒） |
| `skip_tls_verify` | bool | `false` | 是否跳过 TLS 证书验证（自签名证书环境可开启） |
| `connections_per_file` | int | `8` | 单文件多连接分片下载的连接数（越大越快，受服务器和带宽限制） |
| `dry_run` | bool | `false` | 试运行模式：下载数据直接丢弃不落盘，仅统计速度和流量（适合测速） |

### output — 输出配置

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `save_path` | string | `"./download"` | 下载文件保存目录，相对路径基于 `spde-node/` 根目录 |

### proxy — 代理配置

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `http_proxy` | string | `""` | HTTP 代理地址，如 `http://127.0.0.1:7890` |
| `https_proxy` | string | `""` | HTTPS 代理地址，如 `http://127.0.0.1:7890` |

### direct_tasks — 下载任务列表

每个任务包含以下字段：

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `name` | string | — | 任务名称（显示用） |
| `enable` | bool | `true` | 是否启用该任务（false 则跳过） |
| `url` | string | — | 下载 URL（支持 HTTP/HTTPS，建议用支持 Range 的直链） |
| `filename` | string | — | 保存到本地的文件名 |

### 完整配置示例

```yaml
global:
  work_dir: null
  max_concurrent: 2
  resume: true
  retry_times: 5
  timeout: 1800
  skip_tls_verify: true
  connections_per_file: 16
  dry_run: false

output:
  save_path: "../download"

proxy:
  http_proxy: ""
  https_proxy: ""

direct_tasks:
  - name: "iPhone 4.7 12.1.4 固件"
    url: "http://updates-http.cdn-apple.com/2019WinterFCS/fullrestores/041-39257/32129B6C-292C-11E9-9E72-4511412B0A59/iPhone_4.7_12.1.4_16D57_Restore.ipsw"
    filename: "iPhone_4.7_12.1.4_16D57_Restore.ipsw"
  - name: "测试任务（已禁用）"
    enable: false
    url: "http://example.com/file.zip"
    filename: "file.zip"
```

## CLI 命令汇总

| 命令 | 说明 |
|---|---|
| `spde serve` | 启动下载服务，执行所有启用的任务，显示实时进度和最终汇总 |
| `spde config` | 配置相关操作（占位，待实现） |
| `spde stats` | 查看统计信息（占位，待实现） |
| `spde --help` | 显示帮助信息 |
| `spde --version` | 显示版本号 |

### serve 命令输出示例

```
spde work root: "/path/to/spde-node"
[init] all directory & file integrity passed
serve starting ...
max_concurrent: 2
task count: 2
enabled task count: 2

[start] iPhone 4S iOS 9.3.5 固件 -> "/path/to/download/iPhone4,1_9.3.5_13G36_Restore.ipsw"
[progress] iPhone4,1_9.3.5_13G36_Restore.ipsw: 50% (775.8/1538.8 MB) speed: 53.4 MB/s
[done] iPhone4,1_9.3.5_13G36_Restore.ipsw: 1538.8 MB in 28.9s, avg speed: 53.2 MB/s
[history] 2 records appended to "/path/to/spde-node/data/run-history.jsonl"

========== 下载汇总 ==========
总任务数: 2 (成功: 2 失败: 0)
本次下载量: 1538.8 MB (1.50 GB)
历史总下载量: 4513.9 MB (4.41 GB)
总耗时: 28.9s
平均速度: 53.2 MB/s
==============================
```

## run-history.jsonl 记录格式

每条记录为一行 JSON，包含以下字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `timestamp` | string | 完成时间（ISO 8601，带时区） |
| `task_name` | string | 任务名称 |
| `url` | string | 下载 URL |
| `filename` | string | 文件名 |
| `file_size` | int | 文件总大小（字节） |
| `downloaded_bytes` | int | 实际下载字节数（跳过的任务为 0） |
| `elapsed_secs` | float | 耗时（秒） |
| `avg_speed_mbps` | float | 平均速度（MB/s） |
| `status` | string | 任务状态：`success` / `skipped` / `failed` |
| `success_chunks` | int | 成功下载的分片数 |
| `failed_chunks` | int | 失败的分片数 |
| `error_msg` | string/null | 失败原因（成功为 null） |

## 编译

### 本地编译

```bash
# 当前平台
cargo build --release

# Linux x86_64 musl（静态链接）
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

### GitHub Actions 自动编译发布

项目已配置 `.github/workflows/release.yml`，推送 `v*` 格式的 tag 后自动：

1. 并行编译 5 个平台的 release 二进制
2. 自动创建 GitHub Release
3. 自动生成 Release Notes
4. 上传所有平台二进制作为 Assets

```bash
git tag -a v0.3.0 -m "SPDE v0.3.0"
git push origin v0.3.0
```

编译完成后前往 Releases 页面下载：https://github.com/pandamelive/spde/releases

## License

MIT
