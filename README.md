# SPDE (Super-Download-Engine)

PandaNetPL 生态统一下载中心，单点 CLI 实现。支持本地独立运行与 PK 主控集中调度两种模式。

## 生态标准

本项目属于 **PandaNetOS 生态项目群**，遵循全系统权威标准仓库 [PandaNetOS](https://github.com/pandamelive/PandaNetOS) 的规范：

- **强制依赖** `pandanetos` 共享库（path 依赖），统一协议路径常量（`protocol::paths`）、响应格式（`ApiResponse`/`ApiError`）、错误码、时间格式（UTC RFC3339）与配置标准，禁止维护私有协议与常量。
- **标准一致性**：节点上报、任务领取、心跳等协议路径均与 PandaNetOS《标准规范》一致，不得出现与标准不一致的私有端点。

> 当前版本：v0.3.0
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

### v0.3.0 新增：Agent 模式（PK 主控集中调度）

- **`spde agent` 子命令**：接入 PandaNetPL Controller（PK），由主控统一下发下载任务
- **节点自动注册**：启动时向 PK 注册节点信息（hostname、平台、架构、版本），获取永久 node_id
- **局域网自动发现**：未指定 master 时自动扫描局域网（端口 5566/8080/8/8000/3000）寻找 PK 服务端
- **WebSocket 实时通信**：长连接接收任务变更通知，自动重连（断线 3 秒重试）
- **动态任务同步**：PK 推送配置变更后自动拉取最新任务列表，增量启动/取消下载任务
- **实时状态上报**：每 10 秒通过 WebSocket 上报活跃任务数、累计下载量、忙碌状态、最近错误
- **任务级回报**：每个任务开始/完成时通过 WebSocket 回传详细统计（dispatch_id、task_id、速度、分片状态等）
- **Token 鉴权**：支持 Bearer Token 与 PK 通信，API 与 WebSocket 均携带鉴权头

## 快速开始

### 模式一：本地独立运行（serve）

```bash
# 直接运行（首次启动自动生成 spde-node/ 目录和配置模板）
./spde serve

# 编辑配置
vim spde-node/config/config.yaml

# 再次运行开始下载
./spde serve
```

### 模式二：接入 PK 主控（agent）

```bash
# 指定 PK 地址和 token 启动
./spde agent --master http://10.0.0.8:5566 --token your_token

# 也可在 config.yaml 的 controller 段预配置，启动时无需参数
./spde agent

# 不指定 master 时自动扫描局域网寻找 PK
./spde agent
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

### agent — Agent 模式配置

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `master` | string | `""` | PK 主控地址（如 `http://10.0.0.8:5566`），也可通过 `--master` 命令行参数覆盖 |
| `node_id` | string/null | `null` | 节点 UUID，留空时使用本地 `node-id.json` 中的标识 |
| `heartbeat_interval_secs` | int | `5` | 心跳间隔（秒） |

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

### controller — PK 主控连接配置

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `url` | string | `""` | PK 主控地址（如 `http://10.0.0.8:5566`），优先级低于 `--master` 命令行参数 |
| `token` | string | `""` | 与 PK 通信的 Bearer Token，优先级低于 `--token` 命令行参数 |

### direct_tasks — 下载任务列表

每个任务包含以下字段：

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `name` | string | — | 任务名称（显示用） |
| `enable` | bool | `true` | 是否启用该任务（false 则跳过） |
| `url` | string | — | 下载 URL（支持 HTTP/HTTPS，建议用支持 Range 的直链） |
| `filename` | string | — | 保存到本地的文件名 |
| `task_id` | string/null | `null` | 任务唯一标识（Agent 模式下由 PK 下发） |
| `dispatch_id` | string/null | `null` | 调度实例标识（Agent 模式下由 PK 下发，用于任务同步和回报） |

### 完整配置示例

```yaml
agent:
  master: ""
  node_id: null
  heartbeat_interval_secs: 5

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

controller:
  url: ""
  token: ""

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
| `spde serve` | 启动本地下载服务，执行 config.yaml 中所有启用的任务，显示实时进度和最终汇总 |
| `spde agent [--master URL] [--token TOKEN]` | 接入 PK 主控，注册节点、拉取任务、实时回传统计；未指定 master 时自动扫描局域网 |
| `spde config` | 配置相关操作 |
| `spde stats` | 查看统计信息 |
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

### agent 命令输出示例

```
spde work root: "/path/to/spde-node"
[init] all directory & file integrity passed
[agent] no master specified, scanning local network ...
[discover] scanning 1 subnet(s) on ports [5566, 8080, 80, 8000, 3000] ...
[discover] found PK at http://192.168.1.100:5566
[agent] master = http://192.168.1.100:5566
[agent] registered node_id=550e8400-e29b-41d4-a716-446655440000
[ws] connected to ws://192.168.1.100:5566/api/v1/agent/ws?node_id=...
[agent] config fetched: 3 tasks, max_concurrent=4
[agent] started task Ubuntu 22.04 ISO (dispatch_id=...)
[download] start Ubuntu 22.04 ISO -> "/path/to/download/ubuntu-22.04.iso"
[ws] config_changed received
[agent] config changed, re-fetching
[agent] cancel removed task dispatch_id=...
```

## Agent 模式通信协议

SPDE Agent 与 PK 主控之间通过 HTTP REST + WebSocket 双协议通信：

### HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/agent/register` | 节点注册，提交 hostname/platform/arch/version，返回 node_id 和轮询间隔 |
| GET | `/api/v1/nodes/{node_id}/config.yaml` | 拉取该节点的最新下载配置（含任务列表） |
| GET | `/api/v1/overview` | 服务端探测接口（局域网自动发现时用于验证 PK） |

### WebSocket 接口

连接地址：`ws(s)://{master}/api/v1/agent/ws?node_id={node_id}`

**客户端 → 服务端消息：**

| type | 字段 | 说明 |
|---|---|---|
| `pong` | — | 心跳响应 |
| `status` | `active_tasks`, `bytes_downloaded`, `busy`, `last_error` | 节点状态上报（每 10 秒） |
| `task_started` | `dispatch_id` | 任务开始通知 |
| `task_report` | `dispatch_id`, `task_id`, `task_name`, `url`, `filename`, `file_size`, `downloaded_bytes`, `elapsed_secs`, `avg_speed_mbps`, `status`, `success_chunks`, `failed_chunks`, `error_msg` | 任务完成/失败详细回报 |

**服务端 → 客户端消息：**

| type | 说明 |
|---|---|
| `ping` | 心跳探测 |
| `config_changed` | 配置变更通知，Agent 收到后重新拉取 config.yaml 并同步任务 |

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
