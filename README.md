# SPDE — Super Download Engine

> **PandaNetOS 生态统一下载中心**：单点下载引擎，本地独立运行与 PK 主控集中调度双模式。

SPDE（Super Download Engine）是 **PandaNetOS 生态**的核心下载组件。单二进制、零外部依赖，跨平台提供高性能多协议下载能力——既可独立部署执行本地批量下载，也可作为 Agent 节点接入 PK 主控，参与全网统一调度。

---

## 生态定位

本项目隶属 **PandaNetOS 生态项目群**，以生态权威标准库 [PandaNetOS](https://github.com/PandaNetOS/PandaNetOS) 为准绳：

| 规范维度 | 要求 |
|---|---|
| 共享库依赖 | 强制 path 依赖 `pandanetos`，**禁止**维护私有协议与私有常量 |
| 协议路径 | 全部复用共享库 `protocol::paths` 路径常量 |
| 响应格式 | 统一 `ApiResponse` / `ApiError`、生态错误码、UTC RFC3339 时间格式 |
| 配置标准 | 遵循 PandaNetOS 配置规范，与生态各组件对齐 |
| 标准一致性 | 节点注册、任务领取、心跳、状态上报端点与《PandaNetOS 标准规范》严格一致 |

### 构建信息注入（能力清单标准 3.1）

编译期自动注入统一构建信息，随 Capability Manifest 输出：

`BUILD_TIME` `GIT_COMMIT` `GIT_BRANCH` `RUSTC_VERSION` `TARGET_TRIPLE` `BUILD_PROFILE`

---

## 版本与平台

| 项 | 值 |
|---|---|
| 当前版本 | **v1.1.1** |
| 发布通道 | GitHub Actions Tag（`v*`）自动构建 Release |
| 平台矩阵 | Windows x86_64 · Linux x86_64 musl · Linux aarch64 musl · macOS x86_64 · macOS aarch64 |

---

## 核心特性

### 下载引擎
- **多连接分片下载**：单文件多连接并发，连接数可配，充分跑满带宽
- **分片级失败重试**：失败 chunk 自动重试，次数可配，最终状态精确标记
- **断点续传**：从已下载位置继续，节省时间与流量
- **多协议后端**：HTTP / HTTPS / SSH / SFTP / 本地文件；可编译特性扩充 FTP、Torrent / Magnet
- **细粒度控制**：连接数、分片大小、速度上限、单任务超时——全局与任务级双层配置
- **代理与 TLS**：HTTP(S) 代理支持；跳过 TLS 证书校验（自签名环境）

### Agent 模式（PK 主控集中调度）
- **自动节点注册**：上报 hostname / 平台 / 架构 / 版本，获取永久 `node_id`
- **局域网自动发现**：未指定主控时扫描 5566 / 8080 / 80 / 8000 / 3000 端口
- **WebSocket 实时通道**：长连接接收指令，断线 3 秒自动重连
- **动态任务同步**：`config_changed` 触发增量拉取，任务增删即时生效
- **状态心跳**：每 10 秒上报活跃任务数、累计流量、忙碌状态、最近错误
- **任务级回报**：开始 / 完成 / 失败全程回传（dispatch_id、分片状态、平均速度、错误详情）
- **鉴权体系**：HTTP API 携带 Bearer Token；WebSocket 按 `node_id` 识别节点

### 运行保障
- **启动自检**：目录与配置完整性自动校验，缺失自动生成最小模板
- **节点永久标识**：`data/node-id.json`
- **流水持久化**：`data/run-history.jsonl` 只追加、不修改
- **优雅退出**：SIGINT / SIGTERM 捕获
- **dry-run 测速**：数据不落盘，仅统计速度与流量

---

## 快速开始

### 模式一：本地独立运行

```bash
# 首次启动自动生成 spde-node/ 目录与配置模板
./spde serve

# 按需编辑配置
vim spde-node/config/config.yaml

# 重新运行
./spde serve
```

### 模式二：接入 PK 主控

```bash
# 指定主控地址与 Token
./spde agent --master http://10.0.0.8:5566 --token your_token

# 或预先配置 controller 段，直接启动
./spde agent

# 未指定 master 时自动扫描局域网
./spde agent
```

---

## 目录结构

工作目录固定于二进制同级目录 `spde-node/`：

```
spde-node/
├── bin/                   # 二进制目录
├── config/
│   └── config.yaml        # 主配置文件
└── data/
    ├── node-id.json       # 节点永久唯一标识
    └── run-history.jsonl  # 下载历史流水（追加写入）
```

---

## 配置体系

### agent — Agent 模式配置

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `master` | string | `""` | PK 主控地址；可用 `--master` 覆盖 |
| `node_id` | string/null | `null` | 节点 UUID；留空使用本地 `node-id.json` |
| `heartbeat_interval_secs` | int | `5` | 心跳间隔（秒） |

### global — 全局配置

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `work_dir` | string/null | `null` | 数据目录；相对 `spde-node/`，null 用 `spde-node/data/` |
| `max_concurrent` | int | `4` | 最大并发下载任务数 |
| `resume` | bool | `true` | 断点续传开关 |
| `retry_times` | int | `3` | 分片失败重试次数 |
| `timeout` | int | `1800` | 单任务超时（秒） |
| `skip_tls_verify` | bool | `false` | 跳过 TLS 证书校验 |
| `connections_per_file` | int | `8` | 单文件多连接数（越大越快，受服务端与带宽限制） |
| `dry_run` | bool | `false` | 试运行：不落盘，仅统计速度与流量 |

### output / proxy / controller

| 段 | 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|---|
| output | `save_path` | string | `"./download"` | 保存目录；相对 `spde-node/` |
| proxy | `http_proxy` | string | `""` | HTTP 代理地址 |
| proxy | `https_proxy` | string | `""` | HTTPS 代理地址 |
| controller | `url` | string | `""` | PK 主控地址；优先级低于 `--master` |
| controller | `token` | string | `""` | Bearer Token；优先级低于 `--token` |

### direct_tasks — 下载任务列表

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `name` | string | — | 任务名称（显示用） |
| `enable` | bool | `true` | 是否启用（false 跳过） |
| `url` | string | — | 下载地址（建议支持 Range 的直链） |
| `filename` | string | — | 本地保存文件名 |
| `task_id` | string/null | `null` | 任务唯一标识（Agent 模式由 PK 下发） |
| `dispatch_id` | string/null | `null` | 调度实例标识（Agent 模式用于同步与回报） |

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

---

## CLI 命令

| 命令 | 说明 |
|---|---|
| `spde serve` | 本地下载服务，执行全部启用任务并展示实时进度与汇总 |
| `spde agent [--master URL] [--token TOKEN]` | 接入 PK 主控；未指定 master 自动扫描局域网 |
| `spde config` | 配置管理 |
| `spde stats` | 统计查询 |
| `spde --help` / `spde --version` | 帮助 / 版本 |

### serve 输出示意

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

### agent 输出示意

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

---

## 通信协议

SPDE Agent 与 PK 主控通过 **HTTP REST + WebSocket** 双通道通信。

### HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/agent/register` | 节点注册，返回 `node_id` 与轮询间隔 |
| GET | `/api/v1/nodes/{node_id}/config.yaml` | 拉取节点最新下载配置 |
| GET | `/api/v1/overview` | 局域网发现时的服务端探测 |

### WebSocket 接口

连接：`ws(s)://{master}/api/v1/agent/ws?node_id={node_id}`

**客户端 → 服务端**

| type | 字段 | 说明 |
|---|---|---|
| `pong` | — | 心跳响应 |
| `status` | `active_tasks`, `bytes_downloaded`, `busy`, `last_error` | 每 10 秒状态上报 |
| `task_started` | `dispatch_id` | 任务开始通知 |
| `task_report` | `dispatch_id`, `task_id`, `task_name`, `url`, `filename`, `file_size`, `downloaded_bytes`, `elapsed_secs`, `avg_speed_mbps`, `status`, `success_chunks`, `failed_chunks`, `error_msg` | 完成 / 失败详细回报 |

**服务端 → 客户端**

| type | 说明 |
|---|---|
| `ping` | 心跳探测 |
| `config_changed` | 配置变更通知，触发重新拉取与任务同步 |

---

## 数据记录（run-history.jsonl）

每条记录为一行 JSON：

| 字段 | 类型 | 说明 |
|---|---|---|
| `timestamp` | string | 完成时间（ISO 8601 带时区） |
| `task_name` | string | 任务名称 |
| `url` | string | 下载地址 |
| `filename` | string | 文件名 |
| `file_size` | int | 文件总大小（字节） |
| `downloaded_bytes` | int | 实际下载字节数（跳过为 0） |
| `elapsed_secs` | float | 耗时（秒） |
| `avg_speed_mbps` | float | 平均速度（MB/s） |
| `status` | string | `success` / `skipped` / `failed` |
| `success_chunks` | int | 成功分片数 |
| `failed_chunks` | int | 失败分片数 |
| `error_msg` | string/null | 失败原因（成功为 null） |

---

## 构建与发布

### 本地编译

```bash
# 当前平台
cargo build --release

# Linux x86_64 musl 静态链接
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

> 跨平台交叉编译（Linux musl）依赖 `cross` 工具链，预装 perl 以满足 vendored OpenSSL 编译要求，见 `Cross.toml`。

### GitHub Actions 自动发布

推送 `v*` tag 触发（`.github/workflows/release.yml`）：

1. **五平台并行编译**：Windows x86_64 / Linux x86_64 musl / Linux aarch64 musl / macOS x86_64 / macOS aarch64
2. **构建加速**：sccache + rust-cache；Linux musl 目标走 `cross` 容器化编译
3. **共享库拉取**：自动 checkout `PandaNetOS/PandaNetOS` 并修正 path 依赖
4. **产物命名**：release 二进制文件名携带版本号（如 `spde-v1.1.1-x86_64-linux-musl`）
5. **自动 Release**：生成 Release Notes 并上传全部平台二进制
6. **生态联动**：发布完成后触发 `pcdn-keeper` 上游重建（`repository_dispatch`）

```bash
git tag -a v1.1.1 -m "SPDE v1.1.1"
git push origin v1.1.1
```

下载产物：<https://github.com/pandamelive/spde/releases>

---

## 许可证

MIT
