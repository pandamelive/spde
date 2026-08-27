use anyhow::{Context, Result};
use chrono::Local;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::cli::config::SpdeConfig;
use crate::cli::discover;
use crate::cli::history::get_or_create_node_id;
use crate::cli::paths::SpdePaths;
use crate::cli::ws_client::{TaskProgressParams, TaskReportParams, WsClient};
use crate::downloader::{build_default_manager, DownloadController, DownloadOutput, DownloadTask, ProgressCallback, ProgressSnapshot};

macro_rules! log {
    ($($arg:tt)*) => {{
        let ts = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        std::eprint!("[{}] ", ts);
        std::eprintln!($($arg)*);
    }};
}

const VERSION: &str = env!("CARGO_PKG_VERSION");
const SCAN_PORTS: &[u16] = &[5566, 8080, 80, 8000, 3000];

// ── API 类型 ─────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct RegisterReq {
    node_id: Option<Uuid>,
    hostname: String,
    platform: String,
    arch: String,
    version: String,
    labels: Vec<String>,
    /// 节点能力参数（JSON，灵活扩展，pk 不认识的字段透传）
    capabilities: Option<serde_json::Value>,
    /// 节点上报的最大并发任务数
    max_concurrent: Option<u32>,
    /// 节点上报的最大带宽上限 bps
    max_bandwidth_bps: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct RegisterResp {
    node_id: Uuid,
    poll_interval_secs: u64,
    /// 节点注册后的状态（online/pending）
    #[serde(default)]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiResp<T> {
    data: T,
}

/// 从 PK 领取到的任务详情
#[derive(Debug, Deserialize, Clone)]
struct ClaimResp {
    dispatch_id: Uuid,
    task_id: Uuid,
    name: String,
    url: String,
    filename: String,
    overrides: ClaimOverrides,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(dead_code)]
struct ClaimOverrides {
    #[serde(default)]
    max_concurrent: Option<u32>,
    #[serde(default)]
    connections_per_file: Option<u32>,
    #[serde(default)]
    retry_times: Option<u32>,
    #[serde(default)]
    timeout: Option<u64>,
    #[serde(default)]
    skip_tls_verify: Option<bool>,
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    save_path: Option<String>,
}

// ── 实时进度共享状态 ─────────────────────────────────────
#[derive(Clone)]
struct TaskProgressState {
    dispatch_id: Uuid,
    task_name: String,
    total_size: u64,
    downloaded_bytes: u64,
    speed_bps: u64,
    percent: f64,
    active_connections: u32,
    elapsed_secs: f64,
}

type ProgressMap = Arc<Mutex<HashMap<Uuid, TaskProgressState>>>;

/// WebSocket 进度回调：更新共享状态 + 推送进度消息给 PK
struct WsProgress {
    ws: WsClient,
    dispatch_id: Uuid,
    task_name: String,
    progress_map: ProgressMap,
}

impl ProgressCallback for WsProgress {
    fn on_progress(&self, s: ProgressSnapshot) {
        let state = TaskProgressState {
            dispatch_id: self.dispatch_id,
            task_name: self.task_name.clone(),
            total_size: s.total_size,
            downloaded_bytes: s.downloaded_bytes,
            speed_bps: s.speed_bps,
            percent: s.percent,
            active_connections: s.active_connections,
            elapsed_secs: s.elapsed_secs,
        };

        // 更新共享状态（供 status_loop 汇总总速度）
        let map = self.progress_map.clone();
        let dispatch_id = self.dispatch_id;
        tokio::spawn(async move {
            map.lock().await.insert(dispatch_id, state);
        });

        // 推送单任务进度给 PK
        let ws = self.ws.clone();
        let task_name = self.task_name.clone();
        let dispatch_id = self.dispatch_id;
        let percent = s.percent;
        let downloaded_bytes = s.downloaded_bytes;
        let total_size = s.total_size;
        let speed_bps = s.speed_bps;
        let active_connections = s.active_connections;
        let elapsed_secs = s.elapsed_secs;
        tokio::spawn(async move {
            ws.send_task_progress(TaskProgressParams {
                dispatch_id,
                task_name: &task_name,
                percent,
                downloaded_bytes,
                total_size,
                speed_bps,
                active_connections,
                elapsed_secs,
            })
            .await;
        });
    }

    fn on_complete(&self, _output: DownloadOutput) {
        // 任务完成时从共享状态移除
        let map = self.progress_map.clone();
        let dispatch_id = self.dispatch_id;
        tokio::spawn(async move {
            map.lock().await.remove(&dispatch_id);
        });
    }
}

// ── 工具函数 ─────────────────────────────────────────────

fn platform_pair() -> (String, String) {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let platform = match (os, arch) {
        ("windows", "x86_64") => "windows-x86_64",
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-aarch64",
        ("macos", "x86_64") => "macos-x86_64",
        ("macos", "aarch64") => "macos-aarch64",
        _ => "unknown",
    };
    (platform.into(), arch.into())
}

fn hostname_string() -> String {
    hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "unknown".into())
}

fn api_client(token: &str) -> Result<Client> {
    let mut b = Client::builder().timeout(std::time::Duration::from_secs(30));
    if !token.is_empty() {
        let mut headers = reqwest::header::HeaderMap::new();
        let val = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .context("invalid token")?;
        headers.insert(reqwest::header::AUTHORIZATION, val);
        b = b.default_headers(headers);
    }
    Ok(b.build()?)
}

// ── 主入口 ───────────────────────────────────────────────

/// 注册到 PK（可重复调用，用于定期重新注册或 WebSocket 重连后重新注册）
async fn register_to_pk(
    api: &Client,
    master: &str,
    node_id: Uuid,
    hostname: &str,
    platform: &str,
    arch: &str,
    local_max_concurrent: u32,
) -> Result<RegisterResp> {
    let reg: ApiResp<RegisterResp> = api
        .post(format!("{master}/api/v1/agent/register"))
        .json(&RegisterReq {
            node_id: Some(node_id),
            hostname: hostname.to_string(),
            platform: platform.to_string(),
            arch: arch.to_string(),
            version: VERSION.into(),
            labels: vec![],
            capabilities: Some(build_node_capabilities()),
            max_concurrent: Some(local_max_concurrent),
            max_bandwidth_bps: None,
        })
        .send()
        .await
        .context("register request")?
        .error_for_status()
        .context("register rejected")?
        .json()
        .await
        .context("register json")?;
    Ok(reg.data)
}

pub async fn run_agent(paths: &SpdePaths, master_arg: String, token_arg: String) -> Result<()> {
    // 1. 加载本地 config
    let local_cfg = crate::cli::config::load_config(&paths.config_file)
        .map_err(|e| anyhow::anyhow!("load local config: {e}"))?;

    // 2. 确定 master 地址
    let master = if !master_arg.is_empty() {
        master_arg.trim_end_matches('/').to_string()
    } else if !local_cfg.controller.url.is_empty() {
        local_cfg.controller.url.trim_end_matches('/').to_string()
    } else {
        log!("[agent] no master specified, scanning local network ...");
        discover::discover_pk_wait(SCAN_PORTS).await?
    };

    let token = if !token_arg.is_empty() {
        token_arg
    } else {
        local_cfg.controller.token.clone()
    };

    log!("[agent] master = {}", master);

    // 3. 获取或生成 node_id
    let node_id = get_or_create_node_id(&paths.node_id_file)?;
    let hostname = hostname_string();
    let (platform, arch) = platform_pair();

    // 4. register 到 PK（先从本地配置读取 max_concurrent 上报，注册后 pk 下发的值会覆盖）
    let local_max_concurrent = local_cfg.global.max_concurrent.max(1);
    let api = api_client(&token)?;
    let reg = register_to_pk(&api, &master, node_id, &hostname, &platform, &arch, local_max_concurrent).await?;

    let node_id = reg.node_id;
    if let Some(status) = &reg.status {
        log!("[agent] registered node_id={}, status={}", node_id, status);
        if status == "pending" {
            log!("[agent] 节点待审批，将定期重新注册等待 pk 同意");
        }
    } else {
        log!("[agent] registered node_id={}", node_id);
    }

    // 5. 启动 WebSocket 客户端
    let ws = WsClient::spawn(node_id, master.clone(), token.clone(), paths.base_dir.clone());

    // 6. 拉取全局配置（max_concurrent / dry_run / save_path 等）
    let global_cfg = match fetch_config(&api, &master, node_id).await {
        Ok(cfg) => {
            log!(
                "[agent] global config: max_concurrent={}, dry_run={}, save_path={}",
                cfg.global.max_concurrent, cfg.global.dry_run, cfg.output.save_path
            );
            cfg
        }
        Err(e) => {
            log!("[agent] fetch global config failed, using local defaults: {e}");
            local_cfg.clone()
        }
    };
    let global_cfg = Arc::new(Mutex::new(global_cfg));

    // 7. 共享状态
    let active = Arc::new(AtomicU32::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let last_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let progress_map: ProgressMap = Arc::new(Mutex::new(HashMap::new()));
    let running: Arc<Mutex<HashMap<Uuid, (JoinHandle<()>, Arc<DownloadController>)>>> = Arc::new(Mutex::new(HashMap::new()));

    // 全局并发信号量
    let max_concurrent = {
        let cfg = global_cfg.lock().await;
        cfg.global.max_concurrent.max(1) as usize
    };
    let sem = Arc::new(Semaphore::new(max_concurrent));

    // 任务完成通知通道
    let (task_done_tx, mut task_done_rx) = mpsc::channel::<()>(64);

    // 8. 定期状态上报（通过 WebSocket）
    {
        let ws = ws.clone();
        let active = active.clone();
        let bytes_total = bytes_total.clone();
        let last_error = last_error.clone();
        let progress_map = progress_map.clone();
        // 状态上报间隔：复用 agent.heartbeat_interval_secs，默认10秒
        let report_interval = {
            let cfg = global_cfg.lock().await;
            cfg.agent.heartbeat_interval_secs.max(1)
        };
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(report_interval)).await;
                // 汇总所有活跃任务的总速度
                let total_speed: u64 = {
                    let map = progress_map.lock().await;
                    map.values().map(|s| s.speed_bps).sum()
                };
                let err = last_error.lock().await.clone();
                ws.send_status(
                    active.load(Ordering::Relaxed),
                    bytes_total.load(Ordering::Relaxed),
                    active.load(Ordering::Relaxed) > 0,
                    total_speed,
                    err.as_deref(),
                )
                .await;
            }
        });
    }

    // 8.5 定期重新注册（每5分钟），确保节点状态同步，节点被删除后能重新注册进来
    {
        let api = api.clone();
        let master = master.clone();
        let hostname = hostname.clone();
        let platform = platform.clone();
        let arch = arch.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                match register_to_pk(&api, &master, node_id, &hostname, &platform, &arch, local_max_concurrent).await {
                    Ok(reg) => {
                        if let Some(status) = &reg.status {
                            if status == "pending" {
                                log!("[agent] re-register: node still pending, waiting for pk approval");
                            } else {
                                log!("[agent] re-register: ok, status={}", status);
                            }
                        } else {
                            log!("[agent] re-register: ok");
                        }
                    }
                    Err(e) => {
                        log!("[agent] re-register failed: {e}, will retry in 5min");
                    }
                }
            }
        });
    }

    // 9. 主循环：先占并发槽 → claim 领取 → 执行 → 完成后释放槽
    log!("[agent] entering claim loop (max_concurrent={})", max_concurrent);
    loop {
        // 检查节点是否被 pk 删除，如果被删除则立即暂停任务并重新注册
        if ws.is_node_deleted() {
            log!("[agent] node deleted by pk, pausing all tasks and re-registering...");
            match register_to_pk(&api, &master, node_id, &hostname, &platform, &arch, local_max_concurrent).await {
                Ok(reg) => {
                    if let Some(status) = &reg.status {
                        if status == "online" {
                            log!("[agent] re-register success, status=online, resuming normal operation");
                            ws.clear_node_deleted();
                        } else {
                            log!("[agent] re-register status={}, waiting for pk approval, no new tasks will be claimed", status);
                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                        }
                    } else {
                        log!("[agent] re-register ok (no status field), resuming");
                        ws.clear_node_deleted();
                    }
                }
                Err(e) => {
                    log!("[agent] re-register failed: {e}, retry in 10s");
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                }
            }
            continue;
        }

        // 先获取 permit，确保不会超额领取（permit 在下载 task 内部释放）
        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };

        match claim_task(&api, &master, node_id).await {
            Ok(Some(task)) => {
                log!(
                    "[agent] claimed task: {} (dispatch_id={})",
                    task.name, task.dispatch_id
                );
                let cfg = global_cfg.lock().await.clone();
                let handle = spawn_download_task(
                    task,
                    cfg,
                    ws.clone(),
                    active.clone(),
                    bytes_total.clone(),
                    last_error.clone(),
                    progress_map.clone(),
                    permit,
                    paths.base_dir.clone(),
                    task_done_tx.clone(),
                );
                let dispatch_id = handle.0;
                running.lock().await.insert(dispatch_id, (handle.1, handle.2));
            }
            Ok(None) => {
                // 池子空，释放 permit 后等待通知
                drop(permit);
                tokio::select! {
                    _ = ws.wait_new_task() => {
                        log!("[agent] new task notification, trying claim");
                    }
                    _ = ws.wait_node_deleted() => {
                        log!("[agent] node deleted notification received, will re-register immediately");
                    }
                    _ = ws.wait_config_change() => {
                        if let Ok(cfg) = fetch_config(&api, &master, node_id).await {
                            log!("[agent] global config updated");
                            *global_cfg.lock().await = cfg;
                        }
                    }
                    _ = task_done_rx.recv() => {}
                    _ = tokio::signal::ctrl_c() => {
                        log!("[agent] received exit signal, stopping");
                        break;
                    }
                }
            }
            Err(e) => {
                drop(permit);
                log!("[agent] claim failed: {e}, retry in 5s");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }

    // 清理：取消所有运行中任务
    let mut run = running.lock().await;
    for (dispatch_id, (handle, controller)) in run.drain() {
        log!("[agent] aborting task dispatch_id={}", dispatch_id);
        controller.cancel(); // 先通知下载器取消
        handle.abort();      // 再强制终止任务
    }

    Ok(())
}

// ── 领取任务 ─────────────────────────────────────────────

async fn claim_task(api: &Client, master: &str, node_id: Uuid) -> Result<Option<ClaimResp>> {
    let url = format!("{master}/api/v1/agent/claim");
    let resp = api
        .post(&url)
        .json(&serde_json::json!({ "node_id": node_id }))
        .send()
        .await
        .context("claim request")?;

    if resp.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(None);
    }

    let task: ApiResp<ClaimResp> = resp
        .error_for_status()
        .context("claim rejected")?
        .json()
        .await
        .context("claim json")?;
    Ok(Some(task.data))
}

// ── 拉取全局 config ──────────────────────────────────────

async fn fetch_config(api: &Client, master: &str, node_id: Uuid) -> Result<SpdeConfig> {
    let url = format!("{master}/api/v1/nodes/{node_id}/config.yaml");
    let text = api
        .get(&url)
        .send()
        .await
        .context("fetch config request")?
        .error_for_status()
        .context("fetch config rejected")?
        .text()
        .await
        .context("fetch config body")?;
    let cfg: SpdeConfig = serde_yaml::from_str(&text).context("parse config yaml")?;
    Ok(cfg)
}

/// 构建节点能力参数（上报给 pk，pk 不认识的字段透传）
pub(crate) fn build_node_capabilities() -> serde_json::Value {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static CORES: AtomicUsize = AtomicUsize::new(0);
    let cores = if CORES.load(Ordering::Relaxed) == 0 {
        let c = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        CORES.store(c, Ordering::Relaxed);
        c
    } else {
        CORES.load(Ordering::Relaxed)
    };

    // 支持的协议（根据编译 feature 动态）
    let mut protocols = vec!["http", "https", "ssh", "sftp", "file"];
    #[cfg(feature = "ftp")]
    {
        protocols.push("ftp");
    }
    #[cfg(feature = "torrent")]
    {
        protocols.push("torrent");
        protocols.push("magnet");
    }

    // 支持的 URI 格式
    let mut uri_formats = vec!["http://", "https://", "ssh://", "sftp://", "file://"];
    #[cfg(feature = "ftp")]
    {
        uri_formats.push("ftp://");
    }
    #[cfg(feature = "torrent")]
    {
        uri_formats.push("magnet:?xt=urn:btih:");
    }

    serde_json::json!({
        // ── 一、基本信息 ──
        "basic": {
            "name": "spde",
            "version": VERSION,
            "rust_version": rustc_version_runtime(),
            "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "description": "Super Download Engine - 多协议高性能下载引擎",
        },

        // ── 二、支持的协议 ──
        "supported_protocols": protocols,
        "uri_formats": uri_formats,

        // ── 三、下载功能特性 ──
        "download_features": {
            "resume": true,                    // 断点续传
            "multi_connection": true,          // 多连接分片下载
            "work_stealing": true,             // 工作窃取式调度
            "chunked_download": true,          // 分片下载
            "retry": true,                     // 自动重试
            "proxy": true,                     // 代理支持
            "tls_skip_verify": true,           // 跳过 TLS 证书校验
            "dry_run": true,                   // 干跑模式（不写盘）
            "speed_limit": true,               // 速度限制
            "preallocate": true,               // 文件预分配
            "progress_callback": true,         // 进度回调
            "realtime_progress": true,         // 实时进度上报
            "auto_connections": true,          // 自动连接数估算
            "custom_headers": true,            // 自定义 HTTP Headers
        },

        // ── 四、任务控制能力 ──
        "task_control": {
            "pause": true,                     // 暂停单个任务
            "resume": true,                    // 恢复单个任务
            "cancel": true,                    // 取消单个任务
            "pause_all": true,                 // 暂停所有任务
            "resume_all": true,                // 恢复所有任务
            "cancel_all": true,                // 取消所有任务
            "controller": "DownloadController", // 统一控制器（Arc<AtomicBool>）
            "pause_check_interval_ms": 100,    // 暂停检查间隔
        },

        // ── 五、可配置参数（pk 可下发覆盖） ──
        "configurable_params": {
            "max_concurrent": {
                "type": "u32",
                "default": 4,
                "min": 1,
                "max": 256,
                "description": "最大并发下载任务数",
            },
            "connections_per_file": {
                "type": "u32",
                "default": 16,
                "min": 1,
                "max": 128,
                "description": "每个文件的最大并发连接数",
            },
            "chunk_size": {
                "type": "u64",
                "default": 4194304,
                "unit": "bytes",
                "description": "分片大小（默认 4MB）",
            },
            "retry_times": {
                "type": "u32",
                "default": 3,
                "min": 0,
                "max": 100,
                "description": "下载失败重试次数",
            },
            "timeout_secs": {
                "type": "u64",
                "default": 30,
                "description": "连接/读取超时时间（秒）",
            },
            "speed_limit_bps": {
                "type": "u64",
                "default": 0,
                "unit": "bytes/sec",
                "description": "速度限制（0 = 不限速）",
            },
            "progress_interval_ms": {
                "type": "u64",
                "default": 500,
                "description": "进度上报间隔（毫秒）",
            },
            "save_path": {
                "type": "string",
                "default": "./download",
                "description": "默认保存路径",
            },
            "dry_run": {
                "type": "bool",
                "default": false,
                "description": "干跑模式（不写盘，只测试下载）",
            },
            "skip_tls_verify": {
                "type": "bool",
                "default": false,
                "description": "跳过 TLS 证书校验",
            },
            "proxy": {
                "type": "string",
                "default": "",
                "description": "代理地址（空 = 不使用代理）",
            },
        },

        // ── 六、硬件信息 ──
        "hardware": {
            "cpu_cores": cores,
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "family": std::env::consts::FAMILY,
        },

        // ── 七、通信能力 ──
        "communication": {
            "websocket": true,                 // WebSocket 实时通信
            "http_api": true,                  // HTTP API 通信
            "heartbeat": true,                 // 心跳上报
            "realtime_status": true,           // 实时状态上报
            "task_progress_report": true,      // 单任务进度上报
            "config_pull": true,               // 配置拉取（拉模式）
            "task_claim": true,                // 任务领取（拉模式）
            "heartbeat_interval_secs": 10,     // 默认心跳间隔
            "websocket_reconnect_secs": 3,     // WebSocket 重连间隔
        },

        // ── 八、状态上报字段 ──
        "status_report_fields": {
            "node_level": ["active_tasks", "bytes_downloaded", "total_speed_bps", "last_error"],
            "task_level": ["dispatch_id", "task_name", "percent", "speed_bps", "downloaded_bytes", "total_size", "active_connections", "elapsed_secs"],
        },

        // ── 九、运行模式 ──
        "run_modes": ["agent", "standalone", "cli"],
        "current_mode": "agent",

        // ── 十、编译 feature ──
        "compile_features": {
            "ftp": cfg!(feature = "ftp"),
            "torrent": cfg!(feature = "torrent"),
            "default_features": true,
        },
    })
}

/// 获取 Rust 编译器版本（编译时注入）
fn rustc_version_runtime() -> &'static str {
    option_env!("RUSTC_VERSION").unwrap_or("unknown")
}

fn resolve_save_dir(base_dir: &PathBuf, save_path: &str) -> PathBuf {
    let p = PathBuf::from(save_path);
    if p.is_absolute() {
        p
    } else {
        base_dir.join(p)
    }
}

// ── 任务级参数解析 ──────────────────────────────────────

struct TaskParams {
    connections: u32,
    retry: u32,
    dry_run: bool,
    timeout: u64,
    skip_tls_verify: bool,
    save_dir: PathBuf,
    http_proxy: String,
    https_proxy: String,
}

fn resolve_task_params(
    overrides: &ClaimOverrides,
    cfg: &SpdeConfig,
    base_dir: &PathBuf,
) -> TaskParams {
    let connections = overrides
        .connections_per_file
        .unwrap_or(cfg.global.connections_per_file);
    let retry = overrides.retry_times.unwrap_or(cfg.global.retry_times);
    let dry_run = overrides.dry_run.unwrap_or(cfg.global.dry_run);
    let timeout = overrides.timeout.unwrap_or(cfg.global.timeout);
    let skip_tls_verify = overrides.skip_tls_verify.unwrap_or(cfg.global.skip_tls_verify);
    let save_path = overrides
        .save_path
        .as_deref()
        .unwrap_or(&cfg.output.save_path);
    let save_dir = resolve_save_dir(base_dir, save_path);
    TaskParams {
        connections,
        retry,
        dry_run,
        timeout,
        skip_tls_verify,
        save_dir,
        http_proxy: cfg.proxy.http_proxy.clone(),
        https_proxy: cfg.proxy.https_proxy.clone(),
    }
}

// 保留：旧版 per-task HTTP client 构建，现已统一走 DownloadManager
#[allow(dead_code)]
fn build_task_client(p: &TaskParams) -> Result<Client> {
    let mut builder = Client::builder()
        .timeout(std::time::Duration::from_secs(p.timeout))
        .http1_only()
        .tcp_nodelay(true);
    if p.skip_tls_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if !p.https_proxy.trim().is_empty() {
        builder = builder.proxy(reqwest::Proxy::https(p.https_proxy.trim())?);
    }
    if !p.http_proxy.trim().is_empty() {
        builder = builder.proxy(reqwest::Proxy::http(p.http_proxy.trim())?);
    }
    Ok(builder.build()?)
}

// ── 下载任务 ─────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn spawn_download_task(
    task: ClaimResp,
    cfg: SpdeConfig,
    ws: WsClient,
    active: Arc<AtomicU32>,
    bytes_total: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    progress_map: ProgressMap,
    permit: OwnedSemaphorePermit,
    base_dir: PathBuf,
    task_done_tx: mpsc::Sender<()>,
) -> (Uuid, JoinHandle<()>, Arc<DownloadController>) {
    let dispatch_id = task.dispatch_id;
    let task_id = Some(task.task_id);
    let name = task.name.clone();
    let url = task.url.clone();
    let filename = task.filename.clone();
    let params = resolve_task_params(&task.overrides, &cfg, &base_dir);

    let controller = Arc::new(DownloadController::new());
    let ctrl_clone = controller.clone();
    let handle = tokio::spawn(async move {
        let _controller = ctrl_clone; // 移动到任务中，任务结束后自动 drop
        // permit 已在主循环中获取，这里直接持有直到任务结束
        active.fetch_add(1, Ordering::Relaxed);

        // 统一调度器：所有协议自动路由
        let mgr = build_default_manager();

        let _ = tokio::fs::create_dir_all(&params.save_dir).await;

        // 通知 PK 任务开始
        ws.send_task_started(dispatch_id).await;

        let file_path = params.save_dir.join(&filename);
        log!("[download] start {} -> {:?}", name, file_path);
        let started = Instant::now();

        // 构建统一任务：connections=0 时强制单连接以兼容旧配置语义
        let max_conn = if params.connections == 0 { 1 } else { params.connections };
        let task = DownloadTask {
            uri: url.clone(),
            save_path: file_path,
            max_conn,
            retry_times: params.retry,
            dry_run: params.dry_run,
            skip_tls_verify: params.skip_tls_verify,
            ..Default::default()
        };

        // 构建 WS 进度回调：实时推送单任务进度 + 汇总到共享状态
        let progress_cb: Option<Arc<dyn ProgressCallback>> = Some(Arc::new(WsProgress {
            ws: ws.clone(),
            dispatch_id,
            task_name: name.clone(),
            progress_map: progress_map.clone(),
        }));

        let controller = Arc::new(DownloadController::new());
        let result = mgr.dispatch(task, progress_cb, Some(controller.clone())).await;

        let (status, file_size, downloaded, elapsed, chunks_ok, chunks_fail, err_msg) =
            match result {
                Ok(o) => {
                    // 任务成功时清除 last_error
                    if o.is_success {
                        *last_error.lock().await = None;
                    }
                    (
                        o.status,
                        o.total_size,
                        o.downloaded_bytes,
                        o.elapsed_secs,
                        o.success_chunks as u64,
                        o.failed_chunks as u64,
                        o.error_msg,
                    )
                },
                Err(e) => (
                    "failed".into(),
                    0,
                    0,
                    started.elapsed().as_secs_f64(),
                    0,
                    0,
                    Some(e.to_string()),
                ),
            };

        bytes_total.fetch_add(downloaded, Ordering::Relaxed);
        if let Some(ref e) = err_msg {
            *last_error.lock().await = Some(e.clone());
        }

        let avg = if elapsed > 0.0 {
            downloaded as f64 / elapsed / 1024.0 / 1024.0
        } else {
            0.0
        };

        log!(
            "[download] done {} status={} downloaded={}MB avg={:.1}MB/s",
            name,
            status,
            downloaded / 1024 / 1024,
            avg
        );

        ws.send_task_report(TaskReportParams {
            dispatch_id: Some(dispatch_id),
            task_id,
            task_name: &name,
            url: &url,
            filename: &filename,
            file_size,
            downloaded_bytes: downloaded,
            elapsed_secs: elapsed,
            avg_speed_mbps: avg,
            status: &status,
            success_chunks: chunks_ok,
            failed_chunks: chunks_fail,
            error_msg: err_msg.as_deref(),
        })
        .await;

        active.fetch_sub(1, Ordering::Relaxed);
        drop(permit);
        let _ = task_done_tx.send(()).await;
    });

    (dispatch_id, handle, controller)
}
