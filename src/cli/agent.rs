use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::cli::config::SpdeConfig;
use crate::cli::discover;
use crate::cli::history::get_or_create_node_id;
use crate::cli::paths::SpdePaths;
use crate::cli::ws_client::{TaskReportParams, WsClient};
use crate::download_file;

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
}

#[derive(Debug, Deserialize)]
struct RegisterResp {
    node_id: Uuid,
    poll_interval_secs: u64,
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

pub async fn run_agent(paths: &SpdePaths, master_arg: String, token_arg: String) -> Result<()> {
    // 1. 加载本地 config（取 controller.url/token 作为初始值）
    let local_cfg = crate::cli::config::load_config(&paths.config_file)
        .map_err(|e| anyhow::anyhow!("load local config: {e}"))?;

    // 2. 确定 master 地址：--master > 本地 config.controller.url > 局域网扫描 > 等待
    let master = if !master_arg.is_empty() {
        master_arg.trim_end_matches('/').to_string()
    } else if !local_cfg.controller.url.is_empty() {
        local_cfg.controller.url.trim_end_matches('/').to_string()
    } else {
        eprintln!("[agent] no master specified, scanning local network ...");
        discover::discover_pk_wait(SCAN_PORTS).await?
    };

    let token = if !token_arg.is_empty() {
        token_arg
    } else {
        local_cfg.controller.token.clone()
    };

    eprintln!("[agent] master = {}", master);

    // 3. 获取或生成 node_id
    let node_id = get_or_create_node_id(&paths.node_id_file)?;
    let hostname = hostname_string();
    let (platform, arch) = platform_pair();

    // 4. register 到 PK
    let api = api_client(&token)?;
    let reg: RegisterResp = api
        .post(format!("{master}/api/v1/agent/register"))
        .json(&RegisterReq {
            node_id: Some(node_id),
            hostname,
            platform,
            arch,
            version: VERSION.into(),
            labels: vec![],
        })
        .send()
        .await
        .context("register request")?
        .error_for_status()
        .context("register rejected")?
        .json()
        .await
        .context("register json")?;

    let node_id = reg.node_id;
    eprintln!("[agent] registered node_id={}", node_id);

    // 5. 启动 WebSocket 客户端
    let ws = WsClient::spawn(node_id, master.clone(), token.clone(), paths.base_dir.clone());

    // 6. 共享状态
    let active = Arc::new(AtomicU32::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let last_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let running: Arc<Mutex<HashMap<Uuid, JoinHandle<()>>>> = Arc::new(Mutex::new(HashMap::new()));

    // 7. 定期状态上报（通过 WebSocket）
    {
        let ws = ws.clone();
        let active = active.clone();
        let bytes_total = bytes_total.clone();
        let last_error = last_error.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                let err = last_error.lock().await.clone();
                ws.send_status(
                    active.load(Ordering::Relaxed),
                    bytes_total.load(Ordering::Relaxed),
                    active.load(Ordering::Relaxed) > 0,
                    err.as_deref(),
                )
                .await;
            }
        });
    }

    // 8. 主循环：等待 config 变化 → 拉 config → 同步任务
    loop {
        // 拉取 PK 生成的 config.yaml
        match fetch_config(&api, &master, node_id).await {
            Ok(pk_cfg) => {
                eprintln!(
                    "[agent] config fetched: {} tasks, max_concurrent={}",
                    pk_cfg.direct_tasks.len(),
                    pk_cfg.global.max_concurrent
                );

                // 全局并发信号量（max_concurrent 是节点级，不按任务覆盖）
                let sem = Arc::new(Semaphore::new(pk_cfg.global.max_concurrent.max(1) as usize));

                // 同步任务（每个任务自己解析覆盖参数）
                sync_tasks(
                    &pk_cfg,
                    &running,
                    &ws,
                    &active,
                    &bytes_total,
                    &last_error,
                    &sem,
                    &paths.base_dir,
                    node_id,
                )
                .await;
            }
            Err(e) => {
                eprintln!("[agent] fetch config failed: {e}");
            }
        }

        // 等待下一次 config 变化通知（或 ctrl+c）
        tokio::select! {
            _ = ws.wait_config_change() => {
                eprintln!("[agent] config changed, re-fetching");
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("[agent] received exit signal, stopping");
                break;
            }
        }
    }

    // 清理：取消所有运行中任务
    let mut run = running.lock().await;
    for (dispatch_id, handle) in run.drain() {
        eprintln!("[agent] aborting task dispatch_id={}", dispatch_id);
        handle.abort();
    }

    Ok(())
}

// ── 拉取 config ─────────────────────────────────────────

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
    task: &crate::cli::config::TaskItem,
    cfg: &SpdeConfig,
    base_dir: &PathBuf,
) -> TaskParams {
    let connections = task
        .connections_per_file
        .unwrap_or(cfg.global.connections_per_file);
    let retry = task.retry_times.unwrap_or(cfg.global.retry_times);
    let dry_run = task.dry_run.unwrap_or(cfg.global.dry_run);
    let timeout = task.timeout.unwrap_or(cfg.global.timeout);
    let skip_tls_verify = task.skip_tls_verify.unwrap_or(cfg.global.skip_tls_verify);
    let save_path = task.save_path.as_deref().unwrap_or(&cfg.output.save_path);
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

// ── 任务同步 ─────────────────────────────────────────────

async fn sync_tasks(
    cfg: &SpdeConfig,
    running: &Arc<Mutex<HashMap<Uuid, JoinHandle<()>>>>,
    ws: &WsClient,
    active: &Arc<AtomicU32>,
    bytes_total: &Arc<AtomicU64>,
    last_error: &Arc<Mutex<Option<String>>>,
    sem: &Arc<Semaphore>,
    base_dir: &PathBuf,
    node_id: Uuid,
) {
    let tasks = &cfg.direct_tasks;
    let enabled_ids: HashSet<Uuid> = tasks
        .iter()
        .filter(|t| t.enable)
        .filter_map(|t| t.dispatch_id)
        .collect();

    let mut run = running.lock().await;

    // 停止消失的任务
    run.retain(|dispatch_id, handle| {
        if !enabled_ids.contains(dispatch_id) {
            eprintln!("[agent] cancel removed task dispatch_id={}", dispatch_id);
            handle.abort();
            false
        } else {
            true
        }
    });

    // 启动新任务
    for task in tasks {
        if !task.enable {
            continue;
        }
        let Some(dispatch_id) = task.dispatch_id else {
            continue;
        };
        if run.contains_key(&dispatch_id) {
            continue;
        }

        let params = resolve_task_params(task, cfg, base_dir);
        let handle = spawn_download_task(
            dispatch_id,
            task.task_id,
            task.name.clone(),
            task.url.clone(),
            task.filename.clone(),
            params,
            ws.clone(),
            active.clone(),
            bytes_total.clone(),
            last_error.clone(),
            sem.clone(),
            node_id,
        );
        run.insert(dispatch_id, handle);
        eprintln!("[agent] started task {} (dispatch_id={})", task.name, dispatch_id);
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_download_task(
    dispatch_id: Uuid,
    task_id: Option<Uuid>,
    name: String,
    url: String,
    filename: String,
    params: TaskParams,
    ws: WsClient,
    active: Arc<AtomicU32>,
    bytes_total: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    sem: Arc<Semaphore>,
    _node_id: Uuid,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Ok(permit) = sem.acquire_owned().await else {
            return;
        };
        active.fetch_add(1, Ordering::Relaxed);

        // 任务级参数构建客户端和保存目录
        let dl = match build_task_client(&params) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                eprintln!("[download] build client failed for {}: {}", name, e);
                active.fetch_sub(1, Ordering::Relaxed);
                drop(permit);
                return;
            }
        };
        let _ = tokio::fs::create_dir_all(&params.save_dir).await;

        // 通知 PK 任务开始
        ws.send_task_started(dispatch_id).await;

        let file_path = params.save_dir.join(&filename);
        eprintln!("[download] start {} -> {:?}", name, file_path);
        let started = Instant::now();

        let result = download_file(&dl, &url, file_path, params.connections, params.retry, params.dry_run).await;

        let (status, file_size, downloaded, elapsed, chunks_ok, chunks_fail, err_msg) =
            match result {
                Ok(m) => (
                    m.status,
                    m.total_size,
                    m.downloaded_bytes,
                    m.elapsed_secs,
                    m.success_chunks,
                    m.failed_chunks,
                    m.error_msg,
                ),
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

        eprintln!(
            "[download] done {} status={} downloaded={}MB avg={:.1}MB/s",
            name,
            status,
            downloaded / 1024 / 1024,
            avg
        );

        // 通过 WebSocket 回报告
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
    })
}
