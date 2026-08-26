use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex, Semaphore};
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
    // 1. 加载本地 config
    let local_cfg = crate::cli::config::load_config(&paths.config_file)
        .map_err(|e| anyhow::anyhow!("load local config: {e}"))?;

    // 2. 确定 master 地址
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

    // 6. 拉取全局配置（max_concurrent / dry_run / save_path 等）
    let global_cfg = match fetch_config(&api, &master, node_id).await {
        Ok(cfg) => {
            eprintln!(
                "[agent] global config: max_concurrent={}, dry_run={}, save_path={}",
                cfg.global.max_concurrent, cfg.global.dry_run, cfg.output.save_path
            );
            cfg
        }
        Err(e) => {
            eprintln!("[agent] fetch global config failed, using local defaults: {e}");
            local_cfg.clone()
        }
    };
    let global_cfg = Arc::new(Mutex::new(global_cfg));

    // 7. 共享状态
    let active = Arc::new(AtomicU32::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let last_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let running: Arc<Mutex<HashMap<Uuid, JoinHandle<()>>>> = Arc::new(Mutex::new(HashMap::new()));

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

    // 9. 主循环：claim 领取 → 并发执行 → 完成后继续领取
    eprintln!("[agent] entering claim loop (max_concurrent={})", max_concurrent);
    loop {
        // 如果还有空闲并发槽，尝试领取任务
        let has_permit = sem.available_permits() > 0;
        if has_permit {
            match claim_task(&api, &master, node_id).await {
                Ok(Some(task)) => {
                    eprintln!(
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
                        sem.clone(),
                        paths.base_dir.clone(),
                        task_done_tx.clone(),
                    );
                    let dispatch_id = handle.0;
                    running.lock().await.insert(dispatch_id, handle.1);
                }
                Ok(None) => {
                    // 池子空，等待新任务通知或任务完成通知或全局配置变更
                    tokio::select! {
                        _ = ws.wait_new_task() => {
                            eprintln!("[agent] new task notification, trying claim");
                        }
                        _ = ws.wait_config_change() => {
                            // 全局配置可能变了，重新拉取
                            if let Ok(cfg) = fetch_config(&api, &master, node_id).await {
                                eprintln!("[agent] global config updated");
                                *global_cfg.lock().await = cfg;
                            }
                        }
                        _ = task_done_rx.recv() => {
                            // 有任务完成了，再试试 claim
                        }
                        _ = tokio::signal::ctrl_c() => {
                            eprintln!("[agent] received exit signal, stopping");
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[agent] claim failed: {e}, retry in 5s");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        } else {
            // 并发满了，等待任务完成
            tokio::select! {
                _ = task_done_rx.recv() => {}
                _ = ws.wait_config_change() => {
                    if let Ok(cfg) = fetch_config(&api, &master, node_id).await {
                        *global_cfg.lock().await = cfg;
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("[agent] received exit signal, stopping");
                    break;
                }
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

    let task: ClaimResp = resp
        .error_for_status()
        .context("claim rejected")?
        .json()
        .await
        .context("claim json")?;
    Ok(Some(task))
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
    sem: Arc<Semaphore>,
    base_dir: PathBuf,
    task_done_tx: mpsc::Sender<()>,
) -> (Uuid, JoinHandle<()>) {
    let dispatch_id = task.dispatch_id;
    let task_id = Some(task.task_id);
    let name = task.name.clone();
    let url = task.url.clone();
    let filename = task.filename.clone();
    let params = resolve_task_params(&task.overrides, &cfg, &base_dir);

    let handle = tokio::spawn(async move {
        let Ok(permit) = sem.acquire_owned().await else {
            let _ = task_done_tx.send(()).await;
            return;
        };
        active.fetch_add(1, Ordering::Relaxed);

        let dl = match build_task_client(&params) {
            Ok(c) => Arc::new(c),
            Err(e) => {
                eprintln!("[download] build client failed for {}: {}", name, e);
                active.fetch_sub(1, Ordering::Relaxed);
                drop(permit);
                let _ = task_done_tx.send(()).await;
                return;
            }
        };
        let _ = tokio::fs::create_dir_all(&params.save_dir).await;

        // 通知 PK 任务开始（claim 时已设为 Running，这里再确认一次）
        ws.send_task_started(dispatch_id).await;

        let file_path = params.save_dir.join(&filename);
        eprintln!("[download] start {} -> {:?}", name, file_path);
        let started = Instant::now();

        let result =
            download_file(&dl, &url, file_path, params.connections, params.retry, params.dry_run)
                .await;

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

    (dispatch_id, handle)
}
