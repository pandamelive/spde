use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::cli::config::{resolve_task_params, SpdeConfig, TaskOverrides};
use crate::cli::discover;
use crate::cli::history::get_or_create_node_id;
use crate::cli::new_download;
use crate::cli::paths::SpdePaths;
use crate::cli::ws_client::WsClient;
use crate::service::controller::DownloadController;
use pandanetos::protocol::{paths, RegisterReq, RegisterResp};
use pandanetos::response::ApiResponse;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const SCAN_PORTS: &[u16] = &[5566, 8080, 80, 8000, 3000];

// ── API 类型 ─────────────────────────────────────────────
// 注册请求/响应直接使用共享库 [`pandanetos::protocol::{RegisterReq, RegisterResp}`]

/// 从 PK 领取到的任务详情
#[derive(Debug, Deserialize, Clone)]
struct ClaimResp {
    dispatch_id: Uuid,
    task_id: Uuid,
    name: String,
    url: String,
    filename: String,
    // 任务级覆盖字段（claim 响应中平铺下发，与 config.yaml 的 TaskItem 覆盖字段一致）
    #[serde(flatten)]
    #[serde(default)]
    overrides: TaskOverrides,
}

// ── 实时进度共享状态 ─────────────────────────────────────
/// dispatch_id → 实时速度（供 status_loop 汇总总速度）
type ProgressMap = Arc<Mutex<HashMap<Uuid, u64>>>;

// 旧架构的 WsProgress 进度回调已移除，全部走新架构

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
    let reg: ApiResponse<RegisterResp> = api
        .post(format!("{master}{}", paths::AGENT_REGISTER))
        .json(&RegisterReq {
            node_id: Some(node_id),
            hostname: hostname.to_string(),
            platform: platform.to_string(),
            arch: arch.to_string(),
            version: VERSION.into(),
            labels: vec![],
            capabilities: Some(crate::cli::manifest::build_node_capabilities()),
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
    // 初始化 tracing 日志（支持 RUST_LOG 环境变量）
    match tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init() {
        Ok(_) => eprintln!("[tracing] initialized successfully"),
        Err(e) => eprintln!("[tracing] init failed: {}", e),
    }
    eprintln!("[tracing] RUST_LOG={:?}", std::env::var("RUST_LOG"));

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
    let reg = register_to_pk(
        &api,
        &master,
        node_id,
        &hostname,
        &platform,
        &arch,
        local_max_concurrent,
    )
    .await?;

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
    let ws = WsClient::spawn(node_id, master.clone(), paths.base_dir.clone());

    // 6. 拉取全局配置（max_concurrent / dry_run / save_path 等）
    let global_cfg = match fetch_config(&api, &master, node_id).await {
        Ok(cfg) => {
            log!(
                "[agent] global config: max_concurrent={}, dry_run={}, save_path={}",
                cfg.global.max_concurrent,
                cfg.global.dry_run,
                cfg.output.save_path
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
    type RunningTasks = HashMap<Uuid, (JoinHandle<()>, Arc<DownloadController>)>;
    let running: Arc<Mutex<RunningTasks>> = Arc::new(Mutex::new(HashMap::new()));

    // 全局并发信号量
    let max_concurrent = {
        let cfg = global_cfg.lock().await;
        cfg.global.max_concurrent.max(1) as usize
    };
    let sem = Arc::new(Semaphore::new(max_concurrent));

    // 任务完成通知通道（每完成一个任务推一条 dispatch_id，供主循环清理 running 表）
    let (task_done_tx, mut task_done_rx) = mpsc::channel::<Uuid>(64);

    // 8. 定期状态上报（通过 WebSocket）
    {
        let ws = ws.clone();
        let active = active.clone();
        let bytes_total = bytes_total.clone();
        let last_error = last_error.clone();
        let progress_map = progress_map.clone();
        // 状态上报间隔：复用 agent.heartbeat_interval_secs，默认5秒
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
                    map.values().sum()
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
                match register_to_pk(
                    &api,
                    &master,
                    node_id,
                    &hostname,
                    &platform,
                    &arch,
                    local_max_concurrent,
                )
                .await
                {
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
    log!(
        "[agent] entering claim loop (max_concurrent={})",
        max_concurrent
    );
    loop {
        // 检查节点是否被 pk 删除，如果被删除则立即暂停任务并重新注册
        if ws.is_node_deleted() {
            log!("[agent] node deleted by pk, pausing all tasks and re-registering...");
            match register_to_pk(
                &api,
                &master,
                node_id,
                &hostname,
                &platform,
                &arch,
                local_max_concurrent,
            )
            .await
            {
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

        // 清理已完成任务（running 表只增不减会累积 JoinHandle/锁/句柄）
        while let Ok(done_id) = task_done_rx.try_recv() {
            if running.lock().await.remove(&done_id).is_some() {
                log!(
                    "[agent] task {} finished, removed from running table",
                    done_id
                );
            }
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
                    task.name,
                    task.dispatch_id
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
                running
                    .lock()
                    .await
                    .insert(dispatch_id, (handle.1, handle.2));
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
                    _ = task_done_rx.recv() => {
                        // 有任务完成：顺手清理 running 表中的已完成条目
                        while let Ok(done_id) = task_done_rx.try_recv() {
                            if running.lock().await.remove(&done_id).is_some() {
                                log!(
                                    "[agent] task {} finished, removed from running table",
                                    done_id
                                );
                            }
                        }
                    }
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
        handle.abort(); // 再强制终止任务
    }

    Ok(())
}

// ── 领取任务 ─────────────────────────────────────────────

async fn claim_task(api: &Client, master: &str, node_id: Uuid) -> Result<Option<ClaimResp>> {
    let url = format!("{master}{}", paths::DISPATCH_CLAIM);
    let resp = api
        .post(&url)
        .json(&serde_json::json!({ "node_id": node_id }))
        .send()
        .await
        .context("claim request")?;

    if resp.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(None);
    }

    let task: ApiResponse<ClaimResp> = resp
        .error_for_status()
        .context("claim rejected")?
        .json()
        .await
        .context("claim json")?;
    Ok(Some(task.data))
}

// ── 拉取全局 config ──────────────────────────────────────

async fn fetch_config(api: &Client, master: &str, node_id: Uuid) -> Result<SpdeConfig> {
    // 共享路径常量 paths::NODE_CONFIG_YAML（含 {id} 占位符），替换为实际节点 ID
    let url = format!(
        "{master}{}",
        paths::NODE_CONFIG_YAML.replace("{id}", &node_id.to_string())
    );
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

/// 构建节点能力参数（上报给 pk，标准 4.1：注册时上报完整能力清单）
///
/// 统一走 [`crate::cli::manifest::build_node_capabilities`]，与 `--manifest`
/// 输出同一份标准清单（并保留旧版兼容别名），pk 对未知字段透传不解析。

// ── 下载任务 ─────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn spawn_download_task(
    task: ClaimResp,
    cfg: SpdeConfig,
    ws: WsClient,
    active: Arc<AtomicU32>,
    bytes_total: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    _progress_map: ProgressMap,
    permit: OwnedSemaphorePermit,
    base_dir: PathBuf,
    task_done_tx: mpsc::Sender<Uuid>,
) -> (Uuid, JoinHandle<()>, Arc<DownloadController>) {
    let dispatch_id = task.dispatch_id;
    let _task_id = Some(task.task_id);
    let name = task.name.clone();
    let url = task.url.clone();
    let filename = task.filename.clone();
    let params = resolve_task_params(&task.overrides, &cfg, &base_dir);

    let controller = Arc::new(DownloadController::new());
    let _ctrl_clone = controller.clone();
    let handle = tokio::spawn(async move {
        // ctrl_clone 即主循环持有的同一控制器：外部 cancel/pause 会立刻作用于下载器
        // permit 已在主循环中获取，这里直接持有直到任务结束
        active.fetch_add(1, Ordering::Relaxed);

        // 全部使用新架构（智能下载架构）
        log!(
            "[download] using NEW scheduler for {} (dispatch_id={})",
            name,
            dispatch_id
        );
        let result = new_download::execute_download(
            &url,
            &filename,
            &params,
            dispatch_id,
            &name,
            &ws,
            &active,
            &bytes_total,
            &last_error,
        )
        .await;

        match result {
            Ok(r) => {
                log!(
                    "[download] NEW scheduler done {} success={} downloaded={}MB",
                    name,
                    r.success,
                    r.downloaded_bytes / 1024 / 1024
                );
            }
            Err(e) => {
                log!("[download] NEW scheduler error for {}: {}", name, e);
            }
        }

        active.fetch_sub(1, Ordering::Relaxed);
        drop(permit);
        let _ = task_done_tx.send(dispatch_id).await;
    });

    (dispatch_id, handle, controller)
}
