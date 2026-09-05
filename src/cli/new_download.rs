//! 新下载执行模块（基于统一数据块抽象的智能下载架构）
//!
//! 使用新的四层架构：domain ← service ← infra ← cli
//! 核心特性：
//! - 统一数据块抽象（ChunkFetcher trait）：所有协议实现此接口，调度器协议无关
//! - 智能源池（SourcePool）：源发现/健康检查/评分/调度/淘汰一体化
//! - 自适应控制器（AdaptiveController）：动态调整并发数/分片大小/重试策略
//! - 能力驱动：根据 probe 到的能力自动选择下载方式，不按协议分类
//!
//! 支持协议：HTTP/HTTPS、FTP/FTPS、SSH/SFTP/SCP、BitTorrent（磁力链接/种子文件）、本地文件

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn};
use uuid::Uuid;

use pandanetos::domain::{CancellationToken, DownloadProgress};

use crate::cli::config::{SpdeConfig, TaskOverrides, TaskParams};
use crate::cli::p2p;
use crate::cli::ws_client::{TaskProgressParams, TaskReportParams, WsClient};
use crate::domain::chunk_fetcher::ChunkFetcher;
use crate::infra::disk::writer_factory::{create_writer, WriterType};
use crate::infra::file::fetcher::LocalFileFetcher;
use crate::infra::file::source::FileSource;
use crate::infra::ftp::fetcher::FtpFetcher;
use crate::infra::http::fetcher::{HttpRangeFetcher, HttpStreamFetcher};
use crate::infra::ssh::fetcher::SftpFetcher;
use crate::infra::torrent::fetcher::TorrentPieceFetcher;
use crate::service::chunk_scheduler::{ChunkScheduler, ChunkSchedulerConfig};

/// 新下载任务执行结果
pub struct NewDownloadResult {
    pub dispatch_id: Uuid,
    pub success: bool,
    pub file_size: u64,
    pub downloaded_bytes: u64,
    pub elapsed_secs: f64,
    pub error_msg: Option<String>,
}

/// 把 Option<Duration> 转换成 u64 秒数
///
/// 默认 120s：单 chunk 下载超时不超过 PK 心跳周期的合理上限（5–10s × 30 = 150s），
/// 避免旧默认 1800s 在单点网络异常时把整个 task 拖到 PK 回收。
fn duration_to_secs(d: Option<std::time::Duration>) -> u64 {
    d.map(|d| d.as_secs()).unwrap_or(120)
}

/// 协议识别结果
#[derive(Debug, Clone, Copy)]
pub enum ProtocolType {
    Http,
    Https,
    Ftp,
    Ftps,
    Ssh,
    Sftp,
    Scp,
    Torrent,
    Magnet,
    File,
    Unknown,
}

/// 识别 URL 协议类型
pub fn detect_protocol(url: &str) -> ProtocolType {
    if url.starts_with("magnet:") {
        ProtocolType::Magnet
    } else if url.starts_with("https://") {
        ProtocolType::Https
    } else if url.starts_with("http://") {
        ProtocolType::Http
    } else if url.starts_with("ftps://") {
        ProtocolType::Ftps
    } else if url.starts_with("ftp://") {
        ProtocolType::Ftp
    } else if url.starts_with("sftp://") {
        ProtocolType::Sftp
    } else if url.starts_with("scp://") {
        ProtocolType::Scp
    } else if url.starts_with("ssh://") {
        ProtocolType::Ssh
    } else if url.ends_with(".torrent") {
        ProtocolType::Torrent
    } else if FileSource::is_file_uri(url) {
        ProtocolType::File
    } else {
        ProtocolType::Unknown
    }
}

/// 根据协议类型创建对应的 ChunkFetcher
///
/// # 注意
/// 对于 HTTP 协议，会先 probe 探测是否支持 Range，
/// 然后选择 HttpRangeFetcher 或 HttpStreamFetcher。
pub async fn create_fetcher(
    url: &str,
    protocol: ProtocolType,
    timeout_secs: u64,
    dry_run: bool,
    save_dir: &std::path::Path,
) -> Result<Arc<dyn ChunkFetcher>> {
    match protocol {
        ProtocolType::Http | ProtocolType::Https => {
            // 先 probe 探测是否支持 Range
            let probe_fetcher = HttpRangeFetcher::new(url, timeout_secs);
            match probe_fetcher.probe().await {
                Ok((_, caps)) => {
                    if caps.supports_range {
                        info!(url = %url, "HTTP source supports Range, using HttpRangeFetcher");
                        Ok(Arc::new(HttpRangeFetcher::new(url, timeout_secs)))
                    } else {
                        info!(url = %url, "HTTP source does not support Range, using HttpStreamFetcher");
                        Ok(Arc::new(HttpStreamFetcher::new(url, timeout_secs)))
                    }
                }
                Err(e) => {
                    warn!(url = %url, error = %e, "HTTP probe failed, falling back to HttpStreamFetcher");
                    Ok(Arc::new(HttpStreamFetcher::new(url, timeout_secs)))
                }
            }
        }
        ProtocolType::Ftp | ProtocolType::Ftps => {
            info!(url = %url, "using FtpFetcher");
            Ok(Arc::new(FtpFetcher::new(url, timeout_secs)))
        }
        ProtocolType::Ssh | ProtocolType::Sftp | ProtocolType::Scp => {
            info!(url = %url, "using SftpFetcher");
            Ok(Arc::new(SftpFetcher::new(url, timeout_secs)))
        }
        ProtocolType::Torrent | ProtocolType::Magnet => {
            info!(url = %url, "using TorrentPieceFetcher");
            Ok(Arc::new(TorrentPieceFetcher::new(
                url,
                save_dir,
                timeout_secs,
                dry_run,
            )))
        }
        ProtocolType::File => {
            info!(url = %url, "using LocalFileFetcher");
            let path = if url.starts_with("file://") {
                url.strip_prefix("file://").unwrap()
            } else {
                url
            };
            Ok(Arc::new(LocalFileFetcher::new(path)))
        }
        ProtocolType::Unknown => {
            anyhow::bail!("unsupported protocol: {}", url);
        }
    }
}

/// 执行新架构下载任务
///
/// # 参数
/// - `url`: 下载 URL
/// - `filename`: 保存文件名
/// - `params`: 任务参数
/// - `dispatch_id`: 调度 ID
/// - `task_name`: 任务名称
/// - `ws`: WebSocket 客户端（用于汇报进度）
/// - `bytes_total`: 总下载字节计数（atomic，跨任务累加）
/// - `last_error`: 最后错误信息（用于 status_loop 上报）
/// - `controller`: 下载控制器（外部 pause/cancel 会通过 cancellation token
///   立即作用于底层 fetcher，移除旧实现的"丢弃 _ctrl_clone"导致外部 cancel 失效问题）
/// - `bt_manager`: BT 管理器（仅 BT 任务使用）
///
/// # 返回
/// 下载结果
#[allow(clippy::too_many_arguments)]
pub async fn execute_download(
    url: &str,
    filename: &str,
    params: &TaskParams,
    dispatch_id: Uuid,
    task_name: &str,
    ws: &WsClient,
    bytes_total: &Arc<AtomicU64>,
    last_error: &Arc<Mutex<Option<String>>>,
    controller: Option<&crate::service::controller::DownloadController>,
    bt_manager: Option<&p2p::manager::BtManager>,
) -> Result<NewDownloadResult> {
    let started = Instant::now();

    // 通知 PK 任务开始
    ws.send_task_started(dispatch_id).await;

    // 创建保存目录（dry_run 模式下不创建目录，实现真正的不落盘）
    if !params.dry_run {
        tokio::fs::create_dir_all(&params.save_dir).await?;
    }

    let save_path = params.save_dir.join(filename);
    let timeout_secs = duration_to_secs(params.timeout);

    // 步骤 1：协议识别
    let protocol = detect_protocol(url);
    info!(url = %url, protocol = ?protocol, "protocol detected");

    // P2P 协议（BT/磁力）走独立下载逻辑，不经过通用 ChunkScheduler
    if matches!(protocol, ProtocolType::Torrent | ProtocolType::Magnet) {
        info!(url = %url, "using P2P self-managed download path");

        let (progress_tx, mut progress_rx) = mpsc::channel::<DownloadProgress>(100);
        let cancel = CancellationToken::new();

        // 接入 DownloadController：外部 cancel() 会通过 watcher 在 100ms 内
        // 触发 cancel token，p2p 模块可借此快速终止（之前的 _ctrl_clone 被丢弃，
        // controller 形同虚设，外部 cancel 实际无效）。
        spawn_controller_watcher(controller, &cancel);

        let ws_clone = ws.clone();
        let task_name_clone = task_name.to_string();
        let started_clone = started;
        let progress_handle = tokio::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                let elapsed_secs = started_clone.elapsed().as_secs_f64();
                ws_clone
                    .send_task_progress(TaskProgressParams {
                        dispatch_id,
                        task_name: &task_name_clone,
                        percent: progress.percent,
                        downloaded_bytes: progress.downloaded_bytes,
                        total_size: progress.total_bytes,
                        speed_bps: progress.speed_bps,
                        active_connections: progress.active_connections,
                        elapsed_secs,
                    })
                    .await;
            }
        });

        let p2p_result = p2p::download_p2p(
            protocol,
            url,
            &params.save_dir,
            timeout_secs,
            params.dry_run,
            progress_tx,
            cancel,
            bt_manager,
        )
        .await;

        // P2P 路径同样不再 await forwarding task（fire-and-forget），原因见 HTTP 路径。
        let _ = progress_handle;

        let elapsed = started.elapsed().as_secs_f64();
        let (success, total_bytes, downloaded, error_msg) = match p2p_result {
            Ok(r) => (r.success, r.total_bytes, r.downloaded_bytes, r.error_msg),
            Err(e) => {
                let err_msg = e.to_string();
                error!(url = %url, error = %err_msg, "P2P download failed");
                (false, 0, 0, Some(err_msg))
            }
        };

        bytes_total.fetch_add(downloaded, Ordering::Relaxed);
        let avg_speed_mbps = if elapsed > 0.0 {
            downloaded as f64 / elapsed / 1024.0 / 1024.0
        } else {
            0.0
        };
        let status = if success { "success" } else { "failed" };

        ws.send_task_report(TaskReportParams {
            dispatch_id: Some(dispatch_id),
            task_id: None,
            task_name,
            url,
            filename,
            file_size: total_bytes,
            downloaded_bytes: downloaded,
            elapsed_secs: elapsed,
            avg_speed_mbps,
            status,
            success_chunks: if success { 1 } else { 0 },
            failed_chunks: if success { 0 } else { 1 },
            error_msg: error_msg.as_deref(),
        })
        .await;

        info!(
            url = %url,
            success = success,
            downloaded = downloaded,
            elapsed_secs = elapsed,
            "P2P download completed"
        );

        return Ok(NewDownloadResult {
            dispatch_id,
            success,
            file_size: total_bytes,
            downloaded_bytes: downloaded,
            elapsed_secs: elapsed,
            error_msg,
        });
    }

    // 步骤 2：创建对应的 ChunkFetcher
    let fetcher = create_fetcher(
        url,
        protocol,
        timeout_secs,
        params.dry_run,
        &params.save_dir,
    )
    .await?;

    // 步骤 3：创建统一分片调度器
    let scheduler_config = ChunkSchedulerConfig {
        initial_chunk_size: 4 * 1024 * 1024,
        min_chunk_size: 1 * 1024 * 1024,
        max_chunk_size: 64 * 1024 * 1024,
        max_retries: params.retry as u32,
        initial_retry_interval_ms: 1000,
        progress_interval_ms: 500,
        ..Default::default()
    };
    let scheduler = ChunkScheduler::new(scheduler_config);

    // 步骤 4：添加源到调度器
    scheduler.add_source(fetcher.clone()).await;

    // 步骤 5：创建写入器
    let writer_type = if params.dry_run {
        WriterType::Null
    } else {
        WriterType::Disk
    };

    // 先 probe 获取文件大小（用于预分配）
    let file_size = match fetcher.probe().await {
        Ok((size, _)) => size,
        Err(_) => 0,
    };

    let writer = create_writer(writer_type, Some(save_path.clone()), file_size).await?;

    // 步骤 6：创建进度通道
    let (progress_tx, mut progress_rx) = mpsc::channel::<DownloadProgress>(100);

    // 步骤 7：创建取消令牌
    let cancel = CancellationToken::new();

    // 接入 DownloadController（修 bug #5）：外部 cancel() 会通过 watcher 在 100ms 内
    // 触发 cancel token，scheduler 内部 worker 借此快速退出。
    spawn_controller_watcher(controller, &cancel);

    // 进度转发任务：从通道接收进度，推送给 PK
    let ws_clone = ws.clone();
    let task_name_clone = task_name.to_string();
    let progress_handle = tokio::spawn(async move {
        while let Some(progress) = progress_rx.recv().await {
            let elapsed_secs = started.elapsed().as_secs_f64();
            ws_clone
                .send_task_progress(TaskProgressParams {
                    dispatch_id,
                    task_name: &task_name_clone,
                    percent: progress.percent,
                    downloaded_bytes: progress.downloaded_bytes,
                    total_size: progress.total_bytes,
                    speed_bps: progress.speed_bps,
                    active_connections: progress.active_connections,
                    elapsed_secs,
                })
                .await;
        }
    });

    // 步骤 8：执行下载
    info!(url = %url, "starting download with new architecture");
    let result = scheduler.execute(writer, progress_tx, cancel).await;

    // 不再 await progress_handle：progress forwarding task 在 ws 半死时会卡在
    // `ws.send_task_progress().await` 上，进而阻塞 execute_download 不返回，
    // 最终导致 agent 端的 permit 永久不释放、主循环卡死、任务被 PK 回收。
    // forwarding task 自身没有 panic/泄漏风险（持 channel receiver，
    // sender drop 时自然退出），fire-and-forget 让其后台自然消亡。
    // 关键修复是 progress reporter 已改 try_send + scheduler.execute 末尾触发 cancel，
    // 这里不再需要等转发 task 结束。
    let _ = progress_handle;

    let elapsed = started.elapsed().as_secs_f64();

    let (success, total_bytes, downloaded, error_msg) = match result {
        Ok(r) => {
            if r.success {
                *last_error.lock().await = None;
            } else {
                *last_error.lock().await = r.error.clone();
            }
            (r.success, r.total_bytes, r.total_bytes, r.error)
        }
        Err(e) => {
            let err_msg = e.to_string();
            *last_error.lock().await = Some(err_msg.clone());
            error!(url = %url, error = %err_msg, "download failed");
            (false, file_size, 0, Some(err_msg))
        }
    };

    bytes_total.fetch_add(downloaded, Ordering::Relaxed);

    let avg_speed_mbps = if elapsed > 0.0 {
        downloaded as f64 / elapsed / 1024.0 / 1024.0
    } else {
        0.0
    };

    let status = if success { "success" } else { "failed" };

    // 汇报任务结果
    ws.send_task_report(TaskReportParams {
        dispatch_id: Some(dispatch_id),
        task_id: None,
        task_name,
        url,
        filename,
        file_size: total_bytes,
        downloaded_bytes: downloaded,
        elapsed_secs: elapsed,
        avg_speed_mbps,
        status,
        success_chunks: if success { 1 } else { 0 },
        failed_chunks: if success { 0 } else { 1 },
        error_msg: error_msg.as_deref(),
    })
    .await;

    info!(
        url = %url,
        success = success,
        downloaded = downloaded,
        elapsed_secs = elapsed,
        avg_speed_mbps = avg_speed_mbps,
        "download completed"
    );

    Ok(NewDownloadResult {
        dispatch_id,
        success,
        file_size: total_bytes,
        downloaded_bytes: downloaded,
        elapsed_secs: elapsed,
        error_msg,
    })
}

/// 检查是否应该使用新下载器
///
/// 已完全切换到新架构，始终返回 true。
/// 保留此函数是为了兼容现有调用点，后续可直接移除。
pub fn should_use_new_downloader(
    _url: &str,
    _task_overrides: &TaskOverrides,
    _cfg: &SpdeConfig,
) -> bool {
    true
}

/// 把 `DownloadController` 的取消信号桥接到 `CancellationToken`。
///
/// 监听轮询粒度 100ms：外部 `controller.cancel()` 触发后最多 100ms 内
/// `token.cancel()` 被调用，下游 fetcher/scheduler 借此快速退出。
///
/// 之前 `_ctrl_clone` 被丢弃、controller 完全无效，这是 bug #5 的根因。
fn spawn_controller_watcher(
    controller: Option<&crate::service::controller::DownloadController>,
    token: &pandanetos::domain::CancellationToken,
) {
    if let Some(ctrl) = controller {
        let ctrl = ctrl.clone();
        let token = token.clone();
        tokio::spawn(async move {
            loop {
                if ctrl.is_cancelled() {
                    token.cancel();
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });
    }
}
