//! 新下载执行模块（基于 DownloadScheduler 的智能下载架构）
//!
//! 使用新的四层架构：domain ← service ← infra ← cli
//! 支持多源并发分片、自适应连接数、断点续传、镜像发现、进度平滑。
//!
//! 与旧下载器（`downloader/`）并存，通过配置开关切换。

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

use pandanetos::domain::DownloadProgress;

use crate::cli::config::{SpdeConfig, TaskOverrides, TaskParams};
use crate::cli::ws_client::{TaskProgressParams, TaskReportParams, WsClient};
use crate::domain::DownloadConfig;
use crate::infra::file::downloader::FileChunkDownloader;
use crate::infra::file::source::FileSource;
use crate::infra::ftp::downloader::FtpChunkDownloader;
use crate::infra::ftp::source::FtpSource;
use crate::infra::http::downloader::HttpChunkDownloader;
use crate::infra::http::mirror::dns::DnsMultiIpDiscoverer;
use crate::infra::http::source::HttpSource;
use crate::infra::ssh::downloader::SshChunkDownloader;
use crate::infra::ssh::source::SshSource;
use crate::infra::torrent::downloader::TorrentChunkDownloader;
use crate::infra::torrent::source::TorrentSource;
use crate::service::adaptive::{AdaptiveConfig, AdaptiveController};
use crate::service::scheduler::DownloadScheduler;

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
fn duration_to_secs(d: Option<std::time::Duration>) -> u64 {
    d.map(|d| d.as_secs()).unwrap_or(1800)
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
/// - `active`: 活跃任务计数
/// - `bytes_total`: 总下载字节计数
/// - `last_error`: 最后错误信息
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
    active: &Arc<AtomicU32>,
    bytes_total: &Arc<AtomicU64>,
    last_error: &Arc<Mutex<Option<String>>>,
) -> Result<NewDownloadResult> {
    let started = Instant::now();
    active.fetch_add(1, Ordering::Relaxed);

    // 通知 PK 任务开始
    ws.send_task_started(dispatch_id).await;

    // 创建保存目录（dry_run 模式下不创建目录，实现真正的不落盘）
    if !params.dry_run {
        tokio::fs::create_dir_all(&params.save_dir).await?;
    }
    let save_path = params.save_dir.join(filename);

    // 超时转换
    let timeout_secs = duration_to_secs(params.timeout);

    // 构建下载配置
    let download_config = DownloadConfig {
        max_connections: params.connections,
        min_connections: 1,
        chunk_size: 4 * 1024 * 1024, // 4MB
        retry_times: params.retry as u32,
        timeout_secs,
        resume: params.resume,
        skip_tls_verify: params.skip_tls_verify,
        max_bandwidth_bps: 0,
        enable_mirror_discovery: true,
        enable_adaptive: true,
        enable_progress_smoothing: true,
        save_dir: params.save_dir.clone(),
        dry_run: params.dry_run,
    };

    // 创建下载调度器
    let scheduler = DownloadScheduler::new(download_config);

    // 协议路由：根据 URL 协议类型选择对应的 Source 和 Downloader
    let is_file = FileSource::is_file_uri(url);
    let is_ftp = FtpSource::is_ftp_uri(url);
    let is_ssh = SshSource::is_ssh_uri(url);
    let is_torrent = TorrentSource::is_torrent_uri(url);
    let is_http = url.starts_with("http://") || url.starts_with("https://");

    // 注册镜像发现器（仅 HTTP 协议需要，File 协议不需要）
    let mirror_bus = scheduler.mirror_bus();
    if is_http {
        mirror_bus
            .register(Box::new(DnsMultiIpDiscoverer::new()))
            .await;
    }

    // 创建源和下载器（协议无关的 Box<dyn DownloadSource> 和 Arc<dyn ChunkDownloader>）
    let source: Box<dyn pandanetos::domain::DownloadSource>;
    let downloader: Arc<dyn pandanetos::domain::ChunkDownloader>;

    if is_file {
        // File 协议
        let file_source = FileSource::from_uri(url)?;
        source = Box::new(file_source);
        downloader = Arc::new(FileChunkDownloader::new());
    } else if is_ftp {
        // FTP/FTPS 协议
        let ftp_source = FtpSource::new(url)?;
        source = Box::new(ftp_source);
        downloader = Arc::new(FtpChunkDownloader::new(
            params.skip_tls_verify,
            timeout_secs,
        ));
    } else if is_ssh {
        // SSH/SFTP/SCP 协议
        let ssh_source = SshSource::new(url)?;
        source = Box::new(ssh_source);
        downloader = Arc::new(SshChunkDownloader::new(timeout_secs));
    } else if is_torrent {
        // BitTorrent 协议（磁力链接/种子文件）
        let save_dir = save_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let torrent_source = TorrentSource::new(url, save_dir)?;
        source = Box::new(torrent_source);
        downloader = Arc::new(TorrentChunkDownloader::new(timeout_secs, params.dry_run));
    } else if is_http {
        // HTTP/HTTPS 协议
        source = Box::new(HttpSource::new(url.to_string()));
        downloader = Arc::new(HttpChunkDownloader::new(
            params.skip_tls_verify,
            timeout_secs,
        ));
    } else {
        // 暂不支持的协议
        anyhow::bail!("unsupported protocol for new scheduler: {}", url);
    }

    // 创建进度通道
    let (progress_tx, mut progress_rx) = mpsc::channel::<DownloadProgress>(100);

    // 创建自适应控制器
    let adaptive_config = AdaptiveConfig {
        initial_connections: 2,
        min_connections: 1,
        max_connections: params.connections,
        adjust_interval_secs: 5,
        speed_growth_threshold: 0.05,
        stagnation_limit: 3,
        failure_rate_threshold: 0.3,
        adjust_step: 2,
        enabled: true,
        ..Default::default()
    };
    let _adaptive = AdaptiveController::new(adaptive_config);

    // 进度转发任务：从通道接收进度，推送给 PK
    let ws_clone = ws.clone();
    let task_name_clone = task_name.to_string();
    let progress_handle = tokio::spawn(async move {
        while let Some(progress) = progress_rx.recv().await {
            // 计算百分比（DownloadProgress 没有 percent 字段，需要计算）
            let percent = if progress.total_bytes > 0 {
                progress.downloaded_bytes as f64 / progress.total_bytes as f64 * 100.0
            } else {
                0.0
            };
            let elapsed_secs = started.elapsed().as_secs_f64();

            ws_clone
                .send_task_progress(TaskProgressParams {
                    dispatch_id,
                    task_name: &task_name_clone,
                    percent,
                    downloaded_bytes: progress.downloaded_bytes,
                    total_size: progress.total_bytes,
                    speed_bps: progress.speed_bps,
                    active_connections: progress.active_connections,
                    elapsed_secs,
                })
                .await;
        }
    });

    // 执行下载
    let result = scheduler
        .download(source, downloader, save_path.clone(), progress_tx)
        .await;

    // 等待进度转发完成（progress_rx 已经被 move 到 progress_handle 闭包中）
    let _ = progress_handle.await;

    let elapsed = started.elapsed().as_secs_f64();

    let (success, file_size, downloaded, error_msg) = match result {
        Ok(r) => {
            if r.success {
                *last_error.lock().await = None;
            }
            (r.success, r.total_bytes, r.downloaded_bytes, r.error_msg)
        }
        Err(e) => {
            let err_msg = e.to_string();
            *last_error.lock().await = Some(err_msg.clone());
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

    // 汇报任务结果
    ws.send_task_report(TaskReportParams {
        dispatch_id: Some(dispatch_id),
        task_id: None,
        task_name,
        url,
        filename,
        file_size,
        downloaded_bytes: downloaded,
        elapsed_secs: elapsed,
        avg_speed_mbps: avg_speed_mbps,
        status,
        success_chunks: 0, // 新架构后续补充
        failed_chunks: 0,
        error_msg: error_msg.as_deref(),
    })
    .await;

    active.fetch_sub(1, Ordering::Relaxed);

    Ok(NewDownloadResult {
        dispatch_id,
        success,
        file_size,
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
