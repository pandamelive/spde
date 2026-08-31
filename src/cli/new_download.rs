//! 鏂颁笅杞芥墽琛屾ā鍧楋紙鍩轰簬 DownloadScheduler 鐨勬櫤鑳戒笅杞芥灦鏋勶級
//!
//! 浣跨敤鏂扮殑鍥涘眰鏋舵瀯锛歞omain 鈫?service 鈫?infra 鈫?cli
//! 鏀寔澶氭簮骞跺彂鍒嗙墖銆佽嚜閫傚簲杩炴帴鏁般€佹柇鐐圭画浼犮€侀暅鍍忓彂鐜般€佽繘搴﹀钩婊戙€?
//!
//! 涓庢棫涓嬭浇鍣紙`downloader/`锛夊苟瀛橈紝閫氳繃閰嶇疆寮€鍏冲垏鎹€?

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

/// 鏂颁笅杞戒换鍔℃墽琛岀粨鏋?
pub struct NewDownloadResult {
    pub dispatch_id: Uuid,
    pub success: bool,
    pub file_size: u64,
    pub downloaded_bytes: u64,
    pub elapsed_secs: f64,
    pub error_msg: Option<String>,
}

/// 鎶?Option<Duration> 杞崲鎴?u64 绉掓暟
fn duration_to_secs(d: Option<std::time::Duration>) -> u64 {
    d.map(|d| d.as_secs()).unwrap_or(1800)
}

/// 鎵ц鏂版灦鏋勪笅杞戒换鍔?
///
/// # 鍙傛暟
/// - `url`: 涓嬭浇 URL
/// - `filename`: 淇濆瓨鏂囦欢鍚?
/// - `params`: 浠诲姟鍙傛暟
/// - `dispatch_id`: 璋冨害 ID
/// - `task_name`: 浠诲姟鍚嶇О
/// - `ws`: WebSocket 瀹㈡埛绔紙鐢ㄤ簬姹囨姤杩涘害锛?
/// - `active`: 娲昏穬浠诲姟璁℃暟
/// - `bytes_total`: 鎬讳笅杞藉瓧鑺傝鏁?
/// - `last_error`: 鏈€鍚庨敊璇俊鎭?
///
/// # 杩斿洖
/// 涓嬭浇缁撴灉
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

    // 閫氱煡 PK 浠诲姟寮€濮?
    ws.send_task_started(dispatch_id).await;

    // 鍒涘缓淇濆瓨鐩綍
    tokio::fs::create_dir_all(&params.save_dir).await?;
    let save_path = params.save_dir.join(filename);

    // 瓒呮椂杞崲
    let timeout_secs = duration_to_secs(params.timeout);

    // 鏋勫缓涓嬭浇閰嶇疆
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

    // 鍒涘缓涓嬭浇璋冨害鍣?
    let scheduler = DownloadScheduler::new(download_config);

    // 鍗忚璺敱锛氭牴鎹?URL 鍗忚绫诲瀷閫夋嫨瀵瑰簲鐨?Source 鍜?Downloader
    let is_file = FileSource::is_file_uri(url);
    let is_ftp = FtpSource::is_ftp_uri(url);
    let is_ssh = SshSource::is_ssh_uri(url);
    let is_torrent = TorrentSource::is_torrent_uri(url);
    let is_http = url.starts_with("http://") || url.starts_with("https://");

    // 娉ㄥ唽闀滃儚鍙戠幇鍣紙浠?HTTP 鍗忚闇€瑕侊紝File 鍗忚涓嶉渶瑕侊級
    let mirror_bus = scheduler.mirror_bus();
    if is_http {
        mirror_bus
            .register(Box::new(DnsMultiIpDiscoverer::new()))
            .await;
    }

    // 鍒涘缓婧愬拰涓嬭浇鍣紙鍗忚鏃犲叧鐨?Box<dyn DownloadSource> 鍜?Arc<dyn ChunkDownloader>锛?
    let source: Box<dyn pandanetos::domain::DownloadSource>;
    let downloader: Arc<dyn pandanetos::domain::ChunkDownloader>;

    if is_file {
        // File 鍗忚
        let file_source = FileSource::from_uri(url)?;
        source = Box::new(file_source);
        downloader = Arc::new(FileChunkDownloader::new());
    } else if is_ftp {
        // FTP/FTPS 鍗忚
        let ftp_source = FtpSource::new(url)?;
        source = Box::new(ftp_source);
        downloader = Arc::new(FtpChunkDownloader::new(
            params.skip_tls_verify,
            timeout_secs,
        ));
    } else if is_ssh {
        // SSH/SFTP/SCP 鍗忚
        let ssh_source = SshSource::new(url)?;
        source = Box::new(ssh_source);
        downloader = Arc::new(SshChunkDownloader::new(timeout_secs));
    } else if is_torrent {
        // BitTorrent 鍗忚锛堢鍔涢摼鎺?绉嶅瓙鏂囦欢锛?
        let save_dir = save_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let torrent_source = TorrentSource::new(url, save_dir)?;
        source = Box::new(torrent_source);
        downloader = Arc::new(TorrentChunkDownloader::new(timeout_secs));
    } else if is_http {
        // HTTP/HTTPS 鍗忚
        source = Box::new(HttpSource::new(url.to_string()));
        downloader = Arc::new(HttpChunkDownloader::new(
            params.skip_tls_verify,
            timeout_secs,
        ));
    } else {
        // 鏆備笉鏀寔鐨勫崗璁?
        anyhow::bail!("unsupported protocol for new scheduler: {}", url);
    }

    // 鍒涘缓杩涘害閫氶亾
    let (progress_tx, mut progress_rx) = mpsc::channel::<DownloadProgress>(100);

    // 鍒涘缓鑷€傚簲鎺у埗鍣?
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

    // 杩涘害杞彂浠诲姟锛氫粠閫氶亾鎺ユ敹杩涘害锛屾帹閫佺粰 PK
    let ws_clone = ws.clone();
    let task_name_clone = task_name.to_string();
    let progress_handle = tokio::spawn(async move {
        while let Some(progress) = progress_rx.recv().await {
            // 璁＄畻鐧惧垎姣旓紙DownloadProgress 娌℃湁 percent 瀛楁锛岄渶瑕佽绠楋級
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

    // 鎵ц涓嬭浇
    let result = scheduler
        .download(source, downloader, save_path.clone(), progress_tx)
        .await;

    // 绛夊緟杩涘害杞彂瀹屾垚锛坧rogress_rx 宸茬粡琚?move 鍒?progress_handle 闂寘涓級
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

    // 姹囨姤浠诲姟缁撴灉
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
        success_chunks: 0, // 鏂版灦鏋勫悗缁ˉ鍏?
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

/// 妫€鏌ユ槸鍚﹀簲璇ヤ娇鐢ㄦ柊涓嬭浇鍣?
///
/// 宸插畬鍏ㄥ垏鎹㈠埌鏂版灦鏋勶紝濮嬬粓杩斿洖 true銆?
/// 淇濈暀姝ゅ嚱鏁版槸涓轰簡鍏煎鐜版湁璋冪敤鐐癸紝鍚庣画鍙洿鎺ョЩ闄ゃ€?
pub fn should_use_new_downloader(
    _url: &str,
    _task_overrides: &TaskOverrides,
    _cfg: &SpdeConfig,
) -> bool {
    true
}
