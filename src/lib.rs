//! SPDE — Super-Download-Engine 统一下载中心
//!
//! 核心架构：
//! - `downloader` 模块提供多后端抽象层（HTTP/FTP/本地文件）
//! - `DownloadManager` 统一调度，自动按 URI 路由到对应后端
//! - 旧版 `download_file` API 保留，内部委托给 HttpDownloader

pub mod cli;
pub mod core;
pub mod downloader;

pub use core::{EventMeta, SpdeEvent};
pub use downloader::{
    build_default_manager, DownloadBackend, DownloadManager, DownloadOutput, DownloadTask,
    FileDownloader, HttpDownloader, ProgressCallback, ProgressSnapshot, SshDownloader,
    StderrProgress,
};

#[cfg(feature = "ftp")]
pub use downloader::FtpDownloader;

#[cfg(feature = "torrent")]
pub use downloader::TorrentDownloader;

use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;

// ──────────────────────────────────────────────
// 旧版兼容 API（内部委托给新抽象层）
// ──────────────────────────────────────────────

/// 旧版下载指标（兼容字段）
#[derive(Debug, Default)]
pub struct DownloadMetrics {
    pub downloaded_bytes: u64,
    pub total_size: u64,
    pub success_chunks: u64,
    pub failed_chunks: u64,
    pub elapsed_secs: f64,
    pub status: String,
    pub error_msg: Option<String>,
}

/// 旧版下载参数
#[derive(Debug, Clone)]
pub struct DownloadOption {
    pub url: String,
    pub save_path: PathBuf,
}

/// 旧版多连接分片下载入口（兼容层，内部走 HttpDownloader）
pub async fn download_file(
    _client: &reqwest::Client,
    url: &str,
    file_path: PathBuf,
    connections: u32,
    retry_times: u32,
    dry_run: bool,
) -> Result<DownloadMetrics> {
    let downloader = HttpDownloader::new();
    let name = file_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let progress: Option<Arc<dyn ProgressCallback>> =
        Some(Arc::new(StderrProgress::new(name)));

    let task = DownloadTask {
        uri: url.to_string(),
        save_path: file_path,
        max_conn: connections,
        retry_times,
        dry_run,
        ..Default::default()
    };

    let output = downloader.run(task, progress, None).await?;

    Ok(DownloadMetrics {
        downloaded_bytes: output.downloaded_bytes,
        total_size: output.total_size,
        success_chunks: output.success_chunks as u64,
        failed_chunks: output.failed_chunks as u64,
        elapsed_secs: output.elapsed_secs,
        status: output.status,
        error_msg: output.error_msg,
    })
}

/// 旧版便捷入口
pub async fn run_download(
    _client: &reqwest::Client,
    opt: DownloadOption,
) -> Result<DownloadMetrics> {
    download_file(_client, &opt.url, opt.save_path, 8, 3, false).await
}
