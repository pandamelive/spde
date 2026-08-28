//! SPDE — Super-Download-Engine 统一下载中心
//!
//! 核心架构：
//! - `downloader` 模块提供多后端抽象层（HTTP/FTP/本地文件）
//! - `DownloadManager` 统一调度，自动按 URI 路由到对应后端

pub mod cli;
pub mod downloader;

pub use downloader::{
    build_default_manager, DownloadBackend, DownloadManager, DownloadOutput, DownloadTask,
    FileDownloader, HttpDownloader, ProgressCallback, ProgressSnapshot, SshDownloader,
    StderrProgress,
};

#[cfg(feature = "ftp")]
pub use downloader::FtpDownloader;

#[cfg(feature = "torrent")]
pub use downloader::TorrentDownloader;
