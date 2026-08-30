//! SPDE — Super-Download-Engine 统一下载中心
//!
//! 遵循 PandaNetOS 四层架构：
//! - `cli`     — API 层（CLI / agent 模式 / WebSocket 汇报）
//! - `service` — 服务层（智能调度核心，协议无关）
//! - `domain`  — 领域层（spde 特有的领域模型，核心抽象复用 pandanetos::domain）
//! - `infra`   — 基础设施层（各协议适配器 + 磁盘IO实现）
//!
//! 旧版 `downloader` 模块保留兼容，逐步迁移到新架构。

// TODO: 修复以下警告后移除此 allow
#![allow(clippy::all)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]

pub mod cli;
pub mod domain;
pub mod infra;
pub mod service;

// 旧版下载器（兼容保留，逐步迁移到 infra/ 下的协议适配器）
pub mod downloader;
#[cfg(feature = "ftp")]
pub use downloader::FtpDownloader;
#[cfg(feature = "torrent")]
pub use downloader::TorrentDownloader;
pub use downloader::{
    build_default_manager, DownloadBackend, DownloadManager, DownloadOutput, DownloadTask,
    FileDownloader, HttpDownloader, ProgressCallback, ProgressSnapshot, SshDownloader,
    StderrProgress,
};

// 新版智能下载器导出
pub use infra::disk::file_writer::FileChunkWriter;
pub use infra::http::downloader::HttpChunkDownloader;
pub use infra::http::mirror::dns::DnsMultiIpDiscoverer;
pub use infra::http::source::HttpSource;
pub use service::chunk_scheduler::ChunkScheduler;
pub use service::mirror_bus::MirrorBus;
pub use service::progress::ProgressSmoother;
pub use service::scheduler::DownloadScheduler;
pub use service::source_manager::SourceManager;
pub use service::strategy::multi_source_chunked::MultiSourceChunkedStrategy;

// P0 新架构迁移新增导出
pub use service::controller::DownloadController;
pub use service::adaptive::{AdaptiveConfig, AdaptiveController, AdaptiveStats};
pub use service::cdn_throttle::{CdnThrottleConfig, CdnThrottleDetector, CdnThrottleStats};
pub use infra::file::source::FileSource;
pub use infra::file::downloader::FileChunkDownloader;
#[cfg(feature = "ftp")]
pub use infra::ftp::source::FtpSource;
#[cfg(feature = "ftp")]
pub use infra::ftp::downloader::FtpChunkDownloader;
pub use infra::ssh::source::SshSource;
pub use infra::ssh::downloader::SshChunkDownloader;
#[cfg(feature = "torrent")]
pub use infra::torrent::source::TorrentSource;
#[cfg(feature = "torrent")]
pub use infra::torrent::downloader::TorrentChunkDownloader;
