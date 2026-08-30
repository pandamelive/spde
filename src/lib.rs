//! SPDE — Super-Download-Engine 统一下载中心
//!
//! 遵循 PandaNetOS 四层架构：
//! - `cli`     — API 层（CLI / agent 模式 / WebSocket 汇报）
//! - `service` — 服务层（智能调度核心，协议无关）
//! - `domain`  — 领域层（spde 特有的领域模型，核心抽象复用 pandanetos::domain）
//! - `infra`   — 基础设施层（各协议适配器 + 磁盘IO实现）

pub mod cli;
pub mod domain;
pub mod infra;
pub mod service;

// 核心类型导出
pub use service::controller::DownloadController;
pub use service::scheduler::DownloadScheduler;
pub use service::source_manager::SourceManager;
pub use service::chunk_scheduler::ChunkScheduler;
pub use service::progress::ProgressSmoother;
pub use service::mirror_bus::MirrorBus;
pub use service::strategy::multi_source_chunked::MultiSourceChunkedStrategy;
pub use service::adaptive::{AdaptiveConfig, AdaptiveController, AdaptiveStats};
pub use service::cdn_throttle::{CdnThrottleConfig, CdnThrottleDetector, CdnThrottleStats};

// 协议适配器导出
pub use infra::disk::file_writer::FileChunkWriter;
pub use infra::http::source::HttpSource;
pub use infra::http::downloader::HttpChunkDownloader;
pub use infra::http::mirror::dns::DnsMultiIpDiscoverer;
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
