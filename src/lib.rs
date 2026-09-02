//! SPDE — Super-Download-Engine 统一下载中心
//!
//! 遵循 PandaNetOS 四层架构：
//! - `cli`     — API 层（CLI / agent 模式 / WebSocket 汇报）
//! - `service` — 服务层（智能调度核心，协议无关）
//! - `domain`  — 领域层（spde 特有的领域模型，核心抽象复用 pandanetos::domain）
//! - `infra`   — 基础设施层（各协议适配器 + 磁盘IO实现）
//!
//! 新架构核心：
//! - 统一数据块抽象（ChunkFetcher trait）：所有协议实现此接口，调度器协议无关
//! - 智能源池（SourcePool）：源发现/健康检查/评分/调度/淘汰一体化
//! - 自适应控制器（AdaptiveController）：动态调整并发数/分片大小/重试策略
//! - 能力驱动：根据 probe 到的能力自动选择下载方式，不按协议分类

#![allow(clippy::all)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]

pub mod cli;
pub mod domain;
pub mod infra;
pub mod service;

// ===== 新架构核心导出 =====

/// 统一数据块获取接口（所有协议实现此 trait）
pub use domain::chunk_fetcher::{
    ChunkFetcher, ChunkStats as FetcherChunkStats, SourceCapabilities as FetcherSourceCapabilities,
};

/// 智能源池
pub use domain::source_pool::{
    RatedSource, ScoringConfig, SourceHealth as PoolSourceHealth, SourcePool,
};

/// 自适应控制器
pub use domain::adaptive::{AdaptiveConfig, AdaptiveController, AdaptiveParams, DownloadSnapshot};

/// 统一分片调度器（协议无关）
pub use service::chunk_scheduler::{
    ChunkScheduler, ChunkSchedulerConfig, DownloadResult as SchedulerDownloadResult,
};

// ===== 协议适配器（Fetcher 实现） =====

/// HTTP Range Fetcher（支持范围请求的 HTTP 下载器）
pub use infra::http::fetcher::range::HttpRangeFetcher;

/// HTTP Stream Fetcher（不支持范围请求的 HTTP 流式下载器）
pub use infra::http::fetcher::stream::HttpStreamFetcher;

/// BitTorrent Piece Fetcher（BT 原生下载器）
#[cfg(feature = "torrent")]
pub use infra::torrent::fetcher::piece::TorrentPieceFetcher;

/// FTP Fetcher
#[cfg(feature = "ftp")]
pub use infra::ftp::fetcher::FtpFetcher;

/// SFTP/SSH Fetcher
pub use infra::ssh::fetcher::SftpFetcher;

/// 本地文件 Fetcher
pub use infra::file::fetcher::LocalFileFetcher;

// ===== 基础设施 =====

/// 磁盘文件写入器
pub use infra::disk::file_writer::FileChunkWriter;

// ===== 兼容导出（旧架构，后续逐步移除） =====

pub use infra::http::mirror::dns::DnsMultiIpDiscoverer;
pub use infra::http::source::HttpSource;
pub use service::controller::DownloadController;
pub use service::progress::ProgressSmoother;

#[cfg(feature = "torrent")]
pub use infra::torrent::source::TorrentSource;
