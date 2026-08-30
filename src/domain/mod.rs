//! SPDE 领域层
//!
//! 核心下载抽象（Chunk / DownloadSource / ChunkDownloader / MirrorDiscoverer /
//! DownloadStrategy / ChunkWriter）定义在 pandanetos::domain 中，供整个生态复用。
//! 本模块放置 spde 特有的领域模型和扩展。

pub use pandanetos::domain::{
    Chunk, ChunkDownloader, ChunkSet, ChunkState, ChunkStats, ChunkWriter,
    CancellationToken, DownloadFileInfo, DownloadProgress, DownloadResult, DownloadSource,
    DownloadStrategy, MirrorDiscoverer, SourceCapabilities, SourceHealth,
};

/// 下载任务配置（spde 特有的任务级参数）
#[derive(Debug, Clone)]
pub struct DownloadConfig {
    /// 最大并发连接数（0 = 自动）
    pub max_connections: u32,
    /// 最小并发连接数
    pub min_connections: u32,
    /// 分片大小（字节，0 = 自动）
    pub chunk_size: u64,
    /// 重试次数
    pub retry_times: u32,
    /// 超时（秒）
    pub timeout_secs: u64,
    /// 是否启用断点续传
    pub resume: bool,
    /// 是否跳过 TLS 验证
    pub skip_tls_verify: bool,
    /// 全局带宽限速（字节/秒，0 = 不限）
    pub max_bandwidth_bps: u64,
    /// 是否启用镜像发现
    pub enable_mirror_discovery: bool,
    /// 是否启用自适应连接数
    pub enable_adaptive: bool,
    /// 是否启用进度平滑
    pub enable_progress_smoothing: bool,
    /// 保存目录
    pub save_dir: std::path::PathBuf,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            max_connections: 32,
            min_connections: 4,
            chunk_size: 0, // 自动
            retry_times: 10,
            timeout_secs: 1800,
            resume: true,
            skip_tls_verify: false,
            max_bandwidth_bps: 0,
            enable_mirror_discovery: true,
            enable_adaptive: true,
            enable_progress_smoothing: true,
            save_dir: std::path::PathBuf::from("./download"),
        }
    }
}
