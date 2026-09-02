//! 统一数据块获取接口（协议无关核心抽象）
//!
//! 所有下载协议（HTTP/FTP/SSH/Torrent/File）都实现此 trait，
//! 调度器完全不关心底层协议，只操作 ChunkFetcher 接口。
//!
//! 设计原则：
//! - 协议无关：调度器不需要知道具体协议
//! - 能力驱动：通过 probe() 探测能力，自动选择下载方式
//! - 统一数据块：所有协议都抽象成"获取指定偏移和长度的数据块"
//! - 可插拔：新增协议只需实现此 trait，不需要改调度器

use std::any::Any;
use std::fmt::Debug;

use async_trait::async_trait;
use tokio::io::AsyncWrite;

use pandanetos::error::Result;

/// 源能力描述（probe 探测结果）
///
/// 调度器根据这些能力决定下载方式：
/// - supports_range: 是否支持范围请求（决定能否分片）
/// - supports_multi_connection: 是否支持多连接并发
/// - supports_resume: 是否支持断点续传
/// - immutable: 内容是否不可变（影响缓存策略）
#[derive(Debug, Clone, Copy)]
pub struct SourceCapabilities {
    /// 是否支持范围请求（Range / REST / seek）
    /// true: 可以从任意偏移开始读取，支持分片下载
    /// false: 只能从头开始顺序读取
    pub supports_range: bool,

    /// 是否支持多连接并发
    /// true: 可以同时建立多个连接下载不同分片
    /// false: 只能单连接下载
    pub supports_multi_connection: bool,

    /// 是否支持断点续传
    /// true: 中断后可以从已下载位置继续
    /// false: 中断后需要重新下载
    pub supports_resume: bool,

    /// 内容是否不可变
    /// true: 内容一旦确定就不会改变（如 BT 种子、iOS 固件）
    /// false: 内容可能变化（如动态页面）
    pub immutable: bool,

    /// 最大并发连接数（0 表示无限制）
    pub max_concurrency: u32,

    /// 推荐的分片大小范围（字节）
    /// None: 无特殊要求，调度器自动选择
    pub chunk_size_range: Option<(u64, u64)>,

    /// 协议名称（http/https/ftp/sftp/torrent/file）
    pub protocol: &'static str,
}

impl Default for SourceCapabilities {
    fn default() -> Self {
        Self {
            supports_range: false,
            supports_multi_connection: false,
            supports_resume: false,
            immutable: false,
            max_concurrency: 0,
            chunk_size_range: None,
            protocol: "unknown",
        }
    }
}

/// 数据块获取结果统计
#[derive(Debug, Clone)]
pub struct ChunkStats {
    /// 分片 ID
    pub chunk_id: u32,
    /// 实际下载字节数
    pub bytes_downloaded: u64,
    /// 下载耗时（毫秒）
    pub elapsed_ms: u64,
    /// 平均速度（字节/秒）
    pub speed_bps: u64,
    /// 是否命中缓存
    pub from_cache: bool,
    /// 使用的源标识
    pub source_id: String,
}

/// 统一数据块获取接口（所有下载协议实现此 trait）
///
/// # 设计说明
///
/// 这是下载器的核心抽象。调度器通过此接口获取数据块，
/// 完全不关心底层是 HTTP、FTP、BT 还是本地文件。
///
/// # 协议实现示例
///
/// - HTTP (支持 Range): fetch_chunk 发送 Range 请求
/// - HTTP (不支持 Range): fetch_chunk 从头下载，跳过 offset 前的字节
/// - BitTorrent: fetch_chunk 从 peer 获取 piece
/// - FTP: fetch_chunk 发送 REST 命令后下载
/// - 本地文件: fetch_chunk seek 后 read
///
/// # 线程安全
///
/// 所有方法都接收 &self，实现必须是线程安全的（Send + Sync）。
/// 多 worker 会并发调用 fetch_chunk。
#[async_trait]
pub trait ChunkFetcher: Send + Sync + Debug {
    /// 协议名称（http/https/ftp/sftp/torrent/file）
    fn protocol(&self) -> &'static str;

    /// 源的唯一标识符（用于去重和日志）
    fn identifier(&self) -> String;

    /// 源的显示名称（用于 UI 展示）
    fn display_name(&self) -> String;

    /// 探测源的能力和文件信息
    ///
    /// # 返回
    /// - (file_size, capabilities): 文件大小和能力描述
    ///
    /// # 注意
    /// - file_size 为 0 表示未知（如磁力链接未下载 metadata 时）
    /// - 此方法应该是轻量级的，只发送 HEAD 请求或少量探测
    async fn probe(&self) -> Result<(u64, SourceCapabilities)>;

    /// 获取指定偏移和长度的数据块，写入 writer
    ///
    /// # 参数
    /// - offset: 数据块在文件中的起始偏移（字节）
    /// - length: 数据块长度（字节）
    /// - writer: 异步写入器，数据写入此对象
    ///
    /// # 返回
    /// - ChunkStats: 下载统计信息
    ///
    /// # 错误处理
    /// - 如果 supports_range=false 且 offset>0，实现应从头下载并跳过前面的字节
    /// - 网络错误应返回 Err，调度器会重试
    /// - 取消令牌由调度器通过 tokio::task::abort 处理
    async fn fetch_chunk(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn AsyncWrite + Unpin + Send),
    ) -> Result<ChunkStats>;

    /// 克隆此 fetcher（用于多 worker 并发）
    ///
    /// # 注意
    /// - 实现应尽量轻量，共享内部状态（如连接池）
    /// - 如果内部有连接池，克隆应共享同一个连接池
    fn clone_box(&self) -> Box<dyn ChunkFetcher>;

    /// 用于向下转型（如果需要访问协议特定功能）
    fn as_any(&self) -> &dyn Any;
}

/// 方便的克隆宏
impl Clone for Box<dyn ChunkFetcher> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
