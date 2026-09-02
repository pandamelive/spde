//! BitTorrent 下载源
//!
//! 实现 `DownloadSource` trait，支持磁力链接和 .torrent 文件。
//! 基于 librqbit（纯 Rust BT 客户端库），支持 DHT、PEX、uTP、磁力链接 metadata 交换。
//!
//! URI 格式：
//! - magnet:?xt=urn:btih:...  — 磁力链接
//! - /path/to/file.torrent     — 本地种子文件
//! - http(s)://.../file.torrent — 远程种子文件 URL

use std::any::Any;
use std::path::PathBuf;

use pandanetos::domain::{DownloadSource, SourceCapabilities};

/// BitTorrent 下载源
#[derive(Debug, Clone)]
pub struct TorrentSource {
    /// 原始 URI（磁力链接或种子文件路径）
    uri: String,
    /// 源类型（磁力链接/本地种子/远程种子）
    source_type: TorrentSourceType,
    /// 保存目录
    save_dir: PathBuf,
}

/// BitTorrent 源类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentSourceType {
    /// 磁力链接
    Magnet,
    /// 本地 .torrent 文件
    LocalTorrent,
    /// 远程 .torrent 文件 URL
    RemoteTorrent,
}

impl TorrentSource {
    /// 创建新的 BitTorrent 下载源
    pub fn new(uri: impl Into<String>, save_dir: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let uri_str = uri.into();
        let save_dir = save_dir.into();

        let source_type = if uri_str.starts_with("magnet:") {
            TorrentSourceType::Magnet
        } else if uri_str.starts_with("http://") || uri_str.starts_with("https://") {
            TorrentSourceType::RemoteTorrent
        } else {
            TorrentSourceType::LocalTorrent
        };

        Ok(Self {
            uri: uri_str,
            source_type,
            save_dir,
        })
    }

    /// 获取原始 URI
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// 获取源类型
    pub fn source_type(&self) -> TorrentSourceType {
        self.source_type
    }

    /// 获取保存目录
    pub fn save_dir(&self) -> &PathBuf {
        &self.save_dir
    }

    /// 检查是否是 BitTorrent URI
    pub fn is_torrent_uri(uri: &str) -> bool {
        uri.starts_with("magnet:")
            || uri.ends_with(".torrent")
            || (uri.starts_with("http://") || uri.starts_with("https://"))
                && uri.contains(".torrent")
    }
}

impl DownloadSource for TorrentSource {
    fn protocol(&self) -> &'static str {
        "torrent"
    }

    fn identifier(&self) -> String {
        match self.source_type {
            TorrentSourceType::Magnet => {
                // 磁力链接用 infohash 作为标识
                if let Some(hash) = self.uri.split("xt=urn:btih:").nth(1) {
                    if let Some(end) = hash.find('&') {
                        format!("torrent:magnet:{}", &hash[..end])
                    } else {
                        format!("torrent:magnet:{}", hash)
                    }
                } else {
                    format!("torrent:magnet:{}", self.uri)
                }
            }
            TorrentSourceType::LocalTorrent => {
                format!("torrent:file:{}", self.uri)
            }
            TorrentSourceType::RemoteTorrent => {
                format!("torrent:url:{}", self.uri)
            }
        }
    }

    fn display_name(&self) -> String {
        match self.source_type {
            TorrentSourceType::Magnet => "BitTorrent (磁力链接)".to_string(),
            TorrentSourceType::LocalTorrent => {
                format!("BitTorrent (本地种子: {})", self.uri)
            }
            TorrentSourceType::RemoteTorrent => {
                format!("BitTorrent (远程种子: {})", self.uri)
            }
        }
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            supports_range: false,     // BitTorrent 是 piece 级下载，不支持字节级分片
            supports_concurrent: true, // 支持多 peer 并发下载
            supports_resume: true,     // 支持断点续传（librqbit 支持）
            max_concurrency: 16,       // 最大 peer 连接数
            chunk_size_range: None,    // 无特殊要求（调度器会用单分片下载整个文件）
            immutable: true,           // 种子文件内容不可变
            protocol: "torrent",
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
