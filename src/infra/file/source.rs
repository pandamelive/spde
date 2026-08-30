//! 本地文件下载源
//!
//! 实现 `DownloadSource` trait，支持 file:// 协议。
//! 本地文件支持随机读取和多连接并发读取，适合 SSD 场景下的多线程复制。

use std::any::Any;
use std::path::{Path, PathBuf};

use pandanetos::domain::{DownloadSource, SourceCapabilities};

/// 本地文件下载源
#[derive(Debug, Clone)]
pub struct FileSource {
    /// 文件路径
    path: PathBuf,
}

impl FileSource {
    /// 创建新的本地文件源
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// 从 file:// URI 解析创建
    pub fn from_uri(uri: &str) -> anyhow::Result<Self> {
        let path = uri
            .strip_prefix("file://")
            .or_else(|| uri.strip_prefix("file:"))
            .ok_or_else(|| anyhow::anyhow!("invalid file uri: {}", uri))?;
        Ok(Self::new(PathBuf::from(path)))
    }

    /// 获取文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 检查是否是 file:// URI
    pub fn is_file_uri(uri: &str) -> bool {
        uri.starts_with("file://") || uri.starts_with("file:")
    }
}

impl DownloadSource for FileSource {
    fn protocol(&self) -> &str {
        "file"
    }

    fn identifier(&self) -> String {
        format!("file://{}", self.path.display())
    }

    fn display_name(&self) -> String {
        format!("本地文件: {}", self.path.display())
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            supports_range: true,      // 本地文件支持随机读取
            supports_concurrent: true, // 支持多连接并发读取（SSD 场景）
            supports_resume: true,     // 支持断点续传
            max_concurrency: 8,        // 建议最大并发数（磁盘 IO 限制）
            chunk_size_range: Some((1 * 1024 * 1024, 16 * 1024 * 1024)), // 1MB ~ 16MB
            immutable: true,           // 本地文件内容不可变（下载过程中不会变化）
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
