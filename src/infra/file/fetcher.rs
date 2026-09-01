//! Local File Fetcher（本地文件下载器）
//!
//! 支持本地文件拷贝（file:// 协议或直接路径）。
//! 通过 seek 支持分片读取，速度极快（受限于磁盘 IO）。

use std::any::Any;
use std::path::PathBuf;
use std::time::Instant;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, info};

use pandanetos::error::{CoreError, Result};

use crate::domain::chunk_fetcher::{ChunkFetcher, ChunkStats, SourceCapabilities};

/// Local File Fetcher
#[derive(Debug, Clone)]
pub struct LocalFileFetcher {
    /// 文件路径
    path: PathBuf,
    /// 文件大小（probe 后确定）
    file_size: u64,
}

impl LocalFileFetcher {
    /// 创建新的 Local File Fetcher
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            file_size: 0,
        }
    }

    /// 从 URL 或路径解析文件路径
    fn parse_path(input: &str) -> Result<PathBuf> {
        let path_str = if input.starts_with("file://") {
            input.strip_prefix("file://").unwrap()
        } else {
            input
        };

        let path = PathBuf::from(path_str);
        if !path.exists() {
            return Err(CoreError::NotFound(format!(
                "file not found: {}",
                path.display()
            )));
        }

        Ok(path)
    }
}

#[async_trait]
impl ChunkFetcher for LocalFileFetcher {
    fn protocol(&self) -> &str {
        "file"
    }

    fn identifier(&self) -> String {
        format!("file:{}", self.path.display())
    }

    fn display_name(&self) -> String {
        self.path.display().to_string()
    }

    async fn probe(&self) -> Result<(u64, SourceCapabilities)> {
        let metadata = tokio::fs::metadata(&self.path)
            .await
            .map_err(|e| CoreError::IO(format!("failed to stat file: {}", e)))?;

        let file_size = metadata.len();

        let capabilities = SourceCapabilities {
            supports_range: true,  // 本地文件支持 seek
            supports_multi_connection: true, // 支持多线程并发读取
            supports_resume: true,  // 支持断点续传
            immutable: false,
            max_concurrency: 8,     // 磁盘 IO 限制
            chunk_size_range: Some((1 * 1024 * 1024, 64 * 1024 * 1024)),
            protocol: "file",
        };

        info!(
            path = %self.path.display(),
            file_size = file_size,
            "local file probe completed"
        );

        Ok((file_size, capabilities))
    }

    async fn fetch_chunk(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<ChunkStats> {
        let start = Instant::now();

        debug!(
            path = %self.path.display(),
            offset = offset,
            length = length,
            "reading local file chunk"
        );

        let mut file = tokio::fs::File::open(&self.path)
            .await
            .map_err(|e| CoreError::IO(format!("failed to open file: {}", e)))?;

        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| CoreError::IO(format!("failed to seek: {}", e)))?;

        let mut buf = vec![0u8; length as usize];
        let mut total_read = 0usize;

        while total_read < length as usize {
            let n = file
                .read(&mut buf[total_read..])
                .await
                .map_err(|e| CoreError::IO(format!("failed to read: {}", e)))?;
            if n == 0 {
                break; // EOF
            }
            total_read += n;
        }

        writer
            .write_all(&buf[..total_read])
            .await
            .map_err(|e| CoreError::IO(format!("failed to write: {}", e)))?;

        writer
            .flush()
            .await
            .map_err(|e| CoreError::IO(format!("failed to flush: {}", e)))?;

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let speed_bps = if elapsed_ms > 0 {
            total_read as u64 * 1000 / elapsed_ms
        } else {
            0
        };

        Ok(ChunkStats {
            chunk_id: 0,
            bytes_downloaded: total_read as u64,
            elapsed_ms,
            speed_bps,
            from_cache: false,
            source_id: self.identifier(),
        })
    }

    fn clone_box(&self) -> Box<dyn ChunkFetcher> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
