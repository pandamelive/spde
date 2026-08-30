//! 本地文件分片下载器
//!
//! 实现 `ChunkDownloader` trait，支持本地文件的随机读取和分片复制。
//! 每个分片从源文件的指定偏移量读取，写入目标文件的对应位置。
//! 支持多线程并发读取不同分片，适合 SSD 场景。

use std::time::Instant;

use anyhow::{Context, anyhow};
use pandanetos::error::{CoreError, Result};
use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

use pandanetos::domain::{
    Chunk, ChunkDownloader, ChunkStats, CancellationToken, DownloadFileInfo, DownloadSource,
};

use super::source::FileSource;

/// 本地文件分片下载器
#[derive(Debug, Clone, Default)]
pub struct FileChunkDownloader;

impl FileChunkDownloader {
    /// 创建新的本地文件分片下载器
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ChunkDownloader for FileChunkDownloader {
    fn protocol(&self) -> &str {
        "file"
    }

    /// 探测本地文件的可用性和信息
    async fn probe(&self, source: &dyn DownloadSource) -> Result<DownloadFileInfo> {
        let file_source = source
            .as_any()
            .downcast_ref::<FileSource>()
            .context("source is not a FileSource")?;

        let metadata = tokio::fs::metadata(file_source.path())
            .await
            .with_context(|| format!("failed to stat file: {:?}", file_source.path()))?;

        if !metadata.is_file() {
            return Err(CoreError::External(anyhow!("source is not a regular file: {:?}", file_source.path())));
        }

        Ok(DownloadFileInfo {
            size_bytes: metadata.len(),
            supports_resume: true,
            supports_multi_connection: true,
        })
    }

    /// 下载一个分片（从源文件读取，写入目标文件）
    async fn download_chunk(
        &self,
        source: &dyn DownloadSource,
        chunk: &Chunk,
        writer: &dyn pandanetos::domain::ChunkWriter,
        cancel: &CancellationToken,
    ) -> Result<ChunkStats> {
        let start = Instant::now();
        let file_source = source
            .as_any()
            .downcast_ref::<FileSource>()
            .context("source is not a FileSource")?;

        // 打开源文件
        let mut file = File::open(file_source.path())
            .await
            .with_context(|| format!("failed to open source file: {:?}", file_source.path()))?;

        // 定位到分片偏移量
        file.seek(SeekFrom::Start(chunk.offset))
            .await
            .context("failed to seek to chunk offset")?;

        // 读取分片数据（分块读取，避免大内存占用）
        let mut remaining = chunk.length as usize;
        let mut buf = vec![0u8; 256 * 1024]; // 256KB 读取缓冲区
        let mut written_offset = chunk.offset;

        while remaining > 0 {
            // 检查取消
            if cancel.is_cancelled() {
                return Err(CoreError::External(anyhow!("download cancelled")));
            }

            let to_read = remaining.min(buf.len());
            let n = file
                .read(&mut buf[..to_read])
                .await
                .context("failed to read from source file")?;

            if n == 0 {
                return Err(CoreError::External(anyhow!(
                    "unexpected EOF at offset {} (expected {} more bytes)",
                    written_offset,
                    remaining
                )));
            }

            // 写入目标文件
            writer
                .write_at(written_offset, &buf[..n])
                .await
                .context("failed to write chunk")?;

            written_offset += n as u64;
            remaining -= n;
        }

        let elapsed = start.elapsed().as_secs_f64();

        Ok(ChunkStats {
            chunk_id: chunk.chunk_id,
            source_id: source.identifier(),
            downloaded_bytes: chunk.length,
            elapsed_secs: elapsed,
            success: true,
            error_code: None,
        })
    }
}
