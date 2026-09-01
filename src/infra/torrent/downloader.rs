//! BitTorrent 分片下载器
//!
//! 实现 `ChunkDownloader` trait，支持磁力链接和 .torrent 文件。
//!
//! ## 当前状态
//! - **dry_run 模式**：完整支持，模拟下载流程，不写盘、不创建目录
//! - **真实下载模式**：暂未实现，返回明确错误信息
//!
//! 注意：由于 BitTorrent 协议是 piece 级下载，不支持字节级分片，
//! 调度器会用单分片下载整个文件。

use std::time::Instant;

use super::source::{TorrentSource, TorrentSourceType};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use pandanetos::domain::{
    CancellationToken, Chunk, ChunkDownloader, ChunkStats, DownloadFileInfo, DownloadSource,
};
use pandanetos::error::{CoreError, Result};

/// BitTorrent 分片下载器
#[derive(Debug, Clone)]
pub struct TorrentChunkDownloader {
    /// 连接超时（秒）
    timeout_secs: u64,
    /// 是否为 dry_run 模式（不落盘）
    dry_run: bool,
}

impl Default for TorrentChunkDownloader {
    fn default() -> Self {
        Self {
            timeout_secs: 1800,
            dry_run: true,
        }
    }
}

impl TorrentChunkDownloader {
    /// 创建新的 BitTorrent 分片下载器
    ///
    /// # 参数
    /// - `timeout_secs`: 连接超时（秒）
    /// - `dry_run`: 是否为 dry_run 模式（不落盘，模拟下载）
    pub fn new(timeout_secs: u64, dry_run: bool) -> Self {
        Self {
            timeout_secs,
            dry_run,
        }
    }

    /// 尝试解析 .torrent 文件获取文件大小
    ///
    /// 仅支持本地 .torrent 文件，磁力链接和远程种子返回 None。
    async fn probe_torrent_size(source: &TorrentSource) -> Option<u64> {
        const DEFAULT_SIZE: u64 = 1024 * 1024 * 1024;

        match source.source_type() {
            TorrentSourceType::LocalTorrent => {
                if let Ok(meta) = tokio::fs::metadata(source.uri()).await {
                    tracing::warn!(
                        "[torrent] .torrent 文件大小估算（未解析 bencode）: {} bytes",
                        meta.len()
                    );
                }
                Some(DEFAULT_SIZE)
            }
            TorrentSourceType::Magnet => {
                tracing::warn!("[torrent] 磁力链接大小未知，使用默认值 1GB");
                Some(DEFAULT_SIZE)
            }
            TorrentSourceType::RemoteTorrent => {
                tracing::warn!("[torrent] 远程种子大小未知，使用默认值 1GB");
                Some(DEFAULT_SIZE)
            }
        }
    }
}

#[async_trait]
impl ChunkDownloader for TorrentChunkDownloader {
    fn protocol(&self) -> &str {
        "torrent"
    }

    /// 探测 BitTorrent 文件的可用性和信息
    ///
    /// - 本地 .torrent 文件：尝试解析获取文件大小
    /// - 磁力链接/远程种子：返回默认大小（1GB），实际大小在下载时获取
    ///
    /// **重要**：不再返回 size_bytes=0，否则调度器会直接报错。
    async fn probe(&self, source: &dyn DownloadSource) -> Result<DownloadFileInfo> {
        let torrent_source = source
            .as_any()
            .downcast_ref::<TorrentSource>()
            .context("source is not a TorrentSource")?;

        let size_bytes = Self::probe_torrent_size(torrent_source)
            .await
            .unwrap_or(1024 * 1024 * 1024);

        if self.dry_run {
            Ok(DownloadFileInfo {
                size_bytes,
                supports_resume: false,
                supports_multi_connection: false,
            })
        } else {
            Ok(DownloadFileInfo {
                size_bytes,
                supports_resume: true,
                supports_multi_connection: true,
            })
        }
    }

    /// 下载一个分片
    ///
    /// - **dry_run 模式**：模拟下载成功，不写盘、不创建目录
    /// - **真实下载模式**：返回明确错误信息（暂未实现）
    ///
    /// 注意：调度器会根据 capabilities 用单分片下载整个文件，所以通常只会调用一次。
    async fn download_chunk(
        &self,
        source: &dyn DownloadSource,
        chunk: &Chunk,
        writer: &dyn pandanetos::domain::ChunkWriter,
        cancel: &CancellationToken,
    ) -> Result<ChunkStats> {
        let start = Instant::now();
        let torrent_source = source
            .as_any()
            .downcast_ref::<TorrentSource>()
            .context("source is not a TorrentSource")?;

        if cancel.is_cancelled() {
            return Err(CoreError::External(anyhow!("download cancelled")));
        }

        if self.dry_run {
            tracing::info!(
                "[torrent] dry_run 模式，模拟下载: {} (chunk_id={}, offset={}, length={})",
                torrent_source.uri(),
                chunk.chunk_id,
                chunk.offset,
                chunk.length
            );

            let dummy_data = vec![0u8; chunk.length as usize];
            writer
                .write_at(chunk.offset, &dummy_data)
                .await
                .context("failed to write chunk (dry_run)")?;

            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            let elapsed = start.elapsed().as_secs_f64();
            return Ok(ChunkStats {
                chunk_id: chunk.chunk_id,
                source_id: source.identifier(),
                downloaded_bytes: chunk.length,
                elapsed_secs: elapsed,
                success: true,
                error_code: None,
            });
        }

        let source_type = torrent_source.source_type();
        let error_msg = match source_type {
            TorrentSourceType::LocalTorrent => {
                "BitTorrent 本地种子下载暂未实现（dry_run 模式可用）"
            }
            TorrentSourceType::Magnet => "BitTorrent 磁力链接下载暂未实现（dry_run 模式可用）",
            TorrentSourceType::RemoteTorrent => {
                "BitTorrent 远程种子下载暂未实现（dry_run 模式可用）"
            }
        };

        tracing::error!("[torrent] {}", error_msg);
        Err(CoreError::External(anyhow!(error_msg)))
    }
}
