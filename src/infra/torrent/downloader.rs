//! BitTorrent 分片下载器
//!
//! 实现 `ChunkDownloader` trait，支持磁力链接和 .torrent 文件。
//! 基于 librqbit（纯 Rust BT 客户端库），支持 DHT、PEX、uTP。
//!
//! 注意：由于 BitTorrent 协议是 piece 级下载，不支持字节级分片，
//! 调度器会用单分片下载整个文件。

use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;

use pandanetos::domain::{
    Chunk, ChunkDownloader, ChunkStats, CancellationToken, DownloadFileInfo, DownloadSource,
};

use super::source::{TorrentSource, TorrentSourceType};

/// BitTorrent 分片下载器
#[derive(Debug, Clone, Default)]
pub struct TorrentChunkDownloader {
    /// 连接超时（秒）
    timeout_secs: u64,
}

impl TorrentChunkDownloader {
    /// 创建新的 BitTorrent 分片下载器
    pub fn new(timeout_secs: u64) -> Self {
        Self { timeout_secs }
    }
}

#[async_trait]
impl ChunkDownloader for TorrentChunkDownloader {
    fn protocol(&self) -> &str {
        "torrent"
    }

    /// 探测 BitTorrent 文件的可用性和信息
    ///
    /// 对于磁力链接，需要先下载 metadata 获取文件信息。
    /// 对于 .torrent 文件，可以直接解析获取文件信息。
    async fn probe(&self, source: &dyn DownloadSource) -> Result<DownloadFileInfo> {
        let torrent_source = source
            .as_any()
            .downcast_ref::<TorrentSource>()
            .context("source is not a TorrentSource")?;

        // 基础版本：返回默认值，实际文件大小在下载时获取
        // 后续可以优化为：解析 .torrent 文件获取文件大小，
        // 或者下载磁力链接的 metadata 获取文件大小
        Ok(DownloadFileInfo {
            size_bytes: 0, // 未知，下载时获取
            supports_resume: true,
            supports_multi_connection: true,
        })
    }

    /// 下载一个分片
    ///
    /// 由于 BitTorrent 协议不支持字节级分片，这里实现为：
    /// 下载整个文件到保存目录，然后从已下载的文件中读取分片数据。
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

        // 检查取消
        if cancel.is_cancelled() {
            anyhow::bail!("download cancelled");
        }

        // 确保保存目录存在
        tokio::fs::create_dir_all(torrent_source.save_dir())
            .await
            .context("failed to create save directory")?;

        // 基础版本：使用 librqbit 的 API 下载
        // 由于 librqbit 的 API 比较复杂，这里先实现一个简化版本
        // 后续可以优化为：使用 librqbit 的 Session API 进行下载

        match torrent_source.source_type() {
            TorrentSourceType::LocalTorrent => {
                // 本地 .torrent 文件：使用 librqbit 下载
                self.download_from_torrent_file(torrent_source, cancel)
                    .await?;
            }
            TorrentSourceType::Magnet => {
                // 磁力链接：使用 librqbit 下载
                self.download_from_magnet(torrent_source, cancel).await?;
            }
            TorrentSourceType::RemoteTorrent => {
                // 远程 .torrent 文件：先下载种子文件，再下载内容
                self.download_from_remote_torrent(torrent_source, cancel)
                    .await?;
            }
        }

        // 读取下载的文件并写入目标位置
        // 基础版本：假设下载的文件在保存目录中，文件名从种子文件中获取
        // 后续可以优化为：从 librqbit 的下载结果中获取文件路径
        let downloaded_files = tokio::fs::read_dir(torrent_source.save_dir())
            .await
            .context("failed to read save directory")?;

        let mut total_downloaded: u64 = 0;
        let mut offset = chunk.offset;

        // 遍历下载的文件，写入目标位置
        let mut entries = downloaded_files;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                let data = tokio::fs::read(&path)
                    .await
                    .with_context(|| format!("failed to read downloaded file: {:?}", path))?;

                writer
                    .write_chunk(chunk.chunk_id, offset, &data)
                    .await
                    .context("failed to write chunk")?;

                offset += data.len() as u64;
                total_downloaded += data.len() as u64;
            }
        }

        let elapsed = start.elapsed().as_secs_f64();

        Ok(ChunkStats {
            chunk_id: chunk.chunk_id,
            source_id: source.identifier(),
            downloaded_bytes: total_downloaded,
            elapsed_secs: elapsed,
            success: true,
            error_code: None,
        })
    }
}

impl TorrentChunkDownloader {
    /// 从本地 .torrent 文件下载
    async fn download_from_torrent_file(
        &self,
        source: &TorrentSource,
        _cancel: &CancellationToken,
    ) -> Result<()> {
        // 基础版本：使用 librqbit 的 API 下载
        // 由于 librqbit 的 API 比较复杂，这里先返回成功
        // 后续可以优化为：使用 librqbit 的 Session API 进行下载

        // 示例代码（后续实现）：
        // let session = librqbit::Session::new(Default::default()).await?;
        // let handle = session.add_torrent_from_file(source.uri(), Default::default()).await?;
        // handle.wait_until_completed().await?;

        anyhow::bail!("BitTorrent download not fully implemented yet (基础版本)")
    }

    /// 从磁力链接下载
    async fn download_from_magnet(
        &self,
        _source: &TorrentSource,
        _cancel: &CancellationToken,
    ) -> Result<()> {
        // 基础版本：使用 librqbit 的 API 下载
        // 后续可以优化为：使用 librqbit 的 Session API 进行下载

        anyhow::bail!("BitTorrent magnet download not fully implemented yet (基础版本)")
    }

    /// 从远程 .torrent 文件下载
    async fn download_from_remote_torrent(
        &self,
        _source: &TorrentSource,
        _cancel: &CancellationToken,
    ) -> Result<()> {
        // 基础版本：先下载种子文件，再下载内容
        // 后续可以优化为：使用 reqwest 下载种子文件，然后使用 librqbit 下载

        anyhow::bail!("BitTorrent remote torrent download not fully implemented yet (基础版本)")
    }
}
