//! 统一调度编排器
//!
//! 整个智能下载器的入口，负责：
//! 1. 接收任务（URL + 配置）
//! 2. 调用 MirrorBus 发现所有可用镜像
//! 3. 调用 ChunkDownloader.probe 验证源，获取文件大小和能力
//! 4. 根据能力选择下载策略（StrategySelector）
//! 5. 创建 ChunkSet（分片规划）
//! 6. 加载 ResumeBitmap（断点续传）
//! 7. 执行策略，收集进度
//! 8. 完成后收尾（校验、rename、清理位图）
//!
//! 协议无关，只操作 domain 层的抽象。

use std::path::PathBuf;
use std::sync::Arc;

use pandanetos::domain::{
    CancellationToken, ChunkDownloader, ChunkSet, ChunkWriter, DownloadProgress, DownloadResult,
    DownloadSource, DownloadStrategy, SourceCapabilities,
};
use pandanetos::error::{CoreError, Result};
use tokio::sync::{mpsc, Mutex};

use crate::domain::DownloadConfig;
use crate::service::mirror_bus::MirrorBus;
use crate::service::strategy::multi_source_chunked::MultiSourceChunkedStrategy;

/// 统一调度编排器
pub struct DownloadScheduler {
    /// 镜像发现总线
    mirror_bus: Arc<MirrorBus>,
    /// 下载配置
    config: DownloadConfig,
}

impl DownloadScheduler {
    /// 创建一个新的下载调度器
    pub fn new(config: DownloadConfig) -> Self {
        Self {
            mirror_bus: Arc::new(MirrorBus::new()),
            config,
        }
    }

    /// 获取镜像发现总线（用于注册发现器）
    pub fn mirror_bus(&self) -> Arc<MirrorBus> {
        self.mirror_bus.clone()
    }

    /// 下载一个文件
    ///
    /// # 参数
    /// - `source`：原始下载源
    /// - `downloader`：对应的协议下载器
    /// - `save_path`：保存路径
    /// - `progress_tx`：进度汇报通道
    ///
    /// # 返回
    /// 下载结果
    pub async fn download(
        &self,
        source: Box<dyn DownloadSource>,
        downloader: Arc<dyn ChunkDownloader>,
        save_path: PathBuf,
        progress_tx: mpsc::Sender<DownloadProgress>,
    ) -> Result<DownloadResult> {
        let cancel = CancellationToken::new();

        // 1. 探测原始源，获取文件大小和能力
        let file_info = downloader.probe(source.as_ref()).await?;
        if file_info.size_bytes == 0 {
            return Err(CoreError::InvalidParam(
                "file size is 0 or probe failed".into(),
            ));
        }

        // 用 probe 结果更新 capabilities（服务器实际支持的能力，而非硬编码假设）
        let mut capabilities = source.capabilities();
        capabilities.supports_range = file_info.supports_resume;
        capabilities.supports_concurrent = file_info.supports_multi_connection;
        capabilities.supports_resume = file_info.supports_resume;
        eprintln!(
            "[scheduler] file size: {} bytes, supports_range: {}, supports_concurrent: {}",
            file_info.size_bytes, capabilities.supports_range, capabilities.supports_concurrent
        );

        // 2. 发现所有可用镜像
        let sources = if self.config.enable_mirror_discovery {
            self.mirror_bus
                .discover(source.as_ref(), downloader.as_ref(), file_info.size_bytes)
                .await?
        } else {
            vec![source]
        };

        eprintln!("[scheduler] available sources: {}", sources.len());

        // 3. 选择下载策略
        let strategy = self.select_strategy(&sources, &capabilities, downloader.clone());
        eprintln!("[scheduler] selected strategy: {}", strategy.name());

        // 4. 计算分片大小，创建 ChunkSet
        let chunk_size = self.calculate_chunk_size(
            file_info.size_bytes,
            self.config.max_connections,
            &capabilities,
        );
        let chunk_set = Arc::new(Mutex::new(ChunkSet::new(file_info.size_bytes, chunk_size)));
        eprintln!(
            "[scheduler] chunk size: {} bytes, total chunks: {}",
            chunk_size,
            chunk_set.lock().await.chunks.len()
        );

        // 5. 创建写入器（.part 临时文件）
        let part_path = save_path.with_extension("part");
        let writer = Arc::new(
            crate::infra::disk::file_writer::FileChunkWriter::open(part_path.clone()).await?,
        );

        // 6. 执行策略
        let result = strategy
            .execute(
                sources,
                chunk_set,
                writer.clone(),
                progress_tx,
                cancel.clone(),
            )
            .await?;

        // 7. 收尾：rename .part → 目标文件
        if result.success {
            writer.flush().await?;
            // 关闭 writer 后 rename
            // FileChunkWriter 没有 close 方法，drop 时会自动关闭
            drop(writer);
            tokio::fs::rename(&part_path, &save_path)
                .await
                .map_err(|e| {
                    CoreError::Internal(format!("rename {:?} -> {:?}: {}", part_path, save_path, e))
                })?;
            eprintln!("[scheduler] download complete: {:?}", save_path);
        }

        Ok(result)
    }

    /// 选择下载策略
    fn select_strategy(
        &self,
        _sources: &[Box<dyn DownloadSource>],
        caps: &SourceCapabilities,
        downloader: Arc<dyn ChunkDownloader>,
    ) -> Box<dyn DownloadStrategy> {
        // 目前只有 MultiSourceChunked 策略
        // 后续可扩展：SingleSourceFastest、TorrentNative 等
        let strategy = MultiSourceChunkedStrategy::new(
            0, // 自动
            self.config.max_connections,
            self.config.min_connections,
            downloader,
        );

        if strategy.supports(&[], caps) {
            Box::new(strategy)
        } else {
            // 兜底：还是用 MultiSourceChunked（即使能力不匹配，也能尝试）
            Box::new(strategy)
        }
    }

    /// 计算分片大小
    fn calculate_chunk_size(
        &self,
        total_size: u64,
        connections: u32,
        caps: &SourceCapabilities,
    ) -> u64 {
        // 目标：每个连接平均分到 8-16 个分片
        let target_chunks_per_conn = 12;
        let total_chunks = connections as u64 * target_chunks_per_conn;
        let dynamic = total_size / total_chunks.max(1);

        // 钳制在协议建议的范围内
        let (min, max) = caps
            .chunk_size_range
            .unwrap_or((4 * 1024 * 1024, 64 * 1024 * 1024));

        dynamic.clamp(min, max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_chunk_size() {
        let config = DownloadConfig::default();
        let scheduler = DownloadScheduler::new(config);
        let caps = SourceCapabilities::default();

        // 1GB 文件，8 连接
        let size = 1024 * 1024 * 1024;
        let chunk_size = scheduler.calculate_chunk_size(size, 8, &caps);
        // 目标：1GB / (8*12) = ~10.9MB，钳制在 4-64MB
        assert!(chunk_size >= 4 * 1024 * 1024);
        assert!(chunk_size <= 64 * 1024 * 1024);
    }

    #[test]
    fn test_scheduler_creation() {
        let config = DownloadConfig::default();
        let scheduler = DownloadScheduler::new(config);
        assert_eq!(scheduler.config.max_connections, 32);
    }
}
