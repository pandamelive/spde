//! Unified download scheduler
//!
//! Entry point of the smart downloader, responsible for:
//! 1. Receive task (URL + config)
//! 2. Use MirrorBus to discover all available mirrors
//! 3. Use ChunkDownloader.probe to verify source, get file size and capabilities
//! 4. Select download strategy based on capabilities (StrategySelector)
//! 5. Create ChunkSet (chunk planning)
//! 6. Load ResumeBitmap (resume)
//! 7. Execute strategy, collect progress
//! 8. Finalize after completion (verify, rename, cleanup bitmap)
//!
//! Protocol-agnostic, only operates on domain layer abstractions.

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

/// Unified download scheduler
pub struct DownloadScheduler {
    /// Mirror discovery bus
    mirror_bus: Arc<MirrorBus>,
    /// Download config
    config: DownloadConfig,
}

impl DownloadScheduler {
    /// Create a new download scheduler
    pub fn new(config: DownloadConfig) -> Self {
        Self {
            mirror_bus: Arc::new(MirrorBus::new()),
            config,
        }
    }

    /// Get mirror discovery bus (for registering discoverers)
    pub fn mirror_bus(&self) -> Arc<MirrorBus> {
        self.mirror_bus.clone()
    }

    /// Download a file
    ///
    /// # Parameters
    /// - `source`: original download source
    /// - `downloader`: corresponding protocol downloader
    /// - `save_path`: save path
    /// - `progress_tx`: progress reporting channel
    ///
    /// # Returns
    /// Download result
    pub async fn download(
        &self,
        source: Box<dyn DownloadSource>,
        downloader: Arc<dyn ChunkDownloader>,
        save_path: PathBuf,
        progress_tx: mpsc::Sender<DownloadProgress>,
    ) -> Result<DownloadResult> {
        let cancel = CancellationToken::new();

        // 1. Probe original source, get file size and capabilities
        let file_info = downloader.probe(source.as_ref()).await?;
        if file_info.size_bytes == 0 {
            return Err(CoreError::InvalidParam(
                "file size is 0 or probe failed".into(),
            ));
        }

        // Update capabilities with probe results (actual server capabilities, not hardcoded assumptions)
        let mut capabilities = source.capabilities();
        capabilities.supports_range = file_info.supports_resume;
        capabilities.supports_concurrent = file_info.supports_multi_connection;
        capabilities.supports_resume = file_info.supports_resume;
        eprintln!(
            "[scheduler] file size: {} bytes, supports_range: {}, supports_concurrent: {}",
            file_info.size_bytes, capabilities.supports_range, capabilities.supports_concurrent
        );

        // 2. Discover all available mirrors
        let sources = if self.config.enable_mirror_discovery {
            self.mirror_bus
                .discover(source.as_ref(), downloader.as_ref(), file_info.size_bytes)
                .await?
        } else {
            vec![source]
        };

        eprintln!("[scheduler] available sources: {}", sources.len());

        // 3. Select download strategy
        let strategy = self.select_strategy(&sources, &capabilities, downloader.clone());
        eprintln!("[scheduler] selected strategy: {}", strategy.name());

        // 4. Calculate chunk size, create ChunkSet
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

        // 5. Create writer (dry_run mode uses null writer, does not write to disk)
        let part_path = save_path.with_extension("part");
        let writer: Arc<dyn ChunkWriter> = if self.config.dry_run {
            Arc::new(crate::infra::disk::null_writer::NullChunkWriter::new())
        } else {
            Arc::new(
                crate::infra::disk::file_writer::FileChunkWriter::open(part_path.clone()).await?,
            )
        };

        // 6. Execute strategy
        let result = strategy
            .execute(
                sources,
                chunk_set,
                writer.clone(),
                progress_tx,
                cancel.clone(),
            )
            .await?;

        // 7. Finalize: rename .part -> target file (skip in dry_run mode)
        if result.success && !self.config.dry_run {
            writer.flush().await?;
            drop(writer);
            tokio::fs::rename(&part_path, &save_path)
                .await
                .map_err(|e| {
                    CoreError::Internal(format!("rename {:?} -> {:?}: {}", part_path, save_path, e))
                })?;
            eprintln!("[scheduler] download complete: {:?}", save_path);
        } else if result.success && self.config.dry_run {
            eprintln!(
                "[scheduler] download complete (dry-run, not saved to disk): {:?}",
                save_path
            );
        }

        Ok(result)
    }

    /// Select download strategy
    fn select_strategy(
        &self,
        _sources: &[Box<dyn DownloadSource>],
        caps: &SourceCapabilities,
        downloader: Arc<dyn ChunkDownloader>,
    ) -> Box<dyn DownloadStrategy> {
        // Currently only MultiSourceChunked strategy
        // Future extensions: SingleSourceFastest, TorrentNative, etc.
        let strategy = MultiSourceChunkedStrategy::new(
            0, // auto
            self.config.max_connections,
            self.config.min_connections,
            downloader,
        );

        if strategy.supports(&[], caps) {
            Box::new(strategy)
        } else {
            // Fallback: still use MultiSourceChunked (even if capabilities don't match, can still try)
            Box::new(strategy)
        }
    }

    /// Calculate chunk size
    fn calculate_chunk_size(
        &self,
        total_size: u64,
        connections: u32,
        caps: &SourceCapabilities,
    ) -> u64 {
        // Goal: each connection gets 8-16 chunks on average
        let target_chunks_per_conn = 12;
        let total_chunks = connections as u64 * target_chunks_per_conn;
        let dynamic = total_size / total_chunks.max(1);

        // Clamp within protocol recommended range
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

        // 1GB file, 8 connections
        let size = 1024 * 1024 * 1024;
        let chunk_size = scheduler.calculate_chunk_size(size, 8, &caps);
        // Goal: 1GB / (8*12) = ~10.9MB, clamped to 4-64MB
        assert!(chunk_size >= 4 * 1024 * 1024);
        assert!(chunk_size <= 64 * 1024 * 1024);
    }

    #[test]
    fn test_scheduler_creation() {
        let config = DownloadConfig::default();
        let scheduler = DownloadScheduler::new(config);
        assert_eq!(scheduler.config.max_connections, 32);
    }

    #[test]
    fn test_dry_run_default_enabled() {
        let config = DownloadConfig::default();
        assert!(config.dry_run, "dry_run should default to true");
    }
}
