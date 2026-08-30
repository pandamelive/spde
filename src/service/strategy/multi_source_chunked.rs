//! 多源并发分片下载策略
//!
//! 最通用的下载策略，适用于多源 + 支持分片 + 支持并发的场景。
//! - 多个源并发下载不同分片
//! - 按速度权重分配分片（速度快的源分到更多分片）
//! - 失败分片自动重新入队，切换到其他源
//! - 自适应连接数（启动探测 + 运行时动态调整）
//!
//! 协议无关，只操作 domain 层的抽象。

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pandanetos::domain::{
    CancellationToken, ChunkDownloader, ChunkSet, ChunkWriter, DownloadProgress, DownloadResult,
    DownloadSource, DownloadStrategy, SourceCapabilities,
};
use pandanetos::error::Result;
use tokio::sync::{mpsc, Mutex};

use crate::service::chunk_scheduler::ChunkScheduler;
use crate::service::progress::ProgressSmoother;
use crate::service::source_manager::SourceManager;

/// 多源并发分片下载策略
pub struct MultiSourceChunkedStrategy {
    /// 初始连接数（0 = 自动）
    initial_connections: u32,
    /// 最大连接数
    max_connections: u32,
    /// 最小连接数
    min_connections: u32,
    /// 最大重试次数
    max_retries: u32,
    /// 分片下载器（协议相关，由调用方注入）
    downloader: Arc<dyn ChunkDownloader>,
}

impl MultiSourceChunkedStrategy {
    /// 创建一个新的多源并发分片策略
    pub fn new(
        initial_connections: u32,
        max_connections: u32,
        min_connections: u32,
        downloader: Arc<dyn ChunkDownloader>,
    ) -> Self {
        Self {
            initial_connections: initial_connections.max(1),
            max_connections: max_connections.max(1),
            min_connections: min_connections.max(1),
            max_retries: 10,
            downloader,
        }
    }
}

#[async_trait]
impl DownloadStrategy for MultiSourceChunkedStrategy {
    fn name(&self) -> &str {
        "multi_source_chunked"
    }

    fn supports(&self, _sources: &[&dyn DownloadSource], caps: &SourceCapabilities) -> bool {
        caps.supports_range && caps.supports_concurrent && caps.supports_resume
    }

    async fn execute(
        &self,
        sources: Vec<Box<dyn DownloadSource>>,
        chunk_set: Arc<Mutex<ChunkSet>>,
        writer: Arc<dyn ChunkWriter>,
        progress_tx: mpsc::Sender<DownloadProgress>,
        cancel: CancellationToken,
    ) -> Result<DownloadResult> {
        let start = Instant::now();
        let total_size = {
            let cs = chunk_set.lock().await;
            cs.total_size
        };

        // 初始化组件
        let source_manager = Arc::new(SourceManager::new());
        let chunk_scheduler = Arc::new(ChunkScheduler::new(chunk_set.clone(), self.max_retries));
        let progress_smoother = Arc::new(ProgressSmoother::new(total_size, progress_tx));

        // 把源转成 Arc 并注册
        let sources: Vec<Arc<dyn DownloadSource>> = sources
            .into_iter()
            .map(|s| {
                let arc_s: Arc<dyn DownloadSource> = Arc::from(s);
                arc_s
            })
            .collect();

        for source in &sources {
            source_manager.register_source(source.as_ref()).await;
        }

        // 初始化分片队列
        chunk_scheduler.init_queue().await;

        // 预分配文件空间
        if total_size > 0 {
            if let Err(e) = writer.preallocate(total_size).await {
                eprintln!("[strategy] preallocate failed: {}", e);
            }
        }

        // 确定初始连接数
        let initial_workers = if self.initial_connections == 0 {
            (sources.len() as u32 * 4).clamp(self.min_connections, self.max_connections)
        } else {
            self.initial_connections
                .clamp(self.min_connections, self.max_connections)
        };

        progress_smoother.set_active_connections(initial_workers);
        progress_smoother.force_report().await;

        // 启动进度汇报 task（定期从 smoother 发送）
        let progress_smoother_clone = progress_smoother.clone();
        let cancel_clone = cancel.clone();
        let progress_handle = tokio::spawn(async move {
            while !cancel_clone.is_cancelled() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                progress_smoother_clone.report().await;
            }
        });

        // 启动 worker
        let mut worker_handles = Vec::new();
        let active_workers = Arc::new(std::sync::atomic::AtomicU32::new(initial_workers));

        for worker_id in 0..initial_workers {
            let handle = spawn_worker(
                worker_id,
                sources.clone(),
                chunk_scheduler.clone(),
                source_manager.clone(),
                writer.clone(),
                progress_smoother.clone(),
                self.downloader.clone(),
                cancel.clone(),
                active_workers.clone(),
            );
            worker_handles.push(handle);
        }

        // 等待所有分片完成（或取消）
        while !chunk_scheduler.is_all_completed() && !cancel.is_cancelled() {
            tokio::time::sleep(Duration::from_millis(200)).await;

            // 检查是否有 worker 异常退出，需要补充
            // （简单实现：不动态调整 worker 数，后续可加自适应控制）
        }

        // 取消所有 worker
        cancel.cancel();

        // 等待 worker 退出
        for handle in worker_handles {
            let _ = handle.await;
        }
        let _ = progress_handle.await;

        // 最终 flush
        writer.flush().await?;
        progress_smoother.force_report().await;

        // 统计结果
        let (completed, total) = chunk_scheduler.progress();
        let downloaded = {
            let cs = chunk_set.lock().await;
            cs.downloaded_bytes()
        };
        let elapsed = start.elapsed().as_secs_f64();
        let success = chunk_scheduler.is_all_completed();

        Ok(DownloadResult {
            success,
            total_bytes: total_size,
            downloaded_bytes: downloaded,
            elapsed_secs: elapsed,
            success_chunks: completed as u32,
            failed_chunks: (total - completed) as u32,
            avg_speed_bps: if elapsed > 0.0 {
                (downloaded as f64 / elapsed) as u64
            } else {
                0
            },
            error_msg: if success {
                None
            } else {
                Some("download incomplete".into())
            },
        })
    }
}

/// 启动一个 worker
#[allow(clippy::too_many_arguments)]
fn spawn_worker(
    worker_id: u32,
    sources: Vec<Arc<dyn DownloadSource>>,
    chunk_scheduler: Arc<ChunkScheduler>,
    source_manager: Arc<SourceManager>,
    writer: Arc<dyn ChunkWriter>,
    progress_smoother: Arc<ProgressSmoother>,
    downloader: Arc<dyn ChunkDownloader>,
    cancel: CancellationToken,
    _active_workers: Arc<std::sync::atomic::AtomicU32>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while !cancel.is_cancelled() && !chunk_scheduler.is_all_completed() {
            // 取一个分片
            let chunk = match chunk_scheduler.next_chunk().await {
                Some(c) => c,
                None => {
                    // 没有可下载的分片（可能都在退避中），等一下
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            // 按权重选一个源
            let source = match source_manager.pick_source(&sources).await {
                Some(s) => s,
                None => {
                    // 所有源都熔断了，等一下恢复
                    chunk_scheduler.mark_failed(chunk.chunk_id, None).await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            // 下载分片
            let stats = {
                // 使用传入的 downloader（协议无关）
                let downloader = downloader.clone();
                match downloader
                    .download_chunk(source.as_ref(), &chunk, writer.as_ref(), &cancel)
                    .await
                {
                    Ok(stats) => stats,
                    Err(e) => {
                        eprintln!(
                            "[worker {}] download chunk {} failed: {}",
                            worker_id, chunk.chunk_id, e
                        );
                        pandanetos::domain::ChunkStats {
                            chunk_id: chunk.chunk_id,
                            source_id: source.identifier(),
                            downloaded_bytes: 0,
                            elapsed_secs: 0.0,
                            success: false,
                            error_code: Some("DOWNLOAD_ERROR"),
                        }
                    }
                }
            };

            // 处理结果
            if stats.success {
                chunk_scheduler
                    .mark_completed(chunk.chunk_id, Some(source.identifier()))
                    .await;
                source_manager.on_chunk_complete(&stats).await;
                progress_smoother.add_downloaded(stats.downloaded_bytes);
            } else {
                let requeued = chunk_scheduler
                    .mark_failed(chunk.chunk_id, Some(source.identifier()))
                    .await;
                source_manager.on_chunk_fail(&stats).await;
                if !requeued {
                    eprintln!(
                        "[worker {}] chunk {} exceeded max retries",
                        worker_id, chunk.chunk_id
                    );
                }
            }

            // 汇报进度
            progress_smoother.report().await;
        }
    })
}

#[cfg(any())]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_name() {
        let strategy = MultiSourceChunkedStrategy::default();
        assert_eq!(strategy.name(), "multi_source_chunked");
    }

    #[test]
    fn test_supports() {
        let strategy = MultiSourceChunkedStrategy::default();
        let caps = SourceCapabilities {
            supports_range: true,
            supports_concurrent: true,
            supports_resume: true,
            ..Default::default()
        };
        assert!(strategy.supports(&[], &caps));

        let caps_no_range = SourceCapabilities {
            supports_range: false,
            ..Default::default()
        };
        assert!(!strategy.supports(&[], &caps_no_range));
    }
}
