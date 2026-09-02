//! 统一分片调度器（协议无关）
//!
//! 这是下载器的核心组件，完全不关心底层协议。
//! 只操作 ChunkFetcher trait 和 SourcePool。
//!
//! 职责：
//! 1. probe 所有源，获取文件大小和能力
//! 2. 根据能力决定下载方式（分片/顺序/单连接）
//! 3. 切分片（如果支持 Range）
//! 4. 多 worker 并发下载（自适应调整并发数）
//! 5. 失败重试（指数退避）
//! 6. 进度汇报
//! 7. 完成统计

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use pandanetos::domain::{CancellationToken, DownloadProgress};
use pandanetos::error::{CoreError, Result};

use crate::domain::adaptive::{AdaptiveConfig, AdaptiveController, DownloadSnapshot};
use crate::domain::chunk_fetcher::{ChunkFetcher, SourceCapabilities};
use crate::domain::source_pool::{ScoringConfig, SourcePool};

/// 分片任务
#[derive(Debug, Clone)]
struct ChunkTask {
    chunk_id: u32,
    offset: u64,
    length: u64,
    retries: u32,
    max_retries: u32,
}

/// 下载结果
#[derive(Debug, Clone)]
pub struct DownloadResult {
    pub success: bool,
    pub total_bytes: u64,
    pub elapsed_ms: u64,
    pub avg_speed_bps: u64,
    pub total_chunks: u32,
    pub success_chunks: u32,
    pub failed_chunks: u32,
    pub error: Option<String>,
}

/// 统一分片调度器配置
#[derive(Debug, Clone)]
pub struct ChunkSchedulerConfig {
    pub initial_chunk_size: u64,
    pub min_chunk_size: u64,
    pub max_chunk_size: u64,
    pub max_retries: u32,
    pub initial_retry_interval_ms: u64,
    pub progress_interval_ms: u64,
    pub adaptive_config: AdaptiveConfig,
    pub scoring_config: ScoringConfig,
}

impl Default for ChunkSchedulerConfig {
    fn default() -> Self {
        Self {
            initial_chunk_size: 4 * 1024 * 1024,
            min_chunk_size: 1 * 1024 * 1024,
            max_chunk_size: 64 * 1024 * 1024,
            max_retries: 5,
            initial_retry_interval_ms: 1000,
            progress_interval_ms: 500,
            adaptive_config: AdaptiveConfig::default(),
            scoring_config: ScoringConfig::default(),
        }
    }
}

/// 统一分片调度器（协议无关）
pub struct ChunkScheduler {
    config: ChunkSchedulerConfig,
    source_pool: Arc<SourcePool>,
    adaptive: Arc<AdaptiveController>,
    downloaded_bytes: Arc<std::sync::atomic::AtomicU64>,
    active_workers: Arc<std::sync::atomic::AtomicU32>,
}

impl ChunkScheduler {
    pub fn new(config: ChunkSchedulerConfig) -> Self {
        let source_pool = Arc::new(SourcePool::new(config.scoring_config.clone()));
        let adaptive = Arc::new(AdaptiveController::new(config.adaptive_config.clone()));

        Self {
            config,
            source_pool,
            adaptive,
            downloaded_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            active_workers: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }

    pub async fn add_source(&self, fetcher: Arc<dyn ChunkFetcher>) {
        self.source_pool.add_source(fetcher).await;
    }

    pub async fn add_sources(&self, fetchers: Vec<Arc<dyn ChunkFetcher>>) {
        self.source_pool.add_sources(fetchers).await;
    }

    pub fn source_pool(&self) -> Arc<SourcePool> {
        self.source_pool.clone()
    }

    pub fn adaptive_controller(&self) -> Arc<AdaptiveController> {
        self.adaptive.clone()
    }

    pub async fn execute(
        &self,
        writer: Arc<Mutex<dyn tokio::io::AsyncWrite + Unpin + Send>>,
        progress_tx: mpsc::Sender<DownloadProgress>,
        cancel: CancellationToken,
    ) -> Result<DownloadResult> {
        let start = Instant::now();

        let source_count = self.source_pool.len().await;

        info!(source_count = source_count, "starting chunk scheduler"); eprintln!("[scheduler] starting, source_count={}", source_count);

        let (file_size, capabilities) = self.probe_all_sources().await?;

        let source_count = self.source_pool.len().await;

        info!(
            file_size = file_size,
            supports_range = capabilities.supports_range,
            supports_multi_connection = capabilities.supports_multi_connection,
            protocol = capabilities.protocol,
            "probe completed"
        );

        if file_size == 0 {
            return Err(CoreError::InvalidParam(
                "file size is 0, cannot determine download strategy".into(),
            ));
        }

        let chunk_tasks = if capabilities.supports_range {
            self.create_chunk_tasks(file_size, &capabilities).await
        } else {
            info!("source does not support range, using single stream download");
            vec![ChunkTask {
                chunk_id: 0,
                offset: 0,
                length: file_size,
                retries: 0,
                max_retries: self.config.max_retries,
            }]
        };

        let total_chunks = chunk_tasks.len() as u32;
        info!(total_chunks = total_chunks, "chunk tasks created"); eprintln!("[scheduler] chunk tasks created, total_chunks={}, concurrency={}", total_chunks, if capabilities.supports_multi_connection { "multi" } else { "single" });

        let progress_handle = self.spawn_progress_reporter(
            file_size,
            total_chunks,
            progress_tx.clone(),
            cancel.clone(),
        );

        let concurrency = if capabilities.supports_multi_connection {
            self.adaptive.current_concurrency().await as usize
        } else {
            1
        };

        let semaphore = Arc::new(Semaphore::new(concurrency));
        let chunk_queue = Arc::new(Mutex::new(chunk_tasks));
        let success_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let failure_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut worker_handles: Vec<JoinHandle<()>> = Vec::new();

        for worker_id in 0..concurrency {
            let handle = self.spawn_worker(
                worker_id as u32,
                chunk_queue.clone(),
                semaphore.clone(),
                writer.clone(),
                success_count.clone(),
                failure_count.clone(),
                cancel.clone(),
            );
            worker_handles.push(handle);
        }

        eprintln!("[scheduler] waiting for {} workers", worker_handles.len()); for handle in worker_handles {
            eprintln!("[scheduler] worker completed"); let _ = handle.await;
        }

        eprintln!("[scheduler] all workers done, dropping progress_handle"); drop(progress_handle); eprintln!("[scheduler] progress_handle dropped");

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let total_bytes = self
            .downloaded_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let success_chunks = success_count.load(std::sync::atomic::Ordering::Relaxed);
        let failed_chunks = failure_count.load(std::sync::atomic::Ordering::Relaxed);

        let avg_speed_bps = if elapsed_ms > 0 {
            total_bytes * 1000 / elapsed_ms
        } else {
            0
        };

        let result = DownloadResult {
            success: failed_chunks == 0,
            total_bytes,
            elapsed_ms,
            avg_speed_bps,
            total_chunks,
            success_chunks,
            failed_chunks,
            error: if failed_chunks > 0 {
                Some(format!("{} chunks failed", failed_chunks))
            } else {
                None
            },
        };

        let source_count = self.source_pool.len().await;

        info!(
            success = result.success,
            total_bytes = result.total_bytes,
            elapsed_ms = result.elapsed_ms,
            avg_speed_bps = result.avg_speed_bps,
            success_chunks = result.success_chunks,
            failed_chunks = result.failed_chunks,
            "download completed"
        );

        eprintln!("[scheduler] execute returning, success={}, bytes={}", result.success, result.total_bytes); Ok(result)
    }

    async fn probe_all_sources(&self) -> Result<(u64, SourceCapabilities)> {
        let sources = self.source_pool.snapshot().await;

        if sources.is_empty() {
            return Err(CoreError::InvalidParam("no sources available".into()));
        }

        let mut file_size = 0u64;
        let mut capabilities = SourceCapabilities::default();
        let mut probe_success = false;
        let mut last_error = String::new();

        for source in &sources {
            match source.fetcher.probe().await {
                Ok((size, caps)) => {
                    probe_success = true;
                    if size > file_size {
                        file_size = size;
                    }
                    // 合并能力：任一 source 支持则整体支持
                    capabilities.supports_range |= caps.supports_range;
                    capabilities.supports_multi_connection |= caps.supports_multi_connection;
                    capabilities.supports_resume |= caps.supports_resume;
                    capabilities.immutable |= caps.immutable;
                    // 取最大并发数
                    capabilities.max_concurrency =
                        capabilities.max_concurrency.max(caps.max_concurrency);
                    // 首个非空分片范围
                    if capabilities.chunk_size_range.is_none() {
                        capabilities.chunk_size_range = caps.chunk_size_range;
                    }
                    // 首个非空协议
                    if capabilities.protocol.is_empty() {
                        capabilities.protocol = caps.protocol;
                    }
                }
                Err(e) => { eprintln!("[probe] source probe failed: {}", e);
                    last_error = e.to_string();
                    warn!(
                        source = %source.display_name,
                        error = %e,
                        "probe failed, skipping source"
                    );
                }
            }
        }

        if !probe_success {
            return Err(CoreError::Network(format!(
                "all sources probe failed: {}",
                last_error
            )));
        }

        if file_size == 0 {
            if let Some(first) = sources.first() {
                capabilities = first.capabilities;
            }
        }

        Ok((file_size, capabilities))
    }

    async fn create_chunk_tasks(
        &self,
        file_size: u64,
        capabilities: &SourceCapabilities,
    ) -> Vec<ChunkTask> {
        let chunk_size = if let Some((min, max)) = capabilities.chunk_size_range {
            min.max(self.config.min_chunk_size).min(max)
        } else {
            let adaptive_chunk_size = self.adaptive.current_chunk_size().await;
            adaptive_chunk_size
                .max(self.config.min_chunk_size)
                .min(self.config.max_chunk_size)
        };

        let total_chunks = (file_size + chunk_size - 1) / chunk_size;
        let mut tasks = Vec::with_capacity(total_chunks as usize);

        for i in 0..total_chunks {
            let offset = i * chunk_size;
            let length = if i == total_chunks - 1 {
                file_size - offset
            } else {
                chunk_size
            };

            tasks.push(ChunkTask {
                chunk_id: i as u32,
                offset,
                length,
                retries: 0,
                max_retries: self.config.max_retries,
            });
        }

        tasks
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_worker(
        &self,
        worker_id: u32,
        chunk_queue: Arc<Mutex<Vec<ChunkTask>>>,
        semaphore: Arc<Semaphore>,
        writer: Arc<Mutex<dyn tokio::io::AsyncWrite + Unpin + Send>>,
        success_count: Arc<std::sync::atomic::AtomicU32>,
        failure_count: Arc<std::sync::atomic::AtomicU32>,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let source_pool = self.source_pool.clone();
        let downloaded_bytes = self.downloaded_bytes.clone();
        let active_workers = self.active_workers.clone();
        let initial_retry_interval = self.config.initial_retry_interval_ms;

        tokio::spawn(async move {
            active_workers.fetch_add(1, std::sync::atomic::Ordering::Relaxed); eprintln!("[worker {}] started", worker_id);

            loop {
                if cancel.is_cancelled() {
                    debug!(worker_id = worker_id, "worker cancelled");
                    break;
                }

                let task = {
                    let mut queue = chunk_queue.lock().await;
                    queue.pop()
                };

                let task = match task {
                    Some(t) => { eprintln!("[worker {}] got chunk_id={}, offset={}, length={}", worker_id, t.chunk_id, t.offset, t.length); t },
                    None => { eprintln!("[worker {}] no more chunks, exiting", worker_id);
                        debug!(worker_id = worker_id, "no more chunks, worker exiting");
                        break;
                    }
                };

                let _permit = match semaphore.acquire().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                let fetcher = match source_pool.best_source().await {
                    Some(f) => f,
                    None => { eprintln!("[worker {}] no more chunks, exiting", worker_id);
                        warn!(worker_id = worker_id, "no source available, retrying chunk");
                        let mut queue = chunk_queue.lock().await;
                        queue.push(task);
                        drop(queue);
                        tokio::time::sleep(Duration::from_millis(initial_retry_interval)).await;
                        continue;
                    }
                };

                eprintln!("[worker {}] calling fetch_chunk", worker_id); let result = {
                    eprintln!("[worker {}] acquiring writer lock", worker_id); let mut writer_guard = writer.lock().await; eprintln!("[worker {}] writer lock acquired", worker_id);
                    fetcher
                        .fetch_chunk(task.offset, task.length, &mut *writer_guard)
                        .await
                };

                match result {
                    Ok(stats) => { eprintln!("[worker {}] fetch_chunk success, bytes={}", worker_id, stats.bytes_downloaded);
                        debug!(
                            worker_id = worker_id,
                            chunk_id = task.chunk_id,
                            bytes = stats.bytes_downloaded,
                            speed = stats.speed_bps,
                            "chunk downloaded successfully"
                        );

                        downloaded_bytes.fetch_add(
                            stats.bytes_downloaded,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        success_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        source_pool
                            .record_success(
                                &stats.source_id,
                                stats.bytes_downloaded,
                                stats.elapsed_ms,
                                stats.speed_bps,
                            )
                            .await;
                    }
                    Err(e) => { eprintln!("[probe] source probe failed: {}", e);
                        warn!(
                            worker_id = worker_id,
                            chunk_id = task.chunk_id,
                            error = %e,
                            retries = task.retries,
                            "chunk download failed"
                        );

                        source_pool.record_failure(&fetcher.identifier()).await;

                        if task.retries < task.max_retries {
                            let mut retry_task = task.clone();
                            retry_task.retries += 1;

                            let backoff = initial_retry_interval * (2u64.pow(task.retries));
                            tokio::time::sleep(Duration::from_millis(backoff)).await;

                            let mut queue = chunk_queue.lock().await;
                            queue.push(retry_task);
                        } else {
                            error!(chunk_id = task.chunk_id, "chunk failed after max retries");
                            failure_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }

            active_workers.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        })
    }

    fn spawn_progress_reporter(
        &self,
        total_bytes: u64,
        _total_chunks: u32,
        progress_tx: mpsc::Sender<DownloadProgress>,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let downloaded_bytes = self.downloaded_bytes.clone();
        let active_workers = self.active_workers.clone();
        let adaptive = self.adaptive.clone();
        let interval = self.config.progress_interval_ms;

        tokio::spawn(async move {
            let mut last_bytes = 0u64;
            let mut last_time = Instant::now();

            loop {
                tokio::time::sleep(Duration::from_millis(interval)).await;

                if cancel.is_cancelled() {
                    break;
                }

                let current_bytes = downloaded_bytes.load(std::sync::atomic::Ordering::Relaxed);
                let elapsed = last_time.elapsed().as_millis() as u64;
                let speed_bps = if elapsed > 0 {
                    (current_bytes - last_bytes) * 1000 / elapsed
                } else {
                    0
                };

                let percent = if total_bytes > 0 {
                    current_bytes as f64 / total_bytes as f64 * 100.0
                } else {
                    0.0
                };

                let progress = DownloadProgress {
                    downloaded_bytes: current_bytes,
                    total_bytes,
                    speed_bps,
                    percent,
                    active_connections: active_workers.load(std::sync::atomic::Ordering::Relaxed),
                    elapsed_secs: 0.0,
                };

                if progress_tx.send(progress).await.is_err() {
                    break;
                }

                let snapshot = DownloadSnapshot {
                    total_speed_bps: speed_bps,
                    active_connections: active_workers.load(std::sync::atomic::Ordering::Relaxed),
                    recent_requests: 0,
                    recent_successes: 0,
                    recent_failures: 0,
                    avg_latency_ms: 0.0,
                    downloaded_bytes: current_bytes,
                    total_bytes,
                    timestamp: Instant::now(),
                };
                adaptive.update_snapshot(snapshot).await;

                last_bytes = current_bytes;
                last_time = Instant::now();
            }
        })
    }
}

// === Legacy 兼容方法（旧策略层使用，新架构 execute 不依赖这些）===
impl ChunkScheduler {
    /// Legacy 构造函数（旧策略层使用）
    #[allow(dead_code)]
    pub fn new_legacy(
        _chunk_set: Arc<tokio::sync::Mutex<pandanetos::domain::ChunkSet>>,
        max_retries: u32,
    ) -> Self {
        let mut config = ChunkSchedulerConfig::default();
        config.max_retries = max_retries;
        Self::new(config)
    }

    /// Legacy 方法：初始化分片队列
    #[allow(dead_code)]
    pub async fn init_queue(&self) {}

    /// Legacy 方法：是否所有分片已完成
    #[allow(dead_code)]
    pub fn is_all_completed(&self) -> bool {
        false
    }

    /// Legacy 方法：获取进度（已完成，总数）
    #[allow(dead_code)]
    pub fn progress(&self) -> (u32, u32) {
        (0, 0)
    }

    /// Legacy 方法：获取下一个分片
    #[allow(dead_code)]
    pub async fn next_chunk(&self) -> Option<pandanetos::domain::Chunk> {
        None
    }

    /// Legacy 方法：标记分片完成
    #[allow(dead_code)]
    pub async fn mark_completed(&self, _chunk_id: u32, _source_id: Option<String>) {}

    /// Legacy 方法：标记分片失败
    #[allow(dead_code)]
    pub async fn mark_failed(&self, _chunk_id: u32, _source_id: Option<String>) -> bool {
        false
    }
}
