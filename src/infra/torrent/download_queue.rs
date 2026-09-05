//! 元数据下载队列
//!
//! 管理待下载的 infohash 队列，支持并发控制、优先级、去重、重试。
//!
//! 工作流程：
//! 1. 从 PDC 获取 infohash 列表
//! 2. 加入下载队列（去重、按优先级排序）
//! 3. 并发下载 metadata（限制并发数）
//! 4. 下载成功推送到 pk，失败重试（指数退避）
//! 5. 定期从 PDC 获取新的 infohash

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use pandanetos::bittorrent::{Infohash, MetadataInfo};

use super::metadata::MetadataDownloader;
use crate::infra::pdc_client::PdcClient;
use crate::infra::pk_client::PkClient;

/// 下载队列配置
#[derive(Debug, Clone)]
pub struct DownloadQueueConfig {
    /// 最大并发下载数
    pub max_concurrent: usize,
    /// 最大重试次数
    pub max_retries: u32,
    /// 初始重试间隔（秒）
    pub initial_retry_delay: u64,
    /// 队列最大长度
    pub max_queue_size: usize,
    /// 下载超时（秒）
    pub download_timeout: u64,
}

impl Default for DownloadQueueConfig {
    fn default() -> Self {
        DownloadQueueConfig {
            max_concurrent: 8,
            max_retries: 3,
            initial_retry_delay: 5,
            max_queue_size: 10000,
            download_timeout: 30,
        }
    }
}

/// 队列中的下载任务
#[derive(Debug, Clone)]
struct DownloadTask {
    infohash: Infohash,
    priority: u8,
    retries: u32,
    next_attempt: Option<Instant>,
    source: String,
}

/// 下载队列
pub struct DownloadQueue {
    config: DownloadQueueConfig,
    queue: Mutex<VecDeque<DownloadTask>>,
    in_progress: Mutex<HashSet<Infohash>>,
    completed: Mutex<HashSet<Infohash>>,
    failed: Mutex<HashMap<Infohash, u32>>,
    metadata_downloader: Arc<MetadataDownloader>,
    pdc_client: Option<Arc<PdcClient>>,
    pk_client: Option<Arc<PkClient>>,
}

impl DownloadQueue {
    /// 创建新的下载队列
    pub fn new(
        config: DownloadQueueConfig,
        metadata_downloader: Arc<MetadataDownloader>,
    ) -> Self {
        DownloadQueue {
            config,
            queue: Mutex::new(VecDeque::new()),
            in_progress: Mutex::new(HashSet::new()),
            completed: Mutex::new(HashSet::new()),
            failed: Mutex::new(HashMap::new()),
            metadata_downloader,
            pdc_client: None,
            pk_client: None,
        }
    }

    /// 设置 PDC 客户端
    pub fn with_pdc(mut self, pdc_client: Arc<PdcClient>) -> Self {
        self.pdc_client = Some(pdc_client);
        self
    }

    /// 设置 pk 客户端
    pub fn with_pk(mut self, pk_client: Arc<PkClient>) -> Self {
        self.pk_client = Some(pk_client);
        self
    }

    /// 添加 infohash 到队列
    pub async fn enqueue(&self, infohash: Infohash, source: &str) -> bool {
        // 检查是否已完成或正在进行
        if self.completed.lock().await.contains(&infohash) {
            return false;
        }
        if self.in_progress.lock().await.contains(&infohash) {
            return false;
        }

        let mut queue = self.queue.lock().await;

        // 检查队列中是否已存在
        if queue.iter().any(|t| t.infohash == infohash) {
            return false;
        }

        // 检查队列长度
        if queue.len() >= self.config.max_queue_size {
            warn!("[queue] 队列已满，丢弃 {}", infohash);
            return false;
        }

        queue.push_back(DownloadTask {
            infohash,
            priority: 0,
            retries: 0,
            next_attempt: None,
            source: source.to_string(),
        });

        debug!("[queue] 加入队列: {} (来源: {})", infohash, source);
        true
    }

    /// 批量添加 infohash
    pub async fn enqueue_batch(&self, infohashes: &[Infohash], source: &str) -> usize {
        let mut count = 0;
        for ih in infohashes {
            if self.enqueue(*ih, source).await {
                count += 1;
            }
        }
        count
    }

    /// 获取队列长度
    pub async fn queue_len(&self) -> usize {
        self.queue.lock().await.len()
    }

    /// 获取统计信息
    pub async fn stats(&self) -> QueueStats {
        QueueStats {
            queued: self.queue.lock().await.len(),
            in_progress: self.in_progress.lock().await.len(),
            completed: self.completed.lock().await.len(),
            failed: self.failed.lock().await.len(),
        }
    }

    /// 运行下载队列（阻塞，直到队列为空或停止）
    pub async fn run(&self) -> QueueRunResult {
        let mut total_success = 0;
        let mut total_failed = 0;

        loop {
            // 检查是否有任务可执行
            let task = {
                let mut queue = self.queue.lock().await;
                // 找到第一个可以执行的任务（next_attempt 已过或为 None）
                let now = Instant::now();
                let idx = queue.iter().position(|t| {
                    t.next_attempt.map(|t| t <= now).unwrap_or(true)
                });
                idx.and_then(|i| queue.remove(i))
            };

            let Some(task) = task else {
                // 没有可执行的任务
                if self.queue.lock().await.is_empty()
                    && self.in_progress.lock().await.is_empty()
                {
                    break;
                }
                // 等待一下再检查
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            };

            // 检查并发限制
            while self.in_progress.lock().await.len() >= self.config.max_concurrent {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // 标记为进行中
            self.in_progress.lock().await.insert(task.infohash);

            // 执行下载
            let downloader = self.metadata_downloader.clone();
            let pk_client = self.pk_client.clone();
            let infohash = task.infohash;
            let source = task.source.clone();

            let result = tokio::time::timeout(
                Duration::from_secs(self.config.download_timeout),
                tokio::task::spawn_blocking(move || {
                    // 从 PDC 获取 peer（如果有 PDC 客户端）
                    // 简化：直接用 metadata_downloader 的默认 peer 发现
                    downloader.download_from_peers(infohash, &[])
                }),
            )
            .await;

            // 移除进行中标记
            self.in_progress.lock().await.remove(&infohash);

            match result {
                Ok(Ok(Ok(metadata))) => {
                    info!("[queue] 下载成功: {} ({})", metadata.name, infohash);
                    self.completed.lock().await.insert(infohash);
                    total_success += 1;

                    // 推送到 pk
                    if let Some(pk) = &pk_client {
                        if let Err(e) = pk.submit_metadata(&metadata).await {
                            warn!("[queue] 推送到 pk 失败: {}", e);
                        }
                    }
                }
                Ok(Ok(Err(e))) => {
                    debug!("[queue] 下载失败: {}: {}", infohash, e);
                    self.handle_failure(task).await;
                    total_failed += 1;
                }
                Ok(Err(e)) => {
                    debug!("[queue] 下载任务 panic: {}: {}", infohash, e);
                    self.handle_failure(task).await;
                    total_failed += 1;
                }
                Err(_) => {
                    debug!("[queue] 下载超时: {}", infohash);
                    self.handle_failure(task).await;
                    total_failed += 1;
                }
            }
        }

        QueueRunResult {
            success: total_success,
            failed: total_failed,
        }
    }

    /// 处理下载失败（重试或标记为最终失败）
    async fn handle_failure(&self, mut task: DownloadTask) {
        task.retries += 1;

        if task.retries >= self.config.max_retries {
            warn!(
                "[queue] 最终失败: {} (重试 {} 次)",
                task.infohash, task.retries
            );
            self.failed
                .lock()
                .await
                .insert(task.infohash, task.retries);
        } else {
            // 指数退避后重新入队
            let delay = self.config.initial_retry_delay * 2u64.pow(task.retries - 1);
            task.next_attempt = Some(Instant::now() + Duration::from_secs(delay));
            debug!(
                "[queue] 重试 {} (第 {} 次，{}s 后)",
                task.infohash, task.retries, delay
            );
            self.queue.lock().await.push_back(task);
        }
    }
}

/// 队列统计
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    pub queued: usize,
    pub in_progress: usize,
    pub completed: usize,
    pub failed: usize,
}

/// 队列运行结果
#[derive(Debug, Clone, Default)]
pub struct QueueRunResult {
    pub success: usize,
    pub failed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_infohash(hex: &str) -> Infohash {
        Infohash::from_hex(hex).unwrap()
    }

    #[tokio::test]
    async fn test_enqueue() {
        let downloader = Arc::new(MetadataDownloader::new());
        let queue = DownloadQueue::new(Default::default(), downloader);

        let ih = make_infohash("0123456789abcdef0123456789abcdef01234567");
        assert!(queue.enqueue(ih, "test").await);
        assert_eq!(queue.queue_len().await, 1);

        // 重复添加应该失败
        assert!(!queue.enqueue(ih, "test").await);
        assert_eq!(queue.queue_len().await, 1);
    }

    #[tokio::test]
    async fn test_enqueue_batch() {
        let downloader = Arc::new(MetadataDownloader::new());
        let queue = DownloadQueue::new(Default::default(), downloader);

        let ih1 = make_infohash("0123456789abcdef0123456789abcdef01234567");
        let ih2 = make_infohash("fedcba9876543210fedcba9876543210fedcba98");
        let count = queue.enqueue_batch(&[ih1, ih2], "test").await;
        assert_eq!(count, 2);
        assert_eq!(queue.queue_len().await, 2);
    }

    #[tokio::test]
    async fn test_stats() {
        let downloader = Arc::new(MetadataDownloader::new());
        let queue = DownloadQueue::new(Default::default(), downloader);

        let ih = make_infohash("0123456789abcdef0123456789abcdef01234567");
        queue.enqueue(ih, "test").await;

        let stats = queue.stats().await;
        assert_eq!(stats.queued, 1);
        assert_eq!(stats.in_progress, 0);
        assert_eq!(stats.completed, 0);
        assert_eq!(stats.failed, 0);
    }

    #[test]
    fn test_config_default() {
        let config = DownloadQueueConfig::default();
        assert_eq!(config.max_concurrent, 8);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_retry_delay, 5);
    }
}
