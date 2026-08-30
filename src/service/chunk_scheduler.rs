//! 分片调度器
//!
//! 维护分片队列，分配待下载分片给 worker，处理失败重试。
//! 使用无锁队列（`crossbeam::SegQueue`）避免锁竞争，工作窃取式分配。
//! 协议无关，只操作 [`pandanetos::domain::Chunk`] 和 [`ChunkSet`]。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::queue::SegQueue;
use pandanetos::domain::{Chunk, ChunkSet, ChunkState};
use tokio::sync::Mutex;

/// 待下载分片（队列元素）
struct PendingChunk {
    chunk_id: u32,
    offset: u64,
    length: u64,
    retry_count: u32,
    /// 最早可下载时间（指数退避）
    available_at: Instant,
}

/// 分片调度器
pub struct ChunkScheduler {
    /// 分片集合（共享状态，用于统计和最终校验）
    chunk_set: Arc<Mutex<ChunkSet>>,
    /// 待下载分片队列（无锁，工作窃取）
    pending_queue: SegQueue<PendingChunk>,
    /// 已完成分片数（原子计数，用于快速判断是否全部完成）
    completed_count: AtomicU64,
    /// 下载中分片数
    downloading_count: AtomicU64,
    /// 总分片数
    total_chunks: AtomicU64,
    /// 最大重试次数
    max_retries: u32,
    /// 指数退避基数（秒）
    backoff_base_secs: u64,
}

impl ChunkScheduler {
    /// 创建一个新的分片调度器
    pub fn new(chunk_set: Arc<Mutex<ChunkSet>>, max_retries: u32) -> Self {
        Self {
            chunk_set,
            pending_queue: SegQueue::new(),
            completed_count: AtomicU64::new(0),
            downloading_count: AtomicU64::new(0),
            total_chunks: AtomicU64::new(0),
            max_retries,
            backoff_base_secs: 1,
        }
    }

    /// 初始化待下载队列（从 ChunkSet 中加载所有 pending 分片）
    pub async fn init_queue(&self) {
        let chunk_set = self.chunk_set.lock().await;
        let total = chunk_set.chunks.len() as u64;

        for chunk in &chunk_set.chunks {
            if chunk.state == ChunkState::Pending || chunk.state == ChunkState::Failed {
                self.pending_queue.push(PendingChunk {
                    chunk_id: chunk.chunk_id,
                    offset: chunk.offset,
                    length: chunk.length,
                    retry_count: chunk.retry_count,
                    available_at: Instant::now(),
                });
            } else if chunk.state == ChunkState::Completed {
                self.completed_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        // 更新总分片数
        drop(chunk_set);
        self.total_chunks.store(total, Ordering::Relaxed);
    }

    /// 获取下一个待下载分片
    ///
    /// 返回 None 表示当前没有可下载的分片（可能都在退避中，或全部完成）。
    /// 调用方应该等待一段时间后重试，或检查是否全部完成。
    pub async fn next_chunk(&self) -> Option<Chunk> {
        let now = Instant::now();

        // 尝试从队列取一个可用的分片
        // 因为 SegQueue 是 FIFO，退避的分片可能在队首，需要跳过
        // 简单实现：循环取，直到找到一个可用的，或队列为空
        let mut skipped: Vec<PendingChunk> = Vec::new();

        while let Some(pending) = self.pending_queue.pop() {
            if pending.available_at <= now {
                // 可用，返回
                self.downloading_count.fetch_add(1, Ordering::Relaxed);

                // 更新 ChunkSet 中的状态
                let mut chunk_set = self.chunk_set.lock().await;
                if let Some(chunk) = chunk_set
                    .chunks
                    .get_mut(pending.chunk_id as usize)
                {
                    chunk.state = ChunkState::Downloading;
                    chunk.retry_count = pending.retry_count;
                }
                drop(chunk_set);

                // 把跳过的分片重新入队
                for s in skipped {
                    self.pending_queue.push(s);
                }

                return Some(Chunk {
                    chunk_id: pending.chunk_id,
                    offset: pending.offset,
                    length: pending.length,
                    state: ChunkState::Downloading,
                    source_id: None,
                    retry_count: pending.retry_count,
                });
            } else {
                // 还在退避中，跳过
                skipped.push(pending);
            }
        }

        // 队列为空或都在退避中，把跳过的重新入队
        for s in skipped {
            self.pending_queue.push(s);
        }

        None
    }

    /// 标记分片完成
    pub async fn mark_completed(&self, chunk_id: u32, source_id: Option<String>) {
        self.completed_count.fetch_add(1, Ordering::Relaxed);
        self.downloading_count.fetch_sub(1, Ordering::Relaxed);

        let mut chunk_set = self.chunk_set.lock().await;
        if let Some(chunk) = chunk_set.chunks.get_mut(chunk_id as usize) {
            chunk.state = ChunkState::Completed;
            chunk.source_id = source_id;
        }
    }

    /// 标记分片失败，重新入队（指数退避）
    ///
    /// 如果重试次数超过 max_retries，标记为最终失败（不再重试）。
    pub async fn mark_failed(&self, chunk_id: u32, source_id: Option<String>) -> bool {
        self.downloading_count.fetch_sub(1, Ordering::Relaxed);

        let mut chunk_set = self.chunk_set.lock().await;
        let retry_count = if let Some(chunk) = chunk_set.chunks.get_mut(chunk_id as usize) {
            chunk.retry_count += 1;
            chunk.state = ChunkState::Failed;
            chunk.source_id = source_id.clone();
            chunk.retry_count
        } else {
            0
        };

        if retry_count >= self.max_retries {
            // 超过最大重试次数，最终失败
            drop(chunk_set);
            return false;
        }

        // 计算退避时间：base * 2^retry_count，上限 60 秒
        let backoff_secs = self
            .backoff_base_secs
            .saturating_mul(2u64.saturating_pow(retry_count.min(31)))
            .min(60);

        // 重新入队
        if let Some(chunk) = chunk_set.chunks.get(chunk_id as usize) {
            self.pending_queue.push(PendingChunk {
                chunk_id: chunk.chunk_id,
                offset: chunk.offset,
                length: chunk.length,
                retry_count: chunk.retry_count,
                available_at: Instant::now() + Duration::from_secs(backoff_secs),
            });
        }

        true
    }

    /// 是否所有分片都已完成
    pub fn is_all_completed(&self) -> bool {
        self.completed_count.load(Ordering::Relaxed) >= self.total_chunks.load(Ordering::Relaxed)
    }

    /// 获取进度信息
    pub fn progress(&self) -> (u64, u64) {
        (
            self.completed_count.load(Ordering::Relaxed),
            self.total_chunks.load(Ordering::Relaxed),
        )
    }

    /// 获取下载中分片数
    pub fn downloading_count(&self) -> u64 {
        self.downloading_count.load(Ordering::Relaxed)
    }

    /// 获取待下载分片数（队列长度，可能包含退避中的）
    pub fn pending_count(&self) -> usize {
        self.pending_queue.len()
    }

    /// 获取 ChunkSet 引用
    pub fn chunk_set(&self) -> Arc<Mutex<ChunkSet>> {
        self.chunk_set.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_flow() {
        let chunk_set = Arc::new(Mutex::new(ChunkSet::new(1024 * 1024, 256 * 1024))); // 4 个分片
        let scheduler = ChunkScheduler::new(chunk_set.clone(), 3);
        scheduler.init_queue().await;

        assert_eq!(scheduler.progress(), (0, 4));
        assert!(!scheduler.is_all_completed());

        // 取一个分片
        let chunk = scheduler.next_chunk().await.unwrap();
        assert_eq!(chunk.chunk_id, 0);
        assert_eq!(scheduler.downloading_count(), 1);

        // 标记完成
        scheduler.mark_completed(0, None).await;
        assert_eq!(scheduler.progress(), (1, 4));

        // 完成所有
        for i in 1..4 {
            let chunk = scheduler.next_chunk().await.unwrap();
            assert_eq!(chunk.chunk_id, i);
            scheduler.mark_completed(i, None).await;
        }

        assert!(scheduler.is_all_completed());
    }

    #[tokio::test]
    async fn test_failed_retry() {
        let chunk_set = Arc::new(Mutex::new(ChunkSet::new(256 * 1024, 256 * 1024))); // 1 个分片
        let scheduler = ChunkScheduler::new(chunk_set.clone(), 2);
        scheduler.init_queue().await;

        // 第一次失败
        let chunk = scheduler.next_chunk().await.unwrap();
        let requeued = scheduler.mark_failed(chunk.chunk_id, None).await;
        assert!(requeued); // 还可以重试

        // 第二次失败（超过 max_retries）
        let chunk = scheduler.next_chunk().await;
        // 因为退避，可能取不到，等一下
        tokio::time::sleep(Duration::from_secs(2)).await;
        let chunk = scheduler.next_chunk().await.unwrap();
        let requeued = scheduler.mark_failed(chunk.chunk_id, None).await;
        assert!(!requeued); // 最终失败
    }
}
