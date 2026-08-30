//! 进度平滑器
//!
//! 提供平滑的进度和速度计算，避免进度回退和速度剧烈波动。
//! - 滑动窗口瞬时速度（最近 N 个采样点，去掉最高最低取平均）
//! - 进度只增不减（AtomicU64 单调递增）
//! - EMA 指数平滑（过滤速度波动）
//! - 消息合并发送（100ms 内多条进度合并成一条）
//!
//! 协议无关，输出符合 [`pandanetos::domain::DownloadProgress`] 结构。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pandanetos::domain::DownloadProgress;
use tokio::sync::{mpsc, Mutex};

/// 速度采样点
struct SpeedSample {
    timestamp: Instant,
    downloaded_bytes: u64,
}

/// 进度平滑器
pub struct ProgressSmoother {
    /// 已下载字节数（只增不减，单调递增）
    downloaded_bytes: Arc<AtomicU64>,
    /// 总字节数
    total_bytes: u64,
    /// 活跃连接数
    active_connections: Arc<AtomicU64>,
    /// 速度采样历史（滑动窗口）
    speed_history: Mutex<VecDeque<SpeedSample>>,
    /// EMA 平滑后的速度
    ema_speed: Arc<AtomicU64>,
    /// EMA 平滑系数（0.0 - 1.0，越大越重视新数据）
    ema_alpha: f64,
    /// 滑动窗口大小（采样点数）
    window_size: usize,
    /// 进度汇报发送端
    progress_tx: mpsc::Sender<DownloadProgress>,
    /// 最后一次发送时间
    last_send: Mutex<Option<Instant>>,
    /// 合并发送间隔（毫秒）
    merge_interval_ms: u64,
    /// 启动时间
    start_time: Instant,
}

impl ProgressSmoother {
    /// 创建一个新的进度平滑器
    pub fn new(
        total_bytes: u64,
        progress_tx: mpsc::Sender<DownloadProgress>,
    ) -> Self {
        Self {
            downloaded_bytes: Arc::new(AtomicU64::new(0)),
            total_bytes,
            active_connections: Arc::new(AtomicU64::new(0)),
            speed_history: Mutex::new(VecDeque::new()),
            ema_speed: Arc::new(AtomicU64::new(0)),
            ema_alpha: 0.3,
            window_size: 10, // 最近 10 个采样点（约 5 秒）
            progress_tx,
            last_send: Mutex::new(None),
            merge_interval_ms: 100, // 100ms 合并间隔
            start_time: Instant::now(),
        }
    }

    /// 更新已下载字节数（只增不减，单调递增）
    ///
    /// 如果新值小于当前值，忽略（保证单调递增，避免进度回退）。
    pub fn update_downloaded(&self, bytes: u64) {
        loop {
            let current = self.downloaded_bytes.load(Ordering::Relaxed);
            if bytes <= current {
                // 只增不减，忽略更小的值
                return;
            }
            if self
                .downloaded_bytes
                .compare_exchange_weak(current, bytes, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// 增加已下载字节数
    pub fn add_downloaded(&self, bytes: u64) {
        self.downloaded_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 设置活跃连接数
    pub fn set_active_connections(&self, count: u32) {
        self.active_connections
            .store(count as u64, Ordering::Relaxed);
    }

    /// 记录一个速度采样点
    async fn record_sample(&self) {
        let downloaded = self.downloaded_bytes.load(Ordering::Relaxed);
        let mut history = self.speed_history.lock().await;
        history.push_back(SpeedSample {
            timestamp: Instant::now(),
            downloaded_bytes: downloaded,
        });
        // 保持窗口大小
        while history.len() > self.window_size {
            history.pop_front();
        }
    }

    /// 计算滑动窗口瞬时速度（去掉最高最低取平均）
    async fn calculate_window_speed(&self) -> u64 {
        let history = self.speed_history.lock().await;
        if history.len() < 2 {
            return 0;
        }

        // 计算每个采样间隔的速度
        let mut speeds: Vec<u64> = Vec::new();
        for i in 1..history.len() {
            let dt = history[i].timestamp.duration_since(history[i - 1].timestamp);
            let db = history[i].downloaded_bytes.saturating_sub(history[i - 1].downloaded_bytes);
            if dt.as_secs_f64() > 0.0 {
                speeds.push((db as f64 / dt.as_secs_f64()) as u64);
            }
        }

        if speeds.is_empty() {
            return 0;
        }

        // 去掉最高最低（如果有 3 个以上）
        if speeds.len() >= 3 {
            speeds.sort();
            speeds.remove(0); // 最低
            speeds.pop(); // 最高
        }

        // 取平均
        let sum: u64 = speeds.iter().sum();
        sum / speeds.len() as u64
    }

    /// 更新 EMA 平滑速度
    async fn update_ema(&self, instant_speed: u64) {
        let old = self.ema_speed.load(Ordering::Relaxed) as f64;
        let new = old * (1.0 - self.ema_alpha) + instant_speed as f64 * self.ema_alpha;
        self.ema_speed.store(new as u64, Ordering::Relaxed);
    }

    /// 生成当前进度快照
    async fn snapshot(&self) -> DownloadProgress {
        let downloaded = self.downloaded_bytes.load(Ordering::Relaxed);
        let speed = self.ema_speed.load(Ordering::Relaxed);
        let active = self.active_connections.load(Ordering::Relaxed) as u32;

        DownloadProgress {
            downloaded_bytes: downloaded,
            total_bytes: self.total_bytes,
            speed_bps: speed,
            active_connections: active,
        }
    }

    /// 汇报进度（带合并发送）
    ///
    /// 100ms 内的多次调用会合并成一次发送，减少 WebSocket 消息量。
    pub async fn report(&self) {
        // 记录采样点
        self.record_sample().await;

        // 计算窗口速度并更新 EMA
        let window_speed = self.calculate_window_speed().await;
        self.update_ema(window_speed).await;

        // 合并发送：检查距上次发送是否超过合并间隔
        let mut last_send = self.last_send.lock().await;
        let now = Instant::now();
        let should_send = match *last_send {
            Some(last) => now.duration_since(last) >= Duration::from_millis(self.merge_interval_ms),
            None => true,
        };

        if should_send {
            let snapshot = self.snapshot().await;
            // 非阻塞发送，如果通道满了就丢弃（下次再发）
            let _ = self.progress_tx.try_send(snapshot);
            *last_send = Some(now);
        }
    }

    /// 强制发送一次进度（用于任务开始/结束等关键节点）
    pub async fn force_report(&self) {
        let snapshot = self.snapshot().await;
        let _ = self.progress_tx.send(snapshot).await;
        *self.last_send.lock().await = Some(Instant::now());
    }

    /// 获取当前已下载字节数
    pub fn downloaded_bytes(&self) -> u64 {
        self.downloaded_bytes.load(Ordering::Relaxed)
    }

    /// 获取当前平滑速度
    pub fn current_speed(&self) -> u64 {
        self.ema_speed.load(Ordering::Relaxed)
    }

    /// 获取已用时间（秒）
    pub fn elapsed_secs(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_monotonic_progress() {
        let (tx, mut rx) = mpsc::channel(10);
        let smoother = ProgressSmoother::new(1000, tx);

        smoother.update_downloaded(100);
        assert_eq!(smoother.downloaded_bytes(), 100);

        // 更小的值应该被忽略
        smoother.update_downloaded(50);
        assert_eq!(smoother.downloaded_bytes(), 100);

        // 更大的值应该更新
        smoother.update_downloaded(200);
        assert_eq!(smoother.downloaded_bytes(), 200);
    }

    #[tokio::test]
    async fn test_add_downloaded() {
        let (tx, _rx) = mpsc::channel(10);
        let smoother = ProgressSmoother::new(1000, tx);

        smoother.add_downloaded(100);
        smoother.add_downloaded(200);
        assert_eq!(smoother.downloaded_bytes(), 300);
    }

    #[tokio::test]
    async fn test_report_merge() {
        let (tx, mut rx) = mpsc::channel(10);
        let smoother = ProgressSmoother::new(1000, tx);

        // 快速连续汇报，应该只发送一次（合并）
        for _ in 0..5 {
            smoother.add_downloaded(10);
            smoother.report().await;
        }

        // 等一下让消息发送
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut count = 0;
        while let Ok(_) = rx.try_recv() {
            count += 1;
        }
        // 应该只有 1 条消息（合并了）
        assert!(count <= 2, "expected merged messages, got {}", count);
    }
}
