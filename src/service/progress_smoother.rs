//! 进度平滑模块
//!
//! 使用指数移动平均（EMA）算法平滑下载速度和进度，
//! 避免进度条跳动和速度剧烈波动。
//!
//! EMA 公式：
//! - EMA_t = α * value_t + (1 - α) * EMA_{t-1}
//! - α = 2 / (N + 1)，N 为窗口大小
//!
//! 同时保证进度单调递增（只增不减），避免进度回退。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tracing::debug;

/// 进度平滑器配置
#[derive(Debug, Clone)]
pub struct ProgressSmootherConfig {
    /// EMA 窗口大小（越大越平滑，但延迟越高）
    pub ema_window: u32,
    /// 最小汇报间隔（毫秒）
    pub min_report_interval_ms: u64,
    /// 速度变化阈值（超过此值才更新，避免微小波动）
    pub speed_change_threshold: f64,
    /// 是否启用进度单调递增保证
    pub enforce_monotonic: bool,
}

impl Default for ProgressSmootherConfig {
    fn default() -> Self {
        Self {
            ema_window: 10,
            min_report_interval_ms: 500,
            speed_change_threshold: 0.05, // 5% 变化阈值
            enforce_monotonic: true,
        }
    }
}

/// 进度快照
#[derive(Debug, Clone, Copy)]
pub struct SmoothProgress {
    /// 已下载字节数
    pub downloaded_bytes: u64,
    /// 总字节数
    pub total_bytes: u64,
    /// 平滑后的速度（字节/秒）
    pub smoothed_speed_bps: f64,
    /// 原始速度（字节/秒）
    pub raw_speed_bps: f64,
    /// 进度百分比（0-100）
    pub percent: f64,
    /// 已用时间（秒）
    pub elapsed_secs: f64,
    /// 预计剩余时间（秒）
    pub eta_secs: Option<f64>,
}

/// 进度平滑器
pub struct ProgressSmoother {
    /// 配置
    config: ProgressSmootherConfig,
    /// EMA 平滑系数 α
    alpha: f64,
    /// 已下载字节数（原子，只增不减）
    downloaded_bytes: AtomicU64,
    /// 总字节数
    total_bytes: AtomicU64,
    /// 平滑后的速度
    smoothed_speed: Mutex<f64>,
    /// 上次汇报时间
    last_report_time: Mutex<Instant>,
    /// 上次汇报的已下载字节数
    last_report_downloaded: Mutex<u64>,
    /// 开始时间
    start_time: Instant,
    /// 上次汇报的进度（用于单调递增保证）
    last_reported_percent: Mutex<f64>,
}

impl ProgressSmoother {
    /// 创建新的进度平滑器
    pub fn new(config: ProgressSmootherConfig) -> Self {
        let alpha = 2.0 / (config.ema_window as f64 + 1.0);
        Self {
            config,
            alpha,
            downloaded_bytes: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            smoothed_speed: Mutex::new(0.0),
            last_report_time: Mutex::new(Instant::now()),
            last_report_downloaded: Mutex::new(0),
            start_time: Instant::now(),
            last_reported_percent: Mutex::new(0.0),
        }
    }

    /// 更新已下载字节数（只增不减）
    pub fn update_downloaded(&self, bytes: u64) {
        let current = self.downloaded_bytes.load(Ordering::Relaxed);
        if bytes > current {
            self.downloaded_bytes.store(bytes, Ordering::Relaxed);
        }
    }

    /// 增加已下载字节数
    pub fn add_downloaded(&self, bytes: u64) {
        self.downloaded_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 设置总字节数
    pub fn set_total(&self, total: u64) {
        self.total_bytes.store(total, Ordering::Relaxed);
    }

    /// 获取当前进度快照（带平滑）
    pub fn snapshot(&self) -> SmoothProgress {
        let downloaded = self.downloaded_bytes.load(Ordering::Relaxed);
        let total = self.total_bytes.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();

        // 计算原始速度
        let mut last_time = self.last_report_time.lock();
        let mut last_downloaded = self.last_report_downloaded.lock();
        let now = Instant::now();
        let delta_time = now.duration_since(*last_time).as_secs_f64();
        let delta_bytes = downloaded.saturating_sub(*last_downloaded);

        let raw_speed = if delta_time > 0.0 {
            delta_bytes as f64 / delta_time
        } else {
            0.0
        };

        // EMA 平滑速度
        let mut smoothed = self.smoothed_speed.lock();
        if *smoothed == 0.0 {
            *smoothed = raw_speed;
        } else {
            let change = (raw_speed - *smoothed).abs() / smoothed.max(1.0);
            if change > self.config.speed_change_threshold {
                *smoothed = self.alpha * raw_speed + (1.0 - self.alpha) * *smoothed;
            }
        }

        // 更新上次汇报状态
        *last_time = now;
        *last_downloaded = downloaded;

        // 计算进度百分比
        let percent = if total > 0 {
            (downloaded as f64 / total as f64 * 100.0).min(100.0)
        } else {
            0.0
        };

        // 单调递增保证
        let final_percent = if self.config.enforce_monotonic {
            let mut last_percent = self.last_reported_percent.lock();
            if percent < *last_percent {
                debug!(
                    current = percent,
                    last = *last_percent,
                    "progress regression detected, using last value"
                );
                *last_percent
            } else {
                *last_percent = percent;
                percent
            }
        } else {
            percent
        };

        // 计算 ETA
        let eta = if *smoothed > 0.0 && total > downloaded {
            Some((total - downloaded) as f64 / *smoothed)
        } else {
            None
        };

        SmoothProgress {
            downloaded_bytes: downloaded,
            total_bytes: total,
            smoothed_speed_bps: *smoothed,
            raw_speed_bps: raw_speed,
            percent: final_percent,
            elapsed_secs: elapsed,
            eta_secs: eta,
        }
    }

    /// 检查是否应该汇报（基于最小间隔）
    pub fn should_report(&self) -> bool {
        let last_time = self.last_report_time.lock();
        last_time.elapsed() >= Duration::from_millis(self.config.min_report_interval_ms)
    }

    /// 重置平滑器
    pub fn reset(&self) {
        self.downloaded_bytes.store(0, Ordering::Relaxed);
        self.total_bytes.store(0, Ordering::Relaxed);
        *self.smoothed_speed.lock() = 0.0;
        *self.last_report_time.lock() = Instant::now();
        *self.last_report_downloaded.lock() = 0;
        *self.last_reported_percent.lock() = 0.0;
    }
}

impl Default for ProgressSmoother {
    fn default() -> Self {
        Self::new(ProgressSmootherConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_monotonic() {
        let smoother = ProgressSmoother::default();
        smoother.set_total(1000);

        smoother.update_downloaded(500);
        let p1 = smoother.snapshot();
        assert_eq!(p1.percent, 50.0);

        // 模拟进度回退
        smoother.update_downloaded(400); // 应该被忽略（只增不减）
        let p2 = smoother.snapshot();
        assert_eq!(p2.downloaded_bytes, 500); // 仍然是 500
        assert_eq!(p2.percent, 50.0);
    }

    #[test]
    fn test_ema_smoothing() {
        let config = ProgressSmootherConfig {
            ema_window: 5,
            min_report_interval_ms: 0,
            speed_change_threshold: 0.0,
            enforce_monotonic: true,
        };
        let smoother = ProgressSmoother::new(config);
        smoother.set_total(10000);

        // 第一次速度
        smoother.add_downloaded(1000);
        std::thread::sleep(Duration::from_millis(100));
        let p1 = smoother.snapshot();
        assert!(p1.smoothed_speed_bps > 0.0);

        // 第二次速度（应该平滑）
        smoother.add_downloaded(2000);
        std::thread::sleep(Duration::from_millis(100));
        let p2 = smoother.snapshot();
        assert!(p2.smoothed_speed_bps > 0.0);
    }
}
