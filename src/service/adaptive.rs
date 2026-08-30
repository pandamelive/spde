//! 自适应连接数控制器
//!
//! 智能调整并发连接数，目标是跑满带宽但不过载：
//! - **启动探测**：先用少量连接（默认2）探测源的速度和能力
//! - **运行时动态调整**：根据实时速度趋势增加/减少连接数
//! - **CDN 限速识别**：当速度增长停滞时，判定为 CDN 限速，停止增加连接
//! - **退避机制**：连接失败率过高时自动减少连接数
//!
//! 协议无关，只依赖速度采样和连接状态统计。

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// 自适应控制器配置
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// 初始连接数（启动探测用）
    pub initial_connections: u32,
    /// 最小连接数
    pub min_connections: u32,
    /// 最大连接数（硬上限）
    pub max_connections: u32,
    /// 调整间隔（秒）
    pub adjust_interval_secs: u64,
    /// 速度增长阈值（百分比，0.0-1.0）
    /// 当速度增长低于此阈值时，判定为 CDN 限速
    pub speed_growth_threshold: f64,
    /// 连续多少次增长停滞才判定为限速
    pub stagnation_limit: u32,
    /// 失败率阈值（0.0-1.0），超过则减少连接
    pub failure_rate_threshold: f64,
    /// 每次调整的连接数变化量
    pub adjust_step: u32,
    /// 是否启用自适应（false 时固定用 initial_connections）
    pub enabled: bool,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            initial_connections: 2,
            min_connections: 1,
            max_connections: 32,
            adjust_interval_secs: 5,
            speed_growth_threshold: 0.05,
            stagnation_limit: 3,
            failure_rate_threshold: 0.3,
            adjust_step: 2,
            enabled: true,
        }
    }
}

/// 速度采样点
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct SpeedSample {
    /// 采样时间
    timestamp: Instant,
    /// 速度（字节/秒）
    speed_bps: u64,
    /// 当前连接数
    connections: u32,
}

/// 自适应连接数控制器
///
/// 运行在独立的 task 中，定期采样速度并调整连接数。
/// 通过 `current_connections` 原子值对外暴露当前推荐连接数。
pub struct AdaptiveController {
    /// 配置
    config: AdaptiveConfig,
    /// 当前推荐连接数（原子，外部读取）
    current_connections: AtomicU32,
    /// 速度采样历史（滑动窗口）
    samples: Mutex<Vec<SpeedSample>>,
    /// 连续增长停滞次数
    stagnation_count: AtomicU32,
    /// 总请求数（用于计算失败率）
    total_requests: AtomicU64,
    /// 失败请求数
    failed_requests: AtomicU64,
    /// 上次调整时间
    last_adjust: Mutex<Instant>,
    /// 是否处于 CDN 限速状态
    is_throttled: AtomicU32,
    /// 启动时间
    start_time: Instant,
}

impl AdaptiveController {
    /// 创建新的自适应控制器
    pub fn new(config: AdaptiveConfig) -> Arc<Self> {
        let initial = if config.enabled {
            config.initial_connections
        } else {
            config.initial_connections
        };
        Arc::new(Self {
            current_connections: AtomicU32::new(initial),
            samples: Mutex::new(Vec::with_capacity(64)),
            stagnation_count: AtomicU32::new(0),
            total_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            last_adjust: Mutex::new(Instant::now()),
            is_throttled: AtomicU32::new(0),
            start_time: Instant::now(),
            config,
        })
    }

    /// 获取当前推荐连接数
    pub fn current_connections(&self) -> u32 {
        self.current_connections.load(Ordering::Relaxed)
    }

    /// 是否处于 CDN 限速状态
    pub fn is_throttled(&self) -> bool {
        self.is_throttled.load(Ordering::Relaxed) == 1
    }

    /// 记录一次速度采样
    pub async fn record_speed(&self, speed_bps: u64) {
        let mut samples = self.samples.lock().await;
        samples.push(SpeedSample {
            timestamp: Instant::now(),
            speed_bps,
            connections: self.current_connections.load(Ordering::Relaxed),
        });
        // 保留最近 60 个采样点
        if samples.len() > 60 {
            samples.remove(0);
        }
    }

    /// 记录一次请求结果
    pub fn record_request(&self, success: bool) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.failed_requests.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 计算当前失败率
    fn failure_rate(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let failed = self.failed_requests.load(Ordering::Relaxed);
        failed as f64 / total as f64
    }

    /// 计算最近 N 个采样的平均速度
    #[allow(dead_code)]
    async fn average_speed_recent(&self, count: usize) -> u64 {
        let samples = self.samples.lock().await;
        if samples.is_empty() {
            return 0;
        }
        let n = count.min(samples.len());
        let recent: Vec<&SpeedSample> = samples.iter().rev().take(n).collect();
        let sum: u64 = recent.iter().map(|s| s.speed_bps).sum();
        sum / n as u64
    }

    /// 计算速度增长率（最近 vs 之前）
    async fn speed_growth_rate(&self) -> f64 {
        let samples = self.samples.lock().await;
        if samples.len() < 4 {
            return 1.0; // 采样不足，假设增长
        }
        let half = samples.len() / 2;
        let recent_sum: u64 = samples.iter().rev().take(half).map(|s| s.speed_bps).sum();
        let older_sum: u64 = samples
            .iter()
            .rev()
            .skip(half)
            .take(half)
            .map(|s| s.speed_bps)
            .sum();
        let recent_avg = recent_sum as f64 / half as f64;
        let older_avg = older_sum as f64 / half as f64;
        if older_avg < 1.0 {
            return 1.0;
        }
        (recent_avg - older_avg) / older_avg
    }

    /// 执行一次调整决策
    ///
    /// 返回是否进行了调整，以及调整后的连接数
    pub async fn adjust(&self) -> (bool, u32) {
        if !self.config.enabled {
            return (false, self.current_connections.load(Ordering::Relaxed));
        }

        // 检查调整间隔
        let mut last_adjust = self.last_adjust.lock().await;
        if last_adjust.elapsed() < Duration::from_secs(self.config.adjust_interval_secs) {
            return (false, self.current_connections.load(Ordering::Relaxed));
        }
        *last_adjust = Instant::now();
        drop(last_adjust);

        let current = self.current_connections.load(Ordering::Relaxed);
        let failure_rate = self.failure_rate();

        // 1. 失败率过高，减少连接
        if failure_rate > self.config.failure_rate_threshold && current > self.config.min_connections {
            let new_conn = current.saturating_sub(self.config.adjust_step).max(self.config.min_connections);
            self.current_connections.store(new_conn, Ordering::Relaxed);
            self.stagnation_count.store(0, Ordering::Relaxed);
            warn!(
                "自适应: 失败率 {:.1}% 过高，连接数 {} -> {}",
                failure_rate * 100.0,
                current,
                new_conn
            );
            return (true, new_conn);
        }

        // 2. 已经是最大连接数，不再增加
        if current >= self.config.max_connections {
            return (false, current);
        }

        // 3. 已经判定为 CDN 限速，不再增加
        if self.is_throttled() {
            debug!("自适应: 已判定 CDN 限速，保持连接数 {}", current);
            return (false, current);
        }

        // 4. 检查速度增长
        let growth_rate = self.speed_growth_rate().await;
        if growth_rate < self.config.speed_growth_threshold {
            // 速度增长停滞
            let stagnation = self.stagnation_count.fetch_add(1, Ordering::Relaxed) + 1;
            if stagnation >= self.config.stagnation_limit {
                // 连续多次停滞，判定为 CDN 限速
                self.is_throttled.store(1, Ordering::Relaxed);
                info!(
                    "自适应: 速度连续 {} 次增长停滞（增长率 {:.2}%），判定为 CDN 限速，固定连接数 {}",
                    stagnation,
                    growth_rate * 100.0,
                    current
                );
                return (true, current);
            }
            debug!(
                "自适应: 速度增长停滞 {}/{}（增长率 {:.2}%），保持连接数 {}",
                stagnation,
                self.config.stagnation_limit,
                growth_rate * 100.0,
                current
            );
            return (false, current);
        }

        // 5. 速度仍在增长，增加连接数
        let new_conn = (current + self.config.adjust_step).min(self.config.max_connections);
        self.current_connections.store(new_conn, Ordering::Relaxed);
        self.stagnation_count.store(0, Ordering::Relaxed);
        info!(
            "自适应: 速度增长 {:.2}%，连接数 {} -> {}",
            growth_rate * 100.0,
            current,
            new_conn
        );
        (true, new_conn)
    }

    /// 运行自适应控制循环（在独立 task 中调用）
    ///
    /// 定期采样速度并调整连接数，直到取消令牌被触发。
    pub async fn run_loop(
        self: Arc<Self>,
        speed_rx: tokio::sync::mpsc::Receiver<u64>,
        cancel: pandanetos::domain::CancellationToken,
    ) {
        if !self.config.enabled {
            debug!("自适应控制已禁用");
            return;
        }

        let mut speed_rx = speed_rx;
        let interval = Duration::from_secs(self.config.adjust_interval_secs);

        info!(
            "自适应控制启动: 初始连接={}, 最大={}, 调整间隔={}s",
            self.config.initial_connections, self.config.max_connections, self.config.adjust_interval_secs
        );

        loop {
            if cancel.is_cancelled() {
                info!("自适应控制停止（取消）");
                break;
            }

            // 接收速度采样（带超时，避免无限等待）
            match tokio::time::timeout(interval, speed_rx.recv()).await {
                Ok(Some(speed)) => {
                    self.record_speed(speed).await;
                }
                Ok(None) => {
                    // 通道关闭
                    debug!("自适应控制: 速度通道关闭");
                    break;
                }
                Err(_) => {
                    // 超时，没有新速度采样，用上次的
                }
            }

            // 执行调整
            self.adjust().await;
        }
    }

    /// 获取统计信息
    pub async fn stats(&self) -> AdaptiveStats {
        let samples = self.samples.lock().await;
        AdaptiveStats {
            current_connections: self.current_connections.load(Ordering::Relaxed),
            is_throttled: self.is_throttled(),
            failure_rate: self.failure_rate(),
            total_requests: self.total_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            stagnation_count: self.stagnation_count.load(Ordering::Relaxed),
            sample_count: samples.len(),
            elapsed_secs: self.start_time.elapsed().as_secs(),
        }
    }
}

/// 自适应控制器统计信息
#[derive(Debug, Clone)]
pub struct AdaptiveStats {
    /// 当前连接数
    pub current_connections: u32,
    /// 是否处于 CDN 限速状态
    pub is_throttled: bool,
    /// 失败率
    pub failure_rate: f64,
    /// 总请求数
    pub total_requests: u64,
    /// 失败请求数
    pub failed_requests: u64,
    /// 连续增长停滞次数
    pub stagnation_count: u32,
    /// 采样点数
    pub sample_count: usize,
    /// 运行时长（秒）
    pub elapsed_secs: u64,
}
