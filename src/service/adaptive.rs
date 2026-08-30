//! 自适应连接数控制器
//!
//! 智能调整并发连接数，目标是跑满带宽但不过载：
//! - **启动探测**：先用少量连接（默认2）探测源的速度和能力
//! - **运行时动态调整**：根据实时速度趋势增加/减少连接数
//! - **CDN 限速识别**：当速度增长停滞时，判定为 CDN 限速，停止增加连接
//! - **退避机制**：连接失败率过高时自动减少连接数
//! - **限速恢复探测**：判定为 CDN 限速后，定期试探是否恢复
//! - **指数退避**：连续失败时连接数减少量指数增长
//! - **动态阈值**：连接数越多，增长阈值越低，避免高连接数时误判
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
    /// 每次调整的连接数变化量（基础步长）
    pub adjust_step: u32,
    /// 是否启用自适应（false 时固定用 initial_connections）
    pub enabled: bool,
    /// 限速恢复探测间隔（秒）
    /// 判定为 CDN 限速后，每隔这么久试探一次是否恢复
    pub throttle_recovery_interval_secs: u64,
    /// 指数退避因子（失败率过高时，连接数减少的倍数）
    pub exponential_backoff_factor: f64,
    /// 动态阈值：连接数越多，增长阈值越低（避免高连接数时误判）
    pub dynamic_threshold_enabled: bool,
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
            throttle_recovery_interval_secs: 60, // 60秒试探一次
            exponential_backoff_factor: 1.5,     // 每次失败减少 1.5 倍
            dynamic_threshold_enabled: true,     // 启用动态阈值
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
    /// 上次限速恢复探测时间
    last_recovery_probe: Mutex<Instant>,
    /// 指数退避计数（连续失败次数，用于计算退避量）
    backoff_count: AtomicU32,
    /// 限速前的连接数（恢复试探时用）
    pre_throttle_connections: AtomicU32,
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
            last_recovery_probe: Mutex::new(Instant::now()),
            backoff_count: AtomicU32::new(0),
            pre_throttle_connections: AtomicU32::new(initial),
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

    /// 计算动态速度增长阈值
    ///
    /// 连接数越多，增长阈值越低（避免高连接数时误判限速）。
    /// 例如：2连接时阈值 5%，16连接时阈值 2%，32连接时阈值 1%。
    fn dynamic_growth_threshold(&self, current_connections: u32) -> f64 {
        if !self.config.dynamic_threshold_enabled {
            return self.config.speed_growth_threshold;
        }

        let base = self.config.speed_growth_threshold;
        let max_conn = self.config.max_connections.max(1) as f64;
        let conn = current_connections.max(1) as f64;

        // 线性衰减：连接数从 1 到 max，阈值从 base*2 衰减到 base*0.3
        let ratio = (conn - 1.0) / (max_conn - 1.0);
        let threshold = base * (2.0 - 1.7 * ratio);

        threshold.max(base * 0.2) // 最低不低于 base 的 20%
    }

    /// 计算指数退避后的连接数减少量
    ///
    /// 连续失败次数越多，减少量越大（指数增长）。
    fn exponential_backoff_step(&self) -> u32 {
        let count = self.backoff_count.load(Ordering::Relaxed);
        let base = self.config.adjust_step as f64;
        let factor = self.config.exponential_backoff_factor;

        // 指数增长：base * factor^count
        let step = (base * factor.powi(count as i32)).round() as u32;
        step.max(1) // 至少减少 1
    }

    /// 检查是否需要进行限速恢复探测
    ///
    /// 判定为 CDN 限速后，每隔一段时间试探一次是否恢复。
    /// 如果恢复，解除限速状态并继续增加连接。
    async fn check_throttle_recovery(&self) -> bool {
        if !self.is_throttled() {
            return false;
        }

        let mut last_probe = self.last_recovery_probe.lock().await;
        if last_probe.elapsed() < Duration::from_secs(self.config.throttle_recovery_interval_secs) {
            return false;
        }
        *last_probe = Instant::now();
        drop(last_probe);

        // 检查最近的速度是否有增长迹象
        let growth_rate = self.speed_growth_rate().await;
        let current = self.current_connections.load(Ordering::Relaxed);
        let threshold = self.dynamic_growth_threshold(current);

        if growth_rate > threshold {
            // 速度有增长，可能限速已恢复
            self.is_throttled.store(0, Ordering::Relaxed);
            self.stagnation_count.store(0, Ordering::Relaxed);
            self.backoff_count.store(0, Ordering::Relaxed);

            // 恢复到限速前的连接数
            let pre_throttle = self.pre_throttle_connections.load(Ordering::Relaxed);
            let new_conn = pre_throttle.max(current);
            self.current_connections.store(new_conn, Ordering::Relaxed);

            info!(
                "自适应: CDN 限速可能已恢复（增长率 {:.2}% > 阈值 {:.2}%），连接数恢复到 {}",
                growth_rate * 100.0,
                threshold * 100.0,
                new_conn
            );
            return true;
        }

        debug!(
            "自适应: CDN 限速恢复探测（增长率 {:.2}% <= 阈值 {:.2}%），继续保持限速",
            growth_rate * 100.0,
            threshold * 100.0
        );
        false
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

        // 0. 检查限速恢复（如果处于限速状态）
        if self.check_throttle_recovery().await {
            let new_conn = self.current_connections.load(Ordering::Relaxed);
            return (true, new_conn);
        }

        // 1. 失败率过高，减少连接（指数退避）
        if failure_rate > self.config.failure_rate_threshold
            && current > self.config.min_connections
        {
            let backoff_step = self.exponential_backoff_step();
            let new_conn = current
                .saturating_sub(backoff_step)
                .max(self.config.min_connections);
            self.current_connections.store(new_conn, Ordering::Relaxed);
            self.stagnation_count.store(0, Ordering::Relaxed);
            self.backoff_count.fetch_add(1, Ordering::Relaxed);

            warn!(
                "自适应: 失败率 {:.1}% 过高，指数退避（第{}次），连接数 {} -> {}（减少{}）",
                failure_rate * 100.0,
                self.backoff_count.load(Ordering::Relaxed),
                current,
                new_conn,
                backoff_step
            );
            return (true, new_conn);
        }

        // 失败率恢复正常，重置退避计数
        if failure_rate < self.config.failure_rate_threshold * 0.5 {
            self.backoff_count.store(0, Ordering::Relaxed);
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

        // 4. 检查速度增长（使用动态阈值）
        let growth_rate = self.speed_growth_rate().await;
        let threshold = self.dynamic_growth_threshold(current);

        if growth_rate < threshold {
            // 速度增长停滞
            let stagnation = self.stagnation_count.fetch_add(1, Ordering::Relaxed) + 1;
            if stagnation >= self.config.stagnation_limit {
                // 连续多次停滞，判定为 CDN 限速
                self.is_throttled.store(1, Ordering::Relaxed);
                self.pre_throttle_connections
                    .store(current, Ordering::Relaxed);

                info!(
                    "自适应: 速度连续 {} 次增长停滞（增长率 {:.2}% < 动态阈值 {:.2}%），判定为 CDN 限速，固定连接数 {}",
                    stagnation,
                    growth_rate * 100.0,
                    threshold * 100.0,
                    current
                );
                return (true, current);
            }

            debug!(
                "自适应: 速度增长停滞 {}/{}（增长率 {:.2}% < 动态阈值 {:.2}%），保持连接数 {}",
                stagnation,
                self.config.stagnation_limit,
                growth_rate * 100.0,
                threshold * 100.0,
                current
            );
            return (false, current);
        }

        // 5. 速度仍在增长，增加连接数（自适应步长：增长越快，增加越多）
        let growth_ratio = if threshold > 0.0 {
            (growth_rate / threshold).min(3.0).max(1.0)
        } else {
            1.0
        };
        let adaptive_step = (self.config.adjust_step as f64 * growth_ratio).round() as u32;
        let new_conn = (current + adaptive_step).min(self.config.max_connections);
        self.current_connections.store(new_conn, Ordering::Relaxed);
        self.stagnation_count.store(0, Ordering::Relaxed);

        info!(
            "自适应: 速度增长 {:.2}% > 动态阈值 {:.2}%，连接数 {} -> {}（增加{}）",
            growth_rate * 100.0,
            threshold * 100.0,
            current,
            new_conn,
            adaptive_step
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
            self.config.initial_connections,
            self.config.max_connections,
            self.config.adjust_interval_secs
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
        let current = self.current_connections.load(Ordering::Relaxed);
        AdaptiveStats {
            current_connections: current,
            is_throttled: self.is_throttled(),
            failure_rate: self.failure_rate(),
            total_requests: self.total_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            stagnation_count: self.stagnation_count.load(Ordering::Relaxed),
            sample_count: samples.len(),
            elapsed_secs: self.start_time.elapsed().as_secs(),
            dynamic_threshold: self.dynamic_growth_threshold(current),
            backoff_count: self.backoff_count.load(Ordering::Relaxed),
            pre_throttle_connections: self.pre_throttle_connections.load(Ordering::Relaxed),
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
    /// 当前动态阈值
    pub dynamic_threshold: f64,
    /// 指数退避计数
    pub backoff_count: u32,
    /// 限速前的连接数
    pub pre_throttle_connections: u32,
}
