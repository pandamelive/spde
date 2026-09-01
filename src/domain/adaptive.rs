//! 自适应控制器（Adaptive Controller）
//!
//! 实时监控下载状态，动态调整：
//! - 并发连接数（1-32）
//! - 分片大小（1MB-64MB）
//! - 源选择策略（基于实时评分）
//! - 重试策略（指数退避）
//! - 超时时间（基于网络状况）
//!
//! 设计原则：
//! - 保守启动：初始用保守参数，避免一开始就打爆网络
//! - 渐进调整：参数逐步变化，避免剧烈波动
//! - 实时反馈：基于实际下载效果调整，不依赖预设值
//! - 安全边界：所有参数都有上下限，防止极端值

use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info};

/// 自适应配置
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// 初始并发连接数
    pub initial_concurrency: u32,
    /// 最小并发连接数
    pub min_concurrency: u32,
    /// 最大并发连接数
    pub max_concurrency: u32,

    /// 初始分片大小（字节）
    pub initial_chunk_size: u64,
    /// 最小分片大小（字节）
    pub min_chunk_size: u64,
    /// 最大分片大小（字节）
    pub max_chunk_size: u64,

    /// 初始超时时间（秒）
    pub initial_timeout_secs: u64,
    /// 最小超时时间（秒）
    pub min_timeout_secs: u64,
    /// 最大超时时间（秒）
    pub max_timeout_secs: u64,

    /// 调整间隔（秒）：每隔多久检查一次并调整参数
    pub adjust_interval_secs: u64,

    /// 速度提升阈值（百分比）：速度提升超过此值则增加并发
    pub speed_increase_threshold: f64,
    /// 速度下降阈值（百分比）：速度下降超过此值则减少并发
    pub speed_decrease_threshold: f64,

    /// 错误率阈值（百分比）：错误率超过此值则减少并发
    pub error_rate_threshold: f64,

    /// 并发调整步长（每次增加/减少的连接数）
    pub concurrency_step: u32,

    /// 分片调整因子（每次乘以/除以此因子）
    pub chunk_size_factor: f64,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            initial_concurrency: 4,
            min_concurrency: 1,
            max_concurrency: 32,

            initial_chunk_size: 4 * 1024 * 1024, // 4MB
            min_chunk_size: 1 * 1024 * 1024,     // 1MB
            max_chunk_size: 64 * 1024 * 1024,    // 64MB

            initial_timeout_secs: 30,
            min_timeout_secs: 10,
            max_timeout_secs: 120,

            adjust_interval_secs: 5,

            speed_increase_threshold: 20.0, // 速度提升 20% 以上
            speed_decrease_threshold: 20.0, // 速度下降 20% 以上

            error_rate_threshold: 10.0, // 错误率超过 10%

            concurrency_step: 2,
            chunk_size_factor: 2.0,
        }
    }
}

/// 下载状态快照（用于自适应决策）
#[derive(Debug, Clone)]
pub struct DownloadSnapshot {
    /// 当前总速度（字节/秒）
    pub total_speed_bps: u64,
    /// 当前活跃连接数
    pub active_connections: u32,
    /// 最近一段时间的请求总数
    pub recent_requests: u64,
    /// 最近一段时间的成功数
    pub recent_successes: u64,
    /// 最近一段时间的失败数
    pub recent_failures: u64,
    /// 平均延迟（毫秒）
    pub avg_latency_ms: f64,
    /// 已下载字节数
    pub downloaded_bytes: u64,
    /// 总字节数
    pub total_bytes: u64,
    /// 快照时间
    pub timestamp: Instant,
}
impl Default for DownloadSnapshot {
    fn default() -> Self {
        Self {
            total_speed_bps: 0,
            active_connections: 0,
            recent_requests: 0,
            recent_successes: 0,
            recent_failures: 0,
            avg_latency_ms: 0.0,
            downloaded_bytes: 0,
            total_bytes: 0,
            timestamp: Instant::now(),
        }
    }
}

impl DownloadSnapshot {
    /// 计算错误率（百分比）
    pub fn error_rate(&self) -> f64 {
        if self.recent_requests == 0 {
            return 0.0;
        }
        self.recent_failures as f64 / self.recent_requests as f64 * 100.0
    }

    /// 计算进度百分比
    pub fn progress_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        self.downloaded_bytes as f64 / self.total_bytes as f64 * 100.0
    }
}

/// 当前自适应参数
#[derive(Debug, Clone)]
pub struct AdaptiveParams {
    /// 当前并发连接数
    pub concurrency: u32,
    /// 当前分片大小（字节）
    pub chunk_size: u64,
    /// 当前超时时间（秒）
    pub timeout_secs: u64,
    /// 最后一次调整时间
    pub last_adjusted_at: Instant,
    /// 调整次数
    pub adjust_count: u64,
}

impl Default for AdaptiveParams {
    fn default() -> Self {
        Self {
            concurrency: 4,
            chunk_size: 4 * 1024 * 1024,
            timeout_secs: 30,
            last_adjusted_at: Instant::now(),
            adjust_count: 0,
        }
    }
}

/// 自适应控制器
///
/// # 用法
///
/// ```rust
/// use spde::domain::adaptive::{AdaptiveController, AdaptiveConfig, DownloadSnapshot};
///
/// let controller = AdaptiveController::new(AdaptiveConfig::default());
///
/// // 定期更新快照
/// let snapshot = DownloadSnapshot {
///     total_speed_bps: 10_000_000,
///     active_connections: 8,
///     ..Default::default()
/// };
/// controller.update_snapshot(snapshot).await;
///
/// // 获取当前参数
/// let params = controller.current_params().await;
/// ```
pub struct AdaptiveController {
    /// 配置
    config: AdaptiveConfig,

    /// 当前参数
    params: RwLock<AdaptiveParams>,

    /// 历史快照（用于比较速度变化）
    history: RwLock<Vec<DownloadSnapshot>>,

    /// 最大历史记录数
    max_history: usize,
}

impl AdaptiveController {
    /// 创建新的自适应控制器
    pub fn new(config: AdaptiveConfig) -> Self {
        let initial_params = AdaptiveParams {
            concurrency: config.initial_concurrency,
            chunk_size: config.initial_chunk_size,
            timeout_secs: config.initial_timeout_secs,
            last_adjusted_at: Instant::now(),
            adjust_count: 0,
        };

        Self {
            config,
            params: RwLock::new(initial_params),
            history: RwLock::new(Vec::new()),
            max_history: 20,
        }
    }

    /// 更新下载快照并触发自适应调整
    pub async fn update_snapshot(&self, snapshot: DownloadSnapshot) {
        let mut history = self.history.write().await;
        history.push(snapshot.clone());

        // 保持历史记录在最大数量内
        if history.len() > self.max_history {
            history.remove(0);
        }

        // 检查是否需要调整
        let should_adjust = {
            let params = self.params.read().await;
            snapshot.timestamp.duration_since(params.last_adjusted_at)
                >= Duration::from_secs(self.config.adjust_interval_secs)
        };

        if should_adjust {
            self.adjust().await;
        }
    }

    /// 执行自适应调整
    async fn adjust(&self) {
        let history = self.history.read().await;
        if history.len() < 2 {
            return; // 历史数据不足，不调整
        }

        let current = history.last().unwrap();
        let previous = &history[history.len() - 2];

        let mut params = self.params.write().await;
        let cfg = &self.config;

        // 计算速度变化百分比
        let speed_change = if previous.total_speed_bps == 0 {
            100.0 // 从 0 开始，视为大幅提升
        } else {
            (current.total_speed_bps as f64 - previous.total_speed_bps as f64)
                / previous.total_speed_bps as f64
                * 100.0
        };

        let error_rate = current.error_rate();

        debug!(
            speed_change = speed_change,
            error_rate = error_rate,
            current_concurrency = params.concurrency,
            "adaptive adjust check"
        );

        // 决策逻辑
        let mut concurrency_changed = false;

        // 情况 1：速度大幅提升且错误率低 → 增加并发
        if speed_change > cfg.speed_increase_threshold
            && error_rate < cfg.error_rate_threshold
            && params.concurrency < cfg.max_concurrency
        {
            params.concurrency =
                (params.concurrency + cfg.concurrency_step).min(cfg.max_concurrency);
            concurrency_changed = true;
            info!(
                old_concurrency = params.concurrency - cfg.concurrency_step,
                new_concurrency = params.concurrency,
                reason = "speed increased, error rate low",
                "adaptive: increased concurrency"
            );
        }
        // 情况 2：速度大幅下降或错误率高 → 减少并发
        else if (speed_change < -cfg.speed_decrease_threshold
            || error_rate > cfg.error_rate_threshold)
            && params.concurrency > cfg.min_concurrency
        {
            params.concurrency =
                (params.concurrency - cfg.concurrency_step).max(cfg.min_concurrency);
            concurrency_changed = true;
            info!(
                old_concurrency = params.concurrency + cfg.concurrency_step,
                new_concurrency = params.concurrency,
                speed_change = speed_change,
                error_rate = error_rate,
                reason = "speed decreased or error rate high",
                "adaptive: decreased concurrency"
            );
        }

        // 分片大小调整（基于延迟和速度）
        // 高延迟 + 高速度 → 增大分片（减少请求次数）
        // 低延迟 + 低速度 → 减小分片（增加并行度）
        if current.avg_latency_ms > 500.0 && current.total_speed_bps > 1_000_000 {
            // 高延迟高速度，增大分片
            let new_chunk_size = (params.chunk_size as f64 * cfg.chunk_size_factor) as u64;
            params.chunk_size = new_chunk_size.min(cfg.max_chunk_size);
        } else if current.avg_latency_ms < 50.0 && current.total_speed_bps < 1_000_000 {
            // 低延迟低速度，减小分片
            let new_chunk_size = (params.chunk_size as f64 / cfg.chunk_size_factor) as u64;
            params.chunk_size = new_chunk_size.max(cfg.min_chunk_size);
        }

        // 超时时间调整（基于错误率和延迟）
        if error_rate > cfg.error_rate_threshold || current.avg_latency_ms > 1000.0 {
            // 网络状况差，增加超时
            params.timeout_secs = (params.timeout_secs + 10).min(cfg.max_timeout_secs);
        } else if error_rate < 1.0 && current.avg_latency_ms < 100.0 {
            // 网络状况好，减少超时
            params.timeout_secs = (params.timeout_secs - 5).max(cfg.min_timeout_secs);
        }

        params.last_adjusted_at = Instant::now();
        params.adjust_count += 1;

        if concurrency_changed {
            debug!(
                concurrency = params.concurrency,
                chunk_size = params.chunk_size,
                timeout_secs = params.timeout_secs,
                adjust_count = params.adjust_count,
                "adaptive params updated"
            );
        }
    }

    /// 获取当前自适应参数
    pub async fn current_params(&self) -> AdaptiveParams {
        self.params.read().await.clone()
    }

    /// 获取当前并发数
    pub async fn current_concurrency(&self) -> u32 {
        self.params.read().await.concurrency
    }

    /// 获取当前分片大小
    pub async fn current_chunk_size(&self) -> u64 {
        self.params.read().await.chunk_size
    }

    /// 获取当前超时时间
    pub async fn current_timeout(&self) -> Duration {
        Duration::from_secs(self.params.read().await.timeout_secs)
    }

    /// 手动设置并发数（用于用户干预）
    pub async fn set_concurrency(&self, concurrency: u32) {
        let mut params = self.params.write().await;
        params.concurrency = concurrency
            .max(self.config.min_concurrency)
            .min(self.config.max_concurrency);
        params.last_adjusted_at = Instant::now();
        info!(concurrency = params.concurrency, "concurrency manually set");
    }

    /// 重置为初始参数
    pub async fn reset(&self) {
        let mut params = self.params.write().await;
        *params = AdaptiveParams {
            concurrency: self.config.initial_concurrency,
            chunk_size: self.config.initial_chunk_size,
            timeout_secs: self.config.initial_timeout_secs,
            last_adjusted_at: Instant::now(),
            adjust_count: 0,
        };
        self.history.write().await.clear();
        info!("adaptive controller reset");
    }

    /// 获取调整统计
    pub async fn stats(&self) -> (u64, usize) {
        let params = self.params.read().await;
        let history = self.history.read().await;
        (params.adjust_count, history.len())
    }
}
