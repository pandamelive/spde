//! CDN 限速识别与规避
//!
//! 智能识别被 CDN 限速的源，并自动规避：
//! - **多源速度对比**：对比多个镜像源的速度，识别异常低速的源
//! - **限速源标记**：连续多次速度低于平均值的源被标记为限速
//! - **自动规避**：限速源的连接数被减少，流量切换到其他源
//! - **恢复探测**：定期试探限速源是否恢复，恢复后自动解除标记
//!
//! 协议无关，只依赖源的速度统计和健康状态。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// CDN 限速检测器配置
#[derive(Debug, Clone)]
pub struct CdnThrottleConfig {
    /// 启用 CDN 限速检测
    pub enabled: bool,
    /// 速度对比窗口大小（采样点数）
    pub speed_window_size: usize,
    /// 限速判定阈值（速度低于平均值的百分比，0.0-1.0）
    /// 例如 0.5 表示速度低于平均值 50% 则判定为限速
    pub throttle_threshold: f64,
    /// 连续多少次低速才判定为限速
    pub stagnation_limit: u32,
    /// 限速源的连接数比例（0.0-1.0）
    /// 例如 0.2 表示限速源只保留 20% 的连接数
    pub throttled_connection_ratio: f64,
    /// 限速恢复探测间隔（秒）
    pub recovery_probe_interval_secs: u64,
    /// 最小源数量（少于这个数量不进行限速检测）
    pub min_sources_for_detection: usize,
}

impl Default for CdnThrottleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            speed_window_size: 10,
            throttle_threshold: 0.5,
            stagnation_limit: 3,
            throttled_connection_ratio: 0.2,
            recovery_probe_interval_secs: 30,
            min_sources_for_detection: 2,
        }
    }
}

/// 单个源的速度采样
#[derive(Debug, Clone, Copy)]
struct SourceSpeedSample {
    /// 采样时间
    timestamp: Instant,
    /// 速度（字节/秒）
    speed_bps: u64,
    /// 活跃连接数
    active_connections: u32,
}

/// 单个源的限速状态
#[derive(Debug)]
struct SourceThrottleState {
    /// 源标识
    source_id: String,
    /// 速度采样历史
    speed_samples: Vec<SourceSpeedSample>,
    /// 连续低速次数
    stagnation_count: u32,
    /// 是否被标记为限速
    is_throttled: bool,
    /// 被标记为限速的时间
    throttled_since: Option<Instant>,
    /// 上次恢复探测时间
    last_recovery_probe: Option<Instant>,
    /// 限速前的连接数
    pre_throttle_connections: u32,
}

impl SourceThrottleState {
    fn new(source_id: String) -> Self {
        Self {
            source_id,
            speed_samples: Vec::with_capacity(16),
            stagnation_count: 0,
            is_throttled: false,
            throttled_since: None,
            last_recovery_probe: None,
            pre_throttle_connections: 0,
        }
    }

    /// 记录一次速度采样
    fn record_speed(&mut self, speed_bps: u64, active_connections: u32, window_size: usize) {
        self.speed_samples.push(SourceSpeedSample {
            timestamp: Instant::now(),
            speed_bps,
            active_connections,
        });
        // 保留最近的采样点
        if self.speed_samples.len() > window_size {
            self.speed_samples.remove(0);
        }
    }

    /// 计算平均速度
    fn average_speed(&self) -> u64 {
        if self.speed_samples.is_empty() {
            return 0;
        }
        let sum: u64 = self.speed_samples.iter().map(|s| s.speed_bps).sum();
        sum / self.speed_samples.len() as u64
    }

    /// 计算每连接速度（用于公平对比，避免连接数多的源速度自然高）
    fn speed_per_connection(&self) -> f64 {
        if self.speed_samples.is_empty() {
            return 0.0;
        }
        let avg_speed = self.average_speed() as f64;
        let avg_conn: f64 = self
            .speed_samples
            .iter()
            .map(|s| s.active_connections.max(1) as f64)
            .sum::<f64>()
            / self.speed_samples.len() as f64;
        if avg_conn < 1.0 {
            return avg_speed;
        }
        avg_speed / avg_conn
    }
}

/// CDN 限速检测器
///
/// 运行在独立的 task 中，定期分析各源的速度，识别限速源并规避。
pub struct CdnThrottleDetector {
    /// 配置
    config: CdnThrottleConfig,
    /// 各源的限速状态
    source_states: Mutex<HashMap<String, SourceThrottleState>>,
    /// 总活跃源数量
    total_sources: AtomicU32,
    /// 是否有至少一个源被限速
    any_throttled: AtomicBool,
    /// 启动时间
    start_time: Instant,
    /// 总检测次数
    total_checks: AtomicU64,
    /// 限速判定次数
    throttle_detections: AtomicU64,
    /// 限速恢复次数
    throttle_recoveries: AtomicU64,
}

impl CdnThrottleDetector {
    /// 创建新的 CDN 限速检测器
    pub fn new(config: CdnThrottleConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            source_states: Mutex::new(HashMap::new()),
            total_sources: AtomicU32::new(0),
            any_throttled: AtomicBool::new(false),
            start_time: Instant::now(),
            total_checks: AtomicU64::new(0),
            throttle_detections: AtomicU64::new(0),
            throttle_recoveries: AtomicU64::new(0),
        })
    }

    /// 注册一个新源
    pub async fn register_source(&self, source_id: &str) {
        let mut states = self.source_states.lock().await;
        if !states.contains_key(source_id) {
            states.insert(source_id.to_string(), SourceThrottleState::new(source_id.to_string()));
            self.total_sources.fetch_add(1, Ordering::Relaxed);
            debug!("CDN限速检测: 注册源 {}", source_id);
        }
    }

    /// 注销一个源
    pub async fn unregister_source(&self, source_id: &str) {
        let mut states = self.source_states.lock().await;
        if states.remove(source_id).is_some() {
            self.total_sources.fetch_sub(1, Ordering::Relaxed);
            debug!("CDN限速检测: 注销源 {}", source_id);
        }
    }

    /// 记录一个源的速度
    pub async fn record_source_speed(&self, source_id: &str, speed_bps: u64, active_connections: u32) {
        let mut states = self.source_states.lock().await;
        if let Some(state) = states.get_mut(source_id) {
            state.record_speed(speed_bps, active_connections, self.config.speed_window_size);
        }
    }

    /// 检查一个源是否被限速
    pub async fn is_source_throttled(&self, source_id: &str) -> bool {
        let states = self.source_states.lock().await;
        states.get(source_id).map(|s| s.is_throttled).unwrap_or(false)
    }

    /// 获取限速源的连接数比例
    pub async fn throttled_connection_ratio(&self, source_id: &str) -> f64 {
        let states = self.source_states.lock().await;
        if states.get(source_id).map(|s| s.is_throttled).unwrap_or(false) {
            self.config.throttled_connection_ratio
        } else {
            1.0
        }
    }

    /// 是否有至少一个源被限速
    pub fn any_throttled(&self) -> bool {
        self.any_throttled.load(Ordering::Relaxed)
    }

    /// 执行一次限速检测
    ///
    /// 对比各源的每连接速度，识别异常低速的源，标记为限速并减少连接数。
    /// 同时检测限速源是否恢复。
    pub async fn check_throttling(&self) {
        if !self.config.enabled {
            return;
        }

        let total_sources = self.total_sources.load(Ordering::Relaxed) as usize;
        if total_sources < self.config.min_sources_for_detection {
            return; // 源太少，不进行检测
        }

        self.total_checks.fetch_add(1, Ordering::Relaxed);

        let mut states = self.source_states.lock().await;

        // 1. 计算所有源的每连接速度
        let mut source_speeds: Vec<(String, f64)> = states
            .iter()
            .map(|(id, state)| (id.clone(), state.speed_per_connection()))
            .collect();

        if source_speeds.is_empty() {
            return;
        }

        // 按速度排序
        source_speeds.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // 计算平均速度（排除最高和最低，避免异常值影响）
        let avg_speed = if source_speeds.len() >= 3 {
            let sum: f64 = source_speeds[1..source_speeds.len() - 1]
                .iter()
                .map(|(_, s)| *s)
                .sum();
            sum / (source_speeds.len() - 2) as f64
        } else {
            let sum: f64 = source_speeds.iter().map(|(_, s)| *s).sum();
            sum / source_speeds.len() as f64
        };

        if avg_speed < 1.0 {
            return; // 平均速度太低，不进行检测
        }

        let threshold = avg_speed * self.config.throttle_threshold;

        // 2. 检测限速源
        let mut any_throttled = false;
        for (source_id, speed) in &source_speeds {
            let state = states.get_mut(source_id).unwrap();

            if state.is_throttled {
                any_throttled = true;
                // 已经是限速源，检查是否恢复
                if self.check_recovery(state, *speed, threshold) {
                    self.throttle_recoveries.fetch_add(1, Ordering::Relaxed);
                    info!(
                        "CDN限速检测: 源 {} 恢复（每连接速度 {:.0} > 阈值 {:.0}）",
                        source_id, speed, threshold
                    );
                }
            } else {
                // 未限速源，检查是否被限速
                if *speed < threshold {
                    state.stagnation_count += 1;
                    if state.stagnation_count >= self.config.stagnation_limit {
                        // 判定为限速
                        state.is_throttled = true;
                        state.throttled_since = Some(Instant::now());
                        state.pre_throttle_connections = state
                            .speed_samples
                            .last()
                            .map(|s| s.active_connections)
                            .unwrap_or(1);
                        self.throttle_detections.fetch_add(1, Ordering::Relaxed);
                        any_throttled = true;

                        warn!(
                            "CDN限速检测: 源 {} 被判定为限速（每连接速度 {:.0} < 阈值 {:.0}，连续{}次），连接数减少到 {:.0}%",
                            source_id,
                            speed,
                            threshold,
                            state.stagnation_count,
                            self.config.throttled_connection_ratio * 100.0
                        );
                    } else {
                        debug!(
                            "CDN限速检测: 源 {} 速度偏低 {}/{}（每连接速度 {:.0} < 阈值 {:.0}）",
                            source_id,
                            state.stagnation_count,
                            self.config.stagnation_limit,
                            speed,
                            threshold
                        );
                    }
                } else {
                    // 速度正常，重置计数
                    state.stagnation_count = 0;
                }
            }
        }

        self.any_throttled.store(any_throttled, Ordering::Relaxed);
    }

    /// 检查限速源是否恢复
    fn check_recovery(&self, state: &mut SourceThrottleState, current_speed: f64, threshold: f64) -> bool {
        let now = Instant::now();

        // 检查是否到了恢复探测时间
        let should_probe = match state.last_recovery_probe {
            Some(last) => {
                now.duration_since(last) >= Duration::from_secs(self.config.recovery_probe_interval_secs)
            }
            None => true, // 第一次探测
        };

        if !should_probe {
            return false;
        }

        state.last_recovery_probe = Some(now);

        // 如果速度恢复到阈值以上，解除限速
        if current_speed > threshold {
            state.is_throttled = false;
            state.throttled_since = None;
            state.stagnation_count = 0;
            return true;
        }

        false
    }

    /// 获取统计信息
    pub async fn stats(&self) -> CdnThrottleStats {
        let states = self.source_states.lock().await;
        let throttled_sources: Vec<String> = states
            .iter()
            .filter(|(_, s)| s.is_throttled)
            .map(|(id, _)| id.clone())
            .collect();

        CdnThrottleStats {
            enabled: self.config.enabled,
            total_sources: self.total_sources.load(Ordering::Relaxed),
            throttled_source_count: throttled_sources.len() as u32,
            throttled_sources,
            any_throttled: self.any_throttled.load(Ordering::Relaxed),
            total_checks: self.total_checks.load(Ordering::Relaxed),
            throttle_detections: self.throttle_detections.load(Ordering::Relaxed),
            throttle_recoveries: self.throttle_recoveries.load(Ordering::Relaxed),
            elapsed_secs: self.start_time.elapsed().as_secs(),
        }
    }
}

/// CDN 限速检测器统计信息
#[derive(Debug, Clone)]
pub struct CdnThrottleStats {
    /// 是否启用
    pub enabled: bool,
    /// 总源数量
    pub total_sources: u32,
    /// 限速源数量
    pub throttled_source_count: u32,
    /// 限速源列表
    pub throttled_sources: Vec<String>,
    /// 是否有至少一个源被限速
    pub any_throttled: bool,
    /// 总检测次数
    pub total_checks: u64,
    /// 限速判定次数
    pub throttle_detections: u64,
    /// 限速恢复次数
    pub throttle_recoveries: u64,
    /// 运行时长（秒）
    pub elapsed_secs: u64,
}
