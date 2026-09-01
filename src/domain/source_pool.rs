//! 智能源池（Source Pool）
//!
//! 统一管理所有可用源，包括：
//! - 源发现（DNS多IP / URL替换 / DHT / PEX / tracker）
//! - 健康检查（延迟 / 速度 / 稳定性 / 成功率）
//! - 评分排序（综合评分，动态调整）
//! - 调度分配（高分源优先分配分片）
//! - 淘汰补充（持续失败的源自动淘汰，定期发现新源）
//!
//! 设计原则：
//! - 协议无关：源池只操作 ChunkFetcher trait，不关心具体协议
//! - 动态管理：源的评分实时更新，调度器始终选最优源
//! - 自愈能力：失败源自动冷却，恢复后自动重新加入

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::domain::chunk_fetcher::{ChunkFetcher, SourceCapabilities};

/// 源发现器 trait
///
/// 各种源发现机制（DNS多IP、URL替换、DHT/PEX、tracker等）实现此接口，
/// 源池调用 discover 方法获取新的下载源。
#[async_trait]
pub trait SourceDiscoverer: Send + Sync {
    /// 发现新的下载源
    ///
    /// # 参数
    /// - source_url: 原始源 URL
    ///
    /// # 返回
    /// 发现的新源列表（ChunkFetcher trait 对象），如果没有发现新源则返回空 vec
    async fn discover(&self, source_url: &str) -> anyhow::Result<Vec<Arc<dyn ChunkFetcher>>>;

    /// 发现器名称（用于日志和监控）
    fn name(&self) -> &str;
}

/// 源的健康状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceHealth {
    /// 健康（可用）
    Healthy,
    /// 降级（速度慢或不稳定，但仍可用）
    Degraded,
    /// 冷却中（连续失败，暂时不可用）
    CoolingDown,
    /// 已淘汰（持续失败，永久移除）
    Dead,
}

/// 带评分的源（Rated Source）
#[derive(Debug, Clone)]
pub struct RatedSource {
    /// 源的 fetcher（协议无关）
    pub fetcher: Arc<dyn ChunkFetcher>,

    /// 源的唯一标识符
    pub id: String,

    /// 源的显示名称
    pub display_name: String,

    /// 源的能力描述
    pub capabilities: SourceCapabilities,

    /// 综合评分（0-100，越高越好）
    pub score: f64,

    /// 健康状态
    pub health: SourceHealth,

    /// 统计信息
    pub stats: SourceStats,

    /// 加入时间
    pub added_at: Instant,

    /// 最后一次成功时间
    pub last_success_at: Option<Instant>,

    /// 最后一次失败时间
    pub last_failure_at: Option<Instant>,

    /// 冷却到期时间（如果在冷却中）
    pub cooldown_until: Option<Instant>,
}

/// 源的统计信息
#[derive(Debug, Clone, Default)]
pub struct SourceStats {
    /// 总请求次数
    pub total_requests: u64,

    /// 成功次数
    pub success_count: u64,

    /// 失败次数
    pub failure_count: u64,

    /// 连续失败次数
    pub consecutive_failures: u32,

    /// 总下载字节数
    pub total_bytes: u64,

    /// 平均延迟（毫秒）
    pub avg_latency_ms: f64,

    /// 平均速度（字节/秒）
    pub avg_speed_bps: f64,

    /// 速度标准差（衡量稳定性）
    pub speed_stddev: f64,
}

impl SourceStats {
    /// 计算成功率
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 1.0;
        }
        self.success_count as f64 / self.total_requests as f64
    }

    /// 记录一次成功
    pub fn record_success(&mut self, bytes: u64, latency_ms: u64, speed_bps: u64) {
        self.total_requests += 1;
        self.success_count += 1;
        self.consecutive_failures = 0;
        self.total_bytes += bytes;

        // 指数移动平均更新延迟和速度
        let alpha = 0.3;
        self.avg_latency_ms = if self.avg_latency_ms == 0.0 {
            latency_ms as f64
        } else {
            self.avg_latency_ms * (1.0 - alpha) + latency_ms as f64 * alpha
        };

        let speed = speed_bps as f64;
        self.avg_speed_bps = if self.avg_speed_bps == 0.0 {
            speed
        } else {
            self.avg_speed_bps * (1.0 - alpha) + speed * alpha
        };

        // 简化的标准差计算
        let diff = speed - self.avg_speed_bps;
        self.speed_stddev = if self.speed_stddev == 0.0 {
            diff.abs()
        } else {
            self.speed_stddev * (1.0 - alpha) + diff.abs() * alpha
        };
    }

    /// 记录一次失败
    pub fn record_failure(&mut self) {
        self.total_requests += 1;
        self.failure_count += 1;
        self.consecutive_failures += 1;
    }
}

/// 评分配置
#[derive(Debug, Clone)]
pub struct ScoringConfig {
    /// 速度权重（0-1）
    pub speed_weight: f64,
    /// 延迟权重（0-1）
    pub latency_weight: f64,
    /// 稳定性权重（0-1）
    pub stability_weight: f64,
    /// 成功率权重（0-1）
    pub success_rate_weight: f64,
    /// 连续失败阈值（超过则进入冷却）
    pub cooldown_threshold: u32,
    /// 冷却时间（秒）
    pub cooldown_duration_secs: u64,
    /// 淘汰阈值（连续失败次数超过则淘汰）
    pub dead_threshold: u32,
    /// 最小源数量（低于此数量时触发源发现）
    pub min_sources: usize,
    /// 最大源数量（超过此数量时淘汰最差的源）
    pub max_sources: usize,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            speed_weight: 0.4,
            latency_weight: 0.2,
            stability_weight: 0.2,
            success_rate_weight: 0.2,
            cooldown_threshold: 3,
            cooldown_duration_secs: 30,
            dead_threshold: 10,
            min_sources: 3,
            max_sources: 20,
        }
    }
}

/// 智能源池
///
/// # 用法
///
/// ```rust
/// use std::sync::Arc;
/// use spde::domain::source_pool::{SourcePool, ScoringConfig};
///
/// let pool = SourcePool::new(ScoringConfig::default());
///
/// // 添加源
/// pool.add_source(fetcher).await;
///
/// // 获取最优源（用于下载分片）
/// let best = pool.best_source().await;
///
/// // 记录结果
/// pool.record_success(&source_id, bytes, latency, speed).await;
/// pool.record_failure(&source_id).await;
/// ```
pub struct SourcePool {
    /// 源集合（按 id 索引）
    sources: RwLock<HashMap<String, RatedSource>>,

    /// 评分配置
    config: ScoringConfig,
}

impl SourcePool {
    /// 创建新的源池
    pub fn new(config: ScoringConfig) -> Self {
        Self {
            sources: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// 添加一个源
    pub async fn add_source(&self, fetcher: Arc<dyn ChunkFetcher>) {
        let id = fetcher.identifier();
        let display_name = fetcher.display_name();

        // probe 获取能力
        let capabilities = match fetcher.probe().await {
            Ok((_, caps)) => caps,
            Err(e) => {
                warn!(source = %id, error = %e, "probe failed, using default capabilities");
                SourceCapabilities::default()
            }
        };

        let rated = RatedSource {
            fetcher: fetcher.clone(),
            id: id.clone(),
            display_name,
            capabilities,
            score: 50.0, // 初始评分中等
            health: SourceHealth::Healthy,
            stats: SourceStats::default(),
            added_at: Instant::now(),
            last_success_at: None,
            last_failure_at: None,
            cooldown_until: None,
        };

        let mut sources = self.sources.write().await;
        if sources.contains_key(&id) {
            debug!(source = %id, "source already exists, updating");
        }
        sources.insert(id.clone(), rated);
        info!(source = %id, total = sources.len(), "source added to pool");
    }

    /// 批量添加源
    pub async fn add_sources(&self, fetchers: Vec<Arc<dyn ChunkFetcher>>) {
        for fetcher in fetchers {
            self.add_source(fetcher).await;
        }
    }

    /// 获取最优源（评分最高的健康源）
    pub async fn best_source(&self) -> Option<Arc<dyn ChunkFetcher>> {
        let sources = self.sources.read().await;
        let now = Instant::now();

        sources
            .values()
            .filter(|s| {
                // 只选健康或降级的源，且不在冷却中
                (s.health == SourceHealth::Healthy || s.health == SourceHealth::Degraded)
                    && s.cooldown_until.map(|t| now >= t).unwrap_or(true)
            })
            .max_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|s| s.fetcher.clone())
    }

    /// 获取前 N 个最优源（用于多连接并发）
    pub async fn top_sources(&self, n: usize) -> Vec<Arc<dyn ChunkFetcher>> {
        let sources = self.sources.read().await;
        let now = Instant::now();

        let mut sorted: Vec<&RatedSource> = sources
            .values()
            .filter(|s| {
                (s.health == SourceHealth::Healthy || s.health == SourceHealth::Degraded)
                    && s.cooldown_until.map(|t| now >= t).unwrap_or(true)
            })
            .collect();

        sorted.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        sorted
            .into_iter()
            .take(n)
            .map(|s| s.fetcher.clone())
            .collect()
    }

    /// 记录一次成功下载
    pub async fn record_success(
        &self,
        source_id: &str,
        bytes: u64,
        latency_ms: u64,
        speed_bps: u64,
    ) {
        let mut sources = self.sources.write().await;
        if let Some(source) = sources.get_mut(source_id) {
            source.stats.record_success(bytes, latency_ms, speed_bps);
            source.last_success_at = Some(Instant::now());
            source.health = SourceHealth::Healthy;
            source.cooldown_until = None;

            // 重新计算评分
            source.score = self.calculate_score(&source.stats);
        }
    }

    /// 记录一次失败下载
    pub async fn record_failure(&self, source_id: &str) {
        let mut sources = self.sources.write().await;
        if let Some(source) = sources.get_mut(source_id) {
            source.stats.record_failure();
            source.last_failure_at = Some(Instant::now());

            // 连续失败处理
            if source.stats.consecutive_failures >= self.config.dead_threshold {
                source.health = SourceHealth::Dead;
                warn!(source = %source_id, "source marked as dead");
            } else if source.stats.consecutive_failures >= self.config.cooldown_threshold {
                source.health = SourceHealth::CoolingDown;
                source.cooldown_until =
                    Some(Instant::now() + Duration::from_secs(self.config.cooldown_duration_secs));
                warn!(source = %source_id, cooldown_secs = self.config.cooldown_duration_secs, "source cooling down");
            } else {
                source.health = SourceHealth::Degraded;
            }

            // 重新计算评分（失败会降低评分）
            source.score = self.calculate_score(&source.stats);
        }
    }

    /// 计算源的综合评分（0-100）
    fn calculate_score(&self, stats: &SourceStats) -> f64 {
        let cfg = &self.config;

        // 速度评分（归一化到 0-100，假设 100MB/s 为满分）
        let max_speed = 100.0 * 1024.0 * 1024.0; // 100 MB/s
        let speed_score = (stats.avg_speed_bps / max_speed * 100.0).min(100.0);

        // 延迟评分（越低越好，100ms 为满分，5000ms 为 0 分）
        let latency_score = if stats.avg_latency_ms == 0.0 {
            50.0 // 未知延迟给中等分
        } else {
            ((5000.0 - stats.avg_latency_ms) / 5000.0 * 100.0)
                .max(0.0)
                .min(100.0)
        };

        // 稳定性评分（标准差越小越稳定）
        let stability_score = if stats.speed_stddev == 0.0 {
            100.0
        } else {
            let cv = stats.speed_stddev / stats.avg_speed_bps.max(1.0); // 变异系数
            (100.0 - cv * 100.0).max(0.0).min(100.0)
        };

        // 成功率评分
        let success_rate_score = stats.success_rate() * 100.0;

        // 加权平均
        let total_weight =
            cfg.speed_weight + cfg.latency_weight + cfg.stability_weight + cfg.success_rate_weight;

        let score = (speed_score * cfg.speed_weight
            + latency_score * cfg.latency_weight
            + stability_score * cfg.stability_weight
            + success_rate_score * cfg.success_rate_weight)
            / total_weight;

        score.max(0.0).min(100.0)
    }

    /// 获取当前源数量
    pub async fn len(&self) -> usize {
        self.sources.read().await.len()
    }

    /// 检查源池是否为空
    pub async fn is_empty(&self) -> bool {
        self.sources.read().await.is_empty()
    }

    /// 获取所有源的快照（用于监控和调试）
    pub async fn snapshot(&self) -> Vec<RatedSource> {
        self.sources.read().await.values().cloned().collect()
    }

    /// 清理已淘汰的源
    pub async fn cleanup_dead(&self) {
        let mut sources = self.sources.write().await;
        let before = sources.len();
        sources.retain(|_, s| s.health != SourceHealth::Dead);
        let removed = before - sources.len();
        if removed > 0 {
            info!(removed, "cleaned up dead sources");
        }
    }

    /// 检查是否需要发现更多源
    pub async fn needs_more_sources(&self) -> bool {
        let healthy_count = self
            .sources
            .read()
            .await
            .values()
            .filter(|s| s.health == SourceHealth::Healthy || s.health == SourceHealth::Degraded)
            .count();
        healthy_count < self.config.min_sources
    }
}
