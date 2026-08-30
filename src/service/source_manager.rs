//! 源管理器
//!
//! 维护所有可用下载源的健康度（速度、成功率、延迟、权重、熔断状态），
//! 按速度权重随机选择源，速度快的源被选中概率高。
//! 协议无关，只操作 [`pandanetos::domain::DownloadSource`] 和 [`SourceHealth`]。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pandanetos::domain::{ChunkStats, DownloadSource, SourceHealth};
use tokio::sync::Mutex;

/// 源管理器
pub struct SourceManager {
    /// 各源的健康度（key = source.identifier()）
    health: Mutex<HashMap<String, SourceHealth>>,
    /// 熔断恢复时间（秒）
    #[allow(dead_code)]
    circuit_breaker_timeout: Duration,
    /// 熔断阈值（连续失败次数）
    circuit_breaker_threshold: u64,
    /// EMA 平滑系数（0.0 - 1.0，越大越重视新数据）
    ema_alpha: f64,
}

impl SourceManager {
    /// 创建一个新的源管理器
    pub fn new() -> Self {
        Self {
            health: Mutex::new(HashMap::new()),
            circuit_breaker_timeout: Duration::from_secs(30),
            circuit_breaker_threshold: 5,
            ema_alpha: 0.3,
        }
    }

    /// 注册一个新源（初始权重为 1，避免权重为 0 导致永远选不到）
    pub async fn register_source(&self, source: &dyn DownloadSource) {
        let id = source.identifier();
        let mut health = self.health.lock().await;
        health.entry(id).or_insert_with(|| SourceHealth {
            weight: 1,
            ..Default::default()
        });
    }

    /// 按速度权重随机选择一个源
    ///
    /// 速度快的源被选中概率高。熔断的源不会被选中。
    /// 如果所有源都熔断了，返回 None。
    pub async fn pick_source(
        &self,
        sources: &[Arc<dyn DownloadSource>],
    ) -> Option<Arc<dyn DownloadSource>> {
        let health = self.health.lock().await;

        // 筛选未熔断的源，计算总权重
        let mut candidates: Vec<(usize, u64)> = Vec::new();
        let mut total_weight: u64 = 0;

        for (i, source) in sources.iter().enumerate() {
            let id = source.identifier();
            if let Some(h) = health.get(&id) {
                if !h.circuit_open && h.weight > 0 {
                    candidates.push((i, h.weight));
                    total_weight += h.weight;
                }
            } else {
                // 未注册的源，给默认权重 1
                candidates.push((i, 1));
                total_weight += 1;
            }
        }

        if candidates.is_empty() || total_weight == 0 {
            return None;
        }

        // 权重随机选择
        let mut r = rand::random::<u64>() % total_weight;
        for (idx, weight) in candidates {
            if r < weight {
                return Some(sources[idx].clone());
            }
            r -= weight;
        }

        // 兜底返回第一个
        Some(sources[0].clone())
    }

    /// 分片下载完成后更新源健康度
    pub async fn on_chunk_complete(&self, stats: &ChunkStats) {
        let mut health = self.health.lock().await;
        let h = health.entry(stats.source_id.clone()).or_default();

        h.success_count += 1;

        // EMA 平滑速度
        if stats.elapsed_secs > 0.0 && stats.downloaded_bytes > 0 {
            let instant_speed = (stats.downloaded_bytes as f64 / stats.elapsed_secs) as u64;
            let old = h.speed_bps as f64;
            let new = old * (1.0 - self.ema_alpha) + instant_speed as f64 * self.ema_alpha;
            h.speed_bps = new as u64;
        }

        // 权重 = 速度（至少为 1，避免权重为 0）
        h.weight = h.speed_bps.max(1);

        // 成功后重置熔断（如果之前熔断了）
        if h.circuit_open {
            h.circuit_open = false;
            h.fail_count = 0;
        }
    }

    /// 分片下载失败后更新源健康度
    pub async fn on_chunk_fail(&self, stats: &ChunkStats) {
        let mut health = self.health.lock().await;
        let h = health.entry(stats.source_id.clone()).or_default();

        h.fail_count += 1;

        // 失败一次权重减半
        h.weight = h.weight.max(2) / 2;

        // 连续失败达到阈值，触发熔断
        if h.fail_count >= self.circuit_breaker_threshold {
            h.circuit_open = true;
            h.weight = 0;
        }
    }

    /// 定期检查熔断的源，超时后自动恢复半开状态（权重设为 1，试探性分配）
    pub async fn tick_circuit_breakers(&self) {
        // 简单实现：熔断的源每 30 秒自动恢复
        // 更完善的实现需要记录熔断时间，这里用 fail_count 重置代替
        let mut health = self.health.lock().await;
        for h in health.values_mut() {
            if h.circuit_open {
                // 恢复半开状态，给一个小权重试探
                h.circuit_open = false;
                h.weight = 1;
                h.fail_count = 0;
            }
        }
    }

    /// 获取所有源的健康度快照（用于监控和日志）
    pub async fn snapshot(&self) -> HashMap<String, SourceHealth> {
        self.health.lock().await.clone()
    }

    /// 获取可用源数量（未熔断的）
    pub async fn available_count(&self) -> usize {
        let health = self.health.lock().await;
        health.values().filter(|h| !h.circuit_open).count()
    }
}

impl Default for SourceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::http::source::HttpSource;

    #[tokio::test]
    async fn test_register_and_pick() {
        let manager = SourceManager::new();
        let source1: Arc<dyn DownloadSource> =
            Arc::new(HttpSource::new("https://a.example.com/file.iso".into()));
        let source2: Arc<dyn DownloadSource> =
            Arc::new(HttpSource::new("https://b.example.com/file.iso".into()));

        manager.register_source(source1.as_ref()).await;
        manager.register_source(source2.as_ref()).await;

        let sources = vec![source1, source2];
        let picked = manager.pick_source(&sources).await;
        assert!(picked.is_some());
    }

    #[tokio::test]
    async fn test_on_chunk_complete_updates_speed() {
        let manager = SourceManager::new();
        let source = HttpSource::new("https://example.com/file.iso".into());
        manager.register_source(&source).await;

        let stats = ChunkStats {
            chunk_id: 0,
            source_id: source.identifier(),
            downloaded_bytes: 1024 * 1024, // 1MB
            elapsed_secs: 1.0,             // 1秒
            success: true,
            error_code: None,
        };
        manager.on_chunk_complete(&stats).await;

        let snapshot = manager.snapshot().await;
        let h = snapshot.get(&source.identifier()).unwrap();
        assert!(h.speed_bps > 0);
        assert!(h.weight > 0);
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        let manager = SourceManager::new();
        let source = HttpSource::new("https://example.com/file.iso".into());
        manager.register_source(&source).await;

        // 连续失败 5 次，触发熔断
        for i in 0..5 {
            let stats = ChunkStats {
                chunk_id: i,
                source_id: source.identifier(),
                downloaded_bytes: 0,
                elapsed_secs: 0.1,
                success: false,
                error_code: Some("TEST_ERROR"),
            };
            manager.on_chunk_fail(&stats).await;
        }

        let snapshot = manager.snapshot().await;
        let h = snapshot.get(&source.identifier()).unwrap();
        assert!(h.circuit_open);
        assert_eq!(h.weight, 0);
    }
}
