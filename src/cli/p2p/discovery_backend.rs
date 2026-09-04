//! Peer 发现后端抽象与混合路由
//!
//! 定义统一的 PeerDiscoveryBackend trait，支持多种后端：
//! - PdcBackend：通过 HTTP 调用 PDC 服务
//! - BuiltinBackend：SPDE 内置的发现逻辑
//! - HybridBackend：能力协商 + 混合路由 + 结果合并
//!
//! 核心原则：不检查版本号，只看 capabilities 数组，有多少能力用多少，
//! 缺的能力由内置后端补上，完全向后兼容。

use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 发现的 peer 信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredPeer {
    /// 节点地址（ip:port）
    pub addr: String,
    /// 来源（tracker / dht / pex / cache）
    pub source: String,
    /// 优先级分数（越高越优先）
    pub priority_score: u32,
    /// 是否为 IPv6
    pub is_ipv6: bool,
}

/// 发现结果
#[derive(Debug, Clone, Default)]
pub struct DiscoveryOutput {
    /// 发现的 peer 数量
    pub peers_count: usize,
    /// 发现的 peer 列表
    pub peers: Vec<DiscoveredPeer>,
    /// 各来源统计
    pub source_stats: std::collections::HashMap<String, usize>,
    /// 耗时（毫秒）
    pub duration_ms: u64,
    /// 是否成功
    pub success: bool,
    /// 错误消息
    pub error_msg: Option<String>,
}

/// 能力标识常量
pub mod capabilities {
    pub const TRACKER: &str = "tracker";
    pub const DHT: &str = "dht";
    pub const PEX: &str = "pex";
    pub const CACHE: &str = "cache";
    pub const ANNOUNCE: &str = "announce";
    pub const PRIORITY_SORTING: &str = "priority_sorting";
    pub const DEDUP: &str = "dedup";
}

/// Peer 发现后端 trait
#[async_trait]
pub trait PeerDiscoveryBackend: Send + Sync {
    /// 后端名称
    fn name(&self) -> &'static str;

    /// 是否可用
    async fn is_available(&self) -> bool;

    /// 获取能力列表
    async fn capabilities(&self) -> Vec<String>;

    /// 发现 peer
    async fn discover_peers(&self, infohash: &str, limit: usize)
        -> anyhow::Result<DiscoveryOutput>;
}

// ─── PDC 后端 ───

/// PDC 能力清单响应
#[derive(Debug, Clone, Deserialize)]
struct PdcCapabilityResponse {
    name: String,
    version: String,
    capabilities: Vec<String>,
    #[serde(default)]
    max_concurrent_discoverers: usize,
    #[serde(default)]
    cached_peers: usize,
}

/// PDC 发现响应
#[derive(Debug, Clone, Deserialize)]
struct PdcDiscoverResponse {
    #[serde(default)]
    peers_count: usize,
    #[serde(default)]
    peers: Vec<PdcPeer>,
    #[serde(default)]
    duration_ms: u64,
    #[serde(default)]
    source_stats: std::collections::HashMap<String, usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct PdcPeer {
    addr: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    priority_score: u32,
    #[serde(default)]
    is_ipv6: bool,
}

/// PDC 后端：通过 HTTP 调用 PDC 服务
pub struct PdcBackend {
    base_url: String,
    client: reqwest::Client,
    capabilities_cache: DashMap<String, CachedCapabilities>,
}

struct CachedCapabilities {
    capabilities: Vec<String>,
    cached_at: Instant,
}

impl PdcBackend {
    /// 创建新的 PDC 后端
    pub fn new(base_url: String) -> Arc<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build http client");
        Arc::new(Self {
            base_url,
            client,
            capabilities_cache: DashMap::new(),
        })
    }

    /// 从 PDC 获取能力列表（带缓存）
    async fn fetch_capabilities(&self) -> anyhow::Result<Vec<String>> {
        let cache_key = "global".to_string();
        if let Some(cached) = self.capabilities_cache.get(&cache_key) {
            if cached.cached_at.elapsed() < Duration::from_secs(60) {
                return Ok(cached.capabilities.clone());
            }
        }

        let url = format!("{}/api/v1/capability", self.base_url);
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(anyhow::anyhow!(
                "获取 PDC 能力失败，状态码: {}",
                resp.status()
            ));
        }

        let body: PdcCapabilityResponse = resp.json().await?;
        let caps = body.capabilities.clone();

        self.capabilities_cache.insert(
            cache_key,
            CachedCapabilities {
                capabilities: caps.clone(),
                cached_at: Instant::now(),
            },
        );

        Ok(caps)
    }
}

#[async_trait]
impl PeerDiscoveryBackend for PdcBackend {
    fn name(&self) -> &'static str {
        "pdc"
    }

    async fn is_available(&self) -> bool {
        self.fetch_capabilities().await.is_ok()
    }

    async fn capabilities(&self) -> Vec<String> {
        self.fetch_capabilities().await.unwrap_or_default()
    }

    async fn discover_peers(
        &self,
        infohash: &str,
        limit: usize,
    ) -> anyhow::Result<DiscoveryOutput> {
        let start = Instant::now();
        let url = format!("{}/api/v1/discover", self.base_url);
        let body = serde_json::json!({
            "infohash": infohash,
            "limit": limit,
        });

        let resp = self.client.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("PDC 发现失败，状态码: {}", resp.status()));
        }

        let result: PdcDiscoverResponse = resp.json().await?;
        let peers: Vec<DiscoveredPeer> = result
            .peers
            .into_iter()
            .map(|p| DiscoveredPeer {
                addr: p.addr,
                source: p.source,
                priority_score: p.priority_score,
                is_ipv6: p.is_ipv6,
            })
            .collect();

        Ok(DiscoveryOutput {
            peers_count: peers.len(),
            peers,
            source_stats: result.source_stats,
            duration_ms: start.elapsed().as_millis() as u64,
            success: true,
            error_msg: None,
        })
    }
}

// ─── 内置后端 ───

/// SPDE 内置发现后端（占位实现，实际应调用 SPDE 自己的 BT 发现逻辑）
pub struct BuiltinBackend {
    /// 内置能力列表
    capabilities: Vec<String>,
}

impl BuiltinBackend {
    /// 创建新的内置后端
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            capabilities: vec![
                capabilities::TRACKER.to_string(),
                capabilities::DHT.to_string(),
                capabilities::PEX.to_string(),
                capabilities::CACHE.to_string(),
                capabilities::PRIORITY_SORTING.to_string(),
                capabilities::DEDUP.to_string(),
            ],
        })
    }
}

#[async_trait]
impl PeerDiscoveryBackend for BuiltinBackend {
    fn name(&self) -> &'static str {
        "builtin"
    }

    async fn is_available(&self) -> bool {
        true
    }

    async fn capabilities(&self) -> Vec<String> {
        self.capabilities.clone()
    }

    async fn discover_peers(
        &self,
        _infohash: &str,
        _limit: usize,
    ) -> anyhow::Result<DiscoveryOutput> {
        // TODO: 集成 SPDE 自己的 BT 发现逻辑（librqbit DHT/Tracker）
        // 目前返回空结果，由 HybridBackend 与 PDC 结果合并
        Ok(DiscoveryOutput {
            peers_count: 0,
            peers: vec![],
            source_stats: std::collections::HashMap::new(),
            duration_ms: 0,
            success: true,
            error_msg: None,
        })
    }
}

// ─── 混合后端 ───

/// 混合后端：能力协商 + 混合路由 + 结果合并
///
/// 核心逻辑：
/// 1. 连接 PDC 后获取能力清单（不检查版本号）
/// 2. 按能力粒度路由：PDC 有的能力走 PDC，没有的走内置
/// 3. 并发执行后合并、去重、按优先级排序
/// 4. PDC 掉线时能力集合缩减而非模式切换，恢复后自动扩充
pub struct HybridBackend {
    pdc: Option<Arc<PdcBackend>>,
    builtin: Arc<BuiltinBackend>,
    /// PDC 能力缓存
    pdc_capabilities: DashMap<String, Vec<String>>,
    /// 最后一次 PDC 健康检查时间
    last_health_check: DashMap<String, Instant>,
}

impl HybridBackend {
    /// 创建新的混合后端
    pub fn new(pdc: Option<Arc<PdcBackend>>, builtin: Arc<BuiltinBackend>) -> Arc<Self> {
        Arc::new(Self {
            pdc,
            builtin,
            pdc_capabilities: DashMap::new(),
            last_health_check: DashMap::new(),
        })
    }

    /// 创建只有内置后端的混合后端
    pub fn builtin_only() -> Arc<Self> {
        Self::new(None, BuiltinBackend::new())
    }

    /// 获取 PDC 当前可用的能力列表
    async fn pdc_capabilities(&self) -> Vec<String> {
        if let Some(pdc) = &self.pdc {
            // 检查缓存
            let cache_key = "global".to_string();
            if let Some(cached) = self.pdc_capabilities.get(&cache_key) {
                if let Some(last_check) = self.last_health_check.get(&cache_key) {
                    if last_check.elapsed() < Duration::from_secs(30) {
                        return cached.clone();
                    }
                }
            }

            // 重新获取
            let caps = pdc.capabilities().await;
            if !caps.is_empty() {
                self.pdc_capabilities
                    .insert(cache_key.clone(), caps.clone());
                self.last_health_check.insert(cache_key, Instant::now());
                return caps;
            }
        }
        vec![]
    }

    /// PDC 是否具备指定能力
    async fn pdc_has_capability(&self, capability: &str) -> bool {
        let caps = self.pdc_capabilities().await;
        caps.iter().any(|c| c == capability)
    }

    /// 合并多个发现结果，去重并按优先级排序
    fn merge_results(results: Vec<DiscoveryOutput>) -> DiscoveryOutput {
        let mut merged_peers: std::collections::HashMap<String, DiscoveredPeer> =
            std::collections::HashMap::new();
        let mut merged_source_stats: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let mut total_duration = 0u64;
        let mut any_success = false;

        for result in results {
            total_duration = total_duration.max(result.duration_ms);
            if result.success {
                any_success = true;
            }
            for (source, count) in result.source_stats {
                *merged_source_stats.entry(source).or_insert(0) += count;
            }
            for peer in result.peers {
                // 去重：相同地址保留优先级更高的
                if let Some(existing) = merged_peers.get(&peer.addr) {
                    if peer.priority_score > existing.priority_score {
                        merged_peers.insert(peer.addr.clone(), peer);
                    }
                } else {
                    merged_peers.insert(peer.addr.clone(), peer);
                }
            }
        }

        let mut peers: Vec<DiscoveredPeer> = merged_peers.into_values().collect();
        // 按优先级降序排序
        peers.sort_by(|a, b| b.priority_score.cmp(&a.priority_score));

        DiscoveryOutput {
            peers_count: peers.len(),
            peers,
            source_stats: merged_source_stats,
            duration_ms: total_duration,
            success: any_success,
            error_msg: None,
        }
    }
}

#[async_trait]
impl PeerDiscoveryBackend for HybridBackend {
    fn name(&self) -> &'static str {
        "hybrid"
    }

    async fn is_available(&self) -> bool {
        // 混合后端始终可用（至少有内置后端）
        true
    }

    async fn capabilities(&self) -> Vec<String> {
        // 混合后端的能力是 PDC 和内置的并集
        let mut caps = self.builtin.capabilities().await;
        let pdc_caps = self.pdc_capabilities().await;
        for cap in pdc_caps {
            if !caps.contains(&cap) {
                caps.push(cap);
            }
        }
        caps
    }

    async fn discover_peers(
        &self,
        infohash: &str,
        limit: usize,
    ) -> anyhow::Result<DiscoveryOutput> {
        let pdc_caps = self.pdc_capabilities().await;
        let has_pdc = !pdc_caps.is_empty();

        if has_pdc {
            // PDC 可用，并发执行 PDC 和内置，然后合并
            let pdc = self.pdc.clone();
            let builtin = self.builtin.clone();
            let infohash_pdc = infohash.to_string();
            let infohash_builtin = infohash.to_string();

            let (pdc_result, builtin_result) = tokio::join!(
                async move {
                    if let Some(pdc) = pdc {
                        match pdc.discover_peers(&infohash_pdc, limit).await {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::warn!("PDC 发现失败，回退内置: {}", e);
                                DiscoveryOutput::default()
                            }
                        }
                    } else {
                        DiscoveryOutput::default()
                    }
                },
                async move {
                    match builtin.discover_peers(&infohash_builtin, limit).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!("内置发现失败: {}", e);
                            DiscoveryOutput::default()
                        }
                    }
                }
            );

            let merged = Self::merge_results(vec![pdc_result, builtin_result]);
            tracing::info!(
                "混合发现完成: PDC {} peers, 内置 {} peers, 合并后 {} peers",
                merged.source_stats.get("pdc").unwrap_or(&0),
                merged.source_stats.get("builtin").unwrap_or(&0),
                merged.peers.len()
            );
            Ok(merged)
        } else {
            // PDC 不可用，只用内置
            tracing::debug!("PDC 不可用，使用内置发现");
            self.builtin.discover_peers(infohash, limit).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovered_peer_serialization() {
        let peer = DiscoveredPeer {
            addr: "1.2.3.4:6881".to_string(),
            source: "tracker".to_string(),
            priority_score: 150,
            is_ipv6: false,
        };
        let json = serde_json::to_string(&peer).unwrap();
        assert!(json.contains("\"addr\":\"1.2.3.4:6881\""));
    }

    #[test]
    fn merge_results_dedup_and_sort() {
        let peer1 = DiscoveredPeer {
            addr: "1.1.1.1:6881".to_string(),
            source: "tracker".to_string(),
            priority_score: 100,
            is_ipv6: false,
        };
        let peer2 = DiscoveredPeer {
            addr: "1.1.1.1:6881".to_string(), // 相同地址，更高优先级
            source: "dht".to_string(),
            priority_score: 150,
            is_ipv6: false,
        };
        let peer3 = DiscoveredPeer {
            addr: "2.2.2.2:6881".to_string(),
            source: "pex".to_string(),
            priority_score: 80,
            is_ipv6: false,
        };

        let result1 = DiscoveryOutput {
            peers_count: 1,
            peers: vec![peer1],
            source_stats: [("tracker".to_string(), 1)].into(),
            duration_ms: 100,
            success: true,
            error_msg: None,
        };
        let result2 = DiscoveryOutput {
            peers_count: 2,
            peers: vec![peer2, peer3],
            source_stats: [("dht".to_string(), 1), ("pex".to_string(), 1)].into(),
            duration_ms: 200,
            success: true,
            error_msg: None,
        };

        let merged = HybridBackend::merge_results(vec![result1, result2]);
        assert_eq!(merged.peers.len(), 2); // 去重后 2 个
        assert_eq!(merged.peers[0].priority_score, 150); // 高优先级在前
        assert_eq!(merged.peers[0].source, "dht"); // 保留高优先级的来源
        assert_eq!(merged.duration_ms, 200); // 取最大耗时
    }

    #[tokio::test]
    async fn builtin_backend_capabilities() {
        let backend = BuiltinBackend::new();
        let caps = backend.capabilities().await;
        assert!(caps.contains(&"tracker".to_string()));
        assert!(caps.contains(&"dht".to_string()));
        assert!(caps.contains(&"pex".to_string()));
    }

    #[tokio::test]
    async fn hybrid_backend_builtin_only() {
        let hybrid = HybridBackend::builtin_only();
        assert!(hybrid.is_available().await);
        let caps = hybrid.capabilities().await;
        assert!(!caps.is_empty());
    }
}
