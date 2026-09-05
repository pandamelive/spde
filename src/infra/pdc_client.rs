//! PDC (PeerDiscoveryCenter) 客户端
//!
//! spde 通过 PDC 的 REST API 获取 peer 列表，实现 S7 集成。
//!
//! API 端点：
//! - POST /api/v1/discover - 发现指定 infohash 的 peer
//! - GET /api/v1/cache/{infohash} - 查询缓存的 peer
//! - GET /health - 健康检查

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, warn};

use pandanetos::bittorrent::{Infohash, PeerInfo, PeerSource};

/// PDC 客户端配置
#[derive(Debug, Clone)]
pub struct PdcClientConfig {
    /// PDC 服务地址（如 http://127.0.0.1:6880）
    pub base_url: String,
    /// 请求超时
    pub timeout: Duration,
    /// 每次请求的最大 peer 数
    pub max_peers: usize,
}

impl Default for PdcClientConfig {
    fn default() -> Self {
        PdcClientConfig {
            base_url: "http://127.0.0.1:6880".to_string(),
            timeout: Duration::from_secs(10),
            max_peers: 50,
        }
    }
}

/// PDC 客户端
pub struct PdcClient {
    config: PdcClientConfig,
    client: Client,
}

impl PdcClient {
    /// 创建新的 PDC 客户端
    pub fn new(config: PdcClientConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .context("构建 HTTP 客户端失败")?;
        Ok(PdcClient { config, client })
    }

    /// 使用默认配置创建
    pub fn with_default() -> Result<Self> {
        Self::new(PdcClientConfig::default())
    }

    /// 检查 PDC 服务是否健康
    pub async fn health_check(&self) -> Result<bool> {
        let url = format!("{}/health", self.config.base_url);
        match self.client.get(&url).send().await {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(e) => {
                warn!("[pdc] 健康检查失败: {}", e);
                Ok(false)
            }
        }
    }

    /// 发现指定 infohash 的 peer
    ///
    /// 调用 PDC 的 /api/v1/discover 端点，触发多源 peer 发现。
    pub async fn discover_peers(&self, infohash: &Infohash) -> Result<Vec<PeerInfo>> {
        let url = format!("{}/api/v1/discover", self.config.base_url);
        let body = serde_json::json!({
            "infohash": infohash.to_hex(),
            "limit": self.config.max_peers,
        });

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("请求 PDC discover 失败: {}", url))?;

        if !resp.status().is_success() {
            return Err(anyhow::anyhow!(
                "PDC discover 返回状态: {}",
                resp.status()
            ));
        }

        let result: DiscoverResponse = resp.json().await.context("解析 PDC 响应失败")?;
        let peers: Vec<PeerInfo> = result
            .peers
            .into_iter()
            .map(|p| PeerInfo {
                addr: p.addr,
                peer_id: None,
                source: PeerSource::Dht,
                uploaded: 0,
                downloaded: 0,
                left: 0,
                last_active: 0,
                connection_attempts: 0,
                connection_successes: 0,
            })
            .collect();

        debug!("[pdc] 发现 {} 个 peer for {}", peers.len(), infohash);
        Ok(peers)
    }

    /// 查询缓存的 peer（不触发新的发现）
    pub async fn get_cached_peers(&self, infohash: &Infohash) -> Result<Vec<PeerInfo>> {
        let url = format!(
            "{}/api/v1/cache/{}",
            self.config.base_url,
            infohash.to_hex()
        );

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("请求 PDC cache 失败: {}", url))?;

        if !resp.status().is_success() {
            return Ok(vec![]); // 缓存未命中返回空
        }

        let result: DiscoverResponse = resp.json().await.unwrap_or_default();
        let peers: Vec<PeerInfo> = result
            .peers
            .into_iter()
            .map(|p| PeerInfo {
                addr: p.addr,
                peer_id: None,
                source: PeerSource::Tracker,
                uploaded: 0,
                downloaded: 0,
                left: 0,
                last_active: 0,
                connection_attempts: 0,
                connection_successes: 0,
            })
            .collect();

        Ok(peers)
    }

    /// 获取 PDC 统计信息
    pub async fn get_stats(&self) -> Result<PdcStats> {
        let url = format!("{}/api/v1/stats", self.config.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("请求 PDC stats 失败: {}", url))?;

        let stats: PdcStats = resp.json().await.context("解析 PDC stats 失败")?;
        Ok(stats)
    }
}

/// PDC discover 响应
#[derive(Debug, Deserialize, Default)]
struct DiscoverResponse {
    peers: Vec<PeerEntry>,
}

/// PDC 返回的 peer 条目
#[derive(Debug, Deserialize)]
struct PeerEntry {
    addr: std::net::SocketAddr,
    #[serde(default)]
    source: Option<String>,
}

/// PDC 统计信息
#[derive(Debug, Deserialize, Default)]
pub struct PdcStats {
    #[serde(default)]
    pub cached_peers: usize,
    #[serde(default)]
    pub cached_infohashes: usize,
    #[serde(default)]
    pub discoverers: Vec<String>,
    #[serde(default)]
    pub uptime_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = PdcClientConfig::default();
        assert_eq!(config.base_url, "http://127.0.0.1:6880");
        assert_eq!(config.max_peers, 50);
    }

    #[test]
    fn test_client_creation() {
        let client = PdcClient::with_default();
        assert!(client.is_ok());
    }

    #[test]
    fn test_discover_response_deserialize() {
        let json = r#"{
            "peers": [
                {"addr": "127.0.0.1:6881", "source": "dht"},
                {"addr": "192.168.1.1:6882", "source": "tracker"}
            ]
        }"#;
        let result: DiscoverResponse = serde_json::from_str(json).unwrap();
        assert_eq!(result.peers.len(), 2);
        assert_eq!(result.peers[0].addr.port(), 6881);
    }

    #[test]
    fn test_pdc_stats_deserialize() {
        let json = r#"{
            "cached_peers": 100,
            "cached_infohashes": 50,
            "discoverers": ["tracker", "dht"],
            "uptime_secs": 3600
        }"#;
        let stats: PdcStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.cached_peers, 100);
        assert_eq!(stats.cached_infohashes, 50);
        assert_eq!(stats.discoverers.len(), 2);
        assert_eq!(stats.uptime_secs, 3600);
    }
}
