//! pk (PandaKeeper) 客户端
//!
//! spde 通过 pk 的 REST API 将下载到的 metadata 推送到索引库。
//!
//! API 端点：
//! - POST /api/v1/torrents - 插入/更新种子索引
//! - GET /api/v1/torrents/{infohash} - 查询种子详情
//! - GET /api/v1/torrents/search?q=... - 搜索种子
//! - GET /api/v1/torrents/stats - 索引统计

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use pandanetos::bittorrent::{Infohash, MetadataInfo};

/// pk 客户端配置
#[derive(Debug, Clone)]
pub struct PkClientConfig {
    /// pk 服务地址（如 http://127.0.0.1:8080）
    pub base_url: String,
    /// 请求超时
    pub timeout: Duration,
}

impl Default for PkClientConfig {
    fn default() -> Self {
        PkClientConfig {
            base_url: "http://127.0.0.1:8080".to_string(),
            timeout: Duration::from_secs(10),
        }
    }
}

/// pk 客户端
pub struct PkClient {
    config: PkClientConfig,
    client: Client,
}

impl PkClient {
    /// 创建新的 pk 客户端
    pub fn new(config: PkClientConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .context("构建 HTTP 客户端失败")?;
        Ok(PkClient { config, client })
    }

    /// 使用默认配置创建
    pub fn with_default() -> Result<Self> {
        Self::new(PkClientConfig::default())
    }

    /// 检查 pk 服务是否可用
    pub async fn health_check(&self) -> bool {
        let url = format!("{}/api/v1/overview", self.config.base_url);
        match self.client.get(&url).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(e) => {
                warn!("[pk] 健康检查失败: {}", e);
                false
            }
        }
    }

    /// 将 metadata 推送到 pk 索引库
    pub async fn submit_metadata(&self, metadata: &MetadataInfo) -> Result<()> {
        let url = format!("{}/api/v1/torrents", self.config.base_url);

        let torrent = PkTorrentEntry {
            infohash: metadata.infohash.to_hex(),
            name: metadata.name.clone(),
            total_length: metadata.total_length as i64,
            piece_length: metadata.piece_length as i64,
            piece_count: metadata.piece_count as i64,
            file_count: metadata.files.len() as i64,
            private: metadata.private,
            created_by: metadata.created_by.clone(),
            creation_date: metadata.creation_date.map(|t| t as i64),
            comment: metadata.comment.clone(),
            seeders: 0,
            leechers: 0,
            completed: 0,
            first_seen: metadata.fetched_at as i64,
            last_updated: metadata.fetched_at as i64,
            source: "spde".to_string(),
            metadata_complete: true,
        };

        let resp = self
            .client
            .post(&url)
            .json(&torrent)
            .send()
            .await
            .with_context(|| format!("推送 metadata 到 pk 失败: {}", url))?;

        if !resp.status().is_success() {
            return Err(anyhow::anyhow!(
                "pk 返回状态: {}",
                resp.status()
            ));
        }

        info!(
            "[pk] 已推送 metadata: {} ({})",
            metadata.name,
            metadata.infohash
        );
        Ok(())
    }

    /// 查询种子详情
    pub async fn get_torrent(&self, infohash: &Infohash) -> Result<Option<PkTorrentEntry>> {
        let url = format!(
            "{}/api/v1/torrents/{}",
            self.config.base_url,
            infohash.to_hex()
        );

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("查询 pk 种子失败: {}", url))?;

        if resp.status() == 404 {
            return Ok(None);
        }

        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("pk 返回状态: {}", resp.status()));
        }

        let torrent: PkTorrentEntry = resp.json().await.context("解析 pk 响应失败")?;
        Ok(Some(torrent))
    }

    /// 搜索种子
    pub async fn search_torrents(&self, query: &str, limit: i64) -> Result<Vec<PkTorrentEntry>> {
        let url = format!(
            "{}/api/v1/torrents?q={}&limit={}",
            self.config.base_url,
            url_encode(query),
            limit
        );

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("搜索 pk 种子失败: {}", url))?;

        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("pk 返回状态: {}", resp.status()));
        }

        let torrents: Vec<PkTorrentEntry> = resp.json().await.context("解析 pk 响应失败")?;
        debug!("[pk] 搜索 '{}' 返回 {} 个结果", query, torrents.len());
        Ok(torrents)
    }

    /// 获取索引统计
    pub async fn get_stats(&self) -> Result<PkIndexStats> {
        let url = format!("{}/api/v1/torrents/stats", self.config.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("获取 pk 统计失败: {}", url))?;

        let stats: PkIndexStats = resp.json().await.context("解析 pk 统计失败")?;
        Ok(stats)
    }
}

/// pk 种子索引条目（与 pk torrent_index::TorrentIndex 对应）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PkTorrentEntry {
    pub infohash: String,
    pub name: String,
    pub total_length: i64,
    pub piece_length: i64,
    pub piece_count: i64,
    pub file_count: i64,
    pub private: bool,
    #[serde(default)]
    pub created_by: Option<String>,
    #[serde(default)]
    pub creation_date: Option<i64>,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub seeders: i64,
    #[serde(default)]
    pub leechers: i64,
    #[serde(default)]
    pub completed: i64,
    #[serde(default)]
    pub first_seen: i64,
    #[serde(default)]
    pub last_updated: i64,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub metadata_complete: bool,
}

/// pk 索引统计
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PkIndexStats {
    pub total_torrents: i64,
    pub with_metadata: i64,
    pub total_seeders: i64,
    pub total_size: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = PkClientConfig::default();
        assert_eq!(config.base_url, "http://127.0.0.1:8080");
    }

    #[test]
    fn test_client_creation() {
        let client = PkClient::with_default();
        assert!(client.is_ok());
    }

    #[test]
    fn test_torrent_entry_serialize() {
        let entry = PkTorrentEntry {
            infohash: "abc123".to_string(),
            name: "Test".to_string(),
            total_length: 1024,
            piece_length: 256,
            piece_count: 4,
            file_count: 1,
            private: false,
            created_by: None,
            creation_date: None,
            comment: None,
            seeders: 10,
            leechers: 2,
            completed: 100,
            first_seen: 1000000,
            last_updated: 1000000,
            source: "spde".to_string(),
            metadata_complete: true,
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"infohash\":\"abc123\""));
        assert!(json.contains("\"name\":\"Test\""));
        assert!(json.contains("\"seeders\":10"));
    }

    #[test]
    fn test_index_stats_deserialize() {
        let json = r#"{
            "total_torrents": 100,
            "with_metadata": 80,
            "total_seeders": 5000,
            "total_size": 1099511627776
        }"#;
        let stats: PkIndexStats = serde_json::from_str(json).unwrap();
        assert_eq!(stats.total_torrents, 100);
        assert_eq!(stats.with_metadata, 80);
        assert_eq!(stats.total_seeders, 5000);
    }
}

/// 简单的 URL 编码
fn url_encode(s: &str) -> String {
    let mut result = String::new();
    for byte in s.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(*byte as char);
            }
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}
