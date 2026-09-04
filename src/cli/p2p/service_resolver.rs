//! Agent 间服务发现客户端（SPDE 侧）
//!
//! 通过 PK 主控的服务注册中心发现 PDC 等服务，支持本地缓存、负载均衡、故障转移。

use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// 服务信息
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub agent_id: Uuid,
    pub name: String,
    pub agent_type: String,
    pub host: String,
    pub port: u16,
    pub capabilities: Vec<String>,
    pub health: String,
    pub load: f32,
    pub version: String,
}

impl ServiceInfo {
    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    pub fn is_healthy(&self) -> bool {
        self.health == "healthy"
    }

    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }
}

/// 缓存条目
struct CacheEntry {
    services: Vec<ServiceInfo>,
    cached_at: Instant,
}

/// 失败记录
struct FailureRecord {
    failed_at: Instant,
    consecutive_failures: u32,
}

/// 服务解析器配置
#[derive(Debug, Clone)]
pub struct ServiceResolverConfig {
    pub master_url: String,
    pub token: Option<String>,
    pub cache_ttl_secs: u64,
    pub failure_cooldown_secs: u64,
    pub max_consecutive_failures: u32,
}

impl Default for ServiceResolverConfig {
    fn default() -> Self {
        Self {
            master_url: "http://127.0.0.1:5566".to_string(),
            token: None,
            cache_ttl_secs: 60,
            failure_cooldown_secs: 30,
            max_consecutive_failures: 3,
        }
    }
}

/// 服务解析器
pub struct ServiceResolver {
    config: ServiceResolverConfig,
    cache: DashMap<String, CacheEntry>,
    failures: DashMap<Uuid, FailureRecord>,
    client: reqwest::Client,
}

impl ServiceResolver {
    /// 创建新的服务解析器
    pub fn new(config: ServiceResolverConfig) -> Arc<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("failed to build http client");
        Arc::new(Self {
            config,
            cache: DashMap::new(),
            failures: DashMap::new(),
            client,
        })
    }

    /// 查询指定类型和能力的服务列表
    pub async fn resolve(
        &self,
        agent_type: &str,
        capability: Option<&str>,
    ) -> Result<Vec<ServiceInfo>> {
        let cache_key = format!("{}:{}", agent_type, capability.unwrap_or("any"));

        // 检查缓存
        if let Some(entry) = self.cache.get(&cache_key) {
            if entry.cached_at.elapsed() < Duration::from_secs(self.config.cache_ttl_secs) {
                return Ok(self.filter_healthy(&entry.services));
            }
        }

        // 从 PK 查询
        let services = self.query_from_master(agent_type, capability).await?;

        // 更新缓存
        self.cache.insert(
            cache_key,
            CacheEntry {
                services: services.clone(),
                cached_at: Instant::now(),
            },
        );

        Ok(self.filter_healthy(&services))
    }

    /// 选择一个 PDC 服务实例
    pub async fn resolve_pdc(&self) -> Result<Option<ServiceInfo>> {
        let services = self.resolve("pdc", Some("tracker")).await?;
        if services.is_empty() {
            return Ok(None);
        }
        // 选择负载最低的
        let selected = services
            .iter()
            .min_by(|a, b| {
                a.load
                    .partial_cmp(&b.load)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned();
        Ok(selected)
    }

    /// 记录服务调用成功
    pub fn record_success(&self, agent_id: Uuid) {
        self.failures.remove(&agent_id);
    }

    /// 记录服务调用失败
    pub fn record_failure(&self, agent_id: Uuid) {
        let mut entry = self
            .failures
            .entry(agent_id)
            .or_insert_with(|| FailureRecord {
                failed_at: Instant::now(),
                consecutive_failures: 0,
            });
        entry.failed_at = Instant::now();
        entry.consecutive_failures += 1;
    }

    /// 从 PK 主控查询服务
    async fn query_from_master(
        &self,
        agent_type: &str,
        capability: Option<&str>,
    ) -> Result<Vec<ServiceInfo>> {
        let mut url = format!(
            "{}/api/v1/agents?agent_type={}",
            self.config.master_url, agent_type
        );
        if let Some(cap) = capability {
            url.push_str(&format!("&capability={}", cap));
        }

        let mut request = self.client.get(&url);
        if let Some(token) = &self.config.token {
            request = request.bearer_auth(token);
        }

        let resp = request
            .send()
            .await
            .with_context(|| "查询服务注册中心失败")?;
        if !resp.status().is_success() {
            return Err(anyhow::anyhow!(
                "查询服务注册中心失败，状态码: {}",
                resp.status()
            ));
        }

        #[derive(Deserialize)]
        struct QueryResponse {
            success: bool,
            data: Option<QueryData>,
        }
        #[derive(Deserialize)]
        struct QueryData {
            agents: Vec<ServiceInfoDto>,
            #[allow(dead_code)]
            total: usize,
        }
        #[derive(Deserialize)]
        struct ServiceInfoDto {
            agent_id: Uuid,
            name: String,
            agent_type: String,
            host: String,
            port: u16,
            #[serde(default)]
            capabilities: Vec<String>,
            #[serde(default)]
            health: String,
            #[serde(default)]
            load: f32,
            #[serde(default)]
            version: String,
        }

        let body: QueryResponse = resp.json().await?;
        let agents = body.data.map(|d| d.agents).unwrap_or_default();
        let services = agents
            .into_iter()
            .map(|dto| ServiceInfo {
                agent_id: dto.agent_id,
                name: dto.name,
                agent_type: dto.agent_type,
                host: dto.host,
                port: dto.port,
                capabilities: dto.capabilities,
                health: dto.health,
                load: dto.load,
                version: dto.version,
            })
            .collect();
        Ok(services)
    }

    /// 过滤掉不健康和冷却中的服务
    fn filter_healthy(&self, services: &[ServiceInfo]) -> Vec<ServiceInfo> {
        services
            .iter()
            .filter(|s| s.is_healthy())
            .filter(|s| {
                if let Some(failure) = self.failures.get(&s.agent_id) {
                    if failure.consecutive_failures >= self.config.max_consecutive_failures
                        && failure.failed_at.elapsed()
                            < Duration::from_secs(self.config.failure_cooldown_secs)
                    {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }

    /// 清空缓存
    pub fn clear_cache(&self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_info_base_url() {
        let info = ServiceInfo {
            agent_id: Uuid::new_v4(),
            name: "pdc-1".to_string(),
            agent_type: "pdc".to_string(),
            host: "10.0.0.5".to_string(),
            port: 6881,
            capabilities: vec!["tracker".to_string()],
            health: "healthy".to_string(),
            load: 0.3,
            version: "0.1.0".to_string(),
        };
        assert_eq!(info.base_url(), "http://10.0.0.5:6881");
        assert!(info.is_healthy());
        assert!(info.has_capability("tracker"));
    }

    #[test]
    fn resolver_config_defaults() {
        let config = ServiceResolverConfig::default();
        assert_eq!(config.cache_ttl_secs, 60);
        assert_eq!(config.failure_cooldown_secs, 30);
        assert_eq!(config.max_consecutive_failures, 3);
    }
}
