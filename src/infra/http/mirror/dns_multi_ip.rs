//! DNS 多 IP 源发现器
//!
//! 对于同一个域名，DNS 可能解析出多个 IP 地址。
//! 每个 IP 地址都可以作为一个独立的下载源，
//! 从而实现多源并发下载，提高下载速度。
//!
//! 例如：cdn.example.com 解析出 1.1.1.1 和 2.2.2.2，
//! 那么可以创建两个源：
//! - http://1.1.1.1/path (Host: cdn.example.com)
//! - http://1.2.2.2/path (Host: cdn.example.com)

use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{debug, info, warn};
use url::Url;

use crate::domain::chunk_fetcher::ChunkFetcher;
use crate::domain::source_pool::SourceDiscoverer;
use crate::infra::http::fetcher::HttpRangeFetcher;

/// DNS 多 IP 源发现器
#[derive(Debug, Clone)]
pub struct DnsMultiIpDiscoverer {
    /// DNS 解析超时
    timeout: Duration,
    /// 已发现的源（去重）
    discovered: Arc<tokio::sync::Mutex<HashSet<String>>>,
}

impl DnsMultiIpDiscoverer {
    /// 创建新的 DNS 多 IP 源发现器
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            discovered: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        }
    }

    /// 解析域名获取所有 IP 地址
    async fn resolve_ips(&self, host: &str) -> anyhow::Result<Vec<IpAddr>> {
        use std::net::ToSocketAddrs;

        let host_port = format!("{}:443", host);
        let addrs: Vec<std::net::SocketAddr> = tokio::task::spawn_blocking(move || {
            host_port.to_socket_addrs().map(|iter| iter.collect())
        })
        .await??;

        let ips: Vec<IpAddr> = addrs.iter().map(|addr| addr.ip()).collect();
        debug!(host = %host, ips = ?ips, "DNS resolved");
        Ok(ips)
    }

    /// 从 URL 中提取主机名
    fn extract_host(&self, url: &str) -> Option<String> {
        Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_string()))
    }
}

impl Default for DnsMultiIpDiscoverer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SourceDiscoverer for DnsMultiIpDiscoverer {
    async fn discover(&self, source_url: &str) -> anyhow::Result<Vec<Arc<dyn ChunkFetcher>>> {
        let host = match self.extract_host(source_url) {
            Some(h) => h,
            None => {
                debug!(url = %source_url, "cannot extract host, skipping DNS discovery");
                return Ok(vec![]);
            }
        };

        // 只对 HTTP/HTTPS 进行 DNS 多 IP 发现
        if !source_url.starts_with("http://") && !source_url.starts_with("https://") {
            return Ok(vec![]);
        }

        let ips = match self.resolve_ips(&host).await {
            Ok(ips) => ips,
            Err(e) => {
                warn!(host = %host, error = %e, "DNS resolution failed");
                return Ok(vec![]);
            }
        };

        if ips.len() <= 1 {
            debug!(host = %host, "only one IP, no multi-source benefit");
            return Ok(vec![]);
        }

        info!(host = %host, ip_count = ips.len(), "discovered multiple IPs, creating multi-source");

        let mut discovered = self.discovered.lock().await;
        let mut new_sources = Vec::new();

        for ip in &ips {
            // 用 IP 替换主机名，保留 Host 头
            let ip_url = if source_url.starts_with("https://") {
                source_url.replacen(&format!("https://{}", host), &format!("https://{}", ip), 1)
            } else {
                source_url.replacen(&format!("http://{}", host), &format!("http://{}", ip), 1)
            };

            // 去重
            if discovered.contains(&ip_url) {
                continue;
            }
            discovered.insert(ip_url.clone());

            // 创建新的 Fetcher
            // 注意：实际使用时需要设置 Host 头为原域名
            let fetcher = HttpRangeFetcher::new(&ip_url, 30);
            new_sources.push(Arc::new(fetcher) as Arc<dyn ChunkFetcher>);
        }

        info!(
            host = %host,
            new_source_count = new_sources.len(),
            "DNS multi-IP discovery completed"
        );

        Ok(new_sources)
    }

    fn name(&self) -> &str {
        "dns-multi-ip"
    }
}
