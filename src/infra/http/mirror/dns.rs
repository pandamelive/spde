//! DNS 多 IP 镜像发现器
//!
//! 实现 [`pandanetos::domain::MirrorDiscoverer`] trait。
//! 解析原始 URL 的域名，获取所有 IP 地址，每个 IP 生成一个绑定了该 IP 的
//! HttpSource。用于突破 CDN 单 IP 限速，多 IP 并发下载。

use std::collections::HashSet;
use std::net::IpAddr;

use async_trait::async_trait;
use pandanetos::domain::{DownloadSource, MirrorDiscoverer};
use pandanetos::error::{CoreError, Result};

use super::super::source::HttpSource;

/// DNS 多 IP 镜像发现器
pub struct DnsMultiIpDiscoverer;

impl DnsMultiIpDiscoverer {
    pub fn new() -> Self {
        Self
    }

    /// 从 URL 中提取主机和端口
    fn extract_host_port(url: &str) -> Result<(String, u16)> {
        let parsed = url::Url::parse(url)
            .map_err(|e| CoreError::InvalidParam(format!("invalid url: {e}")))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| CoreError::InvalidParam("url has no host".into()))?
            .to_string();
        let port = parsed.port_or_known_default().unwrap_or(80);
        Ok((host, port))
    }
}

impl Default for DnsMultiIpDiscoverer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MirrorDiscoverer for DnsMultiIpDiscoverer {
    fn protocol(&self) -> &str {
        "http"
    }

    fn name(&self) -> &str {
        "dns_multi_ip"
    }

    /// 解析域名的所有 IP，每个 IP 生成一个绑定了该 IP 的 HttpSource
    async fn discover(&self, source: &dyn DownloadSource) -> Result<Vec<Box<dyn DownloadSource>>> {
        // 只处理 HTTP 源
        let http_source = match source.as_any().downcast_ref::<HttpSource>() {
            Some(s) => s,
            None => return Ok(vec![]),
        };

        // 如果已经绑定了 IP，不再解析
        if http_source.bind_ip().is_some() {
            return Ok(vec![]);
        }

        let (host, port) = Self::extract_host_port(http_source.url())?;

        // DNS 解析，获取所有 IP
        let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(format!("{host}:{port}"))
            .await
            .map_err(|e| CoreError::Internal(format!("dns resolve {host}: {e}")))?
            .collect();

        if addrs.is_empty() {
            return Ok(vec![]);
        }

        // 去重 IP
        let ips: HashSet<IpAddr> = addrs.iter().map(|a| a.ip()).collect();

        // 每个 IP 生成一个绑定了该 IP 的 HttpSource
        let mut mirrors: Vec<Box<dyn DownloadSource>> = Vec::new();
        for ip in ips {
            // 跳过 IPv6 暂时（reqwest 的 resolve 对 IPv6 支持可能有问题）
            if ip.is_ipv6() {
                continue;
            }
            let mirror = HttpSource::with_bind_ip(http_source.url().to_string(), ip);
            mirrors.push(Box::new(mirror));
        }

        Ok(mirrors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_host_port() {
        let (host, port) =
            DnsMultiIpDiscoverer::extract_host_port("https://example.com:8443/file.iso").unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 8443);

        let (host, port) =
            DnsMultiIpDiscoverer::extract_host_port("http://example.com/file.iso").unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 80);
    }

    #[tokio::test]
    async fn test_discover_skips_already_bound() {
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let source = HttpSource::with_bind_ip("https://example.com/file.iso".into(), ip);
        let discoverer = DnsMultiIpDiscoverer::new();
        let result = discoverer.discover(&source).await.unwrap();
        assert!(result.is_empty());
    }
}
