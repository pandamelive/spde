//! HTTP 下载源
//!
//! 实现 [`pandanetos::domain::DownloadSource`] trait。
//! 支持绑定特定 IP（用于 DNS 多 IP 并发下载，突破单 IP 限速）。

use std::any::Any;
use std::net::IpAddr;

use pandanetos::domain::{DownloadSource, SourceCapabilities};

/// HTTP/HTTPS 下载源
#[derive(Debug, Clone)]
pub struct HttpSource {
    /// 下载 URL
    url: String,
    /// 绑定的特定 IP（用于 DNS 多 IP 并发，None = 用系统 DNS）
    bind_ip: Option<IpAddr>,
    /// 能力声明
    capabilities: SourceCapabilities,
}

impl HttpSource {
    /// 创建一个新的 HTTP 下载源
    pub fn new(url: String) -> Self {
        let capabilities = if url.starts_with("http://") || url.starts_with("https://") {
            SourceCapabilities {
                supports_range: true,
                supports_concurrent: true,
                supports_resume: true,
                max_concurrency: 64,
                chunk_size_range: Some((4 * 1024 * 1024, 64 * 1024 * 1024)),
                immutable: false,
            }
        } else {
            SourceCapabilities::default()
        };

        Self {
            url,
            bind_ip: None,
            capabilities,
        }
    }

    /// 创建绑定特定 IP 的 HTTP 下载源（用于 DNS 多 IP 并发）
    pub fn with_bind_ip(url: String, bind_ip: IpAddr) -> Self {
        let mut source = Self::new(url);
        source.bind_ip = Some(bind_ip);
        source
    }

    /// 获取 URL
    pub fn url(&self) -> &str {
        &self.url
    }

    /// 获取绑定的 IP
    pub fn bind_ip(&self) -> Option<IpAddr> {
        self.bind_ip
    }

    /// 是否为 HTTPS
    pub fn is_https(&self) -> bool {
        self.url.starts_with("https://")
    }
}

impl DownloadSource for HttpSource {
    fn protocol(&self) -> &str {
        if self.is_https() {
            "https"
        } else {
            "http"
        }
    }

    fn identifier(&self) -> String {
        match &self.bind_ip {
            Some(ip) => format!("{}#{}", self.url, ip),
            None => self.url.clone(),
        }
    }

    fn display_name(&self) -> String {
        match &self.bind_ip {
            Some(ip) => format!("{} (via {})", self.url, ip),
            None => self.url.clone(),
        }
    }

    fn capabilities(&self) -> SourceCapabilities {
        self.capabilities.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_source_identifier() {
        let source = HttpSource::new("https://example.com/file.iso".into());
        assert_eq!(source.protocol(), "https");
        assert_eq!(source.identifier(), "https://example.com/file.iso");
        assert!(source.capabilities().supports_range);
        assert_eq!(source.capabilities().max_concurrency, 64);
    }

    #[test]
    fn test_http_source_with_bind_ip() {
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let source = HttpSource::with_bind_ip("https://example.com/file.iso".into(), ip);
        assert_eq!(
            source.identifier(),
            "https://example.com/file.iso#192.168.1.1"
        );
        assert_eq!(source.bind_ip(), Some(ip));
        assert!(source.display_name().contains("via 192.168.1.1"));
    }
}
