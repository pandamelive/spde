//! HTTP 镜像发现器
//!
//! 实现 domain 层的 MirrorDiscoverer trait，从原始 HTTP URL 发现更多可用镜像。

pub mod dns;
pub mod dns_multi_ip;
pub mod url_rewrite;
pub mod url_rule;

pub use dns::DnsMultiIpDiscoverer as OldDnsMultiIpDiscoverer;
pub use dns_multi_ip::DnsMultiIpDiscoverer;
pub use url_rewrite::{UrlRewriteDiscoverer, UrlRewriteRule};
pub use url_rule::{MirrorRuleConfig, UrlRule, UrlRuleDiscoverer};
