//! HTTP 协议适配器
//!
//! 实现 domain 层的 DownloadSource / ChunkDownloader / MirrorDiscoverer trait。

pub mod connection_pool;
pub mod downloader;
pub mod fetcher;
pub mod mirror;
pub mod source;

pub use connection_pool::{ConnectionPoolConfig, HttpConnectionPool};
pub use downloader::HttpChunkDownloader;
pub use fetcher::{HttpRangeFetcher, HttpStreamFetcher};
pub use mirror::dns_multi_ip::DnsMultiIpDiscoverer;
pub use mirror::url_rewrite::{UrlRewriteDiscoverer, UrlRewriteRule};
pub use source::HttpSource;
