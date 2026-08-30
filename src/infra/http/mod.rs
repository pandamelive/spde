//! HTTP 协议适配器
//!
//! 实现 domain 层的 DownloadSource / ChunkDownloader / MirrorDiscoverer trait。

pub mod downloader;
pub mod mirror;
pub mod source;

pub use downloader::HttpChunkDownloader;
pub use source::HttpSource;
