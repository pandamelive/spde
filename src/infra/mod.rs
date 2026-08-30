//! 基础设施层
//!
//! 各协议适配器（HTTP/FTP/SFTP/BT/本地文件）和磁盘IO实现。
//! 实现 domain 层定义的端口 trait。

pub mod disk;
pub mod file;
pub mod http;

pub use disk::file_writer::FileChunkWriter;
pub use http::downloader::HttpChunkDownloader;
pub use http::mirror::dns::DnsMultiIpDiscoverer;
pub use http::source::HttpSource;
