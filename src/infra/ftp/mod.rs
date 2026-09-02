//! FTP/FTPS 协议适配器
//!
//! 实现 domain 层的 DownloadSource / ChunkDownloader trait，
//! 支持 FTP 和 FTPS 协议的分片下载和断点续传。
//!
//! 每个分片独立建立 FTP 连接，使用 REST 命令实现断点续传，
//! 支持多连接并发下载不同分片。

pub mod downloader;
pub mod fetcher;
pub mod source;

pub use downloader::FtpChunkDownloader;
pub use fetcher::FtpFetcher;
pub use source::FtpSource;
