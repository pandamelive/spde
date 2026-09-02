//! 本地文件协议适配器
//!
//! 实现 domain 层的 DownloadSource / ChunkDownloader trait，
//! 支持本地文件复制作为下载源（用于多路径副本并发读取）。
//!
//! 本地文件支持随机读取和多连接并发读取，适合 SSD 场景下的多线程复制。

pub mod downloader;
pub mod fetcher;
pub mod source;

pub use downloader::FileChunkDownloader;
pub use fetcher::LocalFileFetcher;
pub use source::FileSource;
