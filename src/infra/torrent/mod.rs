//! BitTorrent 协议适配器
//!
//! 实现 domain 层的 DownloadSource / ChunkDownloader trait，
//! 支持磁力链接和 .torrent 文件。
//!
//! 基于 librqbit（纯 Rust BT 客户端库），支持 DHT、PEX、uTP。
//!
//! 注意：由于 BitTorrent 协议是 piece 级下载，不支持字节级分片，
//! 调度器会用单分片下载整个文件。

pub mod downloader;
pub mod source;

pub use downloader::TorrentChunkDownloader;
pub use source::{TorrentSource, TorrentSourceType};
