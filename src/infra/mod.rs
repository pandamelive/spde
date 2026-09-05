//! 基础设施层
//!
//! 各协议适配器（HTTP/FTP/SFTP/BT/本地文件）和磁盘IO实现。
//! 实现 domain 层定义的端口 trait。

pub mod disk;
pub mod file;
pub mod ftp;
pub mod http;
pub mod ssh;
pub mod torrent;
pub mod pdc_client;
pub mod pk_client;

pub use disk::file_writer::FileChunkWriter;
pub use file::downloader::FileChunkDownloader;
pub use file::source::FileSource;
pub use ftp::downloader::FtpChunkDownloader;
pub use ftp::source::FtpSource;
pub use http::downloader::HttpChunkDownloader;
pub use http::mirror::dns::DnsMultiIpDiscoverer;
pub use http::source::HttpSource;
pub use ssh::downloader::SshChunkDownloader;
pub use ssh::source::SshSource;
pub use torrent::downloader::TorrentChunkDownloader;
pub use torrent::source::TorrentSource;
