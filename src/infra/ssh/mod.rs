//! SSH/SFTP/SCP 协议适配器
//!
//! 实现 domain 层的 DownloadSource / ChunkDownloader trait，
//! 支持 sftp://、scp://、ssh:// 协议。
//!
//! 内部调用系统自带的 sftp/scp 命令，无需额外编译依赖。
//!
//! 注意：由于通过系统命令实现，不支持分片下载和多连接并发，
//! 调度器会用单分片下载整个文件。

pub mod downloader;
pub mod fetcher;
pub mod source;

pub use downloader::SshChunkDownloader;
pub use fetcher::SftpFetcher;
pub use source::SshSource;
