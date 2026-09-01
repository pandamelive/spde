//! BitTorrent Fetcher 模块
//!
//! 提供 BT 原生下载器：
//! - TorrentPieceFetcher：基于 librqbit 的 BT 原生下载器，支持磁力链接和 .torrent 文件

pub mod piece;

pub use piece::TorrentPieceFetcher;
