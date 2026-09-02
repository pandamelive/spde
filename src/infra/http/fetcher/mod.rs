//! HTTP Fetcher 模块
//!
//! 提供两种 HTTP 下载器：
//! - HttpRangeFetcher：支持 Range 请求的 HTTP 下载器（多连接分片）
//! - HttpStreamFetcher：不支持 Range 请求的 HTTP 流式下载器（单连接顺序）

pub mod range;
pub mod stream;

pub use range::HttpRangeFetcher;
pub use stream::HttpStreamFetcher;
