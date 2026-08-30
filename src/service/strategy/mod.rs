//! 下载策略
//!
//! 可插拔的下载策略实现。不同场景用不同策略：
//! - MultiSourceChunked：多源并发分片下载（默认，最通用）
//! - SingleSourceFastest：单源最快下载（不支持分片时用）

pub mod multi_source_chunked;

pub use multi_source_chunked::MultiSourceChunkedStrategy;
