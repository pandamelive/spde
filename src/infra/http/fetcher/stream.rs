//! HTTP Stream Fetcher（不支持范围请求的 HTTP 流式下载器）
//!
//! 适用于不支持 Range 请求的 HTTP/HTTPS 服务器。
//! 只能从头开始顺序下载，通过跳过 offset 前的字节来模拟分片。
//!
//! 注意：由于不支持 Range，多连接并发没有意义，建议单连接下载。

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use reqwest::Client;
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

use pandanetos::error::{CoreError, Result};

use crate::domain::chunk_fetcher::{ChunkFetcher, ChunkStats, SourceCapabilities};

/// HTTP Stream Fetcher
#[derive(Debug, Clone)]
pub struct HttpStreamFetcher {
    /// 下载 URL
    url: String,
    /// HTTP 客户端（共享连接池）
    client: Arc<Client>,
    /// 超时时间（秒）
    timeout_secs: u64,
    /// 文件大小（probe 后确定，可能为 0 表示未知）
    file_size: u64,
}

impl HttpStreamFetcher {
    /// 创建新的 HTTP Stream Fetcher
    pub fn new(url: impl Into<String>, timeout_secs: u64) -> Self {
        let client = Arc::new(
            Client::builder()
                .timeout(std::time::Duration::from_secs(timeout_secs))
                .connect_timeout(std::time::Duration::from_secs(10))
                .tcp_keepalive(std::time::Duration::from_secs(30))
                .build()
                .expect("failed to build HTTP client"),
        );

        Self {
            url: url.into(),
            client,
            timeout_secs,
            file_size: 0,
        }
    }

    /// 流式下载，跳过 offset 前的字节，写入 length 字节
    async fn stream_download(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<u64> {
        debug!(
            url = %self.url,
            offset = offset,
            length = length,
            "streaming download (no range support)"
        );

        let response = self
            .client
            .get(&self.url)
            .send()
            .await
            .map_err(|e| CoreError::Network(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(CoreError::Network(format!(
                "HTTP status {}: {}",
                response.status(),
                response.status().canonical_reason().unwrap_or("Unknown")
            )));
        }

        let mut stream = response.bytes_stream();
        let mut bytes_to_skip = offset;
        let mut bytes_to_write = length;
        let mut written = 0u64;

        use futures_util::StreamExt;

        // 跳过 offset 前的字节
        while bytes_to_skip > 0 {
            match stream.next().await {
                Some(Ok(chunk)) => {
                    let chunk_len = chunk.len() as u64;
                    if chunk_len <= bytes_to_skip {
                        bytes_to_skip -= chunk_len;
                    } else {
                        // 部分跳过，剩余的写入
                        let start = bytes_to_skip as usize;
                        let to_write = &chunk[start..];
                        let write_len = (to_write.len() as u64).min(bytes_to_write);
                        writer
                            .write_all(&to_write[..write_len as usize])
                            .await
                            .map_err(|e| CoreError::IO(format!("write error: {}", e)))?;
                        written += write_len;
                        bytes_to_write -= write_len;
                        bytes_to_skip = 0;
                    }
                }
                Some(Err(e)) => {
                    return Err(CoreError::Network(format!("read error: {}", e)));
                }
                None => {
                    // 流结束，但还没跳过足够的字节
                    warn!(
                        offset = offset,
                        skipped = offset - bytes_to_skip,
                        "stream ended before skipping offset"
                    );
                    return Ok(written);
                }
            }
        }

        // 写入 length 字节
        while bytes_to_write > 0 {
            match stream.next().await {
                Some(Ok(chunk)) => {
                    let write_len = (chunk.len() as u64).min(bytes_to_write);
                    writer
                        .write_all(&chunk[..write_len as usize])
                        .await
                        .map_err(|e| CoreError::IO(format!("write error: {}", e)))?;
                    written += write_len;
                    bytes_to_write -= write_len;
                }
                Some(Err(e)) => {
                    return Err(CoreError::Network(format!("read error: {}", e)));
                }
                None => {
                    // 流结束
                    break;
                }
            }
        }

        writer
            .flush()
            .await
            .map_err(|e| CoreError::IO(format!("flush error: {}", e)))?;

        Ok(written)
    }
}

#[async_trait]
impl ChunkFetcher for HttpStreamFetcher {
    fn protocol(&self) -> &'static str {
        if self.url.starts_with("https://") {
            "https"
        } else {
            "http"
        }
    }

    fn identifier(&self) -> String {
        format!("http-stream:{}", self.url)
    }

    fn display_name(&self) -> String {
        format!("{} (stream)", self.url)
    }

    async fn probe(&self) -> Result<(u64, SourceCapabilities)> {
        // 发送 HEAD 请求探测
        let response = self
            .client
            .head(&self.url)
            .send()
            .await
            .map_err(|e| CoreError::Network(format!("HEAD request failed: {}", e)))?;

        let file_size = response.content_length().unwrap_or(0);

        // 明确不支持 Range
        let capabilities = SourceCapabilities {
            supports_range: false,
            supports_multi_connection: false, // 不支持多连接并发
            supports_resume: false,           // 不支持断点续传
            immutable: false,
            max_concurrency: 1,     // 只能单连接
            chunk_size_range: None, // 无特殊要求（调度器会用单分片）
            protocol: self.protocol(),
        };

        debug!(
            url = %self.url,
            file_size = file_size,
            "HTTP stream probe completed (no range support)"
        );

        Ok((file_size, capabilities))
    }

    async fn fetch_chunk(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<ChunkStats> {
        let start = Instant::now();

        let downloaded = self.stream_download(offset, length, writer).await?;

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let speed_bps = if elapsed_ms > 0 {
            downloaded * 1000 / elapsed_ms
        } else {
            0
        };

        Ok(ChunkStats {
            chunk_id: 0,
            bytes_downloaded: downloaded,
            elapsed_ms,
            speed_bps,
            from_cache: false,
            source_id: self.identifier(),
        })
    }

    fn clone_box(&self) -> Box<dyn ChunkFetcher> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
