//! HTTP Range Fetcher（支持范围请求的 HTTP 下载器）
//!
//! 适用于支持 Range 请求的 HTTP/HTTPS 服务器。
//! 通过 Range 请求获取指定偏移和长度的数据块。

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use reqwest::Client;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};

use pandanetos::error::{CoreError, Result};

use crate::domain::chunk_fetcher::{ChunkFetcher, ChunkStats, SourceCapabilities};

/// HTTP Range Fetcher
#[derive(Debug, Clone)]
pub struct HttpRangeFetcher {
    /// 下载 URL
    url: String,
    /// HTTP 客户端（共享连接池）
    client: Arc<Client>,
    /// 超时时间（秒）
    timeout_secs: u64,
    /// 是否支持 Range（probe 后确定）
    supports_range: bool,
    /// 文件大小（probe 后确定）
    file_size: u64,
}

impl HttpRangeFetcher {
    /// 创建新的 HTTP Range Fetcher
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
            supports_range: false,
            file_size: 0,
        }
    }

    /// 发送 Range 请求
    async fn fetch_range(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<(u64, u64)> {
        let start = offset;
        let end = offset + length - 1;
        let range_header = format!("bytes={}-{}", start, end);

        debug!(
            url = %self.url,
            range = %range_header,
            "fetching HTTP range"
        );

        let response = self
            .client
            .get(&self.url)
            .header("Range", range_header)
            .send()
            .await
            .map_err(|e| CoreError::Network(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() && response.status() != 206 {
            return Err(CoreError::Network(format!(
                "HTTP status {}: {}",
                response.status(),
                response.status().canonical_reason().unwrap_or("Unknown")
            )));
        }

        let content_length = response.content_length().unwrap_or(length);
        let mut downloaded = 0u64;
        let mut stream = response.bytes_stream();

        use futures_util::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| CoreError::Network(format!("read error: {}", e)))?;
            writer
                .write_all(&chunk)
                .await
                .map_err(|e| CoreError::IO(format!("write error: {}", e)))?;
            downloaded += chunk.len() as u64;
        }

        writer
            .flush()
            .await
            .map_err(|e| CoreError::IO(format!("flush error: {}", e)))?;

        Ok((downloaded, content_length))
    }
}

#[async_trait]
impl ChunkFetcher for HttpRangeFetcher {
    fn protocol(&self) -> &'static str {
        if self.url.starts_with("https://") {
            "https"
        } else {
            "http"
        }
    }

    fn identifier(&self) -> String {
        format!("http:{}", self.url)
    }

    fn display_name(&self) -> String {
        self.url.clone()
    }

    async fn probe(&self) -> Result<(u64, SourceCapabilities)> {
        // 发送 HEAD 请求探测
        let response = self
            .client
            .head(&self.url)
            .send()
            .await
            .map_err(|e| CoreError::Network(format!("HEAD request failed: {}", e)))?;

        let mut file_size = response.content_length().unwrap_or(0);

        // 备用方案：如果 content_length() 返回 0，手动从响应头解析
        if file_size == 0 {
            if let Some(cl_header) = response.headers().get("content-length") {
                if let Ok(cl_str) = cl_header.to_str() {
                    if let Ok(parsed) = cl_str.parse::<u64>() {
                        file_size = parsed;
                    }
                }
            }
        }

        let accepts_ranges = response
            .headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("bytes"))
            .unwrap_or(false);

        // 如果 HEAD 不支持，尝试用 Range 请求探测
        let supports_range = if accepts_ranges {
            true
        } else if file_size > 0 {
            // 尝试发送一个小的 Range 请求
            let test_response = self
                .client
                .get(&self.url)
                .header("Range", "bytes=0-0")
                .send()
                .await;
            match test_response {
                Ok(r) => r.status() == 206,
                Err(_) => false,
            }
        } else {
            false
        };

        let capabilities = SourceCapabilities {
            supports_range,
            supports_multi_connection: supports_range,
            supports_resume: supports_range,
            immutable: false,
            max_concurrency: 16,
            chunk_size_range: Some((1 * 1024 * 1024, 16 * 1024 * 1024)),
            protocol: self.protocol(),
        };

        debug!(
            url = %self.url,
            file_size = file_size,
            supports_range = supports_range,
            "HTTP probe completed"
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

        let (downloaded, _) = self.fetch_range(offset, length, writer).await?;

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let speed_bps = if elapsed_ms > 0 {
            downloaded * 1000 / elapsed_ms
        } else {
            0
        };

        Ok(ChunkStats {
            chunk_id: 0, // 由调度器设置
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
