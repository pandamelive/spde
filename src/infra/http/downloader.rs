//! HTTP 分片下载器
//!
//! 实现 [`pandanetos::domain::ChunkDownloader`] trait。
//! 支持 HTTP/HTTPS、Range 请求、绑定特定 IP（DNS 多 IP 并发）、
//! 连接池复用、流式写入。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::StreamExt;
use pandanetos::domain::{
    CancellationToken, Chunk, ChunkDownloader, ChunkStats, DownloadFileInfo, DownloadSource,
};
use pandanetos::error::{codes, CoreError, Result};
use reqwest::Client;
use tokio::sync::Mutex;

use super::source::HttpSource;

/// HTTP 分片下载器
pub struct HttpChunkDownloader {
    /// 按源缓存的 reqwest Client（每个 bind_ip 一个独立 client）
    clients: Mutex<HashMap<String, Client>>,
    /// 是否跳过 TLS 验证
    skip_tls_verify: bool,
    /// 总超时
    timeout: Duration,
    /// 连接超时
    connect_timeout: Duration,
}

impl HttpChunkDownloader {
    /// 创建一个新的 HTTP 分片下载器
    pub fn new(skip_tls_verify: bool, timeout_secs: u64) -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            skip_tls_verify,
            timeout: Duration::from_secs(timeout_secs.max(1)),
            connect_timeout: Duration::from_secs(30),
        }
    }

    /// 获取或创建指定源的 reqwest Client
    ///
    /// 每个 bind_ip 需要独立的 client，因为 IP 绑定是在 client 构建时设置的。
    async fn get_client(&self, source: &HttpSource) -> Result<Client> {
        let key = source.identifier();
        let mut clients = self.clients.lock().await;

        if let Some(client) = clients.get(&key) {
            return Ok(client.clone());
        }

        let mut builder = Client::builder()
            .timeout(self.timeout)
            .connect_timeout(self.connect_timeout)
            .tcp_nodelay(true)
            .pool_max_idle_per_host(32)
            .pool_idle_timeout(Duration::from_secs(300))
            .user_agent("spde/1.0 (pandanetos downloader)");

        if self.skip_tls_verify {
            builder = builder.danger_accept_invalid_certs(true);
        }

        // 绑定特定 IP（用于 DNS 多 IP 并发下载）
        if let Some(ip) = source.bind_ip() {
            let parsed = url::Url::parse(source.url())
                .map_err(|e| CoreError::InvalidParam(format!("invalid url: {e}")))?;
            let host = parsed
                .host_str()
                .ok_or_else(|| CoreError::InvalidParam("url has no host".into()))?
                .to_string();
            let port = parsed.port_or_known_default().unwrap_or(80);
            let addr = std::net::SocketAddr::new(ip, port);
            builder = builder.resolve(&host, addr);
        }

        let client = builder
            .build()
            .map_err(|e| CoreError::Internal(format!("build client: {e}")))?;

        clients.insert(key, client.clone());
        Ok(client)
    }

    /// 从 source 向下转型为 HttpSource
    fn as_http_source<'a>(&self, source: &'a dyn DownloadSource) -> Result<&'a HttpSource> {
        source.as_any().downcast_ref::<HttpSource>().ok_or_else(|| {
            CoreError::InvalidParam(format!("expected HttpSource, got {}", source.protocol()))
        })
    }
}

#[async_trait]
impl ChunkDownloader for HttpChunkDownloader {
    fn protocol(&self) -> &str {
        "http"
    }

    /// 探测源的可用性和文件信息（HEAD 请求）
    async fn probe(&self, source: &dyn DownloadSource) -> Result<DownloadFileInfo> {
        let http_source = self.as_http_source(source)?;
        let client = self.get_client(http_source).await?;

        let resp = client.head(http_source.url()).send().await.map_err(|e| {
            CoreError::Internal(format!("{}: {}", codes::DOWNLOAD_CONNECTION_FAILED, e))
        })?;

        if !resp.status().is_success() {
            return Err(CoreError::Internal(format!(
                "probe failed: HTTP {}",
                resp.status()
            )));
        }

        // 注意：不能用 resp.content_length()，因为对 HEAD 请求它返回 0
        // （HEAD 没有响应体），必须直接从 Content-Length 头读取资源大小
        let size_bytes = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let supports_resume = resp
            .headers()
            .get("accept-ranges")
            .map(|v| v == "bytes")
            .unwrap_or(false);

        // 支持 Range 就支持多连接
        let supports_multi_connection = supports_resume && size_bytes > 0;

        Ok(DownloadFileInfo {
            size_bytes,
            supports_resume,
            supports_multi_connection,
        })
    }

    /// 下载一个分片（Range 请求，流式写入）
    async fn download_chunk(
        &self,
        source: &dyn DownloadSource,
        chunk: &Chunk,
        writer: &dyn pandanetos::domain::ChunkWriter,
        cancel: &CancellationToken,
    ) -> Result<ChunkStats> {
        let http_source = self.as_http_source(source)?;
        let client = self.get_client(http_source).await?;
        let start = Instant::now();

        // 构建 Range 头
        let range_end = chunk.offset + chunk.length - 1;
        let range_header = format!("bytes={}-{}", chunk.offset, range_end);

        let resp = client
            .get(http_source.url())
            .header("Range", &range_header)
            .send()
            .await
            .map_err(|e| {
                CoreError::Internal(format!("{}: {}", codes::DOWNLOAD_CONNECTION_FAILED, e))
            })?;

        let status = resp.status();
        // 必须是 206 Partial Content 才说明服务器接受了 Range 请求
        // 如果返回 200 OK，说明服务器忽略了 Range 头，返回了整个文件
        if status != reqwest::StatusCode::PARTIAL_CONTENT {
            let error_code = if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
                codes::DOWNLOAD_RANGE_NOT_SATISFIED
            } else if status == reqwest::StatusCode::REQUEST_TIMEOUT {
                codes::DOWNLOAD_TIMEOUT
            } else if !status.is_success() {
                codes::DOWNLOAD_CONNECTION_FAILED
            } else {
                // 200 OK 但不是 206：服务器不支持 Range
                codes::DOWNLOAD_RANGE_NOT_SATISFIED
            };
            return Err(CoreError::Internal(format!(
                "{}: expected 206 Partial Content, got HTTP {} (server may not support Range)",
                error_code, status
            )));
        }

        // 流式读取响应体，写入 writer
        let mut downloaded: u64 = 0;
        let mut stream = resp.bytes_stream();

        while let Some(chunk_result) = stream.next().await {
            // 检查取消
            if cancel.is_cancelled() {
                return Ok(ChunkStats {
                    chunk_id: chunk.chunk_id,
                    source_id: source.identifier(),
                    downloaded_bytes: downloaded,
                    elapsed_secs: start.elapsed().as_secs_f64(),
                    success: false,
                    error_code: Some(codes::INTERNAL_ERROR),
                });
            }

            let data =
                chunk_result.map_err(|e| CoreError::Internal(format!("read stream: {e}")))?;

            if !data.is_empty() {
                let write_offset = chunk.offset + downloaded;
                writer.write_at(write_offset, &data).await.map_err(|e| {
                    CoreError::Internal(format!("{}: {}", codes::DOWNLOAD_DISK_FULL, e))
                })?;
                downloaded += data.len() as u64;
            }
        }

        let elapsed = start.elapsed().as_secs_f64();
        let success = downloaded == chunk.length;

        Ok(ChunkStats {
            chunk_id: chunk.chunk_id,
            source_id: source.identifier(),
            downloaded_bytes: downloaded,
            elapsed_secs: elapsed,
            success,
            error_code: if success {
                None
            } else {
                Some(codes::DOWNLOAD_PARTIAL_CONTENT)
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol() {
        let downloader = HttpChunkDownloader::new(false, 30);
        assert_eq!(downloader.protocol(), "http");
    }

    #[test]
    fn test_as_http_source() {
        let downloader = HttpChunkDownloader::new(false, 30);
        let source = HttpSource::new("https://example.com/file.iso".into());
        let result = downloader.as_http_source(&source);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().url(), "https://example.com/file.iso");
    }
}
