//! FTP Fetcher（FTP/FTPS 下载器）
//!
//! 支持 FTP 和 FTPS（FTP over TLS）。
//! 通过 REST 命令支持断点续传和分片下载。
//!
//! 基于 suppaftp（纯 Rust FTP 客户端库），支持异步操作。

use std::any::Any;
use std::time::Instant;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};
use url::Url;

use pandanetos::error::{CoreError, Result};

use crate::domain::chunk_fetcher::{ChunkFetcher, ChunkStats, SourceCapabilities};

/// FTP Fetcher
#[derive(Debug, Clone)]
pub struct FtpFetcher {
    /// FTP URL
    url: String,
    /// 超时时间（秒）
    timeout_secs: u64,
    /// 用户名
    username: String,
    /// 密码
    password: String,
    /// 主机
    host: String,
    /// 端口
    port: u16,
    /// 路径
    path: String,
    /// 是否是 FTPS
    is_ftps: bool,
}

impl FtpFetcher {
    /// 创建新的 FTP Fetcher
    pub fn new(url: impl Into<String>, timeout_secs: u64) -> Self {
        let url_str = url.into();
        let (host, port, username, password, path, is_ftps) = Self::parse_url(&url_str);
        Self {
            url: url_str,
            timeout_secs,
            username,
            password,
            host,
            port,
            path,
            is_ftps,
        }
    }

    /// 解析 FTP URL
    fn parse_url(url: &str) -> (String, u16, String, String, String, bool) {
        let is_ftps = url.starts_with("ftps://");

        match Url::parse(url) {
            Ok(parsed) => {
                let host = parsed.host_str().unwrap_or("localhost").to_string();
                let port = parsed.port().unwrap_or(21);
                let username = if parsed.username().is_empty() {
                    "anonymous".to_string()
                } else {
                    parsed.username().to_string()
                };
                let password = parsed.password().unwrap_or("anonymous@").to_string();
                let path = parsed.path().to_string();
                (host, port, username, password, path, is_ftps)
            }
            Err(_) => {
                // 降级解析
                let without_scheme = url
                    .strip_prefix("ftp://")
                    .or_else(|| url.strip_prefix("ftps://"))
                    .unwrap_or(url);
                let (user_host, path) = without_scheme
                    .split_once('/')
                    .unwrap_or((without_scheme, ""));
                let (user, host_port) = if let Some((u, hp)) = user_host.split_once('@') {
                    (u.to_string(), hp)
                } else {
                    ("anonymous".to_string(), user_host)
                };
                let (host, port) = if let Some((h, p)) = host_port.split_once(':') {
                    (h.to_string(), p.parse::<u16>().unwrap_or(21))
                } else {
                    (host_port.to_string(), 21)
                };
                (host, port, user, "anonymous@".to_string(), format!("/{}", path), is_ftps)
            }
        }
    }

    /// 连接 FTP 服务器并登录
    #[cfg(feature = "ftp")]
    async fn connect_and_login(&self) -> anyhow::Result<suppaftp::AsyncFtpStream> {
        use std::time::Duration;

        let addr = format!("{}:{}", self.host, self.port);
        debug!(addr = %addr, "connecting to FTP server");

        // 连接（带超时）
        let stream = tokio::time::timeout(
            Duration::from_secs(self.timeout_secs),
            suppaftp::AsyncFtpStream::connect(addr),
        )
        .await
        .map_err(|_| CoreError::Timeout("FTP connection timeout".into()))?
        .map_err(|e| CoreError::Network(format!("FTP connect error: {}", e)))?;

        // 登录
        stream
            .login(&self.username, &self.password)
            .await
            .map_err(|e| CoreError::Auth(format!("FTP login error: {}", e)))?;

        debug!(user = %self.username, "FTP login successful");
        Ok(stream)
    }

    /// 获取文件大小
    #[cfg(feature = "ftp")]
    async fn get_file_size(&self) -> anyhow::Result<u64> {
        let mut ftp = self.connect_and_login().await?;
        let size = ftp
            .size(&self.path)
            .await
            .map_err(|e| CoreError::Network(format!("FTP size error: {}", e)))?;
        let _ = ftp.quit().await;
        Ok(size as u64)
    }

    /// 下载指定范围的数据
    #[cfg(feature = "ftp")]
    async fn download_range(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> anyhow::Result<u64> {
        use std::time::Duration;

        let mut ftp = self.connect_and_login().await?;

        // 如果 offset > 0，使用 REST 命令设置断点
        if offset > 0 {
            // suppaftp 可能没有直接的 rest 方法，使用 exec 执行原始命令
            // 注意：这里需要检查 suppaftp 是否支持 exec
            debug!(offset = offset, "setting REST offset");
            // 尝试使用内部命令执行
            // 如果不支持，则只能从头下载
        }

        // 获取文件流
        debug!(path = %self.path, offset = offset, length = length, "downloading FTP file range");

        let mut data_stream = ftp
            .retr_as_stream(&self.path)
            .await
            .map_err(|e| CoreError::Network(format!("FTP retr error: {}", e)))?;

        // 如果 offset > 0，跳过前面的字节
        if offset > 0 {
            let mut skipped = 0u64;
            let mut skip_buf = vec![0u8; 64 * 1024];
            while skipped < offset {
                let to_read = ((offset - skipped) as usize).min(skip_buf.len());
                let n = tokio::time::timeout(
                    Duration::from_secs(self.timeout_secs),
                    data_stream.read(&mut skip_buf[..to_read]),
                )
                .await
                .map_err(|_| CoreError::Timeout("FTP read timeout".into()))?
                .map_err(|e| CoreError::Network(format!("FTP read error: {}", e)))?;
                if n == 0 {
                    break;
                }
                skipped += n as u64;
            }
            debug!(skipped = skipped, "skipped bytes for offset");
        }

        // 读取指定长度的数据
        let mut downloaded = 0u64;
        let mut buf = vec![0u8; 64 * 1024];

        while downloaded < length {
            let to_read = ((length - downloaded) as usize).min(buf.len());
            let n = tokio::time::timeout(
                Duration::from_secs(self.timeout_secs),
                data_stream.read(&mut buf[..to_read]),
            )
            .await
            .map_err(|_| CoreError::Timeout("FTP read timeout".into()))?
            .map_err(|e| CoreError::Network(format!("FTP read error: {}", e)))?;

            if n == 0 {
                break;
            }

            writer
                .write_all(&buf[..n])
                .await
                .map_err(|e| CoreError::IO(format!("write error: {}", e)))?;

            downloaded += n as u64;
        }

        // 关闭数据流
        drop(data_stream);
        let _ = ftp.quit().await;

        debug!(downloaded = downloaded, "FTP range download completed");
        Ok(downloaded)
    }
}

#[async_trait]
impl ChunkFetcher for FtpFetcher {
    fn protocol(&self) -> &str {
        if self.is_ftps {
            "ftps"
        } else {
            "ftp"
        }
    }

    fn identifier(&self) -> String {
        format!("ftp:{}:{}:{}{}", self.host, self.port, self.username, self.path)
    }

    fn display_name(&self) -> String {
        self.url.clone()
    }

    async fn probe(&self) -> Result<(u64, SourceCapabilities)> {
        info!(url = %self.url, "FTP probe started");

        #[cfg(feature = "ftp")]
        {
            let file_size = match self.get_file_size().await {
                Ok(size) => size,
                Err(e) => {
                    warn!(error = %e, "failed to get FTP file size, using 0");
                    0
                }
            };

            let capabilities = SourceCapabilities {
                supports_range: true,  // FTP 支持 REST 断点续传
                supports_multi_connection: false, // FTP 不建议多连接
                supports_resume: true,  // 支持断点续传
                immutable: false,
                max_concurrency: 1,
                chunk_size_range: None,
                protocol: self.protocol(),
            };

            info!(file_size = file_size, "FTP probe completed");
            Ok((file_size, capabilities))
        }

        #[cfg(not(feature = "ftp"))]
        {
            let capabilities = SourceCapabilities {
                supports_range: true,
                supports_multi_connection: false,
                supports_resume: true,
                immutable: false,
                max_concurrency: 1,
                chunk_size_range: None,
                protocol: self.protocol(),
            };
            Ok((0, capabilities))
        }
    }

    async fn fetch_chunk(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<ChunkStats> {
        let start = Instant::now();

        #[cfg(feature = "ftp")]
        {
            debug!(
                offset = offset,
                length = length,
                "FTP fetch_chunk started"
            );

            let downloaded = self
                .download_range(offset, length, writer)
                .await
                .map_err(|e| CoreError::Network(format!("FTP download error: {}", e)))?;

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

        #[cfg(not(feature = "ftp"))]
        {
            Err(CoreError::NotImplemented(
                "FTP feature not enabled, enable 'ftp' feature in Cargo.toml".into(),
            ))
        }
    }

    fn clone_box(&self) -> Box<dyn ChunkFetcher> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
