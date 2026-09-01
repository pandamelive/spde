//! SFTP Fetcher（SSH/SFTP/SCP 下载器）
//!
//! 支持 SSH/SFTP/SCP 协议。
//! 通过系统命令（sftp/scp/ssh）实现，无需额外编译依赖。
//!
//! 支持断点续传（通过 dd 命令 seek）和分片下载。

use std::any::Any;
use std::process::Stdio;
use std::time::Instant;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tracing::{debug, info, warn};
use url::Url;

use pandanetos::error::{CoreError, Result};

use crate::domain::chunk_fetcher::{ChunkFetcher, ChunkStats, SourceCapabilities};

/// SFTP Fetcher
#[derive(Debug, Clone)]
pub struct SftpFetcher {
    /// 原始 URL
    url: String,
    /// 超时时间（秒）
    timeout_secs: u64,
    /// 主机
    host: String,
    /// 端口
    port: u16,
    /// 用户名
    username: String,
    /// 路径
    path: String,
    /// 协议类型（ssh/sftp/scp）
    protocol_type: String,
}

impl SftpFetcher {
    /// 创建新的 SFTP Fetcher
    pub fn new(url: impl Into<String>, timeout_secs: u64) -> Self {
        let url_str = url.into();
        let (host, port, username, path, protocol_type) = Self::parse_url(&url_str);
        Self {
            url: url_str,
            timeout_secs,
            host,
            port,
            username,
            path,
            protocol_type,
        }
    }

    /// 解析 SSH URL
    fn parse_url(url: &str) -> (String, u16, String, String, String) {
        let protocol_type = if url.starts_with("sftp://") {
            "sftp"
        } else if url.starts_with("scp://") {
            "scp"
        } else {
            "ssh"
        };

        match Url::parse(url) {
            Ok(parsed) => {
                let host = parsed.host_str().unwrap_or("localhost").to_string();
                let port = parsed.port().unwrap_or(22);
                let username = if parsed.username().is_empty() {
                    whoami::username()
                } else {
                    parsed.username().to_string()
                };
                let path = parsed.path().to_string();
                (host, port, username, path, protocol_type.to_string())
            }
            Err(_) => {
                // 降级解析
                let without_scheme = url
                    .strip_prefix("ssh://")
                    .or_else(|| url.strip_prefix("sftp://"))
                    .or_else(|| url.strip_prefix("scp://"))
                    .unwrap_or(url);
                let (user_host, path) = without_scheme
                    .split_once('/')
                    .unwrap_or((without_scheme, ""));
                let (user, host_port) = if let Some((u, hp)) = user_host.split_once('@') {
                    (u.to_string(), hp)
                } else {
                    (whoami::username(), user_host)
                };
                let (host, port) = if let Some((h, p)) = host_port.split_once(':') {
                    (h.to_string(), p.parse::<u16>().unwrap_or(22))
                } else {
                    (host_port.to_string(), 22)
                };
                (host, port, user, format!("/{}", path), protocol_type.to_string())
            }
        }
    }

    /// 构建 SSH 基础参数
    fn ssh_base_args(&self) -> Vec<String> {
        vec![
            "-o".to_string(),
            "StrictHostKeyChecking=no".to_string(),
            "-o".to_string(),
            "UserKnownHostsFile=/dev/null".to_string(),
            "-o".to_string(),
            "LogLevel=ERROR".to_string(),
            "-p".to_string(),
            self.port.to_string(),
        ]
    }

    /// 获取文件大小（通过 ssh + stat 命令）
    async fn get_file_size(&self) -> anyhow::Result<u64> {
        use std::time::Duration;

        let ssh_args = self.ssh_base_args();
        let remote_cmd = format!("stat -c %s '{}' 2>/dev/null || wc -c < '{}'", self.path, self.path);

        debug!(host = %self.host, cmd = %remote_cmd, "getting file size via SSH");

        let output = tokio::time::timeout(
            Duration::from_secs(self.timeout_secs),
            Command::new("ssh")
                .args(&ssh_args)
                .arg(format!("{}@{}", self.username, self.host))
                .arg(&remote_cmd)
                .output(),
        )
        .await
        .map_err(|_| CoreError::Timeout("SSH stat timeout".into()))?
        .map_err(|e| CoreError::Network(format!("SSH command error: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CoreError::Network(format!("SSH stat failed: {}", stderr)).into());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let size: u64 = stdout
            .trim()
            .parse()
            .map_err(|e| CoreError::InvalidParam(format!("invalid file size: {}", e)))?;

        debug!(size = size, "file size obtained");
        Ok(size)
    }

    /// 下载指定范围的数据（通过 ssh + dd 命令）
    async fn download_range(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> anyhow::Result<u64> {
        use std::time::Duration;

        let ssh_args = self.ssh_base_args();

        // 使用 dd 命令读取指定范围
        // dd if=path bs=1 skip=offset count=length 2>/dev/null
        let remote_cmd = format!(
            "dd if='{}' bs=1 skip={} count={} 2>/dev/null",
            self.path, offset, length
        );

        debug!(
            host = %self.host,
            offset = offset,
            length = length,
            "downloading range via SSH dd"
        );

        let mut child = Command::new("ssh")
            .args(&ssh_args)
            .arg(format!("{}@{}", self.username, self.host))
            .arg(&remote_cmd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| CoreError::Network(format!("failed to spawn ssh: {}", e)))?;

        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| CoreError::Network("failed to capture stdout".into()))?;

        // 读取数据并写入 writer
        let mut downloaded = 0u64;
        let mut buf = vec![0u8; 64 * 1024];

        while downloaded < length {
            let to_read = ((length - downloaded) as usize).min(buf.len());
            let n = tokio::time::timeout(
                Duration::from_secs(self.timeout_secs),
                stdout.read(&mut buf[..to_read]),
            )
            .await
            .map_err(|_| CoreError::Timeout("SSH read timeout".into()))?
            .map_err(|e| CoreError::Network(format!("SSH read error: {}", e)))?;

            if n == 0 {
                break;
            }

            writer
                .write_all(&buf[..n])
                .await
                .map_err(|e| CoreError::IO(format!("write error: {}", e)))?;

            downloaded += n as u64;
        }

        // 等待进程结束
        let _ = child.wait().await;

        debug!(downloaded = downloaded, "SSH range download completed");
        Ok(downloaded)
    }
}

#[async_trait]
impl ChunkFetcher for SftpFetcher {
    fn protocol(&self) -> &str {
        &self.protocol_type
    }

    fn identifier(&self) -> String {
        format!("{}:{}@{}:{}{}", self.protocol_type, self.username, self.host, self.port, self.path)
    }

    fn display_name(&self) -> String {
        self.url.clone()
    }

    async fn probe(&self) -> Result<(u64, SourceCapabilities)> {
        info!(url = %self.url, "SFTP probe started");

        let file_size = match self.get_file_size().await {
            Ok(size) => size,
            Err(e) => {
                warn!(error = %e, "failed to get SFTP file size, using 0");
                0
            }
        };

        let capabilities = SourceCapabilities {
            supports_range: true,  // SFTP 支持 seek
            supports_multi_connection: false, // 不建议多连接
            supports_resume: true,  // 支持断点续传
            immutable: false,
            max_concurrency: 1,
            chunk_size_range: None,
            protocol: self.protocol(),
        };

        info!(file_size = file_size, "SFTP probe completed");
        Ok((file_size, capabilities))
    }

    async fn fetch_chunk(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<ChunkStats> {
        let start = Instant::now();

        debug!(
            offset = offset,
            length = length,
            "SFTP fetch_chunk started"
        );

        let downloaded = self
            .download_range(offset, length, writer)
            .await
            .map_err(|e| CoreError::Network(format!("SFTP download error: {}", e)))?;

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
