//! FTP 分片下载器
//!
//! 实现 `ChunkDownloader` trait，支持 FTP/FTPS 协议的分片下载。
//! 每个分片独立建立 FTP 连接，使用 REST 命令实现断点续传。
//! 支持多连接并发下载不同分片。

use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;
use suppaftp::{FtpStream, NativeTlsConnector};

use pandanetos::domain::{
    Chunk, ChunkDownloader, ChunkStats, CancellationToken, DownloadFileInfo, DownloadSource,
};

use super::source::FtpSource;

/// FTP 分片下载器
#[derive(Debug, Clone, Default)]
pub struct FtpChunkDownloader {
    /// 跳过 TLS 验证（FTPS 时使用）
    skip_tls_verify: bool,
    /// 连接超时（秒）
    timeout_secs: u64,
}

impl FtpChunkDownloader {
    /// 创建新的 FTP 分片下载器
    pub fn new(skip_tls_verify: bool, timeout_secs: u64) -> Self {
        Self {
            skip_tls_verify,
            timeout_secs,
        }
    }

    /// 建立 FTP 连接
    async fn connect(&self, source: &FtpSource) -> Result<FtpStream> {
        let addr = format!("{}:{}", source.host(), source.port());

        let ftp = if source.is_ftps() {
            // FTPS：使用 TLS 连接
            let connector = if self.skip_tls_verify {
                NativeTlsConnector::builder()
                    .danger_accept_invalid_certs(true)
                    .danger_accept_invalid_hostnames(true)
                    .build()
                    .context("failed to build TLS connector")?
            } else {
                NativeTlsConnector::builder()
                    .build()
                    .context("failed to build TLS connector")?
            };
            FtpStream::connect_secure_implicit(addr, connector)
                .await
                .context("failed to connect to FTPS server")?
        } else {
            // 普通 FTP
            FtpStream::connect(addr)
                .await
                .context("failed to connect to FTP server")?
        };

        // 登录
        let mut ftp = ftp
            .login(source.username(), source.password())
            .await
            .context("failed to login to FTP server")?;

        // 切换到被动模式（大多数 FTP 服务器需要）
        ftp.transfer_type(suppaftp::types::FileType::Binary)
            .await
            .context("failed to set binary transfer type")?;

        Ok(ftp)
    }
}

#[async_trait]
impl ChunkDownloader for FtpChunkDownloader {
    fn protocol(&self) -> &str {
        "ftp"
    }

    /// 探测 FTP 文件的可用性和信息
    async fn probe(&self, source: &dyn DownloadSource) -> Result<DownloadFileInfo> {
        let ftp_source = source
            .as_any()
            .downcast_ref::<FtpSource>()
            .context("source is not a FtpSource")?;

        let mut ftp = self.connect(ftp_source).await?;

        // 使用 SIZE 命令获取文件大小（大多数 FTP 服务器支持）
        let size = ftp.size(ftp_source.remote_path()).await?;

        // 关闭连接
        let _ = ftp.quit().await;

        Ok(DownloadFileInfo {
            size_bytes: size,
            supports_resume: true,
            supports_multi_connection: true,
        })
    }

    /// 下载一个分片（使用 REST 命令实现断点续传）
    async fn download_chunk(
        &self,
        source: &dyn DownloadSource,
        chunk: &Chunk,
        writer: &dyn pandanetos::domain::ChunkWriter,
        cancel: &CancellationToken,
    ) -> Result<ChunkStats> {
        let start = Instant::now();
        let ftp_source = source
            .as_any()
            .downcast_ref::<FtpSource>()
            .context("source is not a FtpSource")?;

        // 建立连接（每个分片独立连接，支持并发）
        let mut ftp = self.connect(ftp_source).await?;

        // 使用 REST 命令定位到分片偏移量
        ftp.rest(chunk.offset)
            .await
            .context("failed to set REST offset")?;

        // 下载分片数据（使用 RETR 命令，REST 已经定位到偏移量）
        let data = ftp
            .retr_as_buffer(ftp_source.remote_path())
            .await
            .context("failed to retrieve file from FTP")?;

        // 只取分片长度的数据（REST 已经定位到偏移量，data 的开头就是分片数据）
        let chunk_data = if data.len() >= chunk.length as usize {
            &data[..chunk.length as usize]
        } else {
            &data[..]
        };

        // 分块写入目标文件（避免大内存占用）
        let mut offset = chunk.offset;
        let mut remaining = chunk_data.len();
        let mut pos = 0;

        while remaining > 0 {
            // 检查取消
            if cancel.is_cancelled() {
                let _ = ftp.quit().await;
                anyhow::bail!("download cancelled");
            }

            let to_write = remaining.min(256 * 1024); // 256KB 块
            writer
                .write_chunk(chunk.chunk_id, offset, &chunk_data[pos..pos + to_write])
                .await
                .context("failed to write chunk")?;

            offset += to_write as u64;
            pos += to_write;
            remaining -= to_write;
        }

        // 关闭连接
        let _ = ftp.quit().await;

        let elapsed = start.elapsed().as_secs_f64();

        Ok(ChunkStats {
            chunk_id: chunk.chunk_id,
            source_id: source.identifier(),
            downloaded_bytes: chunk.length,
            elapsed_secs: elapsed,
            success: true,
            error_code: None,
        })
    }
}
