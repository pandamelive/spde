//! SSH/SFTP/SCP 分片下载器
//!
//! 实现 `ChunkDownloader` trait，支持 sftp://、scp://、ssh:// 协议。
//! 内部调用系统自带的 sftp/scp 命令，无需额外编译依赖。
//!
//! 注意：由于通过系统命令实现，不支持分片下载和多连接并发，
//! 调度器会用单分片下载整个文件（chunk_size = file_size）。

use std::path::Path;
use std::process::Command;
use std::time::Instant;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use pandanetos::domain::{
    CancellationToken, Chunk, ChunkDownloader, ChunkStats, DownloadFileInfo, DownloadSource,
};
use pandanetos::error::{CoreError, Result};

use super::source::SshSource;

/// SSH/SFTP/SCP 分片下载器
#[derive(Debug, Clone, Default)]
pub struct SshChunkDownloader {
    /// 连接超时（秒）
    timeout_secs: u64,
}

impl SshChunkDownloader {
    /// 创建新的 SSH 分片下载器
    pub fn new(timeout_secs: u64) -> Self {
        Self { timeout_secs }
    }

    /// 构建 sftp 下载命令
    fn build_sftp_command(source: &SshSource, local_path: &Path, timeout_secs: u64) -> Command {
        let mut cmd = Command::new("sftp");
        cmd.arg("-P").arg(source.port().to_string());
        cmd.arg("-o").arg("StrictHostKeyChecking=no");
        cmd.arg("-o")
            .arg(format!("ConnectTimeout={}", timeout_secs.max(1)));
        cmd.arg(format!(
            "{}@{}:{}",
            source.username(),
            source.host(),
            source.remote_path()
        ));
        cmd.arg(local_path);
        cmd
    }

    /// 构建 scp 下载命令
    fn build_scp_command(source: &SshSource, local_path: &Path, timeout_secs: u64) -> Command {
        let mut cmd = Command::new("scp");
        cmd.arg("-P").arg(source.port().to_string());
        cmd.arg("-o").arg("StrictHostKeyChecking=no");
        cmd.arg("-o")
            .arg(format!("ConnectTimeout={}", timeout_secs.max(1)));
        cmd.arg(format!(
            "{}@{}:{}",
            source.username(),
            source.host(),
            source.remote_path()
        ));
        cmd.arg(local_path);
        cmd
    }
}

#[async_trait]
impl ChunkDownloader for SshChunkDownloader {
    fn protocol(&self) -> &str {
        "ssh"
    }

    /// 探测 SSH 文件的可用性和信息
    ///
    /// 通过 sftp 的 ls 命令获取文件大小。如果 sftp 命令不可用，返回默认值。
    async fn probe(&self, source: &dyn DownloadSource) -> Result<DownloadFileInfo> {
        let ssh_source = source
            .as_any()
            .downcast_ref::<SshSource>()
            .context("source is not a SshSource")?;

        let source_clone = ssh_source.clone();
        let timeout_secs = self.timeout_secs;

        // 同步探测放在 spawn_blocking 中
        let size = tokio::task::spawn_blocking(move || -> u64 {
            // 尝试用 sftp 的 ls 命令获取文件大小
            let output = Command::new("sftp")
                .arg("-P")
                .arg(source_clone.port().to_string())
                .arg("-o")
                .arg("StrictHostKeyChecking=no")
                .arg("-o")
                .arg("BatchMode=yes")
                .arg("-o")
                .arg(format!("ConnectTimeout={}", timeout_secs.max(1)))
                .arg(format!(
                    "{}@{}:{}",
                    source_clone.username(),
                    source_clone.host(),
                    source_clone.remote_path()
                ))
                .arg("-ls")
                .output();

            match output {
                Ok(out) if out.status.success() => {
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    // 解析 ls 输出，提取文件大小
                    for line in stdout.lines() {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 5 {
                            if let Ok(size) = parts[4].parse::<u64>() {
                                return size;
                            }
                        }
                    }
                    0
                }
                _ => 0,
            }
        })
        .await
        .unwrap_or(0);

        Ok(DownloadFileInfo {
            size_bytes: size,
            supports_resume: false,
            supports_multi_connection: false,
        })
    }

    /// 下载整个文件（系统命令实现，不支持分片）
    async fn download_chunk(
        &self,
        source: &dyn DownloadSource,
        chunk: &Chunk,
        writer: &dyn pandanetos::domain::ChunkWriter,
        cancel: &CancellationToken,
    ) -> Result<ChunkStats> {
        let start = Instant::now();
        let ssh_source = source
            .as_any()
            .downcast_ref::<SshSource>()
            .context("source is not a SshSource")?;

        // 检查取消
        if cancel.is_cancelled() {
            return Err(CoreError::External(anyhow!("download cancelled")));
        }

        // 创建临时文件用于下载
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!(
            "spde_ssh_{}_{}.tmp",
            chunk.chunk_id,
            uuid::Uuid::new_v4()
        ));

        let source_clone = ssh_source.clone();
        let temp_file_clone = temp_file.clone();
        let timeout_secs = self.timeout_secs;

        // 同步下载放在 spawn_blocking 中
        let download_result = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // 根据协议类型选择下载命令
            let mut cmd = if source_clone.scheme() == "scp" {
                Self::build_scp_command(&source_clone, &temp_file_clone, timeout_secs)
            } else {
                Self::build_sftp_command(&source_clone, &temp_file_clone, timeout_secs)
            };

            let output = cmd
                .output()
                .context("failed to execute ssh/sftp/scp command")?;

            if !output.status.success() {
                return Err(anyhow!(
                    "ssh/sftp/scp command failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }

            Ok(())
        })
        .await
        .context("SSH download task panicked")?;

        // 检查取消
        if cancel.is_cancelled() {
            let _ = std::fs::remove_file(&temp_file);
            return Err(CoreError::External(anyhow!("download cancelled")));
        }

        download_result.map_err(|e| CoreError::External(anyhow!("SSH download failed: {e}")))?;

        // 读取下载的文件
        let data = std::fs::read(&temp_file).context("failed to read downloaded file")?;

        // 清理临时文件
        let _ = std::fs::remove_file(&temp_file);

        // 分块写入目标文件（从 chunk.offset 开始）
        let mut offset = chunk.offset;
        let mut remaining = data.len();
        let mut pos = 0;

        while remaining > 0 {
            if cancel.is_cancelled() {
                return Err(CoreError::External(anyhow!("download cancelled")));
            }

            let to_write = remaining.min(256 * 1024); // 256KB 块
            writer
                .write_at(offset, &data[pos..pos + to_write])
                .await
                .context("failed to write chunk")?;

            offset += to_write as u64;
            pos += to_write;
            remaining -= to_write;
        }

        let elapsed = start.elapsed().as_secs_f64();
        Ok(ChunkStats {
            chunk_id: chunk.chunk_id,
            source_id: source.identifier(),
            downloaded_bytes: data.len() as u64,
            elapsed_secs: elapsed,
            success: true,
            error_code: None,
        })
    }
}
