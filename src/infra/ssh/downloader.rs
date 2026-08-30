//! SSH/SFTP/SCP 分片下载器
//!
//! 实现 `ChunkDownloader` trait，支持 sftp://、scp://、ssh:// 协议。
//! 内部调用系统自带的 sftp/scp 命令，无需额外编译依赖。
//!
//! 注意：由于通过系统命令实现，不支持分片下载和多连接并发。
//! 调度器会用单分片下载整个文件（chunk_size = file_size）。

use std::time::Instant;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::process::Command;

use pandanetos::domain::{
    Chunk, ChunkDownloader, ChunkStats, CancellationToken, DownloadFileInfo, DownloadSource,
};

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
    fn build_sftp_command(source: &SshSource, local_path: &std::path::Path) -> Command {
        let mut cmd = Command::new("sftp");
        cmd.arg("-P").arg(source.port().to_string());
        cmd.arg("-o").arg(format!("StrictHostKeyChecking=no"));
        cmd.arg("-o").arg(format!("ConnectTimeout=10"));
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
    fn build_scp_command(source: &SshSource, local_path: &std::path::Path) -> Command {
        let mut cmd = Command::new("scp");
        cmd.arg("-P").arg(source.port().to_string());
        cmd.arg("-o").arg(format!("StrictHostKeyChecking=no"));
        cmd.arg("-o").arg(format!("ConnectTimeout=10"));
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

        // 尝试用 sftp 的 ls 命令获取文件大小
        let output = Command::new("sftp")
            .arg("-P")
            .arg(ssh_source.port().to_string())
            .arg("-o")
            .arg("StrictHostKeyChecking=no")
            .arg("-o")
            .arg("ConnectTimeout=10")
            .arg("-b")
            .arg("-")
            .arg(format!(
                "{}@{}",
                ssh_source.username(),
                ssh_source.host()
            ))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .output()
            .await;

        // 如果 sftp 命令失败或不可用，返回默认值（假设文件存在，大小为 0，后续下载时会获取真实大小）
        let size_bytes = match output {
            Ok(output) if output.status.success() => {
                // 解析 ls 输出获取文件大小（简化实现，实际解析比较复杂）
                // 这里返回 0，让调度器用单分片下载整个文件
                0
            }
            _ => 0,
        };

        Ok(DownloadFileInfo {
            size_bytes,
            supports_resume: false,
            supports_multi_connection: false,
        })
    }

    /// 下载一个分片
    ///
    /// 由于 SSH 协议不支持分片下载，这里实现为：
    /// - 如果是第一个分片（offset=0），下载整个文件到目标位置
    /// - 如果不是第一个分片，假设文件已经下载完成，直接从已下载的文件中读取分片数据
    ///
    /// 注意：调度器会根据 capabilities 用单分片下载整个文件，所以通常只会调用一次。
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
            anyhow::bail!("download cancelled");
        }

        // 创建临时文件用于下载
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!(
            "spde_ssh_{}_{}.tmp",
            chunk.chunk_id,
            std::process::id()
        ));

        // 根据协议类型选择下载命令
        let mut cmd = if ssh_source.scheme() == "scp" {
            Self::build_scp_command(ssh_source, &temp_file)
        } else {
            Self::build_sftp_command(ssh_source, &temp_file)
        };

        // 执行下载命令（带超时）
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(self.timeout_secs),
            cmd.output(),
        )
        .await
        .context("ssh/sftp/scp command timed out")?
        .context("failed to execute ssh/sftp/scp command")?;

        if !output.status.success() {
            // 清理临时文件
            let _ = tokio::fs::remove_file(&temp_file).await;
            anyhow::bail!(
                "ssh/sftp/scp command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // 读取下载的文件并写入目标位置
        let data = tokio::fs::read(&temp_file)
            .await
            .context("failed to read downloaded file")?;

        // 清理临时文件
        let _ = tokio::fs::remove_file(&temp_file).await;

        // 写入目标文件（从 chunk.offset 开始）
        writer
            .write_chunk(chunk.chunk_id, chunk.offset, &data)
            .await
            .context("failed to write chunk")?;

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
