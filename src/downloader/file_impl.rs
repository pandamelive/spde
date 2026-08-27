//! 本地文件下载后端 — 支持 file:// 协议，高效复制

use super::*;
use anyhow::{Context, Result};
use std::time::Instant;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 本地文件复制下载器
pub struct FileDownloader;

impl Default for FileDownloader {
    fn default() -> Self {
        Self::new()
    }
}

impl FileDownloader {
    pub fn new() -> Self {
        Self
    }

    /// 解析 file:// URI 为本地路径
    fn parse_uri(uri: &str) -> Result<std::path::PathBuf> {
        let path = uri
            .strip_prefix("file://")
            .or_else(|| uri.strip_prefix("file:"))
            .ok_or_else(|| anyhow::anyhow!("invalid file uri: {}", uri))?;
        Ok(std::path::PathBuf::from(path))
    }
}

#[async_trait::async_trait]
impl DownloadBackend for FileDownloader {
    fn name(&self) -> &str {
        "file"
    }

    fn support_uri(&self, uri: &str) -> bool {
        uri.starts_with("file://") || uri.starts_with("file:")
    }

    async fn run(
        &self,
        task: DownloadTask,
        progress: Option<Arc<dyn ProgressCallback>>,
    ) -> Result<DownloadOutput> {
        let start = Instant::now();
        let src = Self::parse_uri(&task.uri)?;
        let dst = &task.save_path;
        let mut output = DownloadOutput::default();

        // 源文件必须存在
        let meta = fs::metadata(&src)
            .await
            .with_context(|| format!("source file not found: {:?}", src))?;
        if !meta.is_file() {
            anyhow::bail!("source is not a file: {:?}", src);
        }
        let total_size = meta.len();
        output.total_size = total_size;

        // 目标已存在且大小一致 → 跳过
        if !task.dry_run {
            if let Ok(dst_meta) = fs::metadata(dst).await {
                if dst_meta.len() == total_size && total_size > 0 {
                    output.status = "skipped".into();
                    output.is_success = true;
                    output.elapsed_secs = start.elapsed().as_secs_f64();
                    if let Some(p) = &progress {
                        p.on_complete(output.clone());
                    }
                    return Ok(output);
                }
            }
        }

        if task.dry_run {
            output.downloaded_bytes = total_size;
            output.is_success = true;
            output.status = "dry-run".into();
            output.elapsed_secs = start.elapsed().as_secs_f64();
            output.avg_speed_mbps = if output.elapsed_secs > 0.0 {
                total_size as f64 / output.elapsed_secs / 1024.0 / 1024.0
            } else {
                0.0
            };
            if let Some(p) = &progress {
                p.on_complete(output.clone());
            }
            return Ok(output);
        }

        // 确保目标目录存在
        if let Some(parent) = dst.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).await.ok();
            }
        }

        // 流式复制（带进度回调）
        let mut src_file = fs::File::open(&src)
            .await
            .context("open source failed")?;
        let mut dst_file = fs::File::create(dst)
            .await
            .context("create destination failed")?;

        let mut buf = vec![0u8; 256 * 1024];
        let dl_start = Instant::now();
        let mut last_progress = Instant::now();
        let mut copied: u64 = 0;

        loop {
            let n = src_file
                .read(&mut buf)
                .await
                .context("read source failed")?;
            if n == 0 {
                break;
            }
            dst_file
                .write_all(&buf[..n])
                .await
                .context("write destination failed")?;
            copied += n as u64;

            // 进度回调
            if let Some(cb) = &progress {
                if last_progress.elapsed() >= task.progress_interval {
                    let elapsed = dl_start.elapsed().as_secs_f64();
                    let speed = if elapsed > 0.0 {
                        (copied as f64 / elapsed) as u64
                    } else {
                        0
                    };
                    let percent = if total_size > 0 {
                        copied as f64 / total_size as f64 * 100.0
                    } else {
                        0.0
                    };
                    cb.on_progress(ProgressSnapshot {
                        task_id: task.task_id.clone(),
                        total_size,
                        downloaded_bytes: copied,
                        speed_bps: speed,
                        active_connections: 1,
                        percent,
                        elapsed_secs: elapsed,
                    });
                    last_progress = Instant::now();
                }
            }
        }

        dst_file.flush().await.context("flush failed")?;

        output.downloaded_bytes = copied;
        output.success_chunks = 1;
        output.is_success = copied == total_size;
        output.status = if output.is_success { "success" } else { "incomplete" }.into();
        output.elapsed_secs = start.elapsed().as_secs_f64();
        output.avg_speed_mbps = if output.elapsed_secs > 0.0 {
            copied as f64 / output.elapsed_secs / 1024.0 / 1024.0
        } else {
            0.0
        };

        if let Some(p) = &progress {
            p.on_complete(output.clone());
        }

        Ok(output)
    }

    async fn stop(&self, _task_id: &str) -> Result<()> {
        Ok(())
    }
}
