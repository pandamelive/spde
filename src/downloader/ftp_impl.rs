//! FTP/FTPS 下载后端 — 支持 ftp:// 协议，断点续传

use super::*;
use anyhow::{anyhow, Context, Result};
use std::time::Instant;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use url::Url;

/// FTP 下载器
pub struct FtpDownloader;

impl Default for FtpDownloader {
    fn default() -> Self {
        Self::new()
    }
}

impl FtpDownloader {
    pub fn new() -> Self {
        Self
    }

    /// 解析 FTP URI，返回 (host:port, username, password, remote_path)
    fn parse_ftp_uri(uri: &str) -> Result<(String, String, String, String)> {
        let parsed = Url::parse(uri).context("invalid ftp url")?;
        if parsed.scheme() != "ftp" && parsed.scheme() != "ftps" {
            anyhow::bail!("not an ftp url: {}", uri);
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow!("ftp url missing host"))?;
        let port = parsed.port().unwrap_or(21);
        let username = if parsed.username().is_empty() {
            "anonymous".to_string()
        } else {
            simple_percent_decode(parsed.username())
        };
        let password = parsed
            .password()
            .map(simple_percent_decode)
            .unwrap_or_else(|| "anonymous@".to_string());
        let path = parsed.path().to_string();
        Ok((format!("{}:{}", host, port), username, password, path))
    }
}

/// 简单的 percent-decoding（处理 %XX）
fn simple_percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

#[async_trait::async_trait]
impl DownloadBackend for FtpDownloader {
    fn name(&self) -> &str {
        "ftp"
    }

    fn support_uri(&self, uri: &str) -> bool {
        uri.starts_with("ftp://") || uri.starts_with("ftps://")
    }

    async fn run(
        &self,
        task: DownloadTask,
        progress: Option<Arc<dyn ProgressCallback>>,
        controller: Option<Arc<DownloadController>>,
    ) -> Result<DownloadOutput> {
        // 任务取消检查
        if let Some(ctrl) = &controller {
            if ctrl.is_cancelled() {
                anyhow::bail!("download cancelled by controller");
            }
        }
        use futures_lite::io::AsyncReadExt as _;
        use suppaftp::types::FileType;
        use suppaftp::AsyncFtpStream;

        let start = Instant::now();
        let (addr, user, pass, remote_path) = Self::parse_ftp_uri(&task.uri)?;
        let mut output = DownloadOutput::default();

        // 连接 FTP
        let mut ftp = AsyncFtpStream::connect(&addr)
            .await
            .with_context(|| format!("connect ftp {} failed", addr))?;
        ftp.login(&user, &pass).await.context("ftp login failed")?;
        ftp.transfer_type(FileType::Binary)
            .await
            .context("set binary mode failed")?;

        // 获取文件大小
        let total_size = ftp.size(&remote_path).await.map(|s| s as u64).unwrap_or(0);
        output.total_size = total_size;

        // 已存在且大小匹配 → 跳过（仅在开启断点续传时）
        if task.resume && !task.dry_run {
            if let Ok(meta) = tokio::fs::metadata(&task.save_path).await {
                if meta.len() == total_size && total_size > 0 {
                    output.status = "skipped".into();
                    output.is_success = true;
                    output.elapsed_secs = start.elapsed().as_secs_f64();
                    ftp.quit().await.ok();
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
            ftp.quit().await.ok();
            if let Some(p) = &progress {
                p.on_complete(output.clone());
            }
            return Ok(output);
        }

        // 确保目录存在
        if let Some(parent) = task.save_path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
        }

        // 断点续传：沿用已有本地大小（resume=false 时从零开始）
        let local_size = if task.resume {
            tokio::fs::metadata(&task.save_path)
                .await
                .map(|m| m.len())
                .unwrap_or(0)
        } else {
            0
        };

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(!task.resume)
            .write(true)
            .read(true)
            .open(&task.save_path)
            .await
            .context("open local file failed")?;

        file.seek(std::io::SeekFrom::Start(local_size))
            .await
            .context("seek failed")?;

        // 设置 REST 断点
        if local_size > 0 && task.resume {
            ftp.resume_transfer(local_size as usize)
                .await
                .context("ftp resume failed")?;
        }

        // 下载流
        let mut data_stream = ftp
            .retr_as_stream(&remote_path)
            .await
            .context("ftp retr failed")?;

        let mut buf = vec![0u8; 64 * 1024];
        let dl_start = Instant::now();
        let mut last_progress = Instant::now();
        let deadline = task.timeout.map(|d| Instant::now() + d);
        loop {
            // 超时检查
            if let Some(d) = deadline {
                if Instant::now() >= d {
                    anyhow::bail!("download timed out");
                }
            }
            let n = data_stream
                .read(&mut buf)
                .await
                .context("read ftp stream failed")?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n])
                .await
                .context("write file failed")?;
            output.downloaded_bytes += n as u64;
            output.success_chunks += 1;

            // 进度回调：每500ms报告一次
            if let Some(cb) = &progress {
                if last_progress.elapsed() >= task.progress_interval {
                    let elapsed = dl_start.elapsed().as_secs_f64();
                    let speed = if elapsed > 0.0 {
                        (output.downloaded_bytes as f64 / elapsed) as u64
                    } else {
                        0
                    };
                    let percent = if total_size > 0 {
                        output.downloaded_bytes as f64 / total_size as f64 * 100.0
                    } else {
                        0.0
                    };
                    cb.on_progress(ProgressSnapshot {
                        task_id: task.task_id.clone(),
                        total_size,
                        downloaded_bytes: output.downloaded_bytes,
                        speed_bps: speed,
                        active_connections: 1,
                        percent,
                        elapsed_secs: elapsed,
                    });
                    last_progress = Instant::now();
                }
            }
        }

        file.flush().await.context("flush failed")?;

        // 完成 FTP 传输
        ftp.finalize_retr_stream(data_stream)
            .await
            .context("finalize ftp stream failed")?;
        ftp.quit().await.ok();

        output.is_success = output.downloaded_bytes + local_size >= total_size || total_size == 0;
        output.status = if output.is_success {
            "success"
        } else {
            "incomplete"
        }
        .into();
        output.elapsed_secs = start.elapsed().as_secs_f64();
        output.avg_speed_mbps = if output.elapsed_secs > 0.0 {
            output.downloaded_bytes as f64 / output.elapsed_secs / 1024.0 / 1024.0
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
