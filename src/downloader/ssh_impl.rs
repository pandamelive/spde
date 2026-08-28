//! SSH 系下载后端 — 支持 sftp:// 和 scp:// 协议
//!
//! 内部调用系统自带的 sftp / scp 命令，无需额外编译依赖。
//! URI 格式：
//! - sftp://user:pass@host:port/path/to/file
//! - scp://user@host:port/path/to/file
//! - ssh://user@host/path (alias for sftp)

use super::*;
use anyhow::{anyhow, Context, Result};
use std::time::Instant;
use tokio::process::Command;
use url::Url;

/// SSH/SFTP/SCP 下载器
pub struct SshDownloader;

impl Default for SshDownloader {
    fn default() -> Self {
        Self::new()
    }
}

impl SshDownloader {
    pub fn new() -> Self {
        Self
    }

    /// 解析 SSH URI，返回 (scheme, user, host, port, remote_path)
    fn parse_uri(uri: &str) -> Result<(String, String, String, u16, String)> {
        let parsed = Url::parse(uri).context("invalid ssh url")?;
        let scheme = parsed.scheme().to_string();
        if !matches!(scheme.as_str(), "sftp" | "scp" | "ssh") {
            anyhow::bail!("not an ssh/sftp/scp url: {}", uri);
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow!("ssh url missing host"))?
            .to_string();
        let port = parsed.port().unwrap_or(22);
        let user = if parsed.username().is_empty() {
            whoami_username()
        } else {
            simple_percent_decode(parsed.username())
        };
        let path = parsed.path().to_string();
        if path.is_empty() || path == "/" {
            anyhow::bail!("ssh url missing remote path");
        }
        Ok((scheme, user, host, port, path))
    }
}

/// 获取当前用户名（失败则回退 "user"）
fn whoami_username() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "user".to_string())
}

/// 简单 percent-decode
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
impl DownloadBackend for SshDownloader {
    fn name(&self) -> &str {
        "ssh"
    }

    fn support_uri(&self, uri: &str) -> bool {
        uri.starts_with("sftp://") || uri.starts_with("scp://") || uri.starts_with("ssh://")
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
        let start = Instant::now();
        let (_scheme, user, host, port, remote_path) = Self::parse_uri(&task.uri)?;
        let mut output = DownloadOutput::default();

        // 确保目标目录存在
        if !task.dry_run {
            if let Some(parent) = task.save_path.parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent).await.ok();
                }
            }
        }

        // 已存在且非空 → 跳过（ssh 协议无法远程获取大小，简单跳过）
        if !task.dry_run {
            if let Ok(meta) = tokio::fs::metadata(&task.save_path).await {
                if meta.len() > 0 {
                    output.total_size = meta.len();
                    output.downloaded_bytes = meta.len();
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
            output.status = "dry-run".into();
            output.is_success = true;
            output.elapsed_secs = start.elapsed().as_secs_f64();
            if let Some(p) = &progress {
                p.on_complete(output.clone());
            }
            return Ok(output);
        }

        // 用 scp 下载（sftp 和 ssh 都走 scp，因为 scp 最简单且有进度输出）
        let target = format!("{}@{}:{}", user, host, remote_path);
        let port_str = port.to_string();

        let mut cmd = Command::new("scp");
        cmd.args([
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            "-P",
            &port_str,
            &target,
        ]);
        cmd.arg(&task.save_path);

        // 如果有密码，用 sshpass（如果安装了）
        if let Ok(password) = std::env::var("SSHPASS") {
            if !password.is_empty() {
                let mut new_cmd = Command::new("sshpass");
                new_cmd.arg("-e").arg("scp");
                for arg in cmd.as_std().get_args() {
                    new_cmd.arg(arg);
                }
                cmd = new_cmd;
            }
        }

        cmd.stderr(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::null());

        let mut child = cmd
            .spawn()
            .context("failed to spawn scp (is openssh-client installed?)")?;

        // 读取 stderr 解析进度
        if let Some(mut stderr) = child.stderr.take() {
            let prog_cb = progress.clone();
            let task_id = task.task_id.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = vec![0u8; 1024];
                let mut line_buf = String::new();
                while let Ok(n) = stderr.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    line_buf.push_str(&String::from_utf8_lossy(&buf[..n]));
                    while let Some(pos) = line_buf.find('\n') {
                        let line = line_buf[..pos].trim().to_string();
                        line_buf = line_buf[pos + 1..].to_string();
                        // scp 进度格式: "file  100%  123MB  5.0MB/s 00:01"
                        if let Some(percent) = parse_scp_progress(&line) {
                            if let Some(cb) = &prog_cb {
                                cb.on_progress(ProgressSnapshot {
                                    task_id: task_id.clone(),
                                    total_size: 0,
                                    downloaded_bytes: 0,
                                    speed_bps: 0,
                                    active_connections: 1,
                                    percent,
                                    elapsed_secs: 0.0,
                                });
                            }
                        }
                    }
                }
            });
        }

        let status = child.wait().await.context("scp process error")?;
        if !status.success() {
            anyhow::bail!("scp failed with exit code: {:?}", status.code());
        }

        // 统计下载结果
        let file_size = tokio::fs::metadata(&task.save_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        output.total_size = file_size;
        output.downloaded_bytes = file_size;
        output.success_chunks = 1;
        output.is_success = true;
        output.status = "success".into();
        output.elapsed_secs = start.elapsed().as_secs_f64();
        output.avg_speed_mbps = if output.elapsed_secs > 0.0 {
            file_size as f64 / output.elapsed_secs / 1024.0 / 1024.0
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

/// 解析 scp 进度行中的百分比
fn parse_scp_progress(line: &str) -> Option<f64> {
    // 格式如: "filename  100%  1234KB  1.2MB/s 00:01"
    if let Some(pct_pos) = line.find('%') {
        let start = line[..pct_pos].rfind(char::is_whitespace)? + 1;
        let num: f64 = line[start..pct_pos].trim().parse().ok()?;
        return Some(num);
    }
    None
}
