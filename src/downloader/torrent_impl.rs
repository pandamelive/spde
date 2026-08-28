//! BitTorrent 下载后端 — 支持磁力链接和 .torrent 文件
//!
//! 基于 librqbit（纯 Rust BT 客户端库），支持 DHT、PEX、uTP、磁力链接 metadata 交换。
//! URI 格式：
//! - magnet:?xt=urn:btih:...  — 磁力链接
//! - /path/to/file.torrent     — 本地种子文件
//! - http(s)://.../file.torrent — 远程种子文件 URL

use super::*;
use anyhow::{anyhow, Context, Result};
use librqbit::{AddTorrent, Session};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// BitTorrent 下载器
pub struct TorrentDownloader;

impl Default for TorrentDownloader {
    fn default() -> Self {
        Self::new()
    }
}

impl TorrentDownloader {
    pub fn new() -> Self {
        Self
    }

    /// 判断 URI 是否为 BT 相关
    fn is_torrent_uri(uri: &str) -> bool {
        uri.starts_with("magnet:") || uri.ends_with(".torrent") || uri.starts_with("torrent://")
    }

    /// 从 URI 构建 AddTorrent
    async fn build_add_torrent(uri: &str) -> Result<AddTorrent<'_>> {
        if uri.starts_with("magnet:") {
            return Ok(AddTorrent::from_url(uri.to_string()));
        }

        // 本地 .torrent 文件
        let path = std::path::Path::new(uri);
        if path.exists() && path.extension().is_some_and(|e| e == "torrent") {
            let bytes = tokio::fs::read(path)
                .await
                .context("read .torrent file failed")?;
            return Ok(AddTorrent::from_bytes(bytes));
        }

        // http(s) .torrent URL
        if uri.starts_with("http://") || uri.starts_with("https://") {
            return Ok(AddTorrent::from_url(uri.to_string()));
        }

        // torrent:// 前缀 → 去掉前缀后按 URL 处理
        if let Some(rest) = uri.strip_prefix("torrent://") {
            let actual = if rest.starts_with("http") {
                rest.to_string()
            } else {
                format!("https://{}", rest)
            };
            return Ok(AddTorrent::from_url(actual));
        }

        anyhow::bail!("unsupported torrent uri: {}", uri)
    }
}

#[async_trait::async_trait]
impl DownloadBackend for TorrentDownloader {
    fn name(&self) -> &str {
        "torrent"
    }

    fn support_uri(&self, uri: &str) -> bool {
        Self::is_torrent_uri(uri)
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
        let mut output = DownloadOutput::default();

        // BT 下载目录：save_path 作为目录
        let download_dir = if task.save_path.is_dir() {
            task.save_path.clone()
        } else {
            // 如果不是目录，用其父目录或创建它
            if let Some(parent) = task.save_path.parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent).await.ok();
                }
            }
            tokio::fs::create_dir_all(&task.save_path).await.ok();
            task.save_path.clone()
        };

        if task.dry_run {
            output.status = "dry-run".into();
            output.is_success = true;
            output.elapsed_secs = start.elapsed().as_secs_f64();
            if let Some(p) = &progress {
                p.on_complete(output.clone());
            }
            return Ok(output);
        }

        // 创建 BT 会话
        let session = Session::new(download_dir.clone())
            .await
            .context("create bittorrent session failed")?;

        // 添加 torrent
        let add_torrent = Self::build_add_torrent(&task.uri).await?;
        let add_result = session
            .add_torrent(add_torrent, None)
            .await
            .context("add torrent failed")?;

        let handle = add_result
            .into_handle()
            .ok_or_else(|| anyhow!("torrent already exists or invalid state"))?;

        // 启动进度监控
        let progress_state = Arc::new(TorrentProgressState {
            downloaded: AtomicU64::new(0),
            total: AtomicU64::new(0),
            speed: AtomicU64::new(0),
            peers: AtomicU32::new(0),
        });

        let progress_task_id = task.task_id.clone();
        let progress_cb = progress.clone();
        let progress_interval = task.progress_interval;
        let progress_handle = tokio::spawn({
            let state = progress_state.clone();
            async move {
                loop {
                    tokio::time::sleep(progress_interval).await;
                    let dl = state.downloaded.load(Ordering::Relaxed);
                    let total = state.total.load(Ordering::Relaxed);
                    let speed = state.speed.load(Ordering::Relaxed);
                    let peers = state.peers.load(Ordering::Relaxed);
                    let percent = if total > 0 {
                        dl as f64 / total as f64 * 100.0
                    } else {
                        0.0
                    };
                    if let Some(cb) = &progress_cb {
                        cb.on_progress(ProgressSnapshot {
                            task_id: progress_task_id.clone(),
                            total_size: total,
                            downloaded_bytes: dl,
                            speed_bps: speed,
                            active_connections: peers,
                            percent,
                            elapsed_secs: start.elapsed().as_secs_f64(),
                        });
                    }
                    // 如果已完成，退出循环
                    if total > 0 && dl >= total {
                        break;
                    }
                }
            }
        });

        // 定期拉取 stats 更新进度状态
        let stats_handle = tokio::spawn({
            let prog_state = progress_state.clone();
            let handle_clone = handle.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let stats = handle_clone.stats();
                    prog_state.total.store(stats.total_bytes, Ordering::Relaxed);
                    prog_state
                        .downloaded
                        .store(stats.progress_bytes, Ordering::Relaxed);
                    if let Some(live) = &stats.live {
                        prog_state
                            .speed
                            .store(live.download_speed.as_bytes(), Ordering::Relaxed);
                        prog_state
                            .peers
                            .store(live.snapshot.peer_stats.live, Ordering::Relaxed);
                    }
                }
            }
        });

        // 等待下载完成（任务级 timeout 生效时为竞速超时）
        let completed = match task.timeout {
            Some(d) => tokio::select! {
                r = handle.wait_until_completed() => Some(r),
                _ = tokio::time::sleep(d) => None,
            },
            None => Some(handle.wait_until_completed().await),
        };
        match completed {
            Some(Ok(())) => {
                output.is_success = true;
                output.status = "success".into();
            }
            Some(Err(e)) => {
                output.is_success = false;
                output.status = "failed".into();
                output.error_msg = Some(e.to_string());
            }
            None => {
                output.is_success = false;
                output.status = "failed".into();
                output.error_msg = Some("download timed out".to_string());
            }
        }

        // 停止进度任务
        progress_handle.abort();
        stats_handle.abort();

        // 收集最终统计
        let final_dl = progress_state.downloaded.load(Ordering::Relaxed);
        let final_total = progress_state.total.load(Ordering::Relaxed);
        output.downloaded_bytes = final_dl;
        output.total_size = final_total;
        output.success_chunks = if output.is_success { 1 } else { 0 };
        output.failed_chunks = if output.is_success { 0 } else { 1 };
        output.elapsed_secs = start.elapsed().as_secs_f64();
        output.avg_speed_mbps = if output.elapsed_secs > 0.0 {
            final_dl as f64 / output.elapsed_secs / 1024.0 / 1024.0
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

/// BT 下载进度共享状态
struct TorrentProgressState {
    downloaded: AtomicU64,
    total: AtomicU64,
    speed: AtomicU64,
    peers: AtomicU32,
}

use std::sync::atomic::AtomicU32;
