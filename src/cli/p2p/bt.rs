//! BT 下载器实现
//!
//! 基于 librqbit，支持磁力链接和 .torrent 文件。
//! BT 协议有自己的分片机制和调度逻辑，不经过通用 ChunkScheduler，
//! 由 librqbit 内部管理 peer 连接、piece 调度和并发下载。
//!
//! # dry_run 模式
//! dry_run=true 时，下载到临时目录（`save_dir/.p2p-dry-run/<uuid>/`），
//! 下载完成或取消后删除临时目录。进度和速度正常汇报，用于测试下载速度。

use std::path::Path;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

use pandanetos::domain::{CancellationToken, DownloadProgress, DownloadResult};
use pandanetos::error::{CoreError, Result};

use super::P2PDownloader;

/// BT 下载器
pub struct BtDownloader;

impl BtDownloader {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BtDownloader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl P2PDownloader for BtDownloader {
    fn protocol_name(&self) -> &'static str {
        "bittorrent"
    }

    async fn download(
        &self,
        url: &str,
        save_dir: &Path,
        timeout_secs: u64,
        dry_run: bool,
        progress_tx: mpsc::Sender<DownloadProgress>,
        cancel: CancellationToken,
    ) -> Result<DownloadResult> {
        let start = Instant::now();

        // dry_run 时使用临时目录，完成后删除
        let (actual_save_dir, temp_dir) = if dry_run {
            let temp = save_dir.join(".p2p-dry-run").join(Uuid::new_v4().to_string());
            tokio::fs::create_dir_all(&temp).await.map_err(|e| {
                CoreError::Internal(format!("failed to create dry-run temp dir: {}", e))
            })?;
            (temp.clone(), Some(temp))
        } else {
            tokio::fs::create_dir_all(save_dir).await.map_err(|e| {
                CoreError::Internal(format!("failed to create save dir: {}", e))
            })?;
            (save_dir.to_path_buf(), None)
        };

        info!(
            url = %url,
            dry_run = dry_run,
            save_dir = ?actual_save_dir,
            "starting BT download"
        );

        let result = self
            .download_internal(
                url,
                &actual_save_dir,
                timeout_secs,
                progress_tx,
                cancel.clone(),
            )
            .await;

        // dry_run 时清理临时目录
        if let Some(temp) = temp_dir {
            if let Err(e) = tokio::fs::remove_dir_all(&temp).await {
                warn!(error = %e, "failed to clean up dry-run temp dir");
            }
        }

        let elapsed_secs = start.elapsed().as_secs_f64();

        match result {
            Ok(r) => {
                info!(
                    success = r.success,
                    downloaded = r.downloaded_bytes,
                    elapsed_secs = elapsed_secs,
                    "BT download completed"
                );
                Ok(r)
            }
            Err(e) => {
                let err_msg = e.to_string();
                warn!(error = %err_msg, "BT download failed");
                Ok(DownloadResult {
                    success: false,
                    total_bytes: 0,
                    downloaded_bytes: 0,
                    elapsed_secs,
                    success_chunks: 0,
                    failed_chunks: 1,
                    avg_speed_bps: 0,
                    error_msg: Some(err_msg),
                })
            }
        }
    }
}

impl BtDownloader {
    /// 内部下载逻辑（不含 dry_run 临时目录处理）
    async fn download_internal(
        &self,
        url: &str,
        save_dir: &Path,
        timeout_secs: u64,
        progress_tx: mpsc::Sender<DownloadProgress>,
        cancel: CancellationToken,
    ) -> Result<DownloadResult> {
        // 步骤 1：创建 librqbit Session
        let mut session_opts = librqbit::SessionOptions::default();
        if let Some(ref mut dht) = session_opts.dht {
            dht.persistence = None; // 禁用 DHT 持久化，避免 Windows 环境卡住
        }
        let session = librqbit::Session::new_with_opts(save_dir.to_path_buf(), session_opts)
            .await
            .map_err(|e| CoreError::Internal(format!("failed to create librqbit session: {}", e)))?;

        let api = librqbit::Api::new(session.clone(), None);

        // 步骤 2：添加 torrent
        let add_opts = librqbit::AddTorrentOptions {
            overwrite: true,
            ..Default::default()
        };

        let add_result = if url.starts_with("magnet:") {
            api.api_add_torrent(
                librqbit::AddTorrent::Url(url.to_string().into()),
                Some(add_opts),
            )
        } else if url.ends_with(".torrent") {
            let data = tokio::fs::read(url)
                .await
                .map_err(|e| CoreError::Internal(format!("failed to read torrent file: {}", e)))?;
            api.api_add_torrent(
                librqbit::AddTorrent::TorrentFileBytes(data.into()),
                Some(add_opts),
            )
        } else {
            return Err(CoreError::InvalidParam(format!(
                "unsupported BT URI: {}",
                url
            )));
        };

        let add_response = add_result
            .await
            .map_err(|e| CoreError::Internal(format!("failed to add torrent: {}", e)))?;
        let torrent_id = add_response
            .id
            .ok_or_else(|| CoreError::Internal("torrent id is None".into()))?;

        info!(torrent_id, "torrent added, waiting for metadata");

        // 步骤 3：等待 metadata
        let metadata_timeout = Duration::from_secs(timeout_secs);
        let metadata_start = Instant::now();
        let total_bytes = loop {
            if cancel.is_cancelled() {
                return Err(CoreError::Internal("cancelled while waiting for metadata".into()));
            }
            if metadata_start.elapsed() > metadata_timeout {
                return Err(CoreError::Internal("timeout waiting for torrent metadata".into()));
            }

            if let Ok(details) =
                api.api_torrent_details(librqbit::api::TorrentIdOrHash::Id(torrent_id))
            {
                // 从 stats 中获取总大小，metadata 就绪后 total_bytes > 0
                if let Some(ref stats) = details.stats {
                    if stats.total_bytes > 0 {
                        break stats.total_bytes;
                    }
                }
                // 备选：从 files 计算总大小
                if details.total_pieces > 0 {
                    if let Some(ref files) = details.files {
                        let total: u64 = files.iter().map(|f| f.length).sum();
                        if total > 0 {
                            break total;
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        };

        info!(
            total_bytes,
            "metadata ready, starting download"
        );

        // 步骤 4：轮询下载进度
        let mut last_downloaded = 0u64;
        let mut last_time = Instant::now();
        let download_timeout = Duration::from_secs(timeout_secs);
        let download_start = Instant::now();
        let mut final_progress_bytes = 0u64;
        let mut final_finished = false;

        loop {
            if cancel.is_cancelled() {
                return Err(CoreError::Internal("BT download cancelled".into()));
            }
            if download_start.elapsed() > download_timeout {
                return Err(CoreError::Internal("BT download timeout".into()));
            }

            if let Ok(details) =
                api.api_torrent_details(librqbit::api::TorrentIdOrHash::Id(torrent_id))
            {
                if let Some(ref stats) = details.stats {
                    let downloaded_bytes = stats.progress_bytes;
                    final_progress_bytes = downloaded_bytes;
                    final_finished = stats.finished;

                    // 计算速度
                    let now = Instant::now();
                    let elapsed = now.duration_since(last_time).as_secs_f64();
                    let speed_bps = if elapsed > 0.0 {
                        ((downloaded_bytes - last_downloaded) as f64 / elapsed) as u64
                    } else {
                        0
                    };

                    // 汇报进度
                    let percent = if total_bytes > 0 {
                        downloaded_bytes as f64 / total_bytes as f64 * 100.0
                    } else {
                        0.0
                    };

                    let progress = DownloadProgress {
                        downloaded_bytes,
                        total_bytes,
                        speed_bps,
                        active_connections: 0, // TODO: 从 stats.live 获取 peer 数
                        percent,
                        elapsed_secs: download_start.elapsed().as_secs_f64(),
                    };

                    if progress_tx.send(progress).await.is_err() {
                        warn!("progress channel closed, stopping BT download");
                        break;
                    }

                    last_downloaded = downloaded_bytes;
                    last_time = now;

                    // 完成判定
                    if stats.finished || (total_bytes > 0 && downloaded_bytes >= total_bytes) {
                        info!("BT download completed");
                        break;
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let elapsed_secs = download_start.elapsed().as_secs_f64();
        let downloaded_bytes = final_progress_bytes;
        let success = final_finished || (total_bytes > 0 && downloaded_bytes >= total_bytes);
        let avg_speed_bps = if elapsed_secs > 0.0 {
            (downloaded_bytes as f64 / elapsed_secs) as u64
        } else {
            0
        };

        Ok(DownloadResult {
            success,
            total_bytes,
            downloaded_bytes,
            elapsed_secs,
            success_chunks: if success { 1 } else { 0 },
            failed_chunks: if success { 0 } else { 1 },
            avg_speed_bps,
            error_msg: if success {
                None
            } else {
                Some("BT download incomplete".into())
            },
        })
    }
}
