//! BT 下载器实现
//!
//! 基于 librqbit，支持磁力链接和 .torrent 文件。
//! BT 协议有自己的分片机制和调度逻辑，不经过通用 ChunkScheduler，
//! 由 librqbit 内部管理 peer 连接、piece 调度和并发下载。
//!
//! # dry_run 模式（不落盘）
//! dry_run=true 时，使用自定义 NullStorage：
//! - pwrite_all 直接丢弃数据（不写磁盘）
//! - pread_exact 返回零填充（不影响下载流程）
//! - 零内存占用，零磁盘写入
//! 注意：不保留已下载 piece，无法上传给其他 peer（BT 互惠协议），
//! 可能影响下载速度，但对于 dry_run 测试场景是可接受的。

use std::io::IoSlice;
use std::path::Path;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use librqbit::api::TorrentIdOrHash;
use librqbit::http_api_types::PeerStatsFilter;
use librqbit::storage::{StorageFactory, StorageFactoryExt, TorrentStorage};
use librqbit::{ManagedTorrentShared, TorrentMetadata};
use tokio::sync::mpsc;
use tracing::warn;

use pandanetos::domain::{CancellationToken, DownloadProgress, DownloadResult};
use pandanetos::error::{CoreError, Result};

use super::P2PDownloader;

pub struct BtDownloader;

impl BtDownloader {
    pub fn new() -> Self { Self }
}

impl Default for BtDownloader {
    fn default() -> Self { Self::new() }
}

// ============================================================
// NullStorage: dry_run 不落盘存储
// pwrite_all 直接丢弃，pread_exact 返回零填充
// ============================================================

#[derive(Debug, Clone, Default)]
struct NullStorage;

impl TorrentStorage for NullStorage {
    fn init(
        &mut self,
        _shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn pread_exact(
        &self,
        _file_id: usize,
        _offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<()> {
        // 返回零填充，peer 会丢弃但不影响下载流程
        for b in buf.iter_mut() {
            *b = 0;
        }
        Ok(())
    }

    fn pwrite_all(
        &self,
        _file_id: usize,
        _offset: u64,
        _buf: &[u8],
    ) -> anyhow::Result<()> {
        // 直接丢弃，不落盘
        Ok(())
    }

    fn pwrite_all_vectored(
        &self,
        _file_id: usize,
        _offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        // 直接丢弃数据，但返回实际写入的字节数以满足 librqbit 断言
        Ok(bufs[0].len() + bufs[1].len())
    }

    fn remove_file(&self, _file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _path: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
        Ok(())
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(NullStorage))
    }
}

#[derive(Debug, Clone, Default)]
struct NullStorageFactory;

impl StorageFactory for NullStorageFactory {
    type Storage = NullStorage;

    fn create(
        &self,
        _shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<Self::Storage> {
        Ok(NullStorage)
    }

    fn clone_box(&self) -> librqbit::storage::BoxStorageFactory {
        NullStorageFactory.boxed()
    }
}

// ============================================================
// BtDownloader 实现
// ============================================================

#[async_trait]
impl P2PDownloader for BtDownloader {
    fn protocol_name(&self) -> &'static str { "bittorrent" }

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

        // dry_run 用 NullStorage 不落盘；非 dry_run 确保 save_dir 存在
        if !dry_run {
            tokio::fs::create_dir_all(save_dir).await
                .map_err(|e| CoreError::Internal(format!("create save dir: {}", e)))?;
        }

        eprintln!("[bt] starting download (standalone mode), dry_run={}, save_dir={:?}", dry_run, save_dir);

        // 独立模式：自己创建 Session（非 BtManager 模式）
        let api = self.create_session_api(save_dir, dry_run).await?;
        let trackers = Self::default_trackers();

        let result = self.download_internal(
            &api, trackers, url, save_dir, timeout_secs, dry_run, progress_tx, cancel.clone(),
        ).await;

        let elapsed_secs = start.elapsed().as_secs_f64();
        match result {
            Ok(r) => {
                eprintln!("[bt] download completed: success={}, downloaded={}, elapsed={:.1}s",
                    r.success, r.downloaded_bytes, elapsed_secs);
                Ok(r)
            }
            Err(e) => {
                let err_msg = e.to_string();
                eprintln!("[bt] download failed: {}", err_msg);
                Ok(DownloadResult {
                    success: false, total_bytes: 0, downloaded_bytes: 0, elapsed_secs,
                    success_chunks: 0, failed_chunks: 1, avg_speed_bps: 0,
                    error_msg: Some(err_msg),
                })
            }
        }
    }
}

impl BtDownloader {
    /// 使用全局 BtManager 的 Api 执行下载（推荐）
    /// 复用已预热的 DHT/LSD，自动注入 trackers
    pub async fn download_with_api(
        &self,
        api: &librqbit::api::Api,
        trackers: Vec<String>,
        url: &str,
        save_dir: &Path,
        timeout_secs: u64,
        dry_run: bool,
        progress_tx: mpsc::Sender<DownloadProgress>,
        cancel: CancellationToken,
    ) -> Result<DownloadResult> {
        let start = Instant::now();

        if !dry_run {
            tokio::fs::create_dir_all(save_dir).await
                .map_err(|e| CoreError::Internal(format!("create save dir: {}", e)))?;
        }

        eprintln!("[bt] starting download (BtManager mode, trackers={}), dry_run={}", trackers.len(), dry_run);

        let result = self.download_internal(
            api, trackers, url, save_dir, timeout_secs, dry_run, progress_tx, cancel.clone(),
        ).await;

        let elapsed_secs = start.elapsed().as_secs_f64();
        match result {
            Ok(r) => {
                eprintln!("[bt] download completed: success={}, downloaded={}, elapsed={:.1}s",
                    r.success, r.downloaded_bytes, elapsed_secs);
                Ok(r)
            }
            Err(e) => {
                let err_msg = e.to_string();
                eprintln!("[bt] download failed: {}", err_msg);
                Ok(DownloadResult {
                    success: false, total_bytes: 0, downloaded_bytes: 0, elapsed_secs,
                    success_chunks: 0, failed_chunks: 1, avg_speed_bps: 0,
                    error_msg: Some(err_msg),
                })
            }
        }
    }

    /// 创建独立 Session 和 Api（非 BtManager 模式）
    async fn create_session_api(&self, save_dir: &Path, dry_run: bool) -> Result<librqbit::api::Api> {
        eprintln!("[bt] step1: creating librqbit session, dry_run={}", dry_run);
        let mut session_opts = librqbit::SessionOptions::default();
        if let Some(ref mut dht) = session_opts.dht {
            dht.persistence = None;
        }
        if dry_run {
            session_opts.default_storage_factory = Some(NullStorageFactory.boxed());
            eprintln!("[bt] step1: using NullStorage (no disk writes)");
        }
        let session = librqbit::Session::new_with_opts(save_dir.to_path_buf(), session_opts)
            .await
            .map_err(|e| CoreError::Internal(format!("create session: {}", e)))?;
        eprintln!("[bt] step1: session created, waiting 3s for DHT/LSD init...");
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(librqbit::api::Api::new(session, None))
    }

    /// 默认公共 tracker 列表
    fn default_trackers() -> Vec<String> {
        vec![
            "udp://tracker.opentrackr.org:1337/announce".to_string(),
            "udp://open.stealth.si:80/announce".to_string(),
            "udp://tracker.torrent.eu.org:451/announce".to_string(),
            "udp://exodus.desync.com:6969/announce".to_string(),
            "udp://tracker.birkenwald.de:6969/announce".to_string(),
            "udp://tracker.moeking.me:6969/announce".to_string(),
            "udp://opentracker.i2p.rocks:6969/announce".to_string(),
            "udp://tracker.dler.org:6969/announce".to_string(),
        ]
    }

    async fn download_internal(
        &self,
        api: &librqbit::api::Api,
        trackers: Vec<String>,
        url: &str,
        save_dir: &Path,
        timeout_secs: u64,
        dry_run: bool,
        progress_tx: mpsc::Sender<DownloadProgress>,
        cancel: CancellationToken,
    ) -> Result<DownloadResult> {
        // 步骤 2：添加 torrent（使用传入的 trackers）
        eprintln!("[bt] step2: adding torrent: {}, trackers={}", url, trackers.len());
        let add_opts = librqbit::AddTorrentOptions {
            overwrite: true,
            peer_limit: Some(10000),
            force_tracker_interval: Some(Duration::from_secs(30)),
            trackers: Some(trackers),
            ..Default::default()
        };

        let add_result = if url.starts_with("magnet:") {
            api.api_add_torrent(librqbit::AddTorrent::Url(url.to_string().into()), Some(add_opts))
        } else if url.ends_with(".torrent") {
            let data = tokio::fs::read(url).await
                .map_err(|e| CoreError::Internal(format!("read torrent: {}", e)))?;
            eprintln!("[bt] step2: torrent file read, {} bytes", data.len());
            api.api_add_torrent(librqbit::AddTorrent::TorrentFileBytes(data.into()), Some(add_opts))
        } else {
            return Err(CoreError::InvalidParam(format!("unsupported BT URI: {}", url)));
        };

        let add_response = add_result.await
            .map_err(|e| CoreError::Internal(format!("add torrent: {}", e)))?;
        let torrent_id = add_response.id
            .ok_or_else(|| CoreError::Internal("torrent id is None".into()))?;
        eprintln!("[bt] step2: torrent added, id={}", torrent_id);

        // 步骤 3：等待 metadata
        eprintln!("[bt] step3: waiting for metadata...");
        let metadata_timeout = Duration::from_secs(timeout_secs);
        let metadata_start = Instant::now();
        let total_bytes = loop {
            if cancel.is_cancelled() {
                return Err(CoreError::Internal("cancelled waiting metadata".into()));
            }
            if metadata_start.elapsed() > metadata_timeout {
                return Err(CoreError::Internal("timeout waiting metadata".into()));
            }

            if let Ok(details) = api.api_torrent_details(librqbit::api::TorrentIdOrHash::Id(torrent_id)) {
                eprintln!("[bt] step3: poll, total_pieces={}, has_stats={}", details.total_pieces, details.stats.is_some());
                if let Some(ref stats) = details.stats {
                    eprintln!("[bt] step3: stats total={}, progress={}, state={:?}", stats.total_bytes, stats.progress_bytes, stats.state);
                    if stats.total_bytes > 0 {
                        break stats.total_bytes;
                    }
                }
                if details.total_pieces > 0 {
                    if let Some(ref files) = details.files {
                        let total: u64 = files.iter().map(|f| f.length).sum();
                        if total > 0 { break total; }
                    }
                }
            } else {
                eprintln!("[bt] step3: api_torrent_details returned Err");
            }

            tokio::time::sleep(Duration::from_millis(1000)).await;
        };

        eprintln!("[bt] step3: metadata ready, total_bytes={}", total_bytes);

        // 步骤 4：轮询下载进度
        let mut last_downloaded = 0u64;
        let mut last_time = Instant::now();
        let download_timeout = Duration::from_secs(timeout_secs);
        let download_start = Instant::now();
        #[allow(unused_assignments)]
        let mut final_progress_bytes = 0u64;
        #[allow(unused_assignments)]
        let mut final_finished = false;

        eprintln!("[bt] step4: starting download progress polling...");

        let mut last_diag = Instant::now();

        loop {
            if cancel.is_cancelled() {
                return Err(CoreError::Internal("download cancelled".into()));
            }
            if download_start.elapsed() > download_timeout {
                return Err(CoreError::Internal("download timeout".into()));
            }

            // 用 api_stats_v1 获取下载进度（api_torrent_details 的 stats 恒为 None）
            if let Ok(stats) = api.api_stats_v1(librqbit::api::TorrentIdOrHash::Id(torrent_id)) {
                let downloaded_bytes = stats.progress_bytes;
                final_progress_bytes = downloaded_bytes;
                final_finished = stats.finished;

                let now = Instant::now();
                let elapsed = now.duration_since(last_time).as_secs_f64();
                let delta = downloaded_bytes.saturating_sub(last_downloaded);
                let speed_bps = if elapsed > 0.5 {
                    (delta as f64 / elapsed) as u64
                } else { 0 };

                let percent = if total_bytes > 0 {
                    downloaded_bytes as f64 / total_bytes as f64 * 100.0
                } else { 0.0 };

                eprintln!("[bt] step4: progress={:.1}% ({}/{} bytes), speed={} B/s, finished={}, state={:?}",
                    percent, downloaded_bytes, total_bytes, speed_bps, stats.finished, stats.state);

                let progress = DownloadProgress {
                    downloaded_bytes, total_bytes, speed_bps,
                    active_connections: 0,
                    percent,
                    elapsed_secs: download_start.elapsed().as_secs_f64(),
                };

                if progress_tx.send(progress).await.is_err() {
                    warn!("progress channel closed");
                    break;
                }

                last_downloaded = downloaded_bytes;
                last_time = now;

                if stats.finished || (total_bytes > 0 && downloaded_bytes >= total_bytes) {
                    eprintln!("[bt] step4: download finished!");
                    break;
                }
            } else {
                eprintln!("[bt] step4: api_stats_v1 returned Err (torrent may still be initializing)");
            }

            // 诊断：peer 连接状态和 DHT（每 5 秒）
            if last_diag.elapsed() > Duration::from_secs(5) {
                last_diag = Instant::now();
                match api.api_peer_stats(
                    TorrentIdOrHash::Id(torrent_id),
                    PeerStatsFilter::default(),
                ) {
                    Ok(peer_stats) => {
                        let total = peer_stats.peers.len();
                        let live = peer_stats.peers.values().filter(|p| p.state == "live").count();
                        let fetched: u64 = peer_stats.peers.values().map(|p| p.counters.fetched_bytes).sum();
                        let errors: u32 = peer_stats.peers.values().map(|p| p.counters.errors).sum();
                        let conn_attempts: u32 = peer_stats.peers.values().map(|p| p.counters.connection_attempts).sum();
                        eprintln!("[bt] diag: peers={}, live={}, fetched={}B, errors={}, conn_attempts={}",
                            total, live, fetched, errors, conn_attempts);
                        for (i, (id, stats)) in peer_stats.peers.iter().take(5).enumerate() {
                            eprintln!("[bt] diag peer[{}]: id={}, state={}, fetched={}B, err={}, client={:?}",
                                i, id, stats.state, stats.counters.fetched_bytes, stats.counters.errors, stats.client_name);
                        }
                    }
                    Err(e) => eprintln!("[bt] diag: api_peer_stats failed: {}", e),
                }
                match api.api_dht_stats() {
                    Ok(dht) => eprintln!("[bt] diag: dht={:?}", dht),
                    Err(e) => eprintln!("[bt] diag: dht failed: {}", e),
                }
            }

            tokio::time::sleep(Duration::from_millis(2000)).await;
        }

        let elapsed_secs = download_start.elapsed().as_secs_f64();
        let downloaded_bytes = final_progress_bytes;
        let success = final_finished || (total_bytes > 0 && downloaded_bytes >= total_bytes);
        let avg_speed_bps = if elapsed_secs > 0.0 {
            (downloaded_bytes as f64 / elapsed_secs) as u64
        } else { 0 };

        eprintln!("[bt] final: success={}, downloaded={}/{}, elapsed={:.1}s, avg_speed={} B/s",
            success, downloaded_bytes, total_bytes, elapsed_secs, avg_speed_bps);

        Ok(DownloadResult {
            success, total_bytes, downloaded_bytes, elapsed_secs,
            success_chunks: if success { 1 } else { 0 },
            failed_chunks: if success { 0 } else { 1 },
            avg_speed_bps,
            error_msg: if success { None } else { Some("incomplete".into()) },
        })
    }
}
