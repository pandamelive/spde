//! Torrent Piece Fetcher（BitTorrent 原生下载器）
//!
//! 基于 librqbit（纯 Rust BT 客户端库），支持：
//! - 磁力链接和 .torrent 文件
//! - DHT / PEX / tracker peer 发现
//! - piece 级分片下载
//! - 多 peer 并发
//! - 断点续传（.part 文件）
//!
//! 设计说明：
//! - 实现 ChunkFetcher trait，调度器协议无关
//! - fetch_chunk 从 BT 网络获取指定 piece
//! - probe 解析磁力链接/种子文件，获取文件大小和 piece 数量
//! - 内部管理 librqbit Session、peer 连接、piece 调度

use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use pandanetos::error::{CoreError, Result};

use crate::domain::chunk_fetcher::{ChunkFetcher, ChunkStats, SourceCapabilities};

/// BT 下载状态（共享）
#[derive(Debug)]
struct TorrentState {
    /// 是否已经初始化
    initialized: bool,
    /// 文件大小（字节）
    file_size: u64,
    /// piece 大小（字节）
    piece_size: u64,
    /// 总 piece 数量
    total_pieces: u32,
    /// 已下载的 piece 集合
    downloaded_pieces: std::collections::HashSet<u32>,
    /// 保存目录
    save_dir: PathBuf,
    /// librqbit Session（延迟初始化）
    #[cfg(feature = "torrent")]
    session: Option<Arc<librqbit::Session>>,
    /// librqbit Api（延迟初始化）
    #[cfg(feature = "torrent")]
    api: Option<librqbit::Api>,
    /// torrent ID（添加后获取）
    torrent_id: Option<usize>,
}

impl TorrentState {
    fn new(save_dir: PathBuf) -> Self {
        Self {
            initialized: false,
            file_size: 0,
            piece_size: 0,
            total_pieces: 0,
            downloaded_pieces: std::collections::HashSet::new(),
            save_dir,
            #[cfg(feature = "torrent")]
            session: None,
            #[cfg(feature = "torrent")]
            api: None,
            torrent_id: None,
        }
    }
}

/// Torrent Piece Fetcher
#[derive(Debug, Clone)]
pub struct TorrentPieceFetcher {
    /// 原始 URI（磁力链接或 .torrent 文件路径）
    uri: String,
    /// 保存目录
    save_dir: PathBuf,
    /// 超时时间（秒）
    timeout_secs: u64,
    /// 是否 dry_run（不落盘）
    dry_run: bool,
    /// 共享状态
    state: Arc<Mutex<TorrentState>>,
}

impl TorrentPieceFetcher {
    /// 创建新的 Torrent Piece Fetcher
    pub fn new(
        uri: impl Into<String>,
        save_dir: impl Into<PathBuf>,
        timeout_secs: u64,
        dry_run: bool,
    ) -> Self {
        let save_dir = save_dir.into();
        Self {
            uri: uri.into(),
            save_dir: save_dir.clone(),
            timeout_secs,
            dry_run,
            state: Arc::new(Mutex::new(TorrentState::new(save_dir))),
        }
    }

    /// 从磁力链接中提取 info hash
    fn extract_info_hash(&self) -> Option<String> {
        if self.uri.starts_with("magnet:") {
            if let Some(hash) = self.uri.split("xt=urn:btih:").nth(1) {
                if let Some(end) = hash.find('&') {
                    return Some(hash[..end].to_string());
                }
                return Some(hash.to_string());
            }
        }
        None
    }

    /// 初始化 BT 下载（创建 librqbit Session，添加 torrent）
    #[cfg(feature = "torrent")]
    async fn ensure_initialized(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.initialized {
            return Ok(());
        }

        info!(uri = %self.uri, "initializing BT download with librqbit");

        // 确保保存目录存在
        if !self.dry_run {
            tokio::fs::create_dir_all(&state.save_dir)
                .await
                .map_err(|e| CoreError::IO(format!("failed to create save dir: {}", e)))?;
        }

        // 创建 librqbit Session
        let session = librqbit::Session::new(state.save_dir.clone())
            .await
            .map_err(|e| CoreError::Network(format!("failed to create librqbit session: {}", e)))?;

        // 创建 Api
        let api = librqbit::Api::new(session.clone());

        // 添加 torrent
        let add_result = if self.uri.starts_with("magnet:") {
            api.api_add_torrent(
                librqbit::AddTorrent::Url {
                    url: self.uri.clone(),
                    overwrite: false,
                    only_files_regex: None,
                    only_files: None,
                },
                None,
            )
            .await
        } else if self.uri.ends_with(".torrent") {
            let data = tokio::fs::read(&self.uri)
                .await
                .map_err(|e| CoreError::IO(format!("failed to read torrent file: {}", e)))?;
            api.api_add_torrent(
                librqbit::AddTorrent::TorrentFile {
                    content: data.into(),
                    overwrite: false,
                    only_files_regex: None,
                    only_files: None,
                },
                None,
            )
            .await
        } else {
            return Err(CoreError::InvalidParam(
                format!("unsupported torrent URI: {}", self.uri),
            ));
        };

        let torrent_id = add_result
            .map_err(|e| CoreError::Network(format!("failed to add torrent: {}", e)))?
            .torrent_id;

        info!(torrent_id = torrent_id, "torrent added successfully");

        // 等待 metadata 下载完成
        let metadata_timeout = Duration::from_secs(self.timeout_secs);
        let start = Instant::now();

        loop {
            if start.elapsed() > metadata_timeout {
                return Err(CoreError::Timeout(
                    "timeout waiting for torrent metadata".into(),
                ));
            }

            if let Ok(details) = api.api_torrent_details(torrent_id.into()) {
                if let Some(info) = details.info {
                    state.file_size = info.total_bytes as u64;
                    state.piece_size = info.piece_length as u64;
                    state.total_pieces = info.num_pieces as u32;
                    break;
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        state.session = Some(session);
        state.api = Some(api);
        state.torrent_id = Some(torrent_id);
        state.initialized = true;

        info!(
            file_size = state.file_size,
            piece_size = state.piece_size,
            total_pieces = state.total_pieces,
            "BT download initialized"
        );

        Ok(())
    }

    /// 非 torrent feature 时的初始化（使用默认值）
    #[cfg(not(feature = "torrent"))]
    async fn ensure_initialized(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.initialized {
            return Ok(());
        }

        warn!("torrent feature not enabled, using default values");
        state.file_size = 0;
        state.piece_size = 4 * 1024 * 1024;
        state.total_pieces = 0;
        state.initialized = true;
        Ok(())
    }

    /// 下载指定 piece
    #[cfg(feature = "torrent")]
    async fn download_piece(
        &self,
        piece_index: u32,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<u64> {
        let state = self.state.lock().await;
        let api = state
            .api
            .as_ref()
            .ok_or_else(|| CoreError::NotInitialized("BT not initialized".into()))?;
        let torrent_id = state
            .torrent_id
            .ok_or_else(|| CoreError::NotInitialized("torrent not added".into()))?;

        let piece_size = if piece_index == state.total_pieces - 1 {
            state.file_size - (piece_index as u64 * state.piece_size)
        } else {
            state.piece_size
        };

        drop(state);

        debug!(
            piece_index = piece_index,
            piece_size = piece_size,
            "downloading BT piece via librqbit"
        );

        // 使用流式 API 下载文件
        let file_id = 0;
        let mut stream = api
            .api_stream(torrent_id.into(), file_id)
            .await
            .map_err(|e| CoreError::Network(format!("failed to create torrent stream: {}", e)))?;

        // 跳过前面的 piece
        use tokio::io::AsyncReadExt;
        let skip_bytes = piece_index as u64 * piece_size;
        if skip_bytes > 0 {
            let mut skipped = 0u64;
            let mut skip_buf = vec![0u8; 64 * 1024];
            while skipped < skip_bytes {
                let to_read = ((skip_bytes - skipped) as usize).min(skip_buf.len());
                let n = tokio::time::timeout(
                    Duration::from_secs(self.timeout_secs),
                    stream.read(&mut skip_buf[..to_read]),
                )
                .await
                .map_err(|_| CoreError::Timeout("BT stream read timeout".into()))?
                .map_err(|e| CoreError::Network(format!("BT stream read error: {}", e)))?;
                if n == 0 {
                    break;
                }
                skipped += n as u64;
            }
        }

        // 读取当前 piece
        let mut downloaded = 0u64;
        let mut buf = vec![0u8; 64 * 1024];

        while downloaded < piece_size {
            let to_read = ((piece_size - downloaded) as usize).min(buf.len());
            let n = tokio::time::timeout(
                Duration::from_secs(self.timeout_secs),
                stream.read(&mut buf[..to_read]),
            )
            .await
            .map_err(|_| CoreError::Timeout("BT piece read timeout".into()))?
            .map_err(|e| CoreError::Network(format!("BT piece read error: {}", e)))?;

            if n == 0 {
                break;
            }

            writer
                .write_all(&buf[..n])
                .await
                .map_err(|e| CoreError::IO(format!("write error: {}", e)))?;

            downloaded += n as u64;
        }

        debug!(
            piece_index = piece_index,
            downloaded = downloaded,
            "BT piece download completed"
        );

        Ok(downloaded)
    }

    /// 非 torrent feature 时的下载（dry_run 模拟）
    #[cfg(not(feature = "torrent"))]
    async fn download_piece(
        &self,
        piece_index: u32,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<u64> {
        let state = self.state.lock().await;
        let piece_size = state.piece_size;
        drop(state);

        if self.dry_run {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let dummy = vec![0u8; piece_size as usize];
            writer
                .write_all(&dummy)
                .await
                .map_err(|e| CoreError::IO(format!("write error: {}", e)))?;
            return Ok(piece_size);
        }

        Err(CoreError::NotImplemented(
            "torrent feature not enabled, enable 'torrent' feature in Cargo.toml".into(),
        ))
    }
}

#[async_trait]
impl ChunkFetcher for TorrentPieceFetcher {
    fn protocol(&self) -> &str {
        "torrent"
    }

    fn identifier(&self) -> String {
        if let Some(hash) = self.extract_info_hash() {
            format!("torrent:{}", hash)
        } else {
            format!("torrent:{}", self.uri)
        }
    }

    fn display_name(&self) -> String {
        if self.uri.starts_with("magnet:") {
            "BitTorrent (磁力链接)".to_string()
        } else if self.uri.ends_with(".torrent") {
            format!("BitTorrent (种子文件: {})", self.uri)
        } else {
            format!("BitTorrent ({})", self.uri)
        }
    }

    async fn probe(&self) -> Result<(u64, SourceCapabilities)> {
        self.ensure_initialized().await?;

        let state = self.state.lock().await;
        let capabilities = SourceCapabilities {
            supports_range: true,
            supports_multi_connection: true,
            supports_resume: true,
            immutable: true,
            max_concurrency: 16,
            chunk_size_range: Some((state.piece_size, state.piece_size)),
            protocol: "torrent",
        };

        debug!(
            file_size = state.file_size,
            piece_size = state.piece_size,
            total_pieces = state.total_pieces,
            "BT probe completed"
        );

        Ok((state.file_size, capabilities))
    }

    async fn fetch_chunk(
        &self,
        offset: u64,
        length: u64,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<ChunkStats> {
        self.ensure_initialized().await?;

        let start = Instant::now();
        let state = self.state.lock().await;
        let piece_size = state.piece_size;
        drop(state);

        if offset % piece_size != 0 {
            return Err(CoreError::InvalidParam(format!(
                "BT offset must be piece-aligned: offset={}, piece_size={}",
                offset, piece_size
            )));
        }

        let piece_index = (offset / piece_size) as u32;
        let downloaded = self.download_piece(piece_index, writer).await?;

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let speed_bps = if elapsed_ms > 0 {
            downloaded * 1000 / elapsed_ms
        } else {
            0
        };

        let mut state = self.state.lock().await;
        state.downloaded_pieces.insert(piece_index);
        drop(state);

        Ok(ChunkStats {
            chunk_id: piece_index,
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
