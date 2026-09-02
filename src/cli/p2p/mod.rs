//! P2P 协议独立下载模块
//!
//! BT、电驴（ed2k）等 P2P 协议有自己的分片机制和调度逻辑，
//! 不适合通用 ChunkScheduler 的随机访问分片模型。
//! 本模块为 P2P 协议提供独立的下载入口，每个协议自己管理下载过程，
//! 外部只负责初始化、轮询进度、获取结果。
//!
//! # 扩展新协议
//!
//! 1. 在本模块下创建新文件（如 `ed2k.rs`）
//! 2. 实现 `P2PDownloader` trait
//! 3. 在 `download_p2p` 函数中添加协议分支
//! 4. 在 CLI 层的协议识别后调用 `download_p2p`

use std::path::Path;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::info;

use pandanetos::domain::{CancellationToken, DownloadProgress, DownloadResult};
use pandanetos::error::{CoreError, Result};

use crate::cli::new_download::ProtocolType;

pub mod bt;
pub mod manager;

/// P2P 下载器 trait（BT、电驴等协议实现此接口）
#[async_trait]
pub trait P2PDownloader: Send + Sync {
    /// 协议名称
    fn protocol_name(&self) -> &'static str;

    /// 执行完整下载
    ///
    /// # 参数
    /// - url: 下载链接（磁力链接/种子文件路径/ed2k 链接等）
    /// - save_dir: 保存目录
    /// - timeout_secs: 超时时间（秒）
    /// - dry_run: 是否 dry_run（不落盘）
    /// - progress_tx: 进度汇报通道
    /// - cancel: 取消令牌
    ///
    /// # 返回
    /// 下载结果
    async fn download(
        &self,
        url: &str,
        save_dir: &Path,
        timeout_secs: u64,
        dry_run: bool,
        progress_tx: mpsc::Sender<DownloadProgress>,
        cancel: CancellationToken,
    ) -> Result<DownloadResult>;
}

/// P2P 下载统一入口
///
/// 根据协议类型选择对应的下载器执行下载。
/// 非 P2P 协议不应调用此函数。
///
/// # 参数
/// - manager: 可选的全局 BtManager（传入则复用预热的 DHT/LSD 和 trackers）
pub async fn download_p2p(
    protocol: ProtocolType,
    url: &str,
    save_dir: &Path,
    timeout_secs: u64,
    dry_run: bool,
    progress_tx: mpsc::Sender<DownloadProgress>,
    cancel: CancellationToken,
    manager: Option<&manager::BtManager>,
) -> Result<DownloadResult> {
    match protocol {
        ProtocolType::Torrent | ProtocolType::Magnet => {
            if let Some(mgr) = manager {
                info!(protocol = "bittorrent", url = %url, "starting P2P download via BtManager");
                mgr.download(url, save_dir, timeout_secs, dry_run, progress_tx, cancel).await
            } else {
                let downloader = bt::BtDownloader::new();
                info!(protocol = downloader.protocol_name(), url = %url, "starting P2P download (standalone)");
                downloader.download(url, save_dir, timeout_secs, dry_run, progress_tx, cancel).await
            }
        }
        // 未来电驴协议：ProtocolType::Ed2k => ...
        _ => {
            Err(CoreError::InvalidParam(format!(
                "unsupported P2P protocol: {:?}",
                protocol
            )))
        }
    }
}
