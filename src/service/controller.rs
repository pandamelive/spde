//! 下载任务控制器
//!
//! 提供任务的暂停、恢复、取消功能。
//! 从旧架构迁移而来，保留兼容接口。
//! 后续可以与 `CancellationToken` 整合。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// 下载任务控制器
///
/// 用于外部控制下载任务的状态：暂停、恢复、取消。
/// 每个运行中的任务持有一个 `Arc<DownloadController>`，
/// 外部可以通过它来控制任务状态。
#[derive(Debug, Clone)]
pub struct DownloadController {
    /// 是否暂停
    paused: Arc<AtomicBool>,
    /// 是否取消
    cancelled: Arc<AtomicBool>,
}

impl Default for DownloadController {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadController {
    /// 创建新的下载控制器
    pub fn new() -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 暂停任务（下载循环会等待，直到恢复或取消）
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// 恢复任务
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    /// 取消任务（下载循环会返回错误，任务终止）
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// 检查任务是否已暂停
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// 检查任务是否已取消
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// 转换为 `CancellationToken`（用于新架构的下载器）
    ///
    /// 注意：这只是一个适配方法，`CancellationToken` 只能取消，不能暂停/恢复。
    /// 如果需要暂停/恢复功能，应该继续使用 `DownloadController`。
    pub fn as_cancel_token(&self) -> pandanetos::domain::CancellationToken {
        let token = pandanetos::domain::CancellationToken::new();
        let controller = self.clone();
        let token_clone = token.clone();
        tokio::spawn(async move {
            loop {
                if controller.is_cancelled() {
                    token_clone.cancel();
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });
        token
    }
}
