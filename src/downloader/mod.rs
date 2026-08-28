//! 下载器抽象层 — 多后端统一调度、进度回调、任务控制

use anyhow::{anyhow, Result};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ──────────────────────────────────────────────
// 任务参数
// ──────────────────────────────────────────────

/// 统一下载任务参数
#[derive(Debug, Clone)]
pub struct DownloadTask {
    /// 资源 URI（http:// / https:// / ftp:// / file:// / magnet: 等）
    pub uri: String,
    /// 保存路径
    pub save_path: PathBuf,
    /// 最大并发连接数（0 = 自动）
    pub max_conn: u32,
    /// 速度限制（字节/秒，0 = 不限）
    pub speed_limit: u64,
    /// 任务唯一标识
    pub task_id: String,
    /// 分片大小（字节，0 = 自动 4MB）
    pub chunk_size: u64,
    /// 重试次数
    pub retry_times: u32,
    /// 自定义 HTTP Headers
    pub headers: Vec<(String, String)>,
    /// 代理地址（空 = 不使用）
    pub proxy: String,
    /// 跳过 TLS 证书校验
    pub skip_tls_verify: bool,
    /// 干跑模式（不写盘）
    pub dry_run: bool,
    /// 进度回调间隔
    pub progress_interval: Duration,
}

impl Default for DownloadTask {
    fn default() -> Self {
        Self {
            uri: String::new(),
            save_path: PathBuf::new(),
            max_conn: 0,
            speed_limit: 0,
            task_id: String::new(),
            chunk_size: 0,
            retry_times: 3,
            headers: Vec::new(),
            proxy: String::new(),
            skip_tls_verify: false,
            dry_run: false,
            progress_interval: Duration::from_millis(500),
        }
    }
}

impl DownloadTask {
    pub fn new(uri: impl Into<String>, save_path: impl Into<PathBuf>) -> Self {
        Self {
            uri: uri.into(),
            save_path: save_path.into(),
            task_id: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        }
    }

    /// 有效并发数（自动模式根据文件大小估算，此处返回默认 8）
    pub fn effective_connections(&self) -> u32 {
        if self.max_conn > 0 {
            self.max_conn
        } else {
            8
        }
    }

    /// 有效分片大小
    pub fn effective_chunk_size(&self) -> u64 {
        if self.chunk_size > 0 {
            self.chunk_size
        } else {
            4 * 1024 * 1024 // 4MB
        }
    }
}

// ──────────────────────────────────────────────
// 输出指标
// ──────────────────────────────────────────────

/// 下载完成/中断输出指标
#[derive(Debug, Clone)]
pub struct DownloadOutput {
    pub total_size: u64,
    pub downloaded_bytes: u64,
    pub success_chunks: u32,
    pub failed_chunks: u32,
    pub is_success: bool,
    pub error_msg: Option<String>,
    pub elapsed_secs: f64,
    pub avg_speed_mbps: f64,
    pub status: String,
}

impl Default for DownloadOutput {
    fn default() -> Self {
        Self {
            total_size: 0,
            downloaded_bytes: 0,
            success_chunks: 0,
            failed_chunks: 0,
            is_success: false,
            error_msg: None,
            elapsed_secs: 0.0,
            avg_speed_mbps: 0.0,
            status: String::new(),
        }
    }
}

// ──────────────────────────────────────────────
// 进度回调
// ──────────────────────────────────────────────

/// 实时进度快照
#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    pub task_id: String,
    pub total_size: u64,
    pub downloaded_bytes: u64,
    pub speed_bps: u64,
    pub active_connections: u32,
    pub percent: f64,
    pub elapsed_secs: f64,
}

/// 进度回调 trait
pub trait ProgressCallback: Send + Sync {
    fn on_progress(&self, snapshot: ProgressSnapshot);
    fn on_complete(&self, output: DownloadOutput);
}

/// 简单的 stderr 进度打印回调
pub struct StderrProgress {
    name: String,
}

impl StderrProgress {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl ProgressCallback for StderrProgress {
    fn on_progress(&self, s: ProgressSnapshot) {
        let mb_done = s.downloaded_bytes as f64 / 1024.0 / 1024.0;
        let mb_total = s.total_size as f64 / 1024.0 / 1024.0;
        let speed_mb = s.speed_bps as f64 / 1024.0 / 1024.0;
        eprintln!(
            "[progress] {}: {:.1}% ({:.1}/{:.1} MB) speed: {:.1} MB/s conns: {}",
            self.name, s.percent, mb_done, mb_total, speed_mb, s.active_connections
        );
    }

    fn on_complete(&self, o: DownloadOutput) {
        if o.is_success {
            eprintln!(
                "[done] {}: {:.1} MB in {:.1}s, avg: {:.1} MB/s",
                self.name,
                o.total_size as f64 / 1024.0 / 1024.0,
                o.elapsed_secs,
                o.avg_speed_mbps
            );
        } else {
            eprintln!(
                "[error] {}: failed after {:.1}s: {}",
                self.name,
                o.elapsed_secs,
                o.error_msg.unwrap_or_else(|| "unknown".into())
            );
        }
    }
}

// ──────────────────────────────────────────────
// 任务控制器（暂停/取消）
// ──────────────────────────────────────────────

/// 下载任务控制器：支持暂停、恢复、取消
/// 可克隆（Arc 内部），在下载循环中定期检查状态
#[derive(Clone)]
pub struct DownloadController {
    paused: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

impl Default for DownloadController {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadController {
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
        // 取消时也解除暂停，让等待中的循环能检测到取消
        self.paused.store(false, Ordering::SeqCst);
    }

    /// 是否暂停
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// 是否已取消
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// 如果暂停则等待，直到恢复或取消。返回 false 表示被取消。
    pub async fn wait_if_paused(&self) -> bool {
        while self.is_paused() {
            if self.is_cancelled() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        !self.is_cancelled()
    }

    /// 检查是否应该继续下载（未取消）。返回 false 表示应终止。
    pub fn should_continue(&self) -> bool {
        !self.is_cancelled()
    }
}

// ──────────────────────────────────────────────
// 后端抽象 Trait
// ──────────────────────────────────────────────

/// 下载后端抽象 Trait
#[async_trait::async_trait]
pub trait DownloadBackend: Send + Sync {
    /// 后端名称
    fn name(&self) -> &str;
    /// 是否支持该 URI
    fn support_uri(&self, uri: &str) -> bool;
    /// 执行下载
    async fn run(
        &self,
        task: DownloadTask,
        progress: Option<Arc<dyn ProgressCallback>>,
        controller: Option<Arc<DownloadController>>,
    ) -> Result<DownloadOutput>;
    /// 停止任务（默认空实现）
    async fn stop(&self, _task_id: &str) -> Result<()> {
        Ok(())
    }
}

// ──────────────────────────────────────────────
// 下载管理器
// ──────────────────────────────────────────────

/// 下载管理器：注册多个后端，自动路由
pub struct DownloadManager {
    backends: Vec<Arc<dyn DownloadBackend>>,
}

impl Default for DownloadManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadManager {
    pub fn new() -> Self {
        Self {
            backends: Vec::new(),
        }
    }

    /// 注册后端（先注册的优先级高）
    pub fn register_backend<B: DownloadBackend + 'static>(&mut self, backend: B) {
        self.backends.push(Arc::new(backend));
    }

    pub fn register_backend_arc(&mut self, backend: Arc<dyn DownloadBackend>) {
        self.backends.push(backend);
    }

    /// 查找匹配的后端
    pub fn find_backend(&self, uri: &str) -> Option<&Arc<dyn DownloadBackend>> {
        self.backends.iter().find(|b| b.support_uri(uri))
    }

    /// 调度下载任务
    pub async fn dispatch(
        &self,
        task: DownloadTask,
        progress: Option<Arc<dyn ProgressCallback>>,
        controller: Option<Arc<DownloadController>>,
    ) -> Result<DownloadOutput> {
        let uri = task.uri.clone();
        let backend = self
            .backends
            .iter()
            .find(|b| b.support_uri(&uri))
            .ok_or_else(|| anyhow!("没有匹配的下载后端，uri:{}", uri))?;
        backend.run(task, progress, controller).await
    }

    /// 已注册后端列表
    pub fn backend_names(&self) -> Vec<&str> {
        self.backends.iter().map(|b| b.name()).collect()
    }
}

// ──────────────────────────────────────────────
// 内置后端导出
// ──────────────────────────────────────────────

pub mod file_impl;
pub mod http_impl;
pub mod ssh_impl;

#[cfg(feature = "ftp")]
pub mod ftp_impl;

#[cfg(feature = "torrent")]
pub mod torrent_impl;

pub use file_impl::FileDownloader;
pub use http_impl::HttpDownloader;
pub use ssh_impl::SshDownloader;

#[cfg(feature = "ftp")]
pub use ftp_impl::FtpDownloader;

#[cfg(feature = "torrent")]
pub use torrent_impl::TorrentDownloader;

/// 构建带全部默认后端的 DownloadManager
pub fn build_default_manager() -> DownloadManager {
    let mut mgr = DownloadManager::new();
    mgr.register_backend(HttpDownloader::new());
    mgr.register_backend(SshDownloader::new());
    mgr.register_backend(FileDownloader::new());
    #[cfg(feature = "ftp")]
    mgr.register_backend(FtpDownloader::new());
    #[cfg(feature = "torrent")]
    mgr.register_backend(TorrentDownloader::new());
    mgr
}
