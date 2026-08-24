//! 下载器抽象层
use anyhow::{anyhow, Result};
use std::path::PathBuf;
use std::sync::Arc;

/// 统一下载任务参数
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub uri: String,
    pub save_path: PathBuf,
    pub max_conn: u32,
    pub speed_limit: u64,
    pub task_id: String,
}

/// 下载完成/中断输出指标
#[derive(Debug, Clone)]
pub struct DownloadOutput {
    pub total_size: u64,
    pub downloaded_bytes: u64,
    pub success_chunks: u32,
    pub failed_chunks: u32,
    pub is_success: bool,
    pub error_msg: Option<String>,
}

/// 下载后端抽象Trait
#[async_trait::async_trait]
pub trait DownloadBackend: Send + Sync {
    fn support_uri(&self, uri: &str) -> bool;
    async fn run(&self, task: DownloadTask) -> Result<DownloadOutput>;
    async fn stop(&self, task_id: &str) -> Result<()>;
}

/// 下载管理器：注册多个后端，自动路由
#[derive(Default)]
pub struct DownloadManager {
    backends: Vec<Arc<dyn DownloadBackend>>,
}

impl DownloadManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_backend<B: DownloadBackend + 'static>(&mut self, backend: B) {
        self.backends.push(Arc::new(backend));
    }

    pub async fn dispatch(&self, task: DownloadTask) -> Result<DownloadOutput> {
        let uri = task.uri.as_str();
        let backend = self
            .backends
            .iter()
            .find(|b| b.support_uri(uri))
            .ok_or_else(|| anyhow!("没有匹配的下载后端，uri:{}", uri))?;
        backend.run(task).await
    }
}

pub mod http_impl;

// ----------------重点修改----------------
// 当前文件内定义的类型，直接pub，不要大括号{}语法！
pub use http_impl::HttpDownloader;
