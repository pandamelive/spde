//! 分片下载内核，纯内存，不操作磁盘
use crate::lib::model::TaskMetrics;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct DownloadOption {
    pub url: String,
    pub proxy: Option<String>,
    pub max_concurrent: u32,
    pub retry: u32,
    pub timeout: Duration,
}

/// 模拟下载，后续替换真实reqwest分片逻辑
pub async fn run_download(task_name: &str, opt: DownloadOption) -> TaskMetrics {
    // TODO: 实现真实HTTP分片下载
    eprintln!("download kernel stub task={} url={}", task_name, opt.url);

    TaskMetrics {
        task_name: task_name.to_string(),
        total_size: 0,
        downloaded_bytes: 0,
        success_chunks: 0,
        failed_chunks: 0,
        is_success: false,
        error_msg: Some("downloader not implemented".to_string()),
    }
}
