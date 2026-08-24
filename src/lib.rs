//! SPDE lib：下载内核，无文件IO，纯内存逻辑

pub mod model {
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    /// 下载任务指标，内核输出，不碰磁盘
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TaskMetrics {
        pub task_name: String,
        pub total_size: u64,
        pub downloaded_bytes: u64,
        pub success_chunks: u32,
        pub failed_chunks: u32,
        pub is_success: bool,
        pub error_msg: Option<String>,
    }

    /// 事件公共字段
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct EventMeta {
        pub node_id: Uuid,
        pub instance_id: Uuid,
        pub unix_ts: i64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "event_kind")]
    pub enum SpdeEvent {
        InstanceStart { meta: EventMeta, version: String },
        TaskRun { meta: EventMeta, metrics: TaskMetrics },
        InstanceExit { meta: EventMeta, normal_exit: bool },
    }
}

pub mod downloader;

// 对外导出，上层直接 use spde::{xxx}
pub use model::{EventMeta, SpdeEvent, TaskMetrics};
pub use downloader::{DownloadOption, run_download};
