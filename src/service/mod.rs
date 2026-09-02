//! 服务层（智能调度核心，协议无关）
//!
//! 遵循 PandaNetOS 四层架构：service 层负责业务逻辑编排，
//! 只依赖 domain 层的抽象，不依赖具体协议实现。

pub mod adaptive;
pub mod bandwidth_scheduler;
pub mod cdn_throttle;
pub mod chunk_scheduler;
pub mod controller;
pub mod mirror_bus;
pub mod progress;
pub mod progress_smoother;
pub mod scheduler;
pub mod source_manager;
pub mod strategy;

pub use adaptive::{AdaptiveConfig, AdaptiveController, AdaptiveStats};
pub use bandwidth_scheduler::{
    BandwidthScheduler, BandwidthSchedulerConfig, BandwidthStrategy, TaskInfo,
};
pub use cdn_throttle::{CdnThrottleConfig, CdnThrottleDetector, CdnThrottleStats};
pub use chunk_scheduler::ChunkScheduler;
pub use controller::DownloadController;
pub use mirror_bus::MirrorBus;
pub use progress::ProgressSmoother;
pub use progress_smoother::{
    ProgressSmoother as NewProgressSmoother, ProgressSmootherConfig, SmoothProgress,
};
pub use scheduler::DownloadScheduler;
pub use source_manager::SourceManager;
pub use strategy::multi_source_chunked::MultiSourceChunkedStrategy;
