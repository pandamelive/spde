//! 全局带宽调度器
//!
//! 管理多个下载任务的全局带宽分配，
//! 避免单个任务占满所有带宽，导致其他任务饥饿。
//!
//! 支持策略：
//! - 公平分配：每个任务平均分配带宽
//! - 优先级分配：高优先级任务获得更多带宽
//! - 最小保证：每个任务至少获得一定带宽
//! - 动态调整：根据任务进度和速度动态调整

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tracing::{debug, info};
use uuid::Uuid;

/// 带宽分配策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BandwidthStrategy {
    /// 公平分配（每个任务平均分配）
    Fair,
    /// 优先级分配（高优先级获得更多）
    Priority,
    /// 最小保证（每个任务至少获得 min_bandwidth）
    MinGuarantee,
    /// 动态调整（根据进度和速度）
    Dynamic,
}

impl Default for BandwidthStrategy {
    fn default() -> Self {
        BandwidthStrategy::Fair
    }
}

/// 任务信息
#[derive(Debug, Clone)]
pub struct TaskInfo {
    /// 任务 ID
    pub id: Uuid,
    /// 优先级（0-10，越高越优先）
    pub priority: u8,
    /// 当前速度（字节/秒）
    pub current_speed: f64,
    /// 已下载字节数
    pub downloaded: u64,
    /// 总字节数
    pub total: u64,
    /// 分配的带宽（字节/秒）
    pub allocated_bandwidth: f64,
    /// 最后更新时间
    pub last_update: Instant,
}

impl TaskInfo {
    pub fn new(id: Uuid, priority: u8) -> Self {
        Self {
            id,
            priority,
            current_speed: 0.0,
            downloaded: 0,
            total: 0,
            allocated_bandwidth: 0.0,
            last_update: Instant::now(),
        }
    }

    /// 计算进度百分比
    pub fn progress(&self) -> f64 {
        if self.total > 0 {
            self.downloaded as f64 / self.total as f64 * 100.0
        } else {
            0.0
        }
    }
}

/// 全局带宽调度器配置
#[derive(Debug, Clone)]
pub struct BandwidthSchedulerConfig {
    /// 总带宽上限（字节/秒，0 表示不限制）
    pub total_bandwidth: u64,
    /// 分配策略
    pub strategy: BandwidthStrategy,
    /// 每个任务最小带宽（字节/秒）
    pub min_bandwidth_per_task: u64,
    /// 每个任务最大带宽（字节/秒，0 表示不限制）
    pub max_bandwidth_per_task: u64,
    /// 调整间隔（毫秒）
    pub adjust_interval_ms: u64,
    /// 最大并发任务数
    pub max_concurrent_tasks: usize,
}

impl Default for BandwidthSchedulerConfig {
    fn default() -> Self {
        Self {
            total_bandwidth: 0, // 不限制
            strategy: BandwidthStrategy::Fair,
            min_bandwidth_per_task: 1024 * 1024, // 1MB/s
            max_bandwidth_per_task: 0,
            adjust_interval_ms: 1000,
            max_concurrent_tasks: 4,
        }
    }
}

/// 全局带宽调度器
#[derive(Clone)]
pub struct BandwidthScheduler {
    /// 配置
    config: Arc<BandwidthSchedulerConfig>,
    /// 任务表
    tasks: Arc<Mutex<HashMap<Uuid, TaskInfo>>>,
    /// 总已下载字节数
    total_downloaded: Arc<AtomicU64>,
    /// 上次调整时间
    last_adjust: Arc<Mutex<Instant>>,
}

impl BandwidthScheduler {
    /// 创建新的全局带宽调度器
    pub fn new(config: BandwidthSchedulerConfig) -> Self {
        Self {
            config: Arc::new(config),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            total_downloaded: Arc::new(AtomicU64::new(0)),
            last_adjust: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// 注册新任务
    pub fn register_task(&self, id: Uuid, priority: u8) -> anyhow::Result<()> {
        let mut tasks = self.tasks.lock();
        if tasks.len() >= self.config.max_concurrent_tasks {
            anyhow::bail!(
                "max concurrent tasks reached: {}/{}",
                tasks.len(),
                self.config.max_concurrent_tasks
            );
        }
        tasks.insert(id, TaskInfo::new(id, priority));
        info!(task_id = %id, priority = priority, "task registered");
        self.adjust_allocation();
        Ok(())
    }

    /// 注销任务
    pub fn unregister_task(&self, id: Uuid) {
        let mut tasks = self.tasks.lock();
        if tasks.remove(&id).is_some() {
            info!(task_id = %id, "task unregistered");
            self.adjust_allocation();
        }
    }

    /// 更新任务状态
    pub fn update_task(&self, id: Uuid, speed: f64, downloaded: u64, total: u64) {
        let mut tasks = self.tasks.lock();
        if let Some(task) = tasks.get_mut(&id) {
            task.current_speed = speed;
            task.downloaded = downloaded;
            task.total = total;
            task.last_update = Instant::now();
        }
        self.total_downloaded.fetch_add(
            downloaded.saturating_sub(self.total_downloaded.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }

    /// 获取任务分配的带宽
    pub fn get_allocated_bandwidth(&self, id: Uuid) -> f64 {
        let tasks = self.tasks.lock();
        tasks.get(&id).map(|t| t.allocated_bandwidth).unwrap_or(0.0)
    }

    /// 获取当前并发任务数
    pub fn concurrent_count(&self) -> usize {
        self.tasks.lock().len()
    }

    /// 获取总已下载字节数
    pub fn total_downloaded(&self) -> u64 {
        self.total_downloaded.load(Ordering::Relaxed)
    }

    /// 检查是否应该调整分配
    pub fn should_adjust(&self) -> bool {
        let last = self.last_adjust.lock();
        last.elapsed() >= Duration::from_millis(self.config.adjust_interval_ms)
    }

    /// 调整带宽分配
    pub fn adjust_allocation(&self) {
        let mut tasks = self.tasks.lock();
        let task_count = tasks.len();
        if task_count == 0 {
            return;
        }

        let total_bw = self.config.total_bandwidth as f64;
        let min_bw = self.config.min_bandwidth_per_task as f64;
        let max_bw = if self.config.max_bandwidth_per_task > 0 {
            self.config.max_bandwidth_per_task as f64
        } else {
            f64::INFINITY
        };

        match self.config.strategy {
            BandwidthStrategy::Fair => {
                // 公平分配
                let per_task = if total_bw > 0.0 {
                    (total_bw / task_count as f64).min(max_bw).max(min_bw)
                } else {
                    max_bw.min(f64::INFINITY)
                };
                for task in tasks.values_mut() {
                    task.allocated_bandwidth = per_task;
                }
            }
            BandwidthStrategy::Priority => {
                // 优先级分配
                let total_priority: u8 = tasks.values().map(|t| t.priority).sum();
                if total_priority > 0 && total_bw > 0.0 {
                    for task in tasks.values_mut() {
                        let share = task.priority as f64 / total_priority as f64;
                        task.allocated_bandwidth = (total_bw * share).min(max_bw).max(min_bw);
                    }
                } else {
                    let per_task = if total_bw > 0.0 {
                        total_bw / task_count as f64
                    } else {
                        max_bw
                    };
                    for task in tasks.values_mut() {
                        task.allocated_bandwidth = per_task.min(max_bw).max(min_bw);
                    }
                }
            }
            BandwidthStrategy::MinGuarantee => {
                // 最小保证
                let guaranteed = min_bw * task_count as f64;
                let remaining = if total_bw > guaranteed {
                    total_bw - guaranteed
                } else {
                    0.0
                };
                let extra_per_task = remaining / task_count as f64;
                for task in tasks.values_mut() {
                    task.allocated_bandwidth = (min_bw + extra_per_task).min(max_bw);
                }
            }
            BandwidthStrategy::Dynamic => {
                // 动态调整：进度慢的任务获得更多带宽
                let avg_progress: f64 = tasks.values().map(|t| t.progress()).sum::<f64>() / task_count as f64;
                for task in tasks.values_mut() {
                    let progress_diff = avg_progress - task.progress();
                    let factor = 1.0 + progress_diff / 100.0; // 进度慢的获得更多
                    let base = if total_bw > 0.0 {
                        total_bw / task_count as f64
                    } else {
                        max_bw
                    };
                    task.allocated_bandwidth = (base * factor).min(max_bw).max(min_bw);
                }
            }
        }

        *self.last_adjust.lock() = Instant::now();
        debug!(task_count = task_count, "bandwidth allocation adjusted");
    }

    /// 获取所有任务状态（用于监控）
    pub fn get_all_tasks(&self) -> Vec<TaskInfo> {
        self.tasks.lock().values().cloned().collect()
    }
}

impl Default for BandwidthScheduler {
    fn default() -> Self {
        Self::new(BandwidthSchedulerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fair_allocation() {
        let config = BandwidthSchedulerConfig {
            total_bandwidth: 100 * 1024 * 1024, // 100MB/s
            strategy: BandwidthStrategy::Fair,
            min_bandwidth_per_task: 1024 * 1024,
            max_bandwidth_per_task: 0,
            adjust_interval_ms: 1000,
            max_concurrent_tasks: 10,
        };
        let scheduler = BandwidthScheduler::new(config);

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        scheduler.register_task(id1, 5).unwrap();
        scheduler.register_task(id2, 5).unwrap();

        let bw1 = scheduler.get_allocated_bandwidth(id1);
        let bw2 = scheduler.get_allocated_bandwidth(id2);
        assert_eq!(bw1, bw2); // 公平分配
        assert!(bw1 > 0.0);
    }

    #[test]
    fn test_max_concurrent() {
        let config = BandwidthSchedulerConfig {
            max_concurrent_tasks: 2,
            ..Default::default()
        };
        let scheduler = BandwidthScheduler::new(config);

        scheduler.register_task(Uuid::new_v4(), 5).unwrap();
        scheduler.register_task(Uuid::new_v4(), 5).unwrap();
        assert!(scheduler.register_task(Uuid::new_v4(), 5).is_err()); // 超过限制
    }
}
