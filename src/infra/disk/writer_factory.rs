//! 写入器工厂
//!
//! 根据配置创建不同类型的写入器：
//! - 磁盘写入器（FileChunkWriter）：写入本地文件，pwrite/seek_write 偏移写并发安全
//! - 空写入器（NullChunkWriter）：丢弃所有数据（dry_run 模式）
//!
//! 所有写入器实现 [`pandanetos::domain::ChunkWriter`] trait，
//! `write_at(offset, data)` 是并发安全的，可直接传给 ChunkScheduler.execute()。
//!
//! 历史变更：之前工厂返回 `Arc<Mutex<dyn AsyncWrite>>`，调度器为了串行化多 worker
//! 的写入把整个 fetch 包在 mutex 里，导致 N 个 worker 实际并发度=1，是 bug #1 的根因。

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use pandanetos::domain::ChunkWriter;

use crate::infra::disk::file_writer::FileChunkWriter;
use crate::infra::disk::null_writer::NullChunkWriter;

/// 写入器类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterType {
    /// 磁盘写入（写入本地文件）
    Disk,
    /// 空写入（丢弃所有数据，dry_run 模式）
    Null,
}

/// 创建写入器（异步）
///
/// # 参数
/// - writer_type: 写入器类型
/// - file_path: 文件路径（仅 Disk 类型需要）
/// - file_size: 文件总大小（用于预分配，0 表示不预分配）
///
/// # 返回
/// - `Arc<dyn ChunkWriter>`：可并发 write_at 的写入器
pub async fn create_writer(
    writer_type: WriterType,
    file_path: Option<PathBuf>,
    file_size: u64,
) -> Result<Arc<dyn ChunkWriter>> {
    match writer_type {
        WriterType::Disk => {
            let path = file_path.ok_or_else(|| anyhow!("file_path is required for Disk writer"))?;

            // 确保父目录存在
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // 打开文件（FileChunkWriter 内部 Arc<std::fs::File> 共享，
            // write_at 用 pwrite/seek_write 偏移写，多 worker 并发安全）
            let writer = FileChunkWriter::open(path).await?;

            // 预分配文件空间（如果文件大小已知）
            if file_size > 0 {
                writer.preallocate(file_size).await?;
            }

            Ok(Arc::new(writer))
        }
        WriterType::Null => Ok(Arc::new(NullChunkWriter::new())),
    }
}
