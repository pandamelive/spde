//! 空写入器（不落盘模式使用）
//!
//! 实现 [`pandanetos::domain::ChunkWriter`] trait，但所有写入操作直接丢弃，
//! 不创建任何文件。用于 dry_run（不落盘）模式下验证下载流程和带宽，
//! 而不实际占用磁盘空间。

use async_trait::async_trait;
use pandanetos::domain::ChunkWriter;
use pandanetos::error::Result;

/// 空写入器：所有数据直接丢弃，不写磁盘
#[derive(Debug, Clone, Default)]
pub struct NullChunkWriter;

impl NullChunkWriter {
    /// 创建一个新的空写入器
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ChunkWriter for NullChunkWriter {
    /// 写入数据：直接丢弃
    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    /// 刷新：无操作
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// 预分配：无操作
    async fn preallocate(&self, _size: u64) -> Result<()> {
        Ok(())
    }

    /// 文件大小：始终返回 0
    async fn file_size(&self) -> Result<u64> {
        Ok(0)
    }
}
