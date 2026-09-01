//! 写入器工厂
//!
//! 根据配置创建不同类型的写入器：
//! - 磁盘写入器（FileChunkWriter）：写入本地文件
//! - 内存写入器（VecWriter）：写入内存缓冲区
//! - 空写入器（NullWriter）：丢弃所有数据（dry_run 模式）
//!
//! 所有写入器都实现 tokio::io::AsyncWrite + Unpin + Send，
//! 可以直接传给 ChunkScheduler.execute()。

use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::infra::disk::null_writer::NullChunkWriter;

/// 写入器类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterType {
    /// 磁盘写入（写入本地文件）
    Disk,
    /// 内存写入（写入 Vec<u8>）
    Memory,
    /// 空写入（丢弃所有数据，dry_run 模式）
    Null,
}

/// 创建写入器
///
/// # 参数
/// - writer_type: 写入器类型
/// - file_path: 文件路径（仅 Disk 类型需要）
/// - file_size: 文件总大小（用于预分配）
///
/// # 返回
/// - Arc<Mutex<dyn AsyncWrite + Unpin + Send>>: 可共享的写入器
pub fn create_writer(
    writer_type: WriterType,
    file_path: Option<PathBuf>,
    file_size: u64,
) -> anyhow::Result<Arc<Mutex<dyn AsyncWrite + Unpin + Send>>> {
    match writer_type {
        WriterType::Disk => {
            let path = file_path.ok_or_else(|| {
                anyhow::anyhow!("file_path is required for Disk writer")
            })?;

            // 确保父目录存在
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // 创建文件并预分配空间
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(&path)?;

            // 预分配空间（如果文件大小已知）
            if file_size > 0 {
                file.set_len(file_size)?;
            }

            let async_file = tokio::fs::File::from_std(file);
            Ok(Arc::new(Mutex::new(async_file)))
        }
        WriterType::Memory => {
            let buffer: Vec<u8> = Vec::with_capacity(file_size as usize);
            Ok(Arc::new(Mutex::new(tokio::io::BufWriter::new(
                MemoryWriter::new(buffer),
            ))))
        }
        WriterType::Null => {
            let null_writer = NullChunkWriter::new();
            Ok(Arc::new(Mutex::new(null_writer)))
        }
    }
}

/// 内存写入器（包装 Vec<u8>）
#[derive(Debug)]
pub struct MemoryWriter {
    buffer: Vec<u8>,
}

impl MemoryWriter {
    pub fn new(buffer: Vec<u8>) -> Self {
        Self { buffer }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buffer
    }

    pub fn get_ref(&self) -> &Vec<u8> {
        &self.buffer
    }
}

impl tokio::io::AsyncWrite for MemoryWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.buffer.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
