//! 分片位图持久化（多连接断点续传）
//!
//! 用位图记录每个分片是否已完成，持久化到磁盘（`.bitmap` 文件）。
//! 重启时加载位图，跳过已完成的分片，实现多连接断点续传。
//!
//! ## 文件格式
//! ```text
//! +----------------+----------------+------------------+
//! |  magic (4B)    |  version (1B) |  total_chunks (4B) |
//! +----------------+----------------+------------------+
//! |  bitmap_data (variable, 1 bit per chunk)            |
//! +------------------------------------------------------+
//! ```
//!
//! - magic: `SPDE` (0x53 0x50 0x44 0x45)
//! - version: 当前为 1
//! - total_chunks: 分片总数（大端序）
//! - bitmap_data: 位图数据，每个 bit 表示一个分片是否完成（1=完成，0=未完成）
//!
//! ## 原子更新
//! 每次更新先写入临时文件，然后 rename 替换原文件，保证崩溃安全。

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tracing::{debug, info, warn};

/// 文件魔数
const MAGIC: &[u8; 4] = b"SPDE";
/// 文件版本
const VERSION: u8 = 1;
/// 头部大小：magic(4) + version(1) + total_chunks(4) = 9
const HEADER_SIZE: usize = 9;

/// 分片位图持久化
///
/// 用位图记录每个分片是否已完成，支持持久化和加载。
/// 线程安全，可被多个 worker 并发更新。
pub struct ResumeBitmap {
    /// 位图文件路径
    path: PathBuf,
    /// 分片总数
    total_chunks: u32,
    /// 已完成分片数（原子计数，用于快速查询进度）
    completed_count: AtomicU64,
    /// 位图数据（每个 u64 表示 64 个分片）
    bitmap: Mutex<Vec<u64>>,
    /// 自上次持久化以来的更新次数（用于节流持久化）
    dirty_count: AtomicU64,
    /// 持久化阈值（每 N 次更新持久化一次）
    persist_threshold: u64,
}

impl ResumeBitmap {
    /// 创建新的位图（如果文件已存在则加载）
    ///
    /// - `path`: 位图文件路径（通常是 `save_path.with_extension("bitmap")`）
    /// - `total_chunks`: 分片总数
    ///
    /// 如果文件已存在且分片数匹配，则加载已有位图；否则创建新位图。
    pub fn new(path: PathBuf, total_chunks: u32) -> Self {
        // 尝试加载已有位图
        if let Some(bitmap) = Self::load_internal(&path, total_chunks) {
            let completed = bitmap.iter().map(|w| w.count_ones() as u64).sum();
            info!(
                "断点续传: 加载位图 {}, 已完成 {}/{} 分片",
                path.display(),
                completed,
                total_chunks
            );
            return Self {
                path,
                total_chunks,
                completed_count: AtomicU64::new(completed),
                bitmap: Mutex::new(bitmap),
                dirty_count: AtomicU64::new(0),
                persist_threshold: 10,
            };
        }

        // 创建新位图
        let words = (total_chunks as usize + 63) / 64;
        let bitmap = vec![0u64; words];
        debug!("创建新位图 {}, 共 {} 分片", path.display(), total_chunks);

        Self {
            path,
            total_chunks,
            completed_count: AtomicU64::new(0),
            bitmap: Mutex::new(bitmap),
            dirty_count: AtomicU64::new(0),
            persist_threshold: 10,
        }
    }

    /// 从文件加载位图（内部方法）
    fn load_internal(path: &Path, expected_chunks: u32) -> Option<Vec<u64>> {
        let mut file = fs::File::open(path).ok()?;
        let mut header = [0u8; HEADER_SIZE];
        file.read_exact(&mut header).ok()?;

        // 校验魔数
        if &header[0..4] != MAGIC {
            warn!("位图文件 {} 魔数不匹配，忽略", path.display());
            return None;
        }

        // 校验版本
        let version = header[4];
        if version != VERSION {
            warn!("位图文件 {} 版本 {} 不支持，忽略", path.display(), version);
            return None;
        }

        // 读取分片总数
        let total_chunks = u32::from_be_bytes([header[5], header[6], header[7], header[8]]);
        if total_chunks != expected_chunks {
            warn!(
                "位图文件 {} 分片数 {} 与预期 {} 不匹配，忽略",
                path.display(),
                total_chunks,
                expected_chunks
            );
            return None;
        }

        // 读取位图数据
        let words = (total_chunks as usize + 63) / 64;
        let mut bitmap_data = vec![0u8; words * 8];
        file.read_exact(&mut bitmap_data).ok()?;

        // 转换为 u64 数组
        let mut bitmap = Vec::with_capacity(words);
        for i in 0..words {
            let bytes = [
                bitmap_data[i * 8],
                bitmap_data[i * 8 + 1],
                bitmap_data[i * 8 + 2],
                bitmap_data[i * 8 + 3],
                bitmap_data[i * 8 + 4],
                bitmap_data[i * 8 + 5],
                bitmap_data[i * 8 + 6],
                bitmap_data[i * 8 + 7],
            ];
            bitmap.push(u64::from_be_bytes(bytes));
        }

        Some(bitmap)
    }

    /// 标记分片为已完成
    ///
    /// 如果分片已经完成，返回 false；否则返回 true 并持久化。
    pub fn mark_completed(&self, chunk_id: u32) -> bool {
        if chunk_id >= self.total_chunks {
            return false;
        }

        let word_idx = (chunk_id / 64) as usize;
        let bit_idx = chunk_id % 64;
        let mask = 1u64 << bit_idx;

        let mut bitmap = self.bitmap.lock();
        if bitmap[word_idx] & mask != 0 {
            // 已经完成
            return false;
        }

        bitmap[word_idx] |= mask;
        drop(bitmap);

        self.completed_count.fetch_add(1, Ordering::Relaxed);
        let dirty = self.dirty_count.fetch_add(1, Ordering::Relaxed) + 1;

        // 达到阈值时持久化
        if dirty >= self.persist_threshold {
            self.dirty_count.store(0, Ordering::Relaxed);
            self.persist();
        }

        true
    }

    /// 检查分片是否已完成
    pub fn is_completed(&self, chunk_id: u32) -> bool {
        if chunk_id >= self.total_chunks {
            return true; // 越界视为已完成，避免重复下载
        }
        let word_idx = (chunk_id / 64) as usize;
        let bit_idx = chunk_id % 64;
        let mask = 1u64 << bit_idx;
        let bitmap = self.bitmap.lock();
        bitmap[word_idx] & mask != 0
    }

    /// 获取已完成分片数
    pub fn completed_count(&self) -> u64 {
        self.completed_count.load(Ordering::Relaxed)
    }

    /// 获取分片总数
    pub fn total_chunks(&self) -> u32 {
        self.total_chunks
    }

    /// 检查是否全部完成
    pub fn is_all_completed(&self) -> bool {
        self.completed_count.load(Ordering::Relaxed) >= self.total_chunks as u64
    }

    /// 获取未完成的分片 ID 列表
    pub fn pending_chunks(&self) -> Vec<u32> {
        let bitmap = self.bitmap.lock();
        let mut pending = Vec::new();
        for chunk_id in 0..self.total_chunks {
            let word_idx = (chunk_id / 64) as usize;
            let bit_idx = chunk_id % 64;
            let mask = 1u64 << bit_idx;
            if bitmap[word_idx] & mask == 0 {
                pending.push(chunk_id);
            }
        }
        pending
    }

    /// 持久化位图到磁盘（原子写入）
    pub fn persist(&self) {
        let bitmap = self.bitmap.lock();
        let mut data = Vec::with_capacity(HEADER_SIZE + bitmap.len() * 8);

        // 写入头部
        data.extend_from_slice(MAGIC);
        data.push(VERSION);
        data.extend_from_slice(&self.total_chunks.to_be_bytes());

        // 写入位图数据
        for word in bitmap.iter() {
            data.extend_from_slice(&word.to_be_bytes());
        }

        drop(bitmap);

        // 原子写入：先写临时文件，再 rename
        let tmp_path = self.path.with_extension("bitmap.tmp");
        match fs::File::create(&tmp_path) {
            Ok(mut file) => {
                if let Err(e) = file.write_all(&data) {
                    warn!("写入位图临时文件失败: {}", e);
                    return;
                }
                if let Err(e) = file.sync_all() {
                    warn!("同步位图临时文件失败: {}", e);
                    return;
                }
                if let Err(e) = fs::rename(&tmp_path, &self.path) {
                    warn!("rename 位图文件失败: {}", e);
                }
            }
            Err(e) => {
                warn!("创建位图临时文件失败: {}", e);
            }
        }
    }

    /// 强制持久化（忽略节流）
    pub fn force_persist(&self) {
        self.persist();
    }

    /// 删除位图文件（任务完成后清理）
    pub fn delete(&self) {
        let _ = fs::remove_file(&self.path);
        let tmp_path = self.path.with_extension("bitmap.tmp");
        let _ = fs::remove_file(&tmp_path);
    }

    /// 获取位图文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ResumeBitmap {
    fn drop(&mut self) {
        // drop 时强制持久化，确保数据不丢失
        self.persist();
    }
}

/// 从 save_path 推导位图文件路径
pub fn bitmap_path_for(save_path: &Path) -> PathBuf {
    save_path.with_extension("bitmap")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_and_mark() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bitmap");
        let bitmap = ResumeBitmap::new(path, 100);

        assert_eq!(bitmap.total_chunks(), 100);
        assert_eq!(bitmap.completed_count(), 0);
        assert!(!bitmap.is_completed(0));
        assert!(!bitmap.is_completed(99));

        assert!(bitmap.mark_completed(0));
        assert!(bitmap.mark_completed(50));
        assert!(bitmap.mark_completed(99));

        assert_eq!(bitmap.completed_count(), 3);
        assert!(bitmap.is_completed(0));
        assert!(bitmap.is_completed(50));
        assert!(bitmap.is_completed(99));
        assert!(!bitmap.is_completed(1));

        // 重复标记应该返回 false
        assert!(!bitmap.mark_completed(0));
        assert_eq!(bitmap.completed_count(), 3);
    }

    #[test]
    fn test_persist_and_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bitmap");

        // 创建并标记一些分片
        {
            let bitmap = ResumeBitmap::new(path.clone(), 200);
            bitmap.mark_completed(0);
            bitmap.mark_completed(100);
            bitmap.mark_completed(199);
            bitmap.force_persist();
        }

        // 重新加载
        let bitmap = ResumeBitmap::new(path, 200);
        assert_eq!(bitmap.completed_count(), 3);
        assert!(bitmap.is_completed(0));
        assert!(bitmap.is_completed(100));
        assert!(bitmap.is_completed(199));
        assert!(!bitmap.is_completed(1));
    }

    #[test]
    fn test_pending_chunks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bitmap");
        let bitmap = ResumeBitmap::new(path, 10);

        bitmap.mark_completed(2);
        bitmap.mark_completed(5);
        bitmap.mark_completed(7);

        let pending = bitmap.pending_chunks();
        assert_eq!(pending, vec![0, 1, 3, 4, 6, 8, 9]);
    }

    #[test]
    fn test_is_all_completed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bitmap");
        let bitmap = ResumeBitmap::new(path, 5);

        assert!(!bitmap.is_all_completed());

        for i in 0..5 {
            bitmap.mark_completed(i);
        }

        assert!(bitmap.is_all_completed());
    }

    #[test]
    fn test_mismatched_chunks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.bitmap");

        // 创建 100 分片的位图
        {
            let bitmap = ResumeBitmap::new(path.clone(), 100);
            bitmap.mark_completed(0);
            bitmap.force_persist();
        }

        // 用 200 分片重新加载，应该忽略旧位图
        let bitmap = ResumeBitmap::new(path, 200);
        assert_eq!(bitmap.completed_count(), 0);
        assert!(!bitmap.is_completed(0));
    }
}
