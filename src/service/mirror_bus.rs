//! 镜像发现总线
//!
//! 维护各协议的 MirrorDiscoverer 注册表，从原始源发现更多可用镜像。
//! - 调用该协议所有已注册的发现器
//! - 聚合结果，按 identifier() 去重
//! - 调用 ChunkDownloader.probe() 验证每个镜像的可用性
//! - 返回可用镜像列表
//!
//! 协议无关，只操作 [`pandanetos::domain::DownloadSource`] 和 [`MirrorDiscoverer`]。

use std::collections::HashSet;

use pandanetos::domain::{ChunkDownloader, DownloadFileInfo, DownloadSource, MirrorDiscoverer};
use pandanetos::error::{CoreError, Result};
use tokio::sync::Mutex;

/// 镜像发现总线
pub struct MirrorBus {
    /// 已注册的镜像发现器
    discoverers: Mutex<Vec<Box<dyn MirrorDiscoverer>>>,
    /// 是否启用镜像发现
    enabled: bool,
}

impl MirrorBus {
    /// 创建一个新的镜像发现总线
    pub fn new() -> Self {
        Self {
            discoverers: Mutex::new(Vec::new()),
            enabled: true,
        }
    }

    /// 注册一个镜像发现器
    pub async fn register(&self, discoverer: Box<dyn MirrorDiscoverer>) {
        let mut discoverers = self.discoverers.lock().await;
        discoverers.push(discoverer);
    }

    /// 启用/禁用镜像发现
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// 从原始源发现所有可用镜像
    ///
    /// # 参数
    /// - `source`：原始下载源
    /// - `downloader`：对应的协议下载器（用于 probe 验证）
    /// - `expected_size`：期望的文件大小（用于验证镜像是否为同一文件）
    ///
    /// # 返回
    /// 可用镜像列表（包含原始源）
    pub async fn discover(
        &self,
        source: &dyn DownloadSource,
        downloader: &dyn ChunkDownloader,
        expected_size: u64,
    ) -> Result<Vec<Box<dyn DownloadSource>>> {
        if !self.enabled {
            return Ok(vec![clone_source(source)]);
        }

        let protocol = source.protocol();
        let discoverers = self.discoverers.lock().await;

        // 收集所有发现器的结果
        let mut all_sources: Vec<Box<dyn DownloadSource>> = vec![clone_source(source)];
        let mut seen: HashSet<String> = HashSet::new();
        seen.insert(source.identifier());

        for discoverer in discoverers.iter() {
            if discoverer.protocol() != protocol {
                continue;
            }

            match discoverer.discover(source).await {
                Ok(mirrors) => {
                    for mirror in mirrors {
                        let id = mirror.identifier();
                        if seen.insert(id) {
                            all_sources.push(mirror);
                        }
                    }
                }
                Err(e) => {
                    // 单个发现器失败不影响其他
                    eprintln!("[mirror] discoverer {} failed: {}", discoverer.name(), e);
                }
            }
        }

        drop(discoverers);

        // 验证每个镜像的可用性（probe）
        let mut verified: Vec<Box<dyn DownloadSource>> = Vec::new();
        for mirror in all_sources {
            match self.verify_source(mirror.as_ref(), downloader, expected_size).await {
                Ok(info) => {
                    if info.size_bytes == expected_size || expected_size == 0 {
                        verified.push(mirror);
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[mirror] verify {} failed: {}",
                        mirror.display_name(),
                        e
                    );
                }
            }
        }

        if verified.is_empty() {
            // 所有镜像都验证失败，至少保留原始源
            verified.push(clone_source(source));
        }

        Ok(verified)
    }

    /// 验证单个源的可用性
    async fn verify_source(
        &self,
        source: &dyn DownloadSource,
        downloader: &dyn ChunkDownloader,
        _expected_size: u64,
    ) -> Result<DownloadFileInfo> {
        // probe 有超时风险，用 tokio::time::timeout 包装
        let probe_result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            downloader.probe(source),
        )
        .await;

        match probe_result {
            Ok(Ok(info)) => Ok(info),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(CoreError::Internal("probe timeout".into())),
        }
    }

    /// 获取已注册的发现器数量
    pub async fn discoverer_count(&self) -> usize {
        self.discoverers.lock().await.len()
    }
}

impl Default for MirrorBus {
    fn default() -> Self {
        Self::new()
    }
}

/// 克隆一个 DownloadSource（需要协议适配器实现 Clone）
///
/// 因为 DownloadSource trait 没有要求 Clone，这里用一个 workaround：
/// 要求所有 DownloadSource 实现都派生 Clone，然后通过 as_any 向下转型克隆。
/// 这是一个临时方案，更好的方案是在 trait 中加 clone_box 方法。
fn clone_source(source: &dyn DownloadSource) -> Box<dyn DownloadSource> {
    // 因为 HttpSource 实现了 Clone，这里用 as_any 向下转型
    // 注意：这要求所有 DownloadSource 实现都派生 Clone
    // 更完善的方案是在 trait 中加 fn clone_box(&self) -> Box<dyn DownloadSource>
    if let Some(http) = source.as_any().downcast_ref::<crate::infra::http::source::HttpSource>()
    {
        return Box::new(http.clone());
    }
    // 兜底：返回一个空的 HttpSource（不应该走到这里）
    Box::new(crate::infra::http::source::HttpSource::new(
        source.display_name(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::http::source::HttpSource;

    #[tokio::test]
    async fn test_register_and_count() {
        let bus = MirrorBus::new();
        assert_eq!(bus.discoverer_count().await, 0);

        let discoverer = crate::infra::http::mirror::dns::DnsMultiIpDiscoverer::new();
        bus.register(Box::new(discoverer)).await;
        assert_eq!(bus.discoverer_count().await, 1);
    }

    #[tokio::test]
    async fn test_disabled_returns_original() {
        let mut bus = MirrorBus::new();
        bus.set_enabled(false);

        let source = HttpSource::new("https://example.com/file.iso".into());
        let downloader = crate::infra::http::downloader::HttpChunkDownloader::new(false, 30);
        let result = bus.discover(&source, &downloader, 0).await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_clone_source() {
        let source = HttpSource::new("https://example.com/file.iso".into());
        let cloned = clone_source(&source);
        assert_eq!(cloned.identifier(), source.identifier());
    }
}
