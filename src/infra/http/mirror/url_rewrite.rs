//! URL 替换规则源发现器
//!
//! 对于一些常见的 CDN 或镜像站点，可以通过 URL 替换规则
//! 自动发现多个镜像源。
//!
//! 例如：
//! - Apple 固件：mesu.apple.com → 多个镜像域名
//! - GitHub  release：github.com → ghproxy.com、mirror.ghproxy.com
//! - Docker 镜像：docker.io → 多个镜像加速器

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, info};

use crate::domain::chunk_fetcher::ChunkFetcher;
use crate::domain::source_pool::SourceDiscoverer;
use crate::infra::http::fetcher::HttpRangeFetcher;

/// URL 替换规则
#[derive(Debug, Clone)]
pub struct UrlRewriteRule {
    /// 匹配的域名前缀
    pub match_prefix: String,
    /// 替换的域名前缀列表
    pub replace_prefixes: Vec<String>,
    /// 规则描述
    pub description: String,
}

impl UrlRewriteRule {
    /// 创建新的 URL 替换规则
    pub fn new(match_prefix: &str, replace_prefixes: Vec<&str>, description: &str) -> Self {
        Self {
            match_prefix: match_prefix.to_string(),
            replace_prefixes: replace_prefixes
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            description: description.to_string(),
        }
    }

    /// 检查 URL 是否匹配此规则
    pub fn matches(&self, url: &str) -> bool {
        url.starts_with(&self.match_prefix)
    }

    /// 应用替换规则，生成新的 URL 列表
    pub fn apply(&self, url: &str) -> Vec<String> {
        if !self.matches(url) {
            return vec![];
        }

        let mut results = Vec::new();
        for replace in &self.replace_prefixes {
            let new_url = url.replacen(&self.match_prefix, replace, 1);
            results.push(new_url);
        }
        results
    }
}

/// URL 替换规则源发现器
#[derive(Debug, Clone)]
pub struct UrlRewriteDiscoverer {
    /// 替换规则列表
    rules: Vec<UrlRewriteRule>,
    /// 已发现的源（去重）
    discovered: Arc<tokio::sync::Mutex<HashSet<String>>>,
}

impl UrlRewriteDiscoverer {
    /// 创建新的 URL 替换规则源发现器（内置常用规则）
    pub fn new() -> Self {
        let rules = Self::default_rules();
        Self {
            rules,
            discovered: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        }
    }

    /// 创建自定义规则的发现器
    pub fn with_rules(rules: Vec<UrlRewriteRule>) -> Self {
        Self {
            rules,
            discovered: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        }
    }

    /// 内置常用替换规则
    fn default_rules() -> Vec<UrlRewriteRule> {
        vec![
            // GitHub release 镜像加速
            UrlRewriteRule::new(
                "https://github.com/",
                vec![
                    "https://ghproxy.com/https://github.com/",
                    "https://mirror.ghproxy.com/https://github.com/",
                    "https://gh-proxy.com/https://github.com/",
                ],
                "GitHub release 镜像加速",
            ),
            // Apple 固件镜像（示例，实际需要根据具体域名配置）
            UrlRewriteRule::new(
                "https://mesu.apple.com/",
                vec![],
                "Apple 固件镜像（需配置具体镜像）",
            ),
            // Docker 镜像加速器（示例）
            UrlRewriteRule::new(
                "https://registry-1.docker.io/",
                vec![
                    "https://docker.mirrors.ustc.edu.cn/",
                    "https://hub-mirror.c.163.com/",
                ],
                "Docker 镜像加速器",
            ),
        ]
    }

    /// 添加自定义规则
    pub fn add_rule(&mut self, rule: UrlRewriteRule) {
        self.rules.push(rule);
    }
}

impl Default for UrlRewriteDiscoverer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SourceDiscoverer for UrlRewriteDiscoverer {
    async fn discover(&self, source_url: &str) -> anyhow::Result<Vec<Arc<dyn ChunkFetcher>>> {
        // 只对 HTTP/HTTPS 进行 URL 替换发现
        if !source_url.starts_with("http://") && !source_url.starts_with("https://") {
            return Ok(vec![]);
        }

        let mut discovered = self.discovered.lock().await;
        let mut new_sources = Vec::new();

        for rule in &self.rules {
            if !rule.matches(source_url) {
                continue;
            }

            let new_urls = rule.apply(source_url);
            for new_url in new_urls {
                // 去重
                if discovered.contains(&new_url) {
                    continue;
                }
                discovered.insert(new_url.clone());

                debug!(
                    original = %source_url,
                    new = %new_url,
                    rule = %rule.description,
                    "discovered mirror via URL rewrite"
                );

                // 创建新的 Fetcher
                let fetcher = HttpRangeFetcher::new(&new_url, 30);
                new_sources.push(Arc::new(fetcher) as Arc<dyn ChunkFetcher>);
            }
        }

        if !new_sources.is_empty() {
            info!(
                url = %source_url,
                new_source_count = new_sources.len(),
                "URL rewrite discovery completed"
            );
        }

        Ok(new_sources)
    }

    fn name(&self) -> &str {
        "url-rewrite"
    }
}
