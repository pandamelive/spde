//! 基于 URL 规则的镜像发现器
//!
//! 识别特定域名的 URL，根据规则生成镜像 URL。
//! 适用于 Apple 固件、Android 固件等有多个 CDN 节点的下载源。
//!
//! ## 支持的规则类型
//! - **域名替换**：将原始域名替换为镜像域名（如 Apple 的多个 CDN 节点）
//! - **路径前缀替换**：替换 URL 路径前缀
//! - **正则替换**：用正则表达式替换 URL 中的特定部分
//!
//! ## Apple 固件示例
//! Apple 固件下载有多个 CDN 节点，可以通过替换域名来获取镜像：
//! - `updates-http.cdn-apple.com` → `updates.cdn-apple.com`
//! - `updates-http.cdn-apple.com` → `appldnld.apple.com`
//! - 等等

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use pandanetos::domain::{DownloadSource, MirrorDiscoverer};
use pandanetos::error::Result;

use super::super::source::HttpSource;

/// URL 替换规则
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UrlRule {
    /// 域名替换
    DomainReplace {
        /// 原始域名
        from: String,
        /// 替换为的域名
        to: String,
    },
    /// 路径前缀替换
    PathPrefixReplace {
        /// 原始路径前缀
        from: String,
        /// 替换为的路径前缀
        to: String,
    },
    /// 正则替换
    RegexReplace {
        /// 正则表达式
        pattern: String,
        /// 替换为的内容
        replacement: String,
    },
}

impl UrlRule {
    /// 应用规则到 URL，返回替换后的 URL（如果匹配）
    pub fn apply(&self, url: &str) -> Option<String> {
        match self {
            UrlRule::DomainReplace { from, to } => {
                if url.contains(from) {
                    Some(url.replace(from, to))
                } else {
                    None
                }
            }
            UrlRule::PathPrefixReplace { from, to } => {
                if url.contains(from) {
                    Some(url.replace(from, to))
                } else {
                    None
                }
            }
            UrlRule::RegexReplace { pattern, replacement } => {
                if let Ok(re) = Regex::new(pattern) {
                    if re.is_match(url) {
                        Some(re.replace_all(url, replacement.as_str()).to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }
}

/// 镜像规则配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorRuleConfig {
    /// 规则名称
    pub name: String,
    /// 匹配的原始域名（只有匹配这个域名的 URL 才会应用规则）
    pub match_domain: String,
    /// 应用的 URL 规则列表
    pub rules: Vec<UrlRule>,
    /// 生成的镜像源的优先级（数值越大优先级越高）
    pub priority: u32,
}

/// 内置的 Apple 固件镜像规则
fn builtin_apple_rules() -> Vec<MirrorRuleConfig> {
    vec![
        MirrorRuleConfig {
            name: "apple-cdn-https".to_string(),
            match_domain: "updates-http.cdn-apple.com".to_string(),
            rules: vec![UrlRule::DomainReplace {
                from: "updates-http.cdn-apple.com".to_string(),
                to: "updates.cdn-apple.com".to_string(),
            }],
            priority: 90,
        },
        MirrorRuleConfig {
            name: "apple-appldnld".to_string(),
            match_domain: "updates-http.cdn-apple.com".to_string(),
            rules: vec![UrlRule::DomainReplace {
                from: "updates-http.cdn-apple.com".to_string(),
                to: "appldnld.apple.com".to_string(),
            }],
            priority: 80,
        },
        MirrorRuleConfig {
            name: "apple-cdn-apple-com".to_string(),
            match_domain: "updates.cdn-apple.com".to_string(),
            rules: vec![UrlRule::DomainReplace {
                from: "updates.cdn-apple.com".to_string(),
                to: "updates-http.cdn-apple.com".to_string(),
            }],
            priority: 85,
        },
    ]
}

/// 基于 URL 规则的镜像发现器
///
/// 识别特定域名的 URL，根据规则生成镜像 URL。
/// 支持内置规则（Apple 固件）和自定义规则。
pub struct UrlRuleDiscoverer {
    /// 规则配置（按 match_domain 分组）
    rules: HashMap<String, Vec<MirrorRuleConfig>>,
    /// HTTP 客户端（用于后续验证镜像可用性）
    client: reqwest::Client,
}

impl UrlRuleDiscoverer {
    /// 创建新的 URL 规则镜像发现器
    ///
    /// - `use_builtin`: 是否使用内置规则（Apple 固件等）
    /// - `custom_rules`: 自定义规则列表
    pub fn new(use_builtin: bool, custom_rules: Vec<MirrorRuleConfig>) -> Arc<Self> {
        let mut rules: HashMap<String, Vec<MirrorRuleConfig>> = HashMap::new();

        if use_builtin {
            for rule in builtin_apple_rules() {
                rules
                    .entry(rule.match_domain.clone())
                    .or_default()
                    .push(rule);
            }
            info!("URL 规则镜像发现器: 加载 {} 条内置 Apple 固件规则", builtin_apple_rules().len());
        }

        for rule in custom_rules {
            rules
                .entry(rule.match_domain.clone())
                .or_default()
                .push(rule);
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Arc::new(Self { rules, client })
    }

    /// 创建默认的 URL 规则镜像发现器（使用内置规则）
    pub fn default() -> Arc<Self> {
        Self::new(true, Vec::new())
    }

    /// 添加自定义规则
    pub fn add_rule(&mut self, rule: MirrorRuleConfig) {
        self.rules
            .entry(rule.match_domain.clone())
            .or_default()
            .push(rule);
    }

    /// 从 URL 中提取域名
    fn extract_domain(url: &str) -> Option<String> {
        url::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(|s| s.to_string()))
    }

    /// 根据规则生成镜像 URL
    fn generate_mirrors(&self, url: &str) -> Vec<(String, u32)> {
        let domain = match Self::extract_domain(url) {
            Some(d) => d,
            None => return Vec::new(),
        };

        let mut mirrors = Vec::new();

        // 查找匹配域名的规则
        if let Some(rules) = self.rules.get(&domain) {
            for rule in rules {
                let mut current_url = url.to_string();
                let mut matched = false;

                // 依次应用规则列表中的所有规则
                for url_rule in &rule.rules {
                    if let Some(new_url) = url_rule.apply(&current_url) {
                        current_url = new_url;
                        matched = true;
                    } else {
                        matched = false;
                        break;
                    }
                }

                if matched && current_url != url {
                    mirrors.push((current_url, rule.priority));
                }
            }
        }

        // 按优先级降序排序
        mirrors.sort_by(|a, b| b.1.cmp(&a.1));

        mirrors
    }

    /// 验证镜像 URL 是否可用（HEAD 请求）
    async fn verify_mirror(&self, url: &str) -> bool {
        match self.client.head(url).send().await {
            Ok(resp) => {
                let status = resp.status();
                // 2xx 或 3xx 都认为可用
                status.is_success() || status.is_redirection()
            }
            Err(_) => false,
        }
    }
}

#[async_trait]
#[async_trait]
impl MirrorDiscoverer for UrlRuleDiscoverer {
    /// 支持的协议
    fn protocol(&self) -> &str {
        "http"
    }

    /// 发现镜像源
    ///
    /// 根据 URL 规则生成镜像 URL，并验证可用性。
    async fn discover(&self, source: &dyn DownloadSource) -> Result<Vec<Box<dyn DownloadSource>>> {
        // 向下转型获取 HttpSource，然后调用 url() 方法
        let url = if let Some(http_source) = source.as_any().downcast_ref::<HttpSource>() {
            http_source.url().to_string()
        } else {
            // 不是 HTTP 源，用 identifier 作为 URL
            source.identifier()
        };

        let mirrors = self.generate_mirrors(&url);

        if mirrors.is_empty() {
            debug!("URL 规则镜像发现器: URL {} 没有匹配的规则", url);
            return Ok(Vec::new());
        }

        debug!(
            "URL 规则镜像发现器: URL {} 生成了 {} 个镜像",
            url,
            mirrors.len()
        );

        let mut result = Vec::new();

        // 验证每个镜像的可用性
        for (mirror_url, _priority) in mirrors {
            let available = self.verify_mirror(&mirror_url).await;
            if available {
                info!(
                    "URL 规则镜像发现器: 镜像 {} 可用",
                    mirror_url
                );
                let http_source = HttpSource::new(mirror_url);
                result.push(Box::new(http_source) as Box<dyn DownloadSource>);
            } else {
                debug!("URL 规则镜像发现器: 镜像 {} 不可用，跳过", mirror_url);
            }
        }

        Ok(result)
    }

    /// 获取发现器名称
    fn name(&self) -> &str {
        "url-rule-discoverer"
    }
}

impl UrlRuleDiscoverer {
    /// 检查是否支持该源（辅助方法，非 trait 方法）
    pub fn supports_source(&self, source: &dyn DownloadSource) -> bool {
        let url = if let Some(http_source) = source.as_any().downcast_ref::<HttpSource>() {
            http_source.url().to_string()
        } else {
            source.identifier()
        };
        let domain = match Self::extract_domain(&url) {
            Some(d) => d,
            None => return false,
        };
        self.rules.contains_key(&domain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domain_replace_rule() {
        let rule = UrlRule::DomainReplace {
            from: "updates-http.cdn-apple.com".to_string(),
            to: "updates.cdn-apple.com".to_string(),
        };

        let url = "http://updates-http.cdn-apple.com/2026/01/iPhone15,2.ipsw";
        let result = rule.apply(url);
        assert_eq!(
            result,
            Some("http://updates.cdn-apple.com/2026/01/iPhone15,2.ipsw".to_string())
        );

        let other_url = "http://example.com/file.zip";
        assert_eq!(rule.apply(other_url), None);
    }

    #[test]
    fn test_regex_replace_rule() {
        let rule = UrlRule::RegexReplace {
            pattern: r"http://([^/]+)/".to_string(),
            replacement: "https://$1/".to_string(),
        };

        let url = "http://example.com/file.zip";
        let result = rule.apply(url);
        assert_eq!(result, Some("https://example.com/file.zip".to_string()));
    }

    #[test]
    fn test_extract_domain() {
        assert_eq!(
            UrlRuleDiscoverer::extract_domain("http://updates-http.cdn-apple.com/2026/01/iPhone.ipsw"),
            Some("updates-http.cdn-apple.com".to_string())
        );
        assert_eq!(
            UrlRuleDiscoverer::extract_domain("https://example.com/path/file.zip"),
            Some("example.com".to_string())
        );
        assert_eq!(UrlRuleDiscoverer::extract_domain("not a url"), None);
    }

    #[test]
    fn test_generate_mirrors_apple() {
        let discoverer = UrlRuleDiscoverer::default();
        let url = "http://updates-http.cdn-apple.com/2026/01/iPhone15,2.ipsw";
        let mirrors = discoverer.generate_mirrors(url);

        // 应该生成 2 个镜像（updates.cdn-apple.com 和 appldnld.apple.com）
        assert!(!mirrors.is_empty());
        assert!(mirrors.iter().any(|(u, _)| u.contains("updates.cdn-apple.com")));
        assert!(mirrors.iter().any(|(u, _)| u.contains("appldnld.apple.com")));
    }

    #[test]
    fn test_generate_mirrors_no_match() {
        let discoverer = UrlRuleDiscoverer::default();
        let url = "http://example.com/file.zip";
        let mirrors = discoverer.generate_mirrors(url);
        assert!(mirrors.is_empty());
    }
}
