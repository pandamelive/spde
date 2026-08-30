//! FTP 下载源
//!
//! 实现 `DownloadSource` trait，支持 ftp:// 和 ftps:// 协议。
//! 支持匿名登录和用户名密码登录，支持断点续传。

use anyhow::Context;
use std::any::Any;

use pandanetos::domain::{DownloadSource, SourceCapabilities};
use url::Url;

/// FTP 下载源
#[derive(Debug, Clone)]
pub struct FtpSource {
    /// 原始 URL
    url: String,
    /// 主机地址
    host: String,
    /// 端口
    port: u16,
    /// 用户名
    username: String,
    /// 密码
    password: String,
    /// 远程路径
    remote_path: String,
    /// 是否是 FTPS
    is_ftps: bool,
}

impl FtpSource {
    /// 创建新的 FTP 下载源
    pub fn new(url: impl Into<String>) -> anyhow::Result<Self> {
        let url_str = url.into();
        let parsed = Url::parse(&url_str).context("invalid ftp url")?;

        if parsed.scheme() != "ftp" && parsed.scheme() != "ftps" {
            anyhow::bail!("not an ftp url: {}", url_str);
        }

        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("no host in ftp url"))?
            .to_string();
        let port = parsed.port().unwrap_or(21);
        let username_raw = parsed.username();
        let username = if username_raw.is_empty() {
            "anonymous".to_string()
        } else {
            username_raw.to_string()
        };
        let password = parsed.password().unwrap_or("anonymous@").to_string();
        let remote_path = parsed.path().to_string();
        let is_ftps = parsed.scheme() == "ftps";

        Ok(Self {
            url: url_str,
            host,
            port,
            username,
            password,
            remote_path,
            is_ftps,
        })
    }

    /// 获取主机地址
    pub fn host(&self) -> &str {
        &self.host
    }

    /// 获取端口
    pub fn port(&self) -> u16 {
        self.port
    }

    /// 获取用户名
    pub fn username(&self) -> &str {
        &self.username
    }

    /// 获取密码
    pub fn password(&self) -> &str {
        &self.password
    }

    /// 获取远程路径
    pub fn remote_path(&self) -> &str {
        &self.remote_path
    }

    /// 是否是 FTPS
    pub fn is_ftps(&self) -> bool {
        self.is_ftps
    }

    /// 检查是否是 FTP URI
    pub fn is_ftp_uri(uri: &str) -> bool {
        uri.starts_with("ftp://") || uri.starts_with("ftps://")
    }
}

impl DownloadSource for FtpSource {
    fn protocol(&self) -> &str {
        if self.is_ftps {
            "ftps"
        } else {
            "ftp"
        }
    }

    fn identifier(&self) -> String {
        format!(
            "{}://{}:{}{}",
            self.protocol(),
            self.host,
            self.port,
            self.remote_path
        )
    }

    fn display_name(&self) -> String {
        format!("FTP: {}:{}{}", self.host, self.port, self.remote_path)
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            supports_range: true,      // FTP 支持 REST 命令实现断点续传
            supports_concurrent: true, // 支持多连接（每个连接独立登录）
            supports_resume: true,     // 支持断点续传
            max_concurrency: 8,        // FTP 服务器通常限制连接数
            chunk_size_range: Some((1 * 1024 * 1024, 16 * 1024 * 1024)), // 1MB ~ 16MB
            immutable: true,           // 远程文件内容不可变
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
