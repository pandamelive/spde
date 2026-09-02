//! SSH/SFTP/SCP 下载源
//!
//! 实现 `DownloadSource` trait，支持 sftp://、scp://、ssh:// 协议。
//! 内部调用系统自带的 sftp/scp 命令，无需额外编译依赖。
//!
//! 注意：由于通过系统命令实现，不支持分片下载和多连接并发，
//! 调度器会用单连接下载整个文件。

use anyhow::Context;
use std::any::Any;

use pandanetos::domain::{DownloadSource, SourceCapabilities};
use url::Url;

/// SSH/SFTP/SCP 下载源
#[derive(Debug, Clone)]
pub struct SshSource {
    /// 原始 URL
    url: String,
    /// 协议类型（sftp/scp/ssh）
    scheme: String,
    /// 用户名
    username: String,
    /// 密码（可选）
    password: Option<String>,
    /// 主机地址
    host: String,
    /// 端口
    port: u16,
    /// 远程路径
    remote_path: String,
}

impl SshSource {
    /// 创建新的 SSH 下载源
    pub fn new(url: impl Into<String>) -> anyhow::Result<Self> {
        let url_str = url.into();
        let parsed = Url::parse(&url_str).context("invalid ssh url")?;

        let scheme = parsed.scheme().to_string();
        if !matches!(scheme.as_str(), "sftp" | "scp" | "ssh") {
            anyhow::bail!("not an ssh/sftp/scp url: {}", url_str);
        }

        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("no host in ssh url"))?
            .to_string();
        let port = parsed.port().unwrap_or(22);
        let username_raw = parsed.username();
        let username = if username_raw.is_empty() {
            "root".to_string()
        } else {
            username_raw.to_string()
        };
        let password = parsed.password().map(|s| s.to_string());
        let remote_path = parsed.path().to_string();

        Ok(Self {
            url: url_str,
            scheme,
            username,
            password,
            host,
            port,
            remote_path,
        })
    }

    /// 获取协议类型
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// 获取用户名
    pub fn username(&self) -> &str {
        &self.username
    }

    /// 获取密码
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// 获取主机地址
    pub fn host(&self) -> &str {
        &self.host
    }

    /// 获取端口
    pub fn port(&self) -> u16 {
        self.port
    }

    /// 获取远程路径
    pub fn remote_path(&self) -> &str {
        &self.remote_path
    }

    /// 检查是否是 SSH URI
    pub fn is_ssh_uri(uri: &str) -> bool {
        uri.starts_with("sftp://") || uri.starts_with("scp://") || uri.starts_with("ssh://")
    }
}

impl DownloadSource for SshSource {
    fn protocol(&self) -> &'static str {
        "sftp"
    }

    fn identifier(&self) -> String {
        format!(
            "{}://{}@{}:{}{}",
            self.scheme, self.username, self.host, self.port, self.remote_path
        )
    }

    fn display_name(&self) -> String {
        format!(
            "{}: {}@{}:{}{}",
            self.scheme.to_uppercase(),
            self.username,
            self.host,
            self.port,
            self.remote_path
        )
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            supports_range: false,      // 通过系统命令实现，不支持分片下载
            supports_concurrent: false, // 不支持多连接并发
            supports_resume: false,     // 不支持断点续传
            max_concurrency: 1,         // 只能单连接
            chunk_size_range: None,     // 无特殊要求（调度器会用单分片下载整个文件）
            immutable: true,            // 远程文件内容不可变
            protocol: "ssh",
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
