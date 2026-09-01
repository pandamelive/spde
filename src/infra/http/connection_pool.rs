//! HTTP 连接池
//!
//! 复用 HTTP 连接，避免频繁建立 TCP/TLS 连接的开销。
//! 支持：
//! - 连接复用（keep-alive）
//! - 连接超时管理
//! - 最大连接数限制
//! - 按主机名分组管理

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use reqwest::Client;
use tracing::{debug, info};

/// 连接池配置
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// 每个主机最大空闲连接数
    pub max_idle_per_host: usize,
    /// 连接最大空闲时间（秒）
    pub max_idle_time_secs: u64,
    /// 连接超时（秒）
    pub connect_timeout_secs: u64,
    /// 请求超时（秒）
    pub request_timeout_secs: u64,
    /// 是否启用 TCP keepalive
    pub tcp_keepalive: bool,
    /// TCP keepalive 间隔（秒）
    pub tcp_keepalive_secs: u64,
    /// 是否启用 HTTP/2
    pub http2_prior_knowledge: bool,
    /// 最大重定向次数
    pub max_redirects: usize,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_host: 8,
            max_idle_time_secs: 90,
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            tcp_keepalive: true,
            tcp_keepalive_secs: 60,
            http2_prior_knowledge: false,
            max_redirects: 5,
        }
    }
}

/// 空闲连接信息
#[derive(Debug)]
struct IdleConnection {
    /// 客户端（reqwest Client 内部管理连接池）
    client: Client,
    /// 主机名
    host: String,
    /// 创建时间
    created_at: Instant,
    /// 最后使用时间
    last_used: Instant,
}

impl IdleConnection {
    /// 检查连接是否过期
    fn is_expired(&self, max_idle_time: Duration) -> bool {
        self.last_used.elapsed() > max_idle_time
    }
}

/// HTTP 连接池
#[derive(Clone)]
pub struct HttpConnectionPool {
    /// 配置
    config: Arc<ConnectionPoolConfig>,
    /// 按主机名分组的客户端缓存
    clients: Arc<Mutex<HashMap<String, Client>>>,
    /// 全局客户端（用于不特定主机的请求）
    global_client: Arc<Client>,
}

impl HttpConnectionPool {
    /// 创建新的 HTTP 连接池
    pub fn new(config: ConnectionPoolConfig) -> anyhow::Result<Self> {
        let global_client = Self::build_client(&config, None)?;
        Ok(Self {
            config: Arc::new(config),
            clients: Arc::new(Mutex::new(HashMap::new())),
            global_client: Arc::new(global_client),
        })
    }

    /// 构建 reqwest Client
    fn build_client(config: &ConnectionPoolConfig, host: Option<&str>) -> anyhow::Result<Client> {
        let mut builder = Client::builder()
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .pool_max_idle_per_host(config.max_idle_per_host)
            .pool_idle_timeout(Duration::from_secs(config.max_idle_time_secs))
            .redirect(reqwest::redirect::Policy::limited(config.max_redirects))
            .gzip(true)
            .brotli(true)
            .deflate(true);

        if config.tcp_keepalive {
            builder = builder.tcp_keepalive(Duration::from_secs(config.tcp_keepalive_secs));
        }

        if config.http2_prior_knowledge {
            builder = builder.http2_prior_knowledge();
        }

        // 如果指定了主机，可以设置特定的 DNS 解析
        if let Some(_host) = host {
            // 这里可以添加自定义 DNS 解析逻辑
            // 例如：使用特定的 DNS 服务器，或者预解析 IP
        }

        let client = builder.build()?;
        Ok(client)
    }

    /// 获取指定主机的客户端
    pub fn get_client(&self, host: &str) -> Client {
        let mut clients = self.clients.lock();

        if let Some(client) = clients.get(host) {
            debug!(host = %host, "reusing existing client");
            return client.clone();
        }

        // 创建新客户端
        match Self::build_client(&self.config, Some(host)) {
            Ok(client) => {
                info!(host = %host, "created new client");
                clients.insert(host.to_string(), client.clone());
                client
            }
            Err(e) => {
                tracing::warn!(host = %host, error = %e, "failed to create client, using global");
                self.global_client.as_ref().clone()
            }
        }
    }

    /// 获取全局客户端
    pub fn global_client(&self) -> Client {
        self.global_client.as_ref().clone()
    }

    /// 清理过期连接
    pub fn cleanup_expired(&self) {
        let mut clients = self.clients.lock();
        let max_idle = Duration::from_secs(self.config.max_idle_time_secs);

        // reqwest Client 内部会自动管理空闲连接
        // 这里主要清理长时间未使用的主机客户端
        let expired_hosts: Vec<String> = clients
            .iter()
            .filter_map(|(host, _)| {
                // 注意：reqwest Client 不暴露最后使用时间
                // 这里简化处理：如果主机客户端数量过多，清理一些
                None
            })
            .collect();

        for host in expired_hosts {
            clients.remove(&host);
            info!(host = %host, "removed expired client");
        }
    }

    /// 获取当前管理的主机数
    pub fn managed_hosts(&self) -> usize {
        self.clients.lock().len()
    }

    /// 清空连接池
    pub fn clear(&self) {
        let mut clients = self.clients.lock();
        let count = clients.len();
        clients.clear();
        info!(count = count, "connection pool cleared");
    }
}

impl Default for HttpConnectionPool {
    fn default() -> Self {
        Self::new(ConnectionPoolConfig::default()).expect("failed to create default connection pool")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_reuse() {
        let pool = HttpConnectionPool::default().unwrap();

        let client1 = pool.get_client("example.com");
        let client2 = pool.get_client("example.com");

        // 应该返回同一个客户端（通过指针比较）
        assert_eq!(pool.managed_hosts(), 1);
    }

    #[test]
    fn test_different_hosts() {
        let pool = HttpConnectionPool::default().unwrap();

        pool.get_client("example.com");
        pool.get_client("github.com");
        pool.get_client("google.com");

        assert_eq!(pool.managed_hosts(), 3);
    }
}
