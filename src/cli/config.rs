use serde::Deserialize;
use std::fs;
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("yaml parse: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

fn default_true() -> bool {
    true
}
fn default_retry() -> u32 {
    3
}
fn default_timeout() -> u64 {
    1800
}
fn default_connections() -> u32 {
    8
}
fn default_max_concurrent() -> u32 {
    4
}
fn default_save_path() -> String {
    "./download".to_string()
}
fn default_heartbeat() -> u64 {
    5
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AgentConfig {
    #[serde(default)]
    pub master: String,
    #[serde(default)]
    pub node_id: Option<Uuid>,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GlobalConfig {
    #[serde(default)]
    pub work_dir: Option<String>,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: u32,
    #[serde(default = "default_true")]
    pub resume: bool,
    #[serde(default = "default_retry")]
    pub retry_times: u32,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    #[serde(default)]
    pub skip_tls_verify: bool,
    #[serde(default = "default_connections")]
    pub connections_per_file: u32,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OutputConfig {
    #[serde(default = "default_save_path")]
    pub save_path: String,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            save_path: default_save_path(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyConfig {
    #[serde(default)]
    pub http_proxy: String,
    #[serde(default)]
    pub https_proxy: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskItem {
    pub name: String,
    #[serde(default = "default_true")]
    pub enable: bool,
    pub url: String,
    pub filename: String,
    #[serde(default)]
    pub task_id: Option<Uuid>,
    #[serde(default)]
    pub dispatch_id: Option<Uuid>,
    // ── 任务级下载参数覆盖（None 时用 global 段默认值） ──
    #[serde(default)]
    pub max_concurrent: Option<u32>,
    #[serde(default)]
    pub connections_per_file: Option<u32>,
    #[serde(default)]
    pub retry_times: Option<u32>,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub skip_tls_verify: Option<bool>,
    #[serde(default)]
    pub dry_run: Option<bool>,
    #[serde(default)]
    pub save_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ControllerConfig {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpdeConfig {
    #[serde(default)]
    pub agent: AgentConfig,
    pub global: GlobalConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub proxy: ProxyConfig,
    #[serde(default)]
    pub controller: ControllerConfig,
    #[serde(default)]
    pub direct_tasks: Vec<TaskItem>,
}

pub fn load_config(path: &Path) -> Result<SpdeConfig, ConfigError> {
    let content = fs::read_to_string(path)?;
    let cfg: SpdeConfig = serde_yaml::from_str(&content)?;
    Ok(cfg)
}
