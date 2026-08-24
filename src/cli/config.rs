use serde::Deserialize;
use std::fs;
use std::path::Path;
use thiserror::Error;

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

#[derive(Debug, Clone, Deserialize)]
pub struct ProxyConfig {
    #[serde(default)]
    pub http_proxy: String,
    #[serde(default)]
    pub https_proxy: String,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            http_proxy: String::new(),
            https_proxy: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskItem {
    pub name: String,
    #[serde(default = "default_true")]
    pub enable: bool,
    pub url: String,
    pub filename: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpdeConfig {
    pub global: GlobalConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub proxy: ProxyConfig,
    #[serde(default)]
    pub direct_tasks: Vec<TaskItem>,
}

pub fn load_config(path: &Path) -> Result<SpdeConfig, ConfigError> {
    let content = fs::read_to_string(path)?;
    let cfg: SpdeConfig = serde_yaml::from_str(&content)?;
    Ok(cfg)
}
