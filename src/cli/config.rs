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

#[derive(Debug, Clone, Deserialize)]
pub struct GlobalConfig {
    pub proxy: Option<String>,
    pub max_concurrent: u32,
    pub retry: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskItem {
    pub name: String,
    pub url: String,
    pub output_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpdeConfig {
    pub global: GlobalConfig,
    pub tasks: Vec<TaskItem>,
}

pub fn load_config(path: &Path) -> Result<SpdeConfig, ConfigError> {
    let content = fs::read_to_string(path)?;
    let cfg: SpdeConfig = serde_yaml::from_str(&content)?;
    Ok(cfg)
}
