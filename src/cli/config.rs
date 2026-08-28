use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
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


/// 任务级参数覆盖（配置或主控下发，None 时回退到 global 段默认值）
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TaskOverrides {
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

/// 覆盖后的任务级下载参数
#[derive(Debug, Clone)]
pub struct TaskParams {
    pub connections: u32,
    pub retry: u32,
    pub dry_run: bool,
    pub skip_tls_verify: bool,
    pub save_dir: PathBuf,
}

/// 解析任务级参数：任务覆盖优先，未覆盖项回退 global 段默认值
pub fn resolve_task_params(overrides: &TaskOverrides, cfg: &SpdeConfig, base_dir: &Path) -> TaskParams {
    let connections = overrides
        .connections_per_file
        .unwrap_or(cfg.global.connections_per_file);
    let retry = overrides.retry_times.unwrap_or(cfg.global.retry_times);
    let dry_run = overrides.dry_run.unwrap_or(cfg.global.dry_run);
    let skip_tls_verify = overrides
        .skip_tls_verify
        .unwrap_or(cfg.global.skip_tls_verify);
    let save_path = overrides.save_path.as_deref().unwrap_or(&cfg.output.save_path);
    let save_dir = resolve_save_dir(base_dir, save_path);
    TaskParams {
        connections,
        retry,
        dry_run,
        skip_tls_verify,
        save_dir,
    }
}

fn resolve_save_dir(base_dir: &Path, save_path: &str) -> PathBuf {
    let p = PathBuf::from(save_path);
    if p.is_absolute() {
        p
    } else {
        base_dir.join(p)
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
    #[serde(flatten)]
    #[serde(default)]
    pub overrides: TaskOverrides,
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn sample_cfg() -> SpdeConfig {
        // 通过完整 YAML 反序列化，验证 flatten 后的 TaskItem 兼容旧配置（不含 overrides 字段）
        let yaml = r#"
global:
  max_concurrent: 4
  retry_times: 3
  timeout: 1800
  skip_tls_verify: false
  connections_per_file: 8
  dry_run: false
output:
  save_path: "./download"
direct_tasks:
  - name: "no-overrides"
    url: "http://example.com/a.iso"
    filename: "a.iso"
  - name: "with-overrides"
    url: "http://example.com/b.iso"
    filename: "b.iso"
    connections_per_file: 16
    retry_times: 9
    skip_tls_verify: true
    dry_run: true
    save_path: "/abs/dir"
"#;
        serde_yaml::from_str(yaml).expect("sample config must parse")
    }

    #[test]
    fn task_overrides_parse_with_flatten() {
        let cfg = sample_cfg();
        assert_eq!(cfg.direct_tasks.len(), 2);

        // 无覆盖字段的任务 -> TaskOverrides 全 None
        let t0 = &cfg.direct_tasks[0];
        assert!(t0.overrides.connections_per_file.is_none());
        assert!(t0.overrides.save_path.is_none());

        // 有覆盖字段的任务 -> 平铺字段进入 TaskOverrides
        let t1 = &cfg.direct_tasks[1];
        assert_eq!(t1.overrides.connections_per_file, Some(16));
        assert_eq!(t1.overrides.retry_times, Some(9));
        assert_eq!(t1.overrides.skip_tls_verify, Some(true));
        assert_eq!(t1.overrides.dry_run, Some(true));
        assert_eq!(t1.overrides.save_path.as_deref(), Some("/abs/dir"));
    }

    #[test]
    fn resolve_falls_back_to_global() {
        let cfg = sample_cfg();
        let base = PathBuf::from("/spde-node");
        let p = resolve_task_params(&cfg.direct_tasks[0].overrides, &cfg, &base);
        assert_eq!(p.connections, 8);
        assert_eq!(p.retry, 3);
        assert!(!p.dry_run);
        assert!(!p.skip_tls_verify);
        // 相对 save_path 基于 base_dir
        assert_eq!(p.save_dir, base.join("./download"));
    }

    #[test]
    fn resolve_prefers_overrides() {
        let cfg = sample_cfg();
        let base = PathBuf::from("/spde-node");
        let p = resolve_task_params(&cfg.direct_tasks[1].overrides, &cfg, &base);
        assert_eq!(p.connections, 16);
        assert_eq!(p.retry, 9);
        assert!(p.dry_run);
        assert!(p.skip_tls_verify);
        // 绝对 save_path 直接使用
        assert_eq!(p.save_dir, PathBuf::from("/abs/dir"));
    }
}
