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
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default)]
    pub use_new_downloader: Option<bool>,
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

/// 浠诲姟绾у弬鏁拌鐩栵紙閰嶇疆鎴栦富鎺т笅鍙戯紝None 鏃跺洖閫€鍒?global 娈甸粯璁ゅ€硷級
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
    #[serde(default)]
    pub use_new_downloader: Option<bool>,
}

/// 瑕嗙洊鍚庣殑浠诲姟绾т笅杞藉弬鏁?
#[derive(Debug, Clone)]
pub struct TaskParams {
    pub connections: u32,
    pub retry: u32,
    pub dry_run: bool,
    pub skip_tls_verify: bool,
    pub save_dir: PathBuf,
    pub resume: bool,
    pub timeout: Option<std::time::Duration>,
}

/// 瑙ｆ瀽浠诲姟绾у弬鏁帮細浠诲姟瑕嗙洊浼樺厛锛屾湭瑕嗙洊椤瑰洖閫€ global 娈甸粯璁ゅ€?
pub fn resolve_task_params(
    overrides: &TaskOverrides,
    cfg: &SpdeConfig,
    base_dir: &Path,
) -> TaskParams {
    // connections=0 鏃跺己鍒跺崟杩炴帴浠ュ吋瀹规棫閰嶇疆璇箟
    let connections = overrides
        .connections_per_file
        .unwrap_or(cfg.global.connections_per_file)
        .max(1);
    let retry = overrides.retry_times.unwrap_or(cfg.global.retry_times);
    let dry_run = overrides.dry_run.unwrap_or(cfg.global.dry_run);
    let skip_tls_verify = overrides
        .skip_tls_verify
        .unwrap_or(cfg.global.skip_tls_verify);
    let save_path = overrides
        .save_path
        .as_deref()
        .unwrap_or(&cfg.output.save_path);
    let save_dir = resolve_save_dir(base_dir, save_path);
    let resume = cfg.global.resume;
    // 瓒呮椂绉掓暟 > 0 鎵嶇敓鏁堬紱0 瑙嗕负涓嶉檺鏃?
    let timeout_secs = overrides.timeout.unwrap_or(cfg.global.timeout);
    let timeout = if timeout_secs > 0 {
        Some(std::time::Duration::from_secs(timeout_secs))
    } else {
        None
    };
    TaskParams {
        connections,
        retry,
        dry_run,
        skip_tls_verify,
        save_dir,
        resume,
        timeout,
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
    // 鈹€鈹€ 浠诲姟绾т笅杞藉弬鏁拌鐩栵紙None 鏃剁敤 global 娈甸粯璁ゅ€硷級 鈹€鈹€
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
    use std::time::Duration;

    fn sample_cfg() -> SpdeConfig {
        // 閫氳繃瀹屾暣 YAML 鍙嶅簭鍒楀寲锛岄獙璇?flatten 鍚庣殑 TaskItem 鍏煎鏃ч厤缃紙涓嶅惈 overrides 瀛楁锛?
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
    timeout: 7200
"#;
        serde_yaml::from_str(yaml).expect("sample config must parse")
    }

    #[test]
    fn task_overrides_parse_with_flatten() {
        let cfg = sample_cfg();
        assert_eq!(cfg.direct_tasks.len(), 2);

        // 鏃犺鐩栧瓧娈电殑浠诲姟 -> TaskOverrides 鍏?None
        let t0 = &cfg.direct_tasks[0];
        assert!(t0.overrides.connections_per_file.is_none());
        assert!(t0.overrides.save_path.is_none());

        // 鏈夎鐩栧瓧娈电殑浠诲姟 -> 骞抽摵瀛楁杩涘叆 TaskOverrides
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
        // 鐩稿 save_path 鍩轰簬 base_dir
        assert_eq!(p.save_dir, base.join("./download"));
        // 鏈鐩栭」鍥為€€ global 娈甸粯璁ゅ€?
        assert!(p.resume);
        assert_eq!(p.timeout, Some(Duration::from_secs(1800)));
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
        // 缁濆 save_path 鐩存帴浣跨敤
        assert_eq!(p.save_dir, PathBuf::from("/abs/dir"));
        // 浠诲姟绾?timeout 瑕嗙洊鐢熸晥
        assert_eq!(p.timeout, Some(Duration::from_secs(7200)));
        assert!(p.resume);
    }

    #[test]
    fn resolve_timeout_zero_disables() {
        // timeout=0 瑙嗕负涓嶉檺鏃?
        let yaml = r#"
global:
  max_concurrent: 4
  retry_times: 3
  timeout: 0
  connections_per_file: 8
  dry_run: false
output:
  save_path: "./download"
direct_tasks:
  - name: "t"
    url: "http://example.com/x.iso"
    filename: "x.iso"
"#;
        let cfg: SpdeConfig = serde_yaml::from_str(yaml).expect("sample config must parse");
        let base = PathBuf::from("/spde-node");
        let p = resolve_task_params(&cfg.direct_tasks[0].overrides, &cfg, &base);
        assert_eq!(p.timeout, None);
    }

    #[test]
    fn resolve_connections_zero_clamps_to_one() {
        // connections=0锛堟垨鏈厤缃椂鎭板ソ涓?0锛夊己鍒跺崟杩炴帴锛屽吋瀹规棫閰嶇疆璇箟
        let yaml = r#"
global:
  max_concurrent: 4
  retry_times: 3
  timeout: 1800
  connections_per_file: 0
  dry_run: false
output:
  save_path: "./download"
direct_tasks:
  - name: "t0"
    url: "http://example.com/x.iso"
    filename: "x.iso"
    connections_per_file: 0
"#;
        let cfg: SpdeConfig = serde_yaml::from_str(yaml).expect("sample config must parse");
        let base = PathBuf::from("/spde-node");
        let p = resolve_task_params(&cfg.direct_tasks[0].overrides, &cfg, &base);
        assert_eq!(p.connections, 1);
    }
}
