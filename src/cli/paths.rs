use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum PathError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("cannot locate exe path")]
    ExePathNotFound,
    #[error("integrity check missing file: {0}")]
    IntegrityMissing(String),
}

#[derive(Debug, Clone)]
pub struct SpdePaths {
    pub base_dir: PathBuf,
    pub bin_dir: PathBuf,
    pub config_dir: PathBuf,
    pub config_file: PathBuf,
    pub data_dir: PathBuf,
    pub node_id_file: PathBuf,
    pub run_history_file: PathBuf,
}

impl SpdePaths {
    /// spde-node文件夹永远固定在exe同级目录，不受调用位置影响
    pub fn from_exe_side() -> Result<Self, PathError> {
        let exe_path = env::current_exe().map_err(|_| PathError::ExePathNotFound)?;
        let exe_parent = exe_path.parent().ok_or(PathError::ExePathNotFound)?;
        let root = exe_parent.join("spde-node");
        Ok(Self::from_root(&root))
    }

    /// 使用自定义根目录
    pub fn from_custom_path(root: &Path) -> Result<Self, PathError> {
        Ok(Self::from_root(root))
    }

    fn from_root(root: &Path) -> Self {
        let base_dir = root.to_path_buf();
        let bin_dir = base_dir.join("bin");
        let config_dir = base_dir.join("config");
        let data_dir = base_dir.join("data");

        let config_file = config_dir.join("config.yaml");
        let node_id_file = data_dir.join("node‑id.json");
        let run_history_file = data_dir.join("run‑history.jsonl");

        Self {
            base_dir,
            bin_dir,
            config_dir,
            config_file,
            data_dir,
            node_id_file,
            run_history_file,
        }
    }

    /// 第一步：缺失就自动创建，已有文件不会覆盖
    pub fn check_and_prepare(&self) -> Result<(), PathError> {
        fs::create_dir_all(&self.bin_dir)?;
        fs::create_dir_all(&self.config_dir)?;
        fs::create_dir_all(&self.data_dir)?;

        if !self.config_file.exists() {
            let minimal_yaml = r#"# SPDE config
agent:
  master: ""
  node_id: null
  heartbeat_interval_secs: 5
global:
  work_dir: null
  max_concurrent: 4
  resume: true
  retry_times: 3
  timeout: 1800
  skip_tls_verify: false
  connections_per_file: 8
  dry_run: true
output:
  save_path: "./download"
proxy:
  http_proxy: ""
  https_proxy: ""
controller:
  url: ""
  token: ""
direct_tasks: []
"#;
            fs::write(&self.config_file, minimal_yaml)?;
            eprintln!("[init] create default config: {:?}", self.config_file);
        }

        if !self.node_id_file.exists() {
            let node_content = format!(r#"{{"node_id":"{}"}}"#, Uuid::new_v4());
            fs::write(&self.node_id_file, node_content.as_bytes())?;
            eprintln!("[init] create node‑id file: {:?}", self.node_id_file);
        }

        if !self.run_history_file.exists() {
            fs::write(&self.run_history_file, b"")?;
            eprintln!(
                "[init] create history log file: {:?}",
                self.run_history_file
            );
        }
        Ok(())
    }

    /// 第二步：完整性校验，全部目录文件必须真实存在
    pub fn verify_integrity(&self) -> Result<(), PathError> {
        let checks = [
            (&self.base_dir, "base directory"),
            (&self.bin_dir, "bin directory"),
            (&self.config_dir, "config directory"),
            (&self.data_dir, "data directory"),
            (&self.config_file, "config.yaml"),
            (&self.node_id_file, "node‑id.json"),
            (&self.run_history_file, "run‑history.jsonl"),
        ];

        for (path, desc) in checks.iter() {
            if !path.exists() {
                return Err(PathError::IntegrityMissing(format!(
                    "{} -> {:?}",
                    desc, path
                )));
            }
        }
        eprintln!("[init] all directory & file integrity passed");
        Ok(())
    }
}
