use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PathError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("base-dir bin directory must exist")]
    BinDirMissing,
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
    pub fn new(base_dir: &Path) -> Self {
        let base_dir = base_dir.to_path_buf();
        let bin_dir = base_dir.join("bin");
        let config_dir = base_dir.join("config");
        let data_dir = base_dir.join("data");
        Self {
            base_dir,
            bin_dir,
            config_dir,
            config_file: config_dir.join("config.yaml"),
            data_dir,
            node_id_file: data_dir.join("node-id.json"),
            run_history_file: data_dir.join("run-history.jsonl"),
        }
    }

    /// 启动自检：自动创建config/data；缺失config.yaml写入最小模板；bin必须已存在
    pub fn check_and_prepare(&self) -> Result<(), PathError> {
        if !self.bin_dir.exists() {
            return Err(PathError::BinDirMissing);
        }
        fs::create_dir_all(&self.config_dir)?;
        fs::create_dir_all(&self.data_dir)?;

        if !self.config_file.exists() {
            let minimal_yaml = r#"# SPDE minimal config
global:
  proxy: null
  max_concurrent: 4
  retry: 2
tasks: []
"#;
            fs::write(&self.config_file, minimal_yaml)?;
            eprintln!("auto generated minimal config: {:?}", self.config_file);
        }
        Ok(())
    }
}
