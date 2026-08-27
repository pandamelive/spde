use anyhow::{Context, Result};
use chrono::Local;
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

use spde::cli::config::{load_config, SpdeConfig};
use spde::cli::paths::SpdePaths;
use spde::{build_default_manager, DownloadTask, ProgressCallback, StderrProgress};

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: SubCommand,
}

#[derive(Debug, Subcommand)]
pub enum SubCommand {
    /// 启动下载服务（本地 config.yaml）
    Serve,
    /// 接入 PK 主控，拉取任务并回传统计
    Agent {
        /// PK 地址，如 http://10.0.0.8:5566
        #[arg(long)]
        master: Option<String>,
        /// 与 PK token 一致
        #[arg(long)]
        token: Option<String>,
    },
    /// 配置相关操作
    Config,
    /// 查看统计信息
    Stats,
}

async fn run_serve_logic(paths: &SpdePaths) -> Result<()> {
    eprintln!("serve starting ...");
    eprintln!("base_dir: {:?}", paths.base_dir);

    let cfg: SpdeConfig = load_config(&paths.config_file)
        .map_err(|e| anyhow::anyhow!("load config failed: {}", e))?;

    eprintln!("max_concurrent: {}", cfg.global.max_concurrent);
    eprintln!("task count: {}", cfg.direct_tasks.len());

    let enabled: Vec<_> = cfg.direct_tasks.iter().filter(|t| t.enable).collect();
    eprintln!("enabled task count: {}", enabled.len());

    if enabled.is_empty() {
        eprintln!("no enabled tasks, exit");
        return Ok(());
    }

    // 统一调度器：所有协议（HTTP/FTP/SFTP/BT/本地文件）自动路由
    let mgr = Arc::new(build_default_manager());
    eprintln!("registered backends: {:?}", mgr.backend_names());

    // 输出目录：绝对路径直接用，相对路径基于 base_dir
    let save_dir = PathBuf::from(&cfg.output.save_path);
    let save_dir = if save_dir.is_absolute() {
        save_dir
    } else {
        paths.base_dir.join(save_dir)
    };
    tokio::fs::create_dir_all(&save_dir)
        .await
        .context("create save dir failed")?;
    eprintln!("save dir: {:?}", save_dir);

    let semaphore = Arc::new(Semaphore::new(cfg.global.max_concurrent as usize));
    let cfg_connections = cfg.global.connections_per_file;
    let cfg_retry = cfg.global.retry_times;
    let cfg_dry_run = cfg.global.dry_run;
    let cfg_skip_tls = cfg.global.skip_tls_verify;
    let cfg_proxy = if !cfg.proxy.https_proxy.trim().is_empty() {
        cfg.proxy.https_proxy.clone()
    } else if !cfg.proxy.http_proxy.trim().is_empty() {
        cfg.proxy.http_proxy.clone()
    } else {
        String::new()
    };

    let overall_start = Instant::now();
    let history_file = paths.run_history_file.clone();

    // 读取历史总下载量
    let mut historical_total: u64 = 0;
    if let Ok(content) = tokio::fs::read_to_string(&history_file).await {
        for line in content.lines() {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(bytes) = json.get("downloaded_bytes").and_then(|v| v.as_u64()) {
                    historical_total += bytes;
                }
            }
        }
    }

    let mut handles = Vec::new();
    for task_cfg in &enabled {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .context("acquire semaphore failed")?;

        let mgr = mgr.clone();
        let url = task_cfg.url.clone();
        let name = task_cfg.name.clone();
        let filename = task_cfg.filename.clone();
        let file_path = save_dir.join(&task_cfg.filename);
        let proxy = cfg_proxy.clone();

        let handle = tokio::spawn(async move {
            eprintln!("[start] {} -> {:?}", name, file_path);

            // 构建统一任务：connections=0 时强制单连接以兼容旧配置语义
            let max_conn = if cfg_connections == 0 { 1 } else { cfg_connections };
            let task = DownloadTask {
                uri: url.clone(),
                save_path: file_path,
                max_conn,
                retry_times: cfg_retry,
                dry_run: cfg_dry_run,
                skip_tls_verify: cfg_skip_tls,
                proxy,
                ..Default::default()
            };

            let progress: Option<Arc<dyn ProgressCallback>> =
                Some(Arc::new(StderrProgress::new(name.clone())));

            let result = mgr.dispatch(task, progress).await;
            drop(permit);

            match result {
                Ok(o) => {
                    eprintln!(
                        "[done] {}  downloaded={}MB  chunks ok={} fail={}",
                        name,
                        o.downloaded_bytes / 1024 / 1024,
                        o.success_chunks,
                        o.failed_chunks
                    );
                    (name, url, filename, Ok(o))
                }
                Err(e) => {
                    eprintln!("[error] {}: {:#}", name, e);
                    (name, url, filename, Err(e))
                }
            }
        });
        handles.push(handle);
    }

    let mut total_bytes: u64 = 0;
    let mut success_count = 0u32;
    let mut fail_count = 0u32;
    let mut history_lines: Vec<String> = Vec::new();

    for h in handles {
        if let Ok((name, url, filename, result)) = h.await {
            let timestamp = Local::now().to_rfc3339();
            let record = match result {
                Ok(o) => {
                    total_bytes += o.downloaded_bytes;
                    if o.status == "failed" || !o.is_success {
                        fail_count += 1;
                    } else {
                        success_count += 1;
                    }
                    json!({
                        "timestamp": timestamp,
                        "task_name": name,
                        "url": url,
                        "filename": filename,
                        "file_size": o.total_size,
                        "downloaded_bytes": o.downloaded_bytes,
                        "elapsed_secs": o.elapsed_secs,
                        "avg_speed_mbps": o.avg_speed_mbps,
                        "status": o.status,
                        "success_chunks": o.success_chunks,
                        "failed_chunks": o.failed_chunks,
                        "error_msg": o.error_msg
                    })
                }
                Err(e) => {
                    fail_count += 1;
                    json!({
                        "timestamp": timestamp,
                        "task_name": name,
                        "url": url,
                        "filename": filename,
                        "file_size": 0,
                        "downloaded_bytes": 0,
                        "elapsed_secs": 0.0,
                        "avg_speed_mbps": 0.0,
                        "status": "failed",
                        "success_chunks": 0,
                        "failed_chunks": 0,
                        "error_msg": e.to_string()
                    })
                }
            };
            history_lines.push(serde_json::to_string(&record).unwrap_or_default());
        }
    }

    // 追加写入 run-history.jsonl
    if !history_lines.is_empty() {
        let content = history_lines.join("\n") + "\n";
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&history_file)
            .await
            .context("open run-history.jsonl failed")?
            .write_all(content.as_bytes())
            .await
            .context("write run-history.jsonl failed")?;
        eprintln!("[history] {} records appended to {:?}", history_lines.len(), history_file);
    }

    let elapsed = overall_start.elapsed().as_secs_f64();
    let total_mb = total_bytes as f64 / 1024.0 / 1024.0;
    let avg_speed = if elapsed > 0.0 { total_mb / elapsed } else { 0.0 };

    eprintln!("");
    eprintln!("========== 下载汇总 ==========");
    eprintln!("总任务数: {} (成功: {} 失败: {})", enabled.len(), success_count, fail_count);
    eprintln!("本次下载量: {:.1} MB ({:.2} GB)", total_mb, total_mb / 1024.0);
    let grand_total = historical_total + total_bytes;
    let grand_total_mb = grand_total as f64 / 1024.0 / 1024.0;
    eprintln!("历史总下载量: {:.1} MB ({:.2} GB)", grand_total_mb, grand_total_mb / 1024.0);
    eprintln!("总耗时: {:.1}s", elapsed);
    eprintln!("平均速度: {:.1} MB/s", avg_speed);
    eprintln!("==============================");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths = SpdePaths::from_exe_side()?;
    eprintln!("spde work root: {:?}", paths.base_dir);
    paths
        .check_and_prepare()
        .context("初始化目录文件失败")?;
    paths
        .verify_integrity()
        .context("目录文件完整性校验失败")?;

    match cli.cmd {
        SubCommand::Serve => {
            run_serve_logic(&paths).await?;
        }
        SubCommand::Agent { master, token } => {
            let master = master.unwrap_or_default();
            let token = token.unwrap_or_default();
            spde::cli::agent::run_agent(&paths, master, token).await?;
        }
        SubCommand::Config => {
            eprintln!("config subcommand done");
        }
        SubCommand::Stats => {
            eprintln!("stats subcommand done");
        }
    }
    Ok(())
}
