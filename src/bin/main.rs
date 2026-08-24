use anyhow::{Context, Result};
use chrono::Local;
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

use spde::cli::config::{load_config, SpdeConfig};
use spde::cli::paths::SpdePaths;
use spde::{download_file, DownloadMetrics};

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: SubCommand,
}

#[derive(Debug, Subcommand)]
pub enum SubCommand {
    /// 启动服务
    Serve,
    /// 配置相关操作
    Config,
    /// 查看统计信息
    Stats,
}

fn build_client(cfg: &SpdeConfig) -> Result<Client> {
    let mut builder = Client::builder()
        .timeout(std::time::Duration::from_secs(cfg.global.timeout))
        .http1_only()
        .tcp_nodelay(true);

    if cfg.global.skip_tls_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }

    if !cfg.proxy.https_proxy.trim().is_empty() {
        let p = reqwest::Proxy::https(cfg.proxy.https_proxy.trim())
            .context("invalid https proxy")?;
        builder = builder.proxy(p);
    }
    if !cfg.proxy.http_proxy.trim().is_empty() {
        let p = reqwest::Proxy::http(cfg.proxy.http_proxy.trim())
            .context("invalid http proxy")?;
        builder = builder.proxy(p);
    }

    Ok(builder.build().context("build http client failed")?)
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

    let client = build_client(&cfg)?;

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
    let client = Arc::new(client);
    let cfg_connections = cfg.global.connections_per_file;
    let cfg_retry = cfg.global.retry_times;
    let cfg_dry_run = cfg.global.dry_run;

    let overall_start = Instant::now();
    let history_file = paths.run_history_file.clone();

    // 读取历史总下载量（从 run-history.jsonl 累加所有记录的 downloaded_bytes）
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
    for task in &enabled {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .context("acquire semaphore failed")?;
        let client = client.clone();
        let url = task.url.clone();
        let name = task.name.clone();
        let filename = task.filename.clone();
        let file_path = save_dir.join(&task.filename);

        let handle = tokio::spawn(async move {
            eprintln!("[start] {} -> {:?}", name, file_path);
            let result = download_file(&client, &url, file_path, cfg_connections, cfg_retry, cfg_dry_run).await;
            drop(permit);
            match result {
                Ok(m) => {
                    eprintln!(
                        "[done] {}  downloaded={}MB  chunks ok={} fail={}",
                        name,
                        m.downloaded_bytes / 1024 / 1024,
                        m.success_chunks,
                        m.failed_chunks
                    );
                    (name, url, filename, Ok(m))
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
                Ok(m) => {
                    total_bytes += m.downloaded_bytes;
                    if m.status == "failed" {
                        fail_count += 1;
                    } else {
                        success_count += 1;
                    }
                    let avg_speed = if m.elapsed_secs > 0.0 {
                        m.downloaded_bytes as f64 / m.elapsed_secs / 1024.0 / 1024.0
                    } else {
                        0.0
                    };
                    json!({
                        "timestamp": timestamp,
                        "task_name": name,
                        "url": url,
                        "filename": filename,
                        "file_size": m.total_size,
                        "downloaded_bytes": m.downloaded_bytes,
                        "elapsed_secs": m.elapsed_secs,
                        "avg_speed_mbps": avg_speed,
                        "status": m.status,
                        "success_chunks": m.success_chunks,
                        "failed_chunks": m.failed_chunks,
                        "error_msg": m.error_msg
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
        SubCommand::Config => {
            eprintln!("config subcommand done");
        }
        SubCommand::Stats => {
            eprintln!("stats subcommand done");
        }
    }

    Ok(())
}
