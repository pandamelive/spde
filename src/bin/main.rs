use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Semaphore};

use pandanetos::domain::{ChunkDownloader, DownloadProgress, DownloadSource};
use spde::cli::config::{load_config, resolve_task_params, SpdeConfig};
use spde::cli::paths::SpdePaths;
use spde::domain::DownloadConfig;
use spde::infra::file::downloader::FileChunkDownloader;
use spde::infra::file::source::FileSource;
#[cfg(feature = "ftp")]
use spde::infra::ftp::downloader::FtpChunkDownloader;
#[cfg(feature = "ftp")]
use spde::infra::ftp::source::FtpSource;
use spde::infra::http::downloader::HttpChunkDownloader;
use spde::infra::http::mirror::dns::DnsMultiIpDiscoverer;
use spde::infra::http::source::HttpSource;
use spde::infra::ssh::downloader::SshChunkDownloader;
use spde::infra::ssh::source::SshSource;
#[cfg(feature = "torrent")]
use spde::infra::torrent::downloader::TorrentChunkDownloader;
#[cfg(feature = "torrent")]
use spde::infra::torrent::source::TorrentSource;
use spde::service::scheduler::DownloadScheduler;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: SubCommand,
}

#[derive(Debug, Subcommand)]
pub enum SubCommand {
    /// 输出自描述能力清单（说明书）
    Manifest,
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

/// 根据 URL 协议类型创建对应的 Source 和 Downloader
fn create_source_and_downloader(
    url: &str,
    save_path: &Path,
    skip_tls_verify: bool,
    timeout_secs: u64,
    dry_run: bool,
) -> Result<(Box<dyn DownloadSource>, Arc<dyn ChunkDownloader>)> {
    if FileSource::is_file_uri(url) {
        let source = FileSource::from_uri(url)?;
        Ok((Box::new(source), Arc::new(FileChunkDownloader::new())))
    } else if url.starts_with("http://") || url.starts_with("https://") {
        Ok((
            Box::new(HttpSource::new(url.to_string())),
            Arc::new(HttpChunkDownloader::new(skip_tls_verify, timeout_secs)),
        ))
    } else if FtpSource::is_ftp_uri(url) {
        #[cfg(feature = "ftp")]
        {
            let source = FtpSource::new(url)?;
            Ok((
                Box::new(source),
                Arc::new(FtpChunkDownloader::new(skip_tls_verify, timeout_secs)),
            ))
        }
        #[cfg(not(feature = "ftp"))]
        {
            anyhow::bail!("ftp support not compiled in, rebuild with --features ftp")
        }
    } else if SshSource::is_ssh_uri(url) {
        let source = SshSource::new(url)?;
        Ok((
            Box::new(source),
            Arc::new(SshChunkDownloader::new(timeout_secs)),
        ))
    } else if TorrentSource::is_torrent_uri(url) {
        #[cfg(feature = "torrent")]
        {
            let save_dir = save_path
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            let source = TorrentSource::new(url, save_dir)?;
            Ok((
                Box::new(source),
                Arc::new(TorrentChunkDownloader::new(timeout_secs, dry_run)),
            ))
        }
        #[cfg(not(feature = "torrent"))]
        {
            anyhow::bail!("torrent support not compiled in, rebuild with --features torrent")
        }
    } else {
        anyhow::bail!("unsupported protocol: {}", url)
    }
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
    let scheduler_config = DownloadConfig {
        max_connections: cfg.global.connections_per_file,
        chunk_size: 4 * 1024 * 1024, // 4MB 默认分片大小
        enable_mirror_discovery: true,
        enable_adaptive: true,
        save_dir: PathBuf::from(&cfg.output.save_path),
        ..Default::default()
    };
    let max_conns = scheduler_config.max_connections;
    let scheduler = Arc::new(DownloadScheduler::new(scheduler_config));

    // 注册镜像发现器（HTTP 专用）
    let mirror_bus = scheduler.mirror_bus();
    mirror_bus
        .register(Box::new(DnsMultiIpDiscoverer::new()))
        .await;
    eprintln!("scheduler initialized: max_connections={}", max_conns);

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

    let semaphore = Arc::new(Semaphore::new(cfg.global.max_concurrent.max(1) as usize));

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

        // 任务级覆盖（config.yaml direct_tasks 内联字段），未覆盖项回退 global 段默认值
        let params = resolve_task_params(&task_cfg.overrides, &cfg, &paths.base_dir);
        let name = task_cfg.name.clone();
        let url = task_cfg.url.clone();
        let filename = task_cfg.filename.clone();
        let file_path = params.save_dir.join(&task_cfg.filename);
        let skip_tls_verify = params.skip_tls_verify;
        let timeout_secs = params.timeout.map(|d| d.as_secs()).unwrap_or(30);

        let scheduler = scheduler.clone();
        let handle = tokio::spawn(async move {
            eprintln!("[start] {} -> {:?}", name, file_path);

            // 根据 URL 协议类型创建对应的 Source 和 Downloader
            let (source, downloader) = match create_source_and_downloader(
                &url,
                &file_path,
                skip_tls_verify,
                timeout_secs,
                params.dry_run,
            ) {
                    Ok(sd) => sd,
                    Err(e) => {
                        eprintln!("[error] {}: create source failed: {:#}", name, e);
                        return (name, url, filename, Err(e));
                    }
                };

            // 创建进度汇报通道
            let (progress_tx, mut progress_rx) = mpsc::channel::<DownloadProgress>(100);
            let progress_name = name.clone();
            tokio::spawn(async move {
                while let Some(progress) = progress_rx.recv().await {
                    let percent = if progress.total_bytes > 0 {
                        progress.downloaded_bytes as f64 / progress.total_bytes as f64 * 100.0
                    } else {
                        0.0
                    };
                    let speed_mbps = progress.speed_bps as f64 / 1024.0 / 1024.0;
                    eprintln!(
                        "[progress] {}: {:.1}% ({}/{} MB) {:.2} MB/s",
                        progress_name,
                        percent,
                        progress.downloaded_bytes / 1024 / 1024,
                        progress.total_bytes / 1024 / 1024,
                        speed_mbps
                    );
                }
            });

            // 执行下载
            let result = scheduler
                .download(source, downloader, file_path.clone(), progress_tx)
                .await;
            drop(permit);

            match result {
                Ok(r) => {
                    eprintln!(
                        "[done] {}  downloaded={}MB",
                        name,
                        r.downloaded_bytes / 1024 / 1024,
                    );
                    // 转换为旧格式的输出（兼容后续统计和历史记录）
                    let output = json!({
                        "total_size": r.total_bytes,
                        "downloaded_bytes": r.downloaded_bytes,
                        "elapsed_secs": r.elapsed_secs,
                        "avg_speed_mbps": if r.elapsed_secs > 0.0 { r.downloaded_bytes as f64 / r.elapsed_secs / 1024.0 / 1024.0 } else { 0.0 },
                        "status": if r.success { "success" } else { "failed" },
                        "is_success": r.success,
                        "success_chunks": 0,
                        "failed_chunks": 0,
                        "error_msg": null
                    });
                    (name, url, filename, Ok(output))
                }
                Err(e) => {
                    eprintln!("[error] {}: {:#}", name, e);
                    (name, url, filename, Err(e.into()))
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
            let timestamp = pandanetos::time::now_rfc3339();
            let record = match result {
                Ok(o) => {
                    let downloaded = o
                        .get("downloaded_bytes")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let status = o
                        .get("status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    total_bytes += downloaded;
                    if status == "failed" {
                        fail_count += 1;
                    } else {
                        success_count += 1;
                    }
                    json!({
                        "timestamp": timestamp,
                        "task_name": name,
                        "url": url,
                        "filename": filename,
                        "file_size": o.get("total_size").and_then(|v| v.as_u64()).unwrap_or(0),
                        "downloaded_bytes": downloaded,
                        "elapsed_secs": o.get("elapsed_secs").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        "avg_speed_mbps": o.get("avg_speed_mbps").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        "status": status,
                        "success_chunks": o.get("success_chunks").and_then(|v| v.as_u64()).unwrap_or(0),
                        "failed_chunks": o.get("failed_chunks").and_then(|v| v.as_u64()).unwrap_or(0),
                        "error_msg": o.get("error_msg").and_then(|v| v.as_str())
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
        eprintln!(
            "[history] {} records appended to {:?}",
            history_lines.len(),
            history_file
        );
    }

    let elapsed = overall_start.elapsed().as_secs_f64();
    let total_mb = total_bytes as f64 / 1024.0 / 1024.0;
    let avg_speed = if elapsed > 0.0 {
        total_mb / elapsed
    } else {
        0.0
    };

    eprintln!();
    eprintln!("========== 下载汇总 ==========");
    eprintln!(
        "总任务数: {} (成功: {} 失败: {})",
        enabled.len(),
        success_count,
        fail_count
    );
    eprintln!(
        "本次下载量: {:.1} MB ({:.2} GB)",
        total_mb,
        total_mb / 1024.0
    );
    let grand_total = historical_total + total_bytes;
    let grand_total_mb = grand_total as f64 / 1024.0 / 1024.0;
    eprintln!(
        "历史总下载量: {:.1} MB ({:.2} GB)",
        grand_total_mb,
        grand_total_mb / 1024.0
    );
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
    paths.check_and_prepare().context("初始化目录文件失败")?;
    paths.verify_integrity().context("目录文件完整性校验失败")?;

    match cli.cmd {
        SubCommand::Manifest => {
            spde::cli::manifest::print_manifest();
            return Ok(());
        }
        SubCommand::Serve => {
            run_serve_logic(&paths).await?;
        }
        SubCommand::Agent { master, token } => {
            let master = master.unwrap_or_default();
            let token = token.unwrap_or_default();
            spde::cli::agent::run_agent(&paths, master, token).await?;
        }
        SubCommand::Config => {
            // 仅校验并展示本地 config.yaml（此处为 CLI 展示用途，沿用本地加载）
            match load_config(&paths.config_file) {
                Ok(cfg) => {
                    eprintln!(
                        "config ok: max_concurrent={}, connections={}, timeout={}s, save_path={}",
                        cfg.global.max_concurrent,
                        cfg.global.connections_per_file,
                        cfg.global.timeout,
                        cfg.output.save_path
                    );
                    eprintln!("controller.url={}", cfg.controller.url);
                    eprintln!("direct_tasks={}", cfg.direct_tasks.len());
                }
                Err(e) => {
                    anyhow::bail!("config invalid: {:#}", e);
                }
            }
        }
        SubCommand::Stats => {
            print_stats(&paths).await?;
        }
    }

    Ok(())
}

/// 汇总 run-history.jsonl 统计（与 serve 汇总输出保持一致）
async fn print_stats(paths: &SpdePaths) -> Result<()> {
    eprintln!("== 统计信息 ==");
    eprintln!("history file: {:?}", paths.run_history_file);

    let content = match tokio::fs::read_to_string(&paths.run_history_file).await {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("(empty: 尚无下载历史)");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let mut record_count = 0usize;
    let mut success_count = 0u32;
    let mut fail_count = 0u32;
    let mut total_bytes: u64 = 0;
    let mut total_elapsed: f64 = 0.0;
    let mut max_speed_mbps: f64 = 0.0;

    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        record_count += 1;
        let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("");
        if status == "failed" {
            fail_count += 1;
        } else {
            success_count += 1;
        }
        if let Some(b) = v.get("downloaded_bytes").and_then(|x| x.as_u64()) {
            total_bytes += b;
        }
        if let Some(e) = v.get("elapsed_secs").and_then(|x| x.as_f64()) {
            total_elapsed += e;
        }
        if let Some(s) = v.get("avg_speed_mbps").and_then(|x| x.as_f64()) {
            if s > max_speed_mbps {
                max_speed_mbps = s;
            }
        }
    }

    if record_count == 0 {
        eprintln!("(empty: 尚无下载历史)");
        return Ok(());
    }

    let avg_speed_mbps = if total_elapsed > 0.0 {
        total_bytes as f64 / 1024.0 / 1024.0 / total_elapsed
    } else {
        0.0
    };
    let total_mb = total_bytes as f64 / 1024.0 / 1024.0;

    eprintln!(
        "总记录数: {} (成功: {} 失败: {})",
        record_count, success_count, fail_count
    );
    eprintln!(
        "累计下载量: {:.1} MB ({:.2} GB)",
        total_mb,
        total_mb / 1024.0
    );
    eprintln!(
        "累计耗时: {:.1} s, 平均速度: {:.1} MB/s, 峰值速度: {:.1} MB/s",
        total_elapsed, avg_speed_mbps, max_speed_mbps
    );

    eprintln!("\n最近 {} 条记录:", record_count.min(10));
    for line in content.lines().rev().take(10) {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
            let ts = v.get("timestamp").and_then(|x| x.as_str()).unwrap_or("-");
            let name = v.get("task_name").and_then(|x| x.as_str()).unwrap_or("-");
            let status = v.get("status").and_then(|x| x.as_str()).unwrap_or("-");
            let mb = v
                .get("downloaded_bytes")
                .and_then(|x| x.as_u64())
                .map(|b| b as f64 / 1024.0 / 1024.0)
                .unwrap_or(0.0);
            eprintln!("  {}  {}  {}  {:.1} MB", ts, status, name, mb);
        }
    }

    Ok(())
}
