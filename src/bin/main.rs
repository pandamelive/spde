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
use spde::cli::new_download::{create_fetcher, detect_protocol, ProtocolType};
use spde::cli::p2p;
use spde::infra::disk::writer_factory::{create_writer, WriterType};
use spde::service::chunk_scheduler::{ChunkScheduler, ChunkSchedulerConfig};
use pandanetos::domain::CancellationToken;

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
    eprintln!("serve starting ... (NEW architecture)");
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

        // 任务级覆盖
        let params = resolve_task_params(&task_cfg.overrides, &cfg, &paths.base_dir);
        let name = task_cfg.name.clone();
        let url = task_cfg.url.clone();
        let filename = task_cfg.filename.clone();
        let timeout_secs = params.timeout.map(|d| d.as_secs()).unwrap_or(30);
        let dry_run = params.dry_run;
        let task_save_dir = params.save_dir.clone();

        let handle = tokio::spawn(async move {
            eprintln!("[start] {} -> {}", name, url);
            let started = Instant::now();

            // 步骤 1：协议识别
            let protocol = detect_protocol(&url);
            eprintln!("[protocol] {}: {:?}", name, protocol);
            // P2P 协议（BT/磁力）走独立下载逻辑，不经过通用 ChunkScheduler
            if matches!(protocol, ProtocolType::Torrent | ProtocolType::Magnet) {
                eprintln!("[p2p] {}: using self-managed download path", name);

                let (progress_tx, mut progress_rx) = mpsc::channel::<DownloadProgress>(100);
                let cancel = CancellationToken::new();

                let progress_name = name.clone();
                let progress_handle = tokio::spawn(async move {
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

                let p2p_result = p2p::download_p2p(
                    protocol,
                    &url,
                    &task_save_dir,
                    timeout_secs,
                    dry_run,
                    progress_tx,
                    cancel,
                )
                .await;
                drop(progress_handle);
                drop(permit);

                let elapsed_secs = started.elapsed().as_secs_f64();
                match p2p_result {
                    Ok(r) => {
                        eprintln!(
                            "[done] {}: success={}, downloaded={}MB, pieces={}/{}, elapsed={:.1}s",
                            name,
                            r.success,
                            r.downloaded_bytes / 1024 / 1024,
                            r.success_chunks,
                            r.success_chunks + r.failed_chunks,
                            elapsed_secs
                        );
                        let output = json!({
                            "total_size": r.total_bytes,
                            "downloaded_bytes": r.downloaded_bytes,
                            "elapsed_secs": elapsed_secs,
                            "avg_speed_mbps": if elapsed_secs > 0.0 { r.downloaded_bytes as f64 / elapsed_secs / 1024.0 / 1024.0 } else { 0.0 },
                            "status": if r.success { "success" } else { "failed" },
                            "is_success": r.success,
                            "success_chunks": r.success_chunks,
                            "failed_chunks": r.failed_chunks,
                            "error_msg": r.error_msg
                        });
                        return (name, url, filename, Ok(output));
                    }
                    Err(e) => {
                        eprintln!("[error] {}: {:#}", name, e);
                        return (name, url, filename, Err(e.to_string()));
                    }
                }
            }

            // 步骤 2：创建对应的 ChunkFetcher
            let fetcher = match create_fetcher(&url, protocol, timeout_secs, dry_run, &task_save_dir).await {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("[error] {}: create fetcher failed: {:#}", name, e);
                    drop(permit);
                    return (name, url, filename, Err(e.to_string()));
                }
            };

            // 步骤 3：probe 获取文件大小
            let file_size = match fetcher.probe().await {
                Ok((size, _)) => {
                    eprintln!("[probe] {}: file_size={} bytes", name, size);
                    size
                }
                Err(e) => {
                    eprintln!("[warn] {}: probe failed: {:#}, using 0", name, e);
                    0
                }
            };

            // 步骤 4：创建写入器
            let save_path = task_save_dir.join(&filename);
            let writer_type = if dry_run { WriterType::Null } else { WriterType::Disk };
            let writer = match create_writer(writer_type, Some(save_path.clone()), file_size) {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("[error] {}: create writer failed: {:#}", name, e);
                    drop(permit);
                    return (name, url, filename, Err(e.to_string()));
                }
            };

            // 步骤 5：创建统一分片调度器
            let scheduler_config = ChunkSchedulerConfig {
                initial_chunk_size: 4 * 1024 * 1024,
                min_chunk_size: 1 * 1024 * 1024,
                max_chunk_size: 64 * 1024 * 1024,
                max_retries: 3,
                initial_retry_interval_ms: 1000,
                progress_interval_ms: 500,
                ..Default::default()
            };
            let scheduler = ChunkScheduler::new(scheduler_config);

            // 步骤 6：添加源到调度器
            scheduler.add_source(fetcher.clone()).await;

            // 步骤 7：创建进度通道
            let (progress_tx, mut progress_rx) = mpsc::channel::<DownloadProgress>(100);

            // 步骤 8：创建取消令牌
            let cancel = CancellationToken::new();

            // 进度汇报任务：输出到控制台
            let progress_name = name.clone();
            let progress_handle = tokio::spawn(async move {
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

            // 步骤 9：执行下载
            let result = scheduler.execute(writer, progress_tx, cancel).await;
            drop(progress_handle);
            drop(permit);

            let elapsed_secs = started.elapsed().as_secs_f64();
            match result {
                Ok(r) => {
                    eprintln!(
                        "[done] {}: success={}, downloaded={}MB, chunks={}/{}, elapsed={:.1}s",
                        name,
                        r.success,
                        r.total_bytes / 1024 / 1024,
                        r.success_chunks,
                        r.total_chunks,
                        elapsed_secs
                    );
                    let output = json!({
                        "total_size": file_size,
                        "downloaded_bytes": r.total_bytes,
                        "elapsed_secs": elapsed_secs,
                        "avg_speed_mbps": if elapsed_secs > 0.0 { r.total_bytes as f64 / elapsed_secs / 1024.0 / 1024.0 } else { 0.0 },
                        "status": if r.success { "success" } else { "failed" },
                        "is_success": r.success,
                        "success_chunks": r.success_chunks,
                        "failed_chunks": r.failed_chunks,
                        "error_msg": r.error
                    });
                    (name, url, filename, Ok(output))
                }
                Err(e) => {
                    eprintln!("[error] {}: {:#}", name, e);
                    (name, url, filename, Err(e.to_string()))
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
                        "error_msg": e
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
    let avg_speed = if elapsed > 0.0 { total_mb / elapsed } else { 0.0 };

    eprintln!();
    eprintln!("========== 下载汇总 (NEW architecture) ==========");
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
    eprintln!("================================================");

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
