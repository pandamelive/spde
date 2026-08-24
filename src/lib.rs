pub mod cli;
pub mod core;
mod downloader;

pub use core::{EventMeta, SpdeEvent};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

#[derive(Debug, Default)]
pub struct DownloadMetrics {
    pub downloaded_bytes: u64,
    pub total_size: u64,
    pub success_chunks: u64,
    pub failed_chunks: u64,
    pub elapsed_secs: f64,
    pub status: String,
    pub error_msg: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DownloadOption {
    pub url: String,
    pub save_path: PathBuf,
}

/// 多连接分片下载入口
pub async fn download_file(
    client: &Client,
    url: &str,
    file_path: PathBuf,
    connections: u32,
    retry_times: u32,
    dry_run: bool,
) -> Result<DownloadMetrics> {
    let name = file_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    if dry_run {
        eprintln!("[dry-run] {} (data will be discarded, not saved to disk)", name);
    }

    // 1. 探测文件大小和 Range 支持
    let (total_size, accept_ranges) = probe_file(client, url).await?;

    // 非 dry_run 且目标文件已存在且大小匹配 → 跳过
    if !dry_run {
        if let Ok(meta) = tokio::fs::metadata(&file_path).await {
            if meta.len() == total_size && total_size > 0 {
                eprintln!(
                    "[skip] {} already downloaded ({:.1} MB)",
                    name,
                    total_size as f64 / 1024.0 / 1024.0
                );
                let mut m = DownloadMetrics::default();
                m.downloaded_bytes = 0;
                m.total_size = total_size;
                m.elapsed_secs = 0.0;
                m.status = "skipped".to_string();
                return Ok(m);
            }
        }
    }

    let downloaded = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let total = total_size;

    // 启动实时进度显示（每500ms刷新）
    let prog_dl = downloaded.clone();
    let prog_name = name.clone();
    let progress_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let dl = prog_dl.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                dl as f64 / elapsed / 1024.0 / 1024.0
            } else {
                0.0
            };
            let percent = if total > 0 { dl * 100 / total } else { 0 };
            eprintln!(
                "[progress] {}: {}% ({:.1}/{:.1} MB) speed: {:.1} MB/s",
                prog_name,
                percent,
                dl as f64 / 1024.0 / 1024.0,
                total as f64 / 1024.0 / 1024.0,
                speed
            );
        }
    });

    // 不支持 Range / 文件太小 / 连接数为1 → 单连接 fallback
    if !accept_ranges || connections <= 1 || total_size < 4 * 1024 * 1024 {
        let result = download_single(client, url, &file_path, downloaded.clone(), dry_run).await;
        progress_handle.abort();
        return print_final(name, total, start, result);
    }

    // 2. 多连接分片下载到 .part 临时文件
    let part_path = PathBuf::from(format!("{}.part", file_path.display()));

    // 预分配文件空间（dry_run 模式跳过）
    if !dry_run {
        let f = File::options()
            .create(true)
            .write(true)
            .read(true)
            .open(&part_path)
            .await
            .context("create part file failed")?;
        f.set_len(total_size).await.context("preallocate failed")?;
    }

    // 计算分片区间
    let conn = connections.max(1) as u64;
    let chunk_size = (total_size + conn - 1) / conn;
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    let mut chunk_start = 0u64;
    while chunk_start < total_size {
        let end = (chunk_start + chunk_size - 1).min(total_size - 1);
        ranges.push((chunk_start, end));
        chunk_start = end + 1;
    }

    // 并发下载所有分片（带重试）
    let client = client.clone();
    let url = url.to_string();
    let part_path_clone = part_path.clone();
    let retry = retry_times.max(1);

    let handles: Vec<_> = ranges
        .into_iter()
        .map(|(s, e)| {
            let c = client.clone();
            let u = url.clone();
            let p = part_path_clone.clone();
            let dl = downloaded.clone();
            let dr = dry_run;
            tokio::spawn(async move {
                let mut last_err: Option<String> = None;
                for attempt in 0..retry {
                    match download_range(&c, &u, &p, s, e, dl.clone(), dr).await {
                        Ok(m) if m.error_msg.is_none() => return Ok(m),
                        Ok(m) => last_err = m.error_msg,
                        Err(e) => last_err = Some(e.to_string()),
                    }
                    if attempt + 1 < retry {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            500 * (attempt + 1) as u64,
                        ))
                        .await;
                    }
                }
                Err(anyhow::anyhow!(
                    "range {}-{} failed after {} retries: {}",
                    s,
                    e,
                    retry,
                    last_err.unwrap_or_else(|| "unknown".into())
                ))
            })
        })
        .collect();

    let mut metrics = DownloadMetrics::default();
    let mut has_error = false;
    for h in handles {
        match h.await {
            Ok(Ok(m)) => {
                metrics.downloaded_bytes += m.downloaded_bytes;
                metrics.success_chunks += m.success_chunks;
                metrics.failed_chunks += m.failed_chunks;
            }
            Ok(Err(e)) => {
                has_error = true;
                metrics.failed_chunks += 1;
                metrics.error_msg = Some(e.to_string());
            }
            Err(e) => {
                has_error = true;
                metrics.failed_chunks += 1;
                metrics.error_msg = Some(format!("join error: {}", e));
            }
        }
    }

    if has_error {
        progress_handle.abort();
        return print_final(
            name,
            total,
            start,
            Err(anyhow::anyhow!(
                "download failed: {}",
                metrics.error_msg.unwrap_or_else(|| "unknown error".into())
            )),
        );
    }

    // 全部成功 → rename 为正式文件（dry_run 模式跳过）
    if !dry_run {
        tokio::fs::rename(&part_path, &file_path)
            .await
            .context("rename part file failed")?;
    }

    metrics.downloaded_bytes = total_size;
    progress_handle.abort();
    print_final(name, total, start, Ok(metrics))
}

/// 探测文件大小和 Range 支持（优先 GET bytes=0-0，HEAD 兜底，各重试3次）
async fn probe_file(client: &Client, url: &str) -> Result<(u64, bool)> {
    let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 SPDE/1.0";
    let mut errors: Vec<String> = Vec::new();

    // 优先 GET bytes=0-0：Content-Range 里的总大小最准确
    for attempt in 0..3u32 {
        match client
            .get(url)
            .header("Range", "bytes=0-0")
            .header("User-Agent", ua)
            .send()
            .await
        {
            Ok(resp) => {
                let accept = resp.status() == 206;
                let total = resp
                    .headers()
                    .get("content-range")
                    .and_then(|v| {
                        v.to_str().ok().and_then(|s| {
                            s.split('/').last().and_then(|t| t.parse::<u64>().ok())
                        })
                    })
                    .or_else(|| resp.content_length())
                    .unwrap_or(0);
                if total > 0 {
                    return Ok((total, accept));
                }
                errors.push(format!("GET attempt {}: total=0 status={}", attempt, resp.status()));
            }
            Err(e) => {
                errors.push(format!("GET attempt {}: {}", attempt, e));
            }
        }
        if attempt < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(500 * (attempt + 1) as u64)).await;
        }
    }

    // fallback: HEAD
    for attempt in 0..3u32 {
        match client.head(url).header("User-Agent", ua).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let total = resp.content_length().unwrap_or(0);
                    let accept = resp
                        .headers()
                        .get("accept-ranges")
                        .map(|v| v == "bytes")
                        .unwrap_or(false);
                    if total > 0 {
                        return Ok((total, accept));
                    }
                    errors.push(format!("HEAD attempt {}: total=0 status={}", attempt, resp.status()));
                } else {
                    errors.push(format!("HEAD attempt {}: status={}", attempt, resp.status()));
                }
            }
            Err(e) => {
                errors.push(format!("HEAD attempt {}: {}", attempt, e));
            }
        }
        if attempt < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(500 * (attempt + 1) as u64)).await;
        }
    }

    anyhow::bail!("failed to probe file size: {}", errors.join(" | "))
}

/// 打印下载最终结果并填充 metrics 元数据
fn print_final(
    name: String,
    total: u64,
    start: Instant,
    result: Result<DownloadMetrics>,
) -> Result<DownloadMetrics> {
    let elapsed = start.elapsed().as_secs_f64();
    match result {
        Ok(mut m) => {
            let speed = if elapsed > 0.0 {
                total as f64 / elapsed / 1024.0 / 1024.0
            } else {
                0.0
            };
            eprintln!(
                "[done] {}: {:.1} MB in {:.1}s, avg speed: {:.1} MB/s",
                name,
                total as f64 / 1024.0 / 1024.0,
                elapsed,
                speed
            );
            m.total_size = total;
            m.elapsed_secs = elapsed;
            if m.status.is_empty() {
                m.status = "success".to_string();
            }
            Ok(m)
        }
        Err(e) => {
            eprintln!("[error] {}: failed after {:.1}s: {}", name, elapsed, e);
            let mut m = DownloadMetrics::default();
            m.total_size = total;
            m.elapsed_secs = elapsed;
            m.status = "failed".to_string();
            m.error_msg = Some(e.to_string());
            Ok(m)
        }
    }
}

/// 单连接下载（fallback，支持断点续传；dry_run 时数据直接丢弃不落盘）
async fn download_single(
    client: &Client,
    url: &str,
    file_path: &Path,
    downloaded: Arc<AtomicU64>,
    dry_run: bool,
) -> Result<DownloadMetrics> {
    let mut metrics = DownloadMetrics::default();

    let local_size = if dry_run {
        0
    } else {
        tokio::fs::metadata(file_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0)
    };

    let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 SPDE/1.0";
    let resp = if local_size > 0 {
        client
            .get(url)
            .header("Range", format!("bytes={}-", local_size))
            .header("User-Agent", ua)
            .send()
            .await
            .context("http request failed")?
    } else {
        client.get(url).header("User-Agent", ua).send().await.context("http request failed")?
    };

    if !resp.status().is_success() {
        anyhow::bail!("http status:{}", resp.status());
    }

    // dry_run 模式不打开文件
    let mut file_opt = if dry_run {
        None
    } else {
        let f = File::options()
            .create(true)
            .append(false)
            .write(true)
            .read(true)
            .open(file_path)
            .await
            .context("open file failed")?;
        Some(f)
    };

    if let Some(file) = file_opt.as_mut() {
        file.seek(SeekFrom::Start(local_size))
            .await
            .context("seek failed")?;
    }

    let mut stream = resp.bytes_stream();
    while let Some(chunk_res) = stream.next().await {
        match chunk_res {
            Ok(chunk) => {
                if let Some(file) = file_opt.as_mut() {
                    file.write_all(&chunk).await.context("write failed")?;
                }
                // dry_run 时数据直接丢弃，但仍统计字节数和进度
                metrics.downloaded_bytes += chunk.len() as u64;
                metrics.success_chunks += 1;
                downloaded.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            }
            Err(e) => {
                metrics.failed_chunks += 1;
                metrics.error_msg = Some(e.to_string());
                break;
            }
        }
    }

    if let Some(file) = file_opt.as_mut() {
        file.flush().await.context("flush failed")?;
    }
    Ok(metrics)
}

/// 下载单个分片（写入文件指定偏移；dry_run 时数据直接丢弃不落盘）
async fn download_range(
    client: &Client,
    url: &str,
    file_path: &Path,
    start: u64,
    end: u64,
    downloaded: Arc<AtomicU64>,
    dry_run: bool,
) -> Result<DownloadMetrics> {
    let mut metrics = DownloadMetrics::default();

    let resp = client
        .get(url)
        .header("Range", format!("bytes={}-{}", start, end))
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 SPDE/1.0")
        .send()
        .await
        .context("range request failed")?;

    let status = resp.status();
    if !status.is_success() && status != 206 {
        anyhow::bail!("range http status:{}", status);
    }

    // dry_run 模式不打开文件
    let mut file_opt = if dry_run {
        None
    } else {
        let f = File::options()
            .write(true)
            .read(true)
            .open(file_path)
            .await
            .context("open part file failed")?;
        Some(f)
    };

    if let Some(file) = file_opt.as_mut() {
        file.seek(SeekFrom::Start(start))
            .await
            .context("seek failed")?;
    }

    let mut stream = resp.bytes_stream();
    while let Some(chunk_res) = stream.next().await {
        match chunk_res {
            Ok(chunk) => {
                if let Some(file) = file_opt.as_mut() {
                    file.write_all(&chunk)
                        .await
                        .context("write range failed")?;
                }
                // dry_run 时数据直接丢弃，但仍统计字节数和进度
                metrics.downloaded_bytes += chunk.len() as u64;
                metrics.success_chunks += 1;
                downloaded.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            }
            Err(e) => {
                metrics.failed_chunks += 1;
                metrics.error_msg = Some(e.to_string());
                break;
            }
        }
    }

    if let Some(file) = file_opt.as_mut() {
        file.flush().await.context("flush range failed")?;
    }
    Ok(metrics)
}

pub async fn run_download(client: &Client, opt: DownloadOption) -> Result<DownloadMetrics> {
    download_file(client, &opt.url, opt.save_path, 8, 3, false).await
}
