//! HTTP(S) 楂樻€ц兘涓嬭浇鍣?鈥?宸ヤ綔绐冨彇寮忓垎鐗囥€佸姩鎬佸苟鍙戙€佹柇鐐圭画浼犮€佽嚜鍔ㄩ噸璇?

use super::*;
use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use parking_lot::Mutex;
use reqwest::Client;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 SPDE/0.6";

/// HTTP(S) 涓嬭浇鍣?
pub struct HttpDownloader {
    client: Client,
}

impl Default for HttpDownloader {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpDownloader {
    pub fn new() -> Self {
        Self {
            client: Self::build_client(None, false),
        }
    }

    /// 甯﹁嚜瀹氫箟閰嶇疆鏋勫缓
    pub fn with_config(proxy: &str, skip_tls: bool) -> Result<Self> {
        Ok(Self {
            client: Self::build_client(Some(proxy), skip_tls),
        })
    }

    fn build_client(proxy: Option<&str>, skip_tls: bool) -> Client {
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(30))
            .tcp_nodelay(true)
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(16)
            .user_agent(USER_AGENT);

        if skip_tls {
            builder = builder.danger_accept_invalid_certs(true);
        }

        if let Some(p) = proxy {
            if !p.trim().is_empty() {
                if let Ok(proxy) = reqwest::Proxy::all(p.trim()) {
                    builder = builder.proxy(proxy);
                }
            }
        }

        builder.build().unwrap_or_else(|_| Client::new())
    }

    /// 鏍规嵁浠诲姟鍙傛暟鑾峰彇鎴栨瀯寤?client
    fn client_for_task(&self, task: &DownloadTask) -> Client {
        if task.proxy.is_empty() && !task.skip_tls_verify && task.headers.is_empty() {
            self.client.clone()
        } else {
            Self::build_client(Some(&task.proxy), task.skip_tls_verify)
        }
    }
}

#[async_trait::async_trait]
impl DownloadBackend for HttpDownloader {
    fn name(&self) -> &str {
        "http"
    }

    fn support_uri(&self, uri: &str) -> bool {
        uri.starts_with("http://") || uri.starts_with("https://")
    }

    async fn run(
        &self,
        task: DownloadTask,
        progress: Option<Arc<dyn ProgressCallback>>,
        controller: Option<Arc<DownloadController>>,
    ) -> Result<DownloadOutput> {
        let name = task
            .save_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        if task.dry_run {
            eprintln!("[dry-run] {} (data discarded, not saved to disk)", name);
        }

        let client = self.client_for_task(&task);

        // 1. 鎺㈡祴鏂囦欢澶у皬鍜?Range 鏀寔
        let (total_size, accept_ranges) = probe_file(&client, &task.uri, &task.headers).await?;

        // 宸插瓨鍦ㄤ笖澶у皬鍖归厤 鈫?璺宠繃锛堜粎鍦ㄥ紑鍚柇鐐圭画浼犳椂锛?
        if task.resume && !task.dry_run {
            if let Ok(meta) = tokio::fs::metadata(&task.save_path).await {
                if meta.len() == total_size && total_size > 0 {
                    eprintln!(
                        "[skip] {} already downloaded ({:.1} MB)",
                        name,
                        total_size as f64 / 1024.0 / 1024.0
                    );
                    let o = DownloadOutput {
                        total_size,
                        status: "skipped".into(),
                        is_success: true,
                        ..Default::default()
                    };
                    if let Some(p) = &progress {
                        p.on_complete(o.clone());
                    }
                    return Ok(o);
                }
            }
        }

        let start = Instant::now();
        let connections = task.effective_connections();
        let chunk_size = task.effective_chunk_size();

        // 涓嶆敮鎸?Range / 鏂囦欢澶皬 / 鍗曡繛鎺?鈫?鍗曡繛鎺?fallback
        let output = if !accept_ranges || connections <= 1 || total_size < chunk_size * 2 {
            download_single(
                &client,
                &task,
                total_size,
                progress.clone(),
                controller.clone(),
            )
            .await
        } else {
            // 宸ヤ綔绐冨彇寮忓杩炴帴鍒嗙墖涓嬭浇
            download_chunked(
                &client,
                &task,
                total_size,
                connections,
                chunk_size,
                &name,
                progress.clone(),
                controller.clone(),
            )
            .await
        };

        let mut output = output?;
        output.elapsed_secs = start.elapsed().as_secs_f64();
        output.avg_speed_mbps = if output.elapsed_secs > 0.0 {
            output.downloaded_bytes as f64 / output.elapsed_secs / 1024.0 / 1024.0
        } else {
            0.0
        };
        if output.status.is_empty() {
            output.status = if output.is_success {
                "success"
            } else {
                "failed"
            }
            .into();
        }

        if let Some(p) = &progress {
            p.on_complete(output.clone());
        }

        Ok(output)
    }

    async fn stop(&self, _task_id: &str) -> Result<()> {
        Ok(())
    }
}

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
// 鏂囦欢鎺㈡祴
// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

async fn probe_file(
    client: &Client,
    url: &str,
    headers: &[(String, String)],
) -> Result<(u64, bool)> {
    let mut errors: Vec<String> = Vec::new();

    // 浼樺厛 GET bytes=0-0
    for attempt in 0..3u32 {
        let mut req = client.get(url).header("Range", "bytes=0-0");
        for (k, v) in headers {
            req = req.header(k, v);
        }
        match req.send().await {
            Ok(resp) => {
                let accept = resp.status() == 206;
                let total = resp
                    .headers()
                    .get("content-range")
                    .and_then(|v| {
                        v.to_str().ok().and_then(|s| {
                            s.split('/').next_back().and_then(|t| t.parse::<u64>().ok())
                        })
                    })
                    .or_else(|| resp.content_length())
                    .unwrap_or(0);
                if total > 0 {
                    return Ok((total, accept));
                }
                errors.push(format!(
                    "GET attempt {}: total=0 status={}",
                    attempt,
                    resp.status()
                ));
            }
            Err(e) => errors.push(format!("GET attempt {}: {}", attempt, e)),
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
        }
    }

    // fallback: HEAD
    for attempt in 0..3u32 {
        let mut req = client.head(url);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        match req.send().await {
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
                    errors.push(format!(
                        "HEAD attempt {}: total=0 status={}",
                        attempt,
                        resp.status()
                    ));
                } else {
                    errors.push(format!(
                        "HEAD attempt {}: status={}",
                        attempt,
                        resp.status()
                    ));
                }
            }
            Err(e) => errors.push(format!("HEAD attempt {}: {}", attempt, e)),
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
        }
    }

    anyhow::bail!("failed to probe file size: {}", errors.join(" | "))
}

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
// 鍗曡繛鎺ヤ笅杞斤紙fallback + 鏂偣缁紶锛?
// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

/// 鍗曡繛鎺ヤ笅杞斤紙鏃?Range 鎴栧崟绾跨▼鍦烘櫙锛?
async fn download_single(
    client: &Client,
    task: &DownloadTask,
    total_size: u64,
    progress: Option<Arc<dyn ProgressCallback>>,
    controller: Option<Arc<DownloadController>>,
) -> Result<DownloadOutput> {
    let deadline = task.timeout.map(|d| Instant::now() + d);

    // 鏂偣缁紶锛氭部鐢ㄥ凡鏈夋湰鍦板ぇ灏忥紙resume=false 鏃朵粠闆跺紑濮嬶級
    let local_size = if task.dry_run || !task.resume {
        0
    } else {
        tokio::fs::metadata(&task.save_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0)
    };

    let mut output = DownloadOutput {
        total_size,
        // 鏂偣缁紶鏃跺凡涓嬭浇瀛楄妭浠?local_size 寮€濮嬶紝閬垮厤杩涘害浠?璺冲彉
        downloaded_bytes: local_size,
        ..Default::default()
    };

    let mut req = client.get(&task.uri);
    for (k, v) in &task.headers {
        req = req.header(k, v);
    }
    if local_size > 0 {
        req = req.header("Range", format!("bytes={}-", local_size));
    }

    let resp = req.send().await.context("http request failed")?;
    if !resp.status().is_success() {
        anyhow::bail!("http status: {}", resp.status());
    }

    let mut file_opt = if task.dry_run {
        None
    } else {
        let f = File::options()
            .create(true)
            .truncate(!task.resume)
            .write(true)
            .read(true)
            .open(&task.save_path)
            .await
            .context("open file failed")?;
        Some(f)
    };

    if let Some(file) = file_opt.as_mut() {
        file.seek(SeekFrom::Start(local_size)).await?;
    }

    let mut stream = resp.bytes_stream();
    let dl_start = Instant::now();
    let mut last_progress = Instant::now();
    while let Some(chunk_res) = stream.next().await {
        // 瓒呮椂妫€鏌ワ紙寰幆寮€濮嬫椂锛屼繚璇佸悗缁凯浠ｄ篃浼氱寮€锛?
        if let Some(d) = deadline {
            if Instant::now() >= d {
                output.error_msg = Some("download timed out".to_string());
                break;
            }
        }
        // 鏆傚仠/鍙栨秷妫€鏌?
        if let Some(ctrl) = &controller {
            if !ctrl.wait_if_paused().await {
                anyhow::bail!("download cancelled by controller");
            }
        }
        match chunk_res {
            Ok(chunk) => {
                if let Some(file) = file_opt.as_mut() {
                    file.write_all(&chunk).await.context("write failed")?;
                }
                output.downloaded_bytes += chunk.len() as u64;
                output.success_chunks += 1;

                // 杩涘害鍥炶皟锛堜笌 file/ftp 鍚庣涓€鑷达細鎸?progress_interval 鑺傛祦锛?
                if let Some(cb) = &progress {
                    if last_progress.elapsed() >= task.progress_interval {
                        let elapsed = dl_start.elapsed().as_secs_f64();
                        let speed = if elapsed > 0.0 {
                            (output.downloaded_bytes as f64 / elapsed) as u64
                        } else {
                            0
                        };
                        let percent = if total_size > 0 {
                            output.downloaded_bytes as f64 / total_size as f64 * 100.0
                        } else {
                            0.0
                        };
                        cb.on_progress(ProgressSnapshot {
                            task_id: task.task_id.clone(),
                            total_size,
                            downloaded_bytes: output.downloaded_bytes,
                            speed_bps: speed,
                            active_connections: 1,
                            percent,
                            elapsed_secs: elapsed,
                        });
                        last_progress = Instant::now();
                    }
                }
            }
            Err(e) => {
                output.failed_chunks += 1;
                output.error_msg = Some(e.to_string());
                break;
            }
        }
    }

    if let Some(file) = file_opt.as_mut() {
        file.flush().await.context("flush failed")?;
    }

    output.is_success = output.error_msg.is_none();
    if output.is_success {
        output.downloaded_bytes = total_size;
    }
    Ok(output)
}

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
// 宸ヤ綔绐冨彇寮忓杩炴帴鍒嗙墖涓嬭浇
// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

struct SharedState {
    /// 寰呬笅杞藉垎鐗囬槦鍒?(start, end)
    queue: Mutex<VecDeque<(u64, u64)>>,
    /// 宸蹭笅杞藉瓧鑺?
    downloaded: AtomicU64,
    /// 鎴愬姛鍒嗙墖鏁?
    success_chunks: AtomicU32,
    /// 澶辫触鍒嗙墖鏁?
    failed_chunks: AtomicU32,
    /// 娲昏穬杩炴帴鏁?
    active_conns: AtomicU32,
    /// 鎬诲ぇ灏?
    total_size: u64,
    /// 寮€濮嬫椂闂?
    start: Instant,
    /// 鏈€鍚庨敊璇?
    last_error: Mutex<Option<String>>,
    /// 瓒呮椂鎴鏃堕棿锛圢one = 涓嶉檺鏃讹級
    deadline: Option<Instant>,
    /// 閫熷害婊戝姩绐楀彛锛?鏃堕棿鎴? 宸蹭笅杞藉瓧鑺?锛岀敤浜庤绠楃灛鏃堕€熷害
    speed_window: Mutex<VecDeque<(Instant, u64)>>,
}

#[allow(clippy::too_many_arguments)]
async fn download_chunked(
    client: &Client,
    task: &DownloadTask,
    total_size: u64,
    connections: u32,
    chunk_size: u64,
    _name: &str,
    progress: Option<Arc<dyn ProgressCallback>>,
    controller: Option<Arc<DownloadController>>,
) -> Result<DownloadOutput> {
    let part_path = std::path::PathBuf::from(format!("{}.part", task.save_path.display()));

    // resume=false 鏃朵涪寮冨凡鏈?.part锛岄伩鍏嶆畫鐣欐棫鏁版嵁锛堝垎鐗囬槦鍒楀缁堜粠 0 閲嶅缓锛?
    if !task.resume && !task.dry_run {
        tokio::fs::remove_file(&part_path).await.ok();
    }

    // 棰勫垎閰嶆枃浠?
    if !task.dry_run {
        let f = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&part_path)
            .await
            .context("create part file failed")?;
        f.set_len(total_size).await.context("preallocate failed")?;
    }

    // 鏋勫缓鍒嗙墖闃熷垪
    let mut queue = VecDeque::new();
    let mut pos = 0u64;
    while pos < total_size {
        let end = (pos + chunk_size - 1).min(total_size - 1);
        queue.push_back((pos, end));
        pos = end + 1;
    }
    let total_chunks = queue.len() as u32;

    let state = Arc::new(SharedState {
        queue: Mutex::new(queue),
        downloaded: AtomicU64::new(0),
        success_chunks: AtomicU32::new(0),
        failed_chunks: AtomicU32::new(0),
        active_conns: AtomicU32::new(0),
        total_size,
        start: Instant::now(),
        last_error: Mutex::new(None),
        deadline: task.timeout.map(|d| Instant::now() + d),
        speed_window: Mutex::new(VecDeque::new()),
    });

    // 鍚姩杩涘害鎶ュ憡
    let progress_state = state.clone();
    let progress_task_id = task.task_id.clone();
    let progress_interval = task.progress_interval;
    let progress_handle = if progress.is_some() {
        let cb = progress.clone().unwrap();
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(progress_interval).await;
                let now = Instant::now();
                let dl = progress_state.downloaded.load(Ordering::Relaxed);
                let elapsed = progress_state.start.elapsed().as_secs_f64();

                // 鐬椂閫熷害锛氭粦鍔ㄧ獥鍙ｏ紙鏈€杩?绉掞級鍐呯殑瀛楄妭宸?/ 鏃堕棿宸?
                let mut window = progress_state.speed_window.lock();
                window.push_back((now, dl));
                // 淇濈暀鏈€杩?绉掔殑鏁版嵁锛堣嚦灏戜繚鐣?涓偣鐢ㄤ簬璁＄畻宸€硷級
                while window.len() > 2
                    && now.duration_since(window.front().unwrap().0).as_secs_f64() > 5.0
                {
                    window.pop_front();
                }
                let speed = if window.len() >= 2 {
                    let (t0, d0) = window.front().unwrap();
                    let (t1, d1) = window.back().unwrap();
                    let dt = t1.duration_since(*t0).as_secs_f64();
                    if dt > 0.0 && *d1 >= *d0 {
                        ((*d1 - *d0) as f64 / dt) as u64
                    } else {
                        0
                    }
                } else {
                    // 绐楀彛鏁版嵁涓嶈冻鏃跺洖閫€鍒版€诲钩鍧囬€熷害
                    if elapsed > 0.0 {
                        (dl as f64 / elapsed) as u64
                    } else {
                        0
                    }
                };
                drop(window);

                let percent = if progress_state.total_size > 0 {
                    dl as f64 / progress_state.total_size as f64 * 100.0
                } else {
                    0.0
                };
                cb.on_progress(ProgressSnapshot {
                    task_id: progress_task_id.clone(),
                    total_size: progress_state.total_size,
                    downloaded_bytes: dl,
                    speed_bps: speed,
                    active_connections: progress_state.active_conns.load(Ordering::Relaxed),
                    percent,
                    elapsed_secs: elapsed,
                });
                if progress_state.success_chunks.load(Ordering::Relaxed)
                    + progress_state.failed_chunks.load(Ordering::Relaxed)
                    >= total_chunks
                {
                    break;
                }
            }
        }))
    } else {
        None
    };

    // 鍚姩 worker
    let mut handles = Vec::new();
    for _ in 0..connections {
        let c = client.clone();
        let url = task.uri.clone();
        let headers = task.headers.clone();
        let part = part_path.clone();
        let st = state.clone();
        let retry = task.retry_times.max(1);
        let dry_run = task.dry_run;
        let speed_limit = task.speed_limit;

        let ctrl_clone = controller.clone();
        handles.push(tokio::spawn(async move {
            st.active_conns.fetch_add(1, Ordering::Relaxed);
            loop {
                // 瓒呮椂妫€鏌?
                if let Some(d) = st.deadline {
                    if Instant::now() >= d {
                        let mut q = st.queue.lock();
                        let remaining = q.len() as u32;
                        q.clear();
                        drop(q);
                        st.failed_chunks.fetch_add(remaining, Ordering::Relaxed);
                        *st.last_error.lock() = Some("download timed out".to_string());
                        break;
                    }
                }
                // 鏆傚仠/鍙栨秷妫€鏌?
                if let Some(ctrl) = &ctrl_clone {
                    if !ctrl.wait_if_paused().await {
                        break;
                    }
                }
                // 浠庨槦鍒楀彇涓€涓垎鐗?
                let range = {
                    let mut q = st.queue.lock();
                    q.pop_front()
                };
                let Some((start, end)) = range else {
                    break;
                };

                // 甯﹂噸璇曚笅杞借鍒嗙墖
                let mut ok = false;
                for attempt in 0..retry {
                    match download_range(
                        &c,
                        &url,
                        &headers,
                        &part,
                        start,
                        end,
                        st.clone(),
                        dry_run,
                        speed_limit,
                    )
                    .await
                    {
                        Ok(()) => {
                            ok = true;
                            break;
                        }
                        Err(e) => {
                            *st.last_error.lock() = Some(e.to_string());
                            if attempt + 1 < retry {
                                tokio::time::sleep(Duration::from_millis(
                                    500 * (attempt + 1) as u64,
                                ))
                                .await;
                            }
                        }
                    }
                }

                if ok {
                    st.success_chunks.fetch_add(1, Ordering::Relaxed);
                } else {
                    st.failed_chunks.fetch_add(1, Ordering::Relaxed);
                    // 澶辫触鐨勫垎鐗囬噸鏂板叆闃熷熬閮紝璁╁叾浠?worker 灏濊瘯
                    let mut q = st.queue.lock();
                    q.push_back((start, end));
                }
            }
            st.active_conns.fetch_sub(1, Ordering::Relaxed);
        }));
    }

    // 绛夊緟鎵€鏈?worker 瀹屾垚
    for h in handles {
        let _ = h.await;
    }

    if let Some(ph) = progress_handle {
        let _ = ph.await;
    }

    let success = state.success_chunks.load(Ordering::Relaxed);
    let failed = state.failed_chunks.load(Ordering::Relaxed);
    let downloaded = state.downloaded.load(Ordering::Relaxed);
    let last_error = state.last_error.lock().clone();

    let output = DownloadOutput {
        total_size,
        downloaded_bytes: if failed == 0 { total_size } else { downloaded },
        success_chunks: success,
        failed_chunks: failed,
        is_success: failed == 0,
        error_msg: if failed > 0 {
            last_error.or(Some("some chunks failed".into()))
        } else {
            None
        },
        ..Default::default()
    };

    // 鍏ㄩ儴鎴愬姛 鈫?rename
    if output.is_success && !task.dry_run {
        tokio::fs::rename(&part_path, &task.save_path)
            .await
            .context("rename part file failed")?;
    }

    Ok(output)
}

/// 涓嬭浇鍗曚釜鍒嗙墖鍒版枃浠舵寚瀹氬亸绉?
#[allow(clippy::too_many_arguments)]
async fn download_range(
    client: &Client,
    url: &str,
    headers: &[(String, String)],
    file_path: &std::path::Path,
    start: u64,
    end: u64,
    state: Arc<SharedState>,
    dry_run: bool,
    speed_limit: u64,
) -> Result<()> {
    let mut req = client
        .get(url)
        .header("Range", format!("bytes={}-{}", start, end));
    for (k, v) in headers {
        req = req.header(k, v);
    }

    let resp = req.send().await.context("range request failed")?;
    let status = resp.status();
    if !status.is_success() && status != 206 {
        anyhow::bail!("range http status: {}", status);
    }

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
    let mut interval = if speed_limit > 0 {
        Some(tokio::time::interval(Duration::from_millis(100)))
    } else {
        None
    };
    let mut window_bytes = 0u64;

    while let Some(chunk_res) = stream.next().await {
        let chunk: Bytes = chunk_res.context("read chunk failed")?;
        let chunk_len = chunk.len() as u64;

        if let Some(file) = file_opt.as_mut() {
            file.write_all(&chunk).await.context("write range failed")?;
        }

        state.downloaded.fetch_add(chunk_len, Ordering::Relaxed);

        // 绠€鍗曢€熷害闄愬埗锛氭瘡 100ms 绐楀彛鍐呬笉瓒呰繃 speed_limit/10 瀛楄妭
        if speed_limit > 0 {
            window_bytes += chunk_len;
            let limit_per_window = speed_limit / 10;
            if window_bytes >= limit_per_window {
                if let Some(tick) = interval.as_mut() {
                    tick.tick().await;
                }
                window_bytes = 0;
            }
        }
    }

    if let Some(file) = file_opt.as_mut() {
        file.flush().await.context("flush range failed")?;
    }

    Ok(())
}

// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
// 鏂版灦鏋勫悗绔細鍩轰簬 DownloadScheduler + HttpChunkDownloader
// 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

use crate::domain::DownloadConfig;
use crate::infra::http::downloader::HttpChunkDownloader;
use crate::infra::http::source::HttpSource;
use crate::service::scheduler::DownloadScheduler;
use pandanetos::domain::{DownloadProgress, DownloadSource};
use tokio::sync::mpsc;

/// 鏂版灦鏋?HTTP 涓嬭浇鍚庣锛坰erve 妯″紡浣跨敤锛?
pub struct ChunkedHttpDownloader;

impl Default for ChunkedHttpDownloader {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedHttpDownloader {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl DownloadBackend for ChunkedHttpDownloader {
    fn name(&self) -> &str {
        "http-chunked"
    }

    fn support_uri(&self, uri: &str) -> bool {
        uri.starts_with("http://") || uri.starts_with("https://")
    }

    async fn run(
        &self,
        task: DownloadTask,
        progress: Option<Arc<dyn ProgressCallback>>,
        _controller: Option<Arc<DownloadController>>,
    ) -> Result<DownloadOutput> {
        let start = Instant::now();
        let task_id = task.task_id.clone();

        // 1. 鏋勫缓涓嬭浇婧?
        let source: Box<dyn DownloadSource> = Box::new(HttpSource::new(task.uri.clone()));

        // 2. 纭繚淇濆瓨鐩綍瀛樺湪
        let save_path = task.save_path.clone();
        if let Some(parent) = save_path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }

        // 3. 鏋勫缓涓嬭浇閰嶇疆
        let config = DownloadConfig {
            max_connections: task.effective_connections(),
            min_connections: 1,
            chunk_size: task.effective_chunk_size(),
            retry_times: task.retry_times,
            timeout_secs: task.timeout.map(|d| d.as_secs()).unwrap_or(1800),
            resume: task.resume,
            skip_tls_verify: task.skip_tls_verify,
            max_bandwidth_bps: task.speed_limit,
            enable_mirror_discovery: false,
            enable_adaptive: true,
            enable_progress_smoothing: true,
            save_dir: save_path
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_default(),
            dry_run: task.dry_run,
        };

        // 4. 鏋勫缓鍒嗙墖涓嬭浇鍣ㄥ拰璋冨害鍣?
        let chunk_downloader = Arc::new(HttpChunkDownloader::new(
            task.skip_tls_verify,
            config.timeout_secs,
        ));
        let scheduler = DownloadScheduler::new(config);

        // 5. 杩涘害閫氶亾
        let (progress_tx, mut progress_rx) = mpsc::channel::<DownloadProgress>(256);

        // 6. 鍚庡彴浠诲姟锛氭帴鏀惰繘搴﹀苟杞彂缁?ProgressCallback
        let progress_clone = progress.clone();
        let task_id_clone = task_id.clone();
        let progress_handle = tokio::spawn(async move {
            while let Some(p) = progress_rx.recv().await {
                if let Some(cb) = &progress_clone {
                    let snapshot = ProgressSnapshot {
                        task_id: task_id_clone.clone(),
                        total_size: p.total_bytes,
                        downloaded_bytes: p.downloaded_bytes,
                        speed_bps: p.speed_bps,
                        active_connections: p.active_connections,
                        percent: if p.total_bytes > 0 {
                            (p.downloaded_bytes as f64 / p.total_bytes as f64) * 100.0
                        } else {
                            0.0
                        },
                        elapsed_secs: start.elapsed().as_secs_f64(),
                    };
                    cb.on_progress(snapshot);
                }
            }
        });

        // 7. 鎵ц涓嬭浇锛坰cheduler 鍐呴儴鍒涘缓 writer 鍜?.part 鏂囦欢锛?
        let result = scheduler
            .download(source, chunk_downloader, save_path.clone(), progress_tx)
            .await;

        // 8. 绛夊緟杩涘害杞彂瀹屾垚
        drop(progress_handle);

        // 9. 杞崲缁撴灉
        let elapsed = start.elapsed().as_secs_f64();
        match result {
            Ok(r) => {
                let output = DownloadOutput {
                    total_size: r.total_bytes,
                    downloaded_bytes: r.downloaded_bytes,
                    success_chunks: r.success_chunks,
                    failed_chunks: r.failed_chunks,
                    is_success: r.success,
                    error_msg: r.error_msg,
                    elapsed_secs: elapsed,
                    avg_speed_mbps: if elapsed > 0.0 {
                        (r.downloaded_bytes as f64 / 1024.0 / 1024.0) / elapsed
                    } else {
                        0.0
                    },
                    status: if r.success {
                        "completed".into()
                    } else {
                        "failed".into()
                    },
                };

                if let Some(cb) = &progress {
                    cb.on_complete(output.clone());
                }

                Ok(output)
            }
            Err(e) => {
                let output = DownloadOutput {
                    total_size: 0,
                    downloaded_bytes: 0,
                    success_chunks: 0,
                    failed_chunks: 0,
                    is_success: false,
                    error_msg: Some(format!("{e:#}")),
                    elapsed_secs: elapsed,
                    avg_speed_mbps: 0.0,
                    status: "error".into(),
                };

                if let Some(cb) = &progress {
                    cb.on_complete(output.clone());
                }

                Err(anyhow!("{e:#}"))
            }
        }
    }
}
