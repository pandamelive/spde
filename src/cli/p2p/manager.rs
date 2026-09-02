//! 全局 BT 管理器
//!
//! 在 spde 启动时创建全局 librqbit Session，DHT/LSD 持续运行，
//! 路由表持续填充。维护最新的公共 tracker 列表，下载任务自动注入。
//!
//! # 设计
//! - 全局单例：spde 启动时初始化一次，serve/agent 模式共用
//! - DHT 预热：任务到来时 DHT 已有足够节点，快速发现 peer
//! - tracker 维护：启动时从远程获取最新列表，之后每4小时自动刷新
//! - 自动注入：下载任务自动添加 trackers、peer_limit 等优化参数

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use librqbit::api::Api;
use librqbit::{Session, SessionOptions};
use tokio::sync::mpsc;

use pandanetos::domain::{CancellationToken, DownloadProgress, DownloadResult};
use pandanetos::error::{CoreError, Result};

use super::bt::BtDownloader;

/// 远程 tracker 列表 URL
const TRACKER_LIST_URL: &str = "https://tracker.adysec.com/trackers_best.txt";
/// tracker 刷新间隔（4小时）
const TRACKER_REFRESH_INTERVAL: Duration = Duration::from_secs(4 * 3600);

pub struct BtManager {
    api: Api,
    trackers: RwLock<Vec<String>>,
}

impl BtManager {
    /// 初始化全局 BT 管理器
    /// 创建 librqbit Session，启动 DHT/LSD，初始化 tracker 列表，
    /// 并启动后台任务定期从远程获取最新 tracker 列表。
    pub async fn new(output_dir: &Path) -> Result<Arc<Self>> {
        eprintln!("[bt-manager] initializing global session, output_dir={:?}", output_dir);

        // 创建全局 Session（DHT/LSD 自动启动）
        let session_opts = SessionOptions::default();
        let session = Session::new_with_opts(output_dir.to_path_buf(), session_opts)
            .await
            .map_err(|e| CoreError::Internal(format!("create global session: {}", e)))?;
        let api = Api::new(session, None);

        // 初始化 tracker 列表（先用默认值，后台任务会立即从远程刷新）
        let trackers = RwLock::new(Self::default_trackers());

        let manager = Arc::new(Self { api, trackers });

        // 启动 tracker 自动刷新后台任务
        manager.spawn_tracker_refresher();

        eprintln!(
            "[bt-manager] initialized, DHT+LSD started, tracker refresher spawned (interval={:?})",
            TRACKER_REFRESH_INTERVAL
        );

        Ok(manager)
    }

    /// 启动 tracker 自动刷新后台任务
    /// 启动后立即从远程获取一次，之后每隔 TRACKER_REFRESH_INTERVAL 获取一次
    fn spawn_tracker_refresher(self: &Arc<Self>) {
        let manager = self.clone();
        tokio::spawn(async move {
            // 启动后立即获取一次（不阻塞 manager 初始化）
            manager.refresh_trackers_once().await;

            // 定期刷新
            loop {
                tokio::time::sleep(TRACKER_REFRESH_INTERVAL).await;
                manager.refresh_trackers_once().await;
            }
        });
    }

    /// 从远程获取最新 tracker 列表并更新
    async fn refresh_trackers_once(&self) {
        match self.fetch_remote_trackers().await {
            Ok(trackers) => {
                if !trackers.is_empty() {
                    let count = trackers.len();
                    self.update_trackers(trackers);
                    eprintln!("[bt-manager] trackers refreshed from remote, count={}", count);
                } else {
                    eprintln!("[bt-manager] remote tracker list is empty, keeping current");
                }
            }
            Err(e) => {
                eprintln!("[bt-manager] fetch remote trackers failed: {}, keeping current", e);
            }
        }
    }

    /// 从远程 URL 获取 tracker 列表
    async fn fetch_remote_trackers(&self) -> Result<Vec<String>> {
        let client = reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(15)).build().map_err(|e| CoreError::Internal(format!("build client: {}", e)))?;
        let response = client.get(TRACKER_LIST_URL)
            .send()
            .await
            .map_err(|e| CoreError::Internal(format!("http get: {}", e)))?;

        let text = response
            .text()
            .await
            .map_err(|e| CoreError::Internal(format!("read body: {}", e)))?;

        let trackers: Vec<String> = text
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();

        Ok(trackers)
    }

    /// 默认公共 tracker 列表（远程获取失败时的兜底）
    fn default_trackers() -> Vec<String> {
        vec![
            "udp://tracker.opentrackr.org:1337/announce".to_string(),
            "udp://open.stealth.si:80/announce".to_string(),
            "udp://tracker.torrent.eu.org:451/announce".to_string(),
            "udp://exodus.desync.com:6969/announce".to_string(),
            "udp://tracker.birkenwald.de:6969/announce".to_string(),
            "udp://tracker.moeking.me:6969/announce".to_string(),
            "udp://opentracker.i2p.rocks:6969/announce".to_string(),
            "udp://tracker.dler.org:6969/announce".to_string(),
        ]
    }

    /// 获取全局 Api 引用
    pub fn api(&self) -> &Api {
        &self.api
    }

    /// 获取当前 tracker 列表（克隆）
    pub fn trackers(&self) -> Vec<String> {
        self.trackers.read().unwrap().clone()
    }

    /// 更新 tracker 列表
    pub fn update_trackers(&self, new_trackers: Vec<String>) {
        let count = new_trackers.len();
        let mut t = self.trackers.write().unwrap();
        *t = new_trackers;
        eprintln!("[bt-manager] trackers updated, count={}", count);
    }

    /// 执行 BT 下载（复用全局 Session，自动注入 trackers）
    pub async fn download(
        &self,
        url: &str,
        save_dir: &Path,
        timeout_secs: u64,
        dry_run: bool,
        progress_tx: mpsc::Sender<DownloadProgress>,
        cancel: CancellationToken,
    ) -> Result<DownloadResult> {
        let downloader = BtDownloader::new();
        downloader
            .download_with_api(
                &self.api,
                self.trackers(),
                url,
                save_dir,
                timeout_secs,
                dry_run,
                progress_tx,
                cancel,
            )
            .await
    }
}
