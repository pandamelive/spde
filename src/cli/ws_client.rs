use anyhow::Result;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

macro_rules! log {
    ($($arg:tt)*) => {{
        let ts = Utc::now().to_rfc3339().to_string();
        std::eprint!("[{}] ", ts);
        std::eprintln!($($arg)*);
    }};
}

use pandanetos::protocol::ServerMsg;

// ── 消息协议（与 PK 端 ws.rs 对应） ──────────────────────

/// 借用实现对应标准库 [`pandanetos::protocol::ClientMsg`]，序列化字节与标准定义一致
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg<'a> {
    Pong,
    Status {
        active_tasks: u32,
        bytes_downloaded: u64,
        busy: bool,
        total_speed_bps: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_error: Option<&'a str>,
    },
    TaskStarted {
        dispatch_id: Uuid,
    },
    TaskProgress {
        dispatch_id: Uuid,
        task_name: &'a str,
        percent: f64,
        downloaded_bytes: u64,
        total_size: u64,
        speed_bps: u64,
        active_connections: u32,
        elapsed_secs: f64,
    },
    TaskReport {
        dispatch_id: Option<Uuid>,
        task_id: Option<Uuid>,
        task_name: &'a str,
        url: &'a str,
        filename: &'a str,
        file_size: u64,
        downloaded_bytes: u64,
        elapsed_secs: f64,
        avg_speed_mbps: f64,
        status: &'a str,
        success_chunks: u64,
        failed_chunks: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_msg: Option<&'a str>,
    },
}

// ── 任务报告参数 ─────────────────────────────────────────

pub struct TaskReportParams<'a> {
    pub dispatch_id: Option<Uuid>,
    pub task_id: Option<Uuid>,
    pub task_name: &'a str,
    pub url: &'a str,
    pub filename: &'a str,
    pub file_size: u64,
    pub downloaded_bytes: u64,
    pub elapsed_secs: f64,
    pub avg_speed_mbps: f64,
    pub status: &'a str,
    pub success_chunks: u64,
    pub failed_chunks: u64,
    pub error_msg: Option<&'a str>,
}

// ── 任务实时进度参数 ────────────────────────────────────
pub struct TaskProgressParams<'a> {
    pub dispatch_id: Uuid,
    pub task_name: &'a str,
    pub percent: f64,
    pub downloaded_bytes: u64,
    pub total_size: u64,
    pub speed_bps: u64,
    pub active_connections: u32,
    pub elapsed_secs: f64,
}

// ── WsClient ─────────────────────────────────────────────

#[derive(Clone)]
pub struct WsClient {
    tx: mpsc::Sender<String>,
    config_notify: Arc<Notify>,
    task_notify: Arc<Notify>,
    connected: Arc<AtomicBool>,
    /// 节点已被 pk 删除，应暂停任务并重新注册
    node_deleted: Arc<AtomicBool>,
    /// 节点被删除时通知主循环
    deleted_notify: Arc<Notify>,
}

impl WsClient {
    /// 启动 WebSocket 客户端（后台自动重连）
    pub fn spawn(node_id: Uuid, master: String, token: String, base_dir: PathBuf) -> Self {
        let (tx, rx) = mpsc::channel::<String>(128);
        let config_notify = Arc::new(Notify::new());
        let task_notify = Arc::new(Notify::new());
        let connected = Arc::new(AtomicBool::new(false));
        let node_deleted = Arc::new(AtomicBool::new(false));
        let deleted_notify = Arc::new(Notify::new());

        tokio::spawn(connection_loop(
            node_id,
            master,
            token,
            rx,
            config_notify.clone(),
            task_notify.clone(),
            connected.clone(),
            base_dir,
            node_deleted.clone(),
            deleted_notify.clone(),
        ));

        Self {
            tx,
            config_notify,
            task_notify,
            connected,
            node_deleted,
            deleted_notify,
        }
    }

    /// 等待 PK 推送 config_changed 通知
    pub async fn wait_config_change(&self) {
        self.config_notify.notified().await;
    }

    /// 等待 PK 推送 new_task 通知（共享待下发池有新任务）
    pub async fn wait_new_task(&self) {
        self.task_notify.notified().await;
    }

    /// 检查节点是否已被 pk 删除
    pub fn is_node_deleted(&self) -> bool {
        self.node_deleted.load(Ordering::SeqCst)
    }

    /// 清除节点删除标志（重新注册成功后调用）
    pub fn clear_node_deleted(&self) {
        self.node_deleted.store(false, Ordering::SeqCst);
    }

    /// 等待节点被删除（用于主循环唤醒）
    pub async fn wait_node_deleted(&self) {
        self.deleted_notify.notified().await;
    }

    /// 主动触发一次 config 拉取（用于首次连接或重连后）
    pub fn notify_config_change(&self) {
        self.config_notify.notify_waiters();
    }

    /// 主动触发一次任务领取（用于首次连接或重连后）
    pub fn notify_new_task(&self) {
        self.task_notify.notify_waiters();
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub async fn send_status(
        &self,
        active_tasks: u32,
        bytes_downloaded: u64,
        busy: bool,
        total_speed_bps: u64,
        last_error: Option<&str>,
    ) {
        let msg = ClientMsg::Status {
            active_tasks,
            bytes_downloaded,
            busy,
            total_speed_bps,
            last_error,
        };
        self.send_json(&msg).await;
    }

    pub async fn send_task_started(&self, dispatch_id: Uuid) {
        let msg = ClientMsg::TaskStarted { dispatch_id };
        self.send_json(&msg).await;
    }

    pub async fn send_task_progress(&self, p: TaskProgressParams<'_>) {
        let msg = ClientMsg::TaskProgress {
            dispatch_id: p.dispatch_id,
            task_name: p.task_name,
            percent: p.percent,
            downloaded_bytes: p.downloaded_bytes,
            total_size: p.total_size,
            speed_bps: p.speed_bps,
            active_connections: p.active_connections,
            elapsed_secs: p.elapsed_secs,
        };
        self.send_json(&msg).await;
    }

    pub async fn send_task_report(&self, p: TaskReportParams<'_>) {
        let msg = ClientMsg::TaskReport {
            dispatch_id: p.dispatch_id,
            task_id: p.task_id,
            task_name: p.task_name,
            url: p.url,
            filename: p.filename,
            file_size: p.file_size,
            downloaded_bytes: p.downloaded_bytes,
            elapsed_secs: p.elapsed_secs,
            avg_speed_mbps: p.avg_speed_mbps,
            status: p.status,
            success_chunks: p.success_chunks,
            failed_chunks: p.failed_chunks,
            error_msg: p.error_msg,
        };
        self.send_json(&msg).await;
    }

    async fn send_json<T: Serialize>(&self, msg: &T) {
        if let Ok(json) = serde_json::to_string(msg) {
            let _ = self.tx.send(json).await;
        }
    }
}

// ── 连接循环 ─────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn connection_loop(
    node_id: Uuid,
    master: String,
    token: String,
    mut rx: mpsc::Receiver<String>,
    config_notify: Arc<Notify>,
    task_notify: Arc<Notify>,
    connected: Arc<AtomicBool>,
    base_dir: PathBuf,
    node_deleted: Arc<AtomicBool>,
    deleted_notify: Arc<Notify>,
) {
    let ws_base = ws_base(&master);
    let ws_url = format!("{}/api/v1/agent/ws?node_id={}", ws_base, node_id);

    loop {
        match connect_ws(&ws_url, &token).await {
            Ok(ws_stream) => {
                connected.store(true, Ordering::SeqCst);
                log!("[ws] connected to {}", ws_url);
                // 连接成功后通知拉一次 config 和尝试领取任务
                config_notify.notify_waiters();
                task_notify.notify_waiters();

                let (mut write, mut read) = ws_stream.split();

                loop {
                    tokio::select! {
                        // 收到 PK 消息
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Ok(server_msg) = serde_json::from_str::<ServerMsg>(&text) {
                                        match server_msg {
                                            ServerMsg::ConfigChanged => {
                                                log!("[ws] config_changed received");
                                                config_notify.notify_waiters();
                                            }
                                            ServerMsg::NewTask => {
                                                log!("[ws] new_task received");
                                                task_notify.notify_waiters();
                                            }
                                            ServerMsg::Ping => {
                                                let pong = serde_json::to_string(&ClientMsg::Pong).unwrap_or_default();
                                                if write.send(Message::Text(pong.into())).await.is_err() {
                                                    break;
                                                }
                                            }
                                            ServerMsg::NodeDeleted => {
                                                log!("[ws] node_deleted received, pausing tasks and triggering re-register");
                                                node_deleted.store(true, Ordering::SeqCst);
                                                deleted_notify.notify_waiters();
                                                task_notify.notify_waiters();
                                                config_notify.notify_waiters();
                                            }
                                            ServerMsg::DeleteFile { filename, save_path } => {
                                                let dir = match save_path {
                                                    Some(sp) if !sp.is_empty() => {
                                                        let p = PathBuf::from(&sp);
                                                        if p.is_absolute() { p } else { base_dir.join(p) }
                                                    }
                                                    _ => base_dir.join("download"),
                                                };
                                                let file_path = dir.join(&filename);
                                                log!("[ws] delete_file requested: {:?}", file_path);
                                                match tokio::fs::remove_file(&file_path).await {
                                                    Ok(_) => log!("[ws] deleted: {:?}", file_path),
                                                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                                    Err(e) => log!("[ws] delete failed: {e}"),
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Ok(Message::Close(_))) => break,
                                Some(Err(_)) => break,
                                None => break,
                                _ => {}
                            }
                        }
                        // 发送本地消息
                        Some(outgoing) = rx.recv() => {
                            if write.send(Message::Text(outgoing.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                }

                connected.store(false, Ordering::SeqCst);
                log!("[ws] disconnected, reconnecting in 3s");
            }
            Err(e) => {
                log!("[ws] connect failed: {e}, retry in 3s");
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn connect_ws(
    ws_url: &str,
    _token: &str,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    // 直接传 URL，tungstenite 自动构建完整握手请求（含 Sec-WebSocket-Key）
    // PK 端 WebSocket 不验证 token，仅通过 node_id query param 识别节点
    let (ws_stream, _resp) = connect_async(ws_url).await?;
    Ok(ws_stream)
}

fn ws_base(master: &str) -> String {
    if master.starts_with("https://") {
        master.replacen("https://", "wss://", 1)
    } else {
        master.replacen("http://", "ws://", 1)
    }
}
