use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

// ── 消息协议（与 PK 端 ws.rs 对应） ──────────────────────

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg<'a> {
    Pong,
    Status {
        active_tasks: u32,
        bytes_downloaded: u64,
        busy: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_error: Option<&'a str>,
    },
    TaskStarted {
        dispatch_id: Uuid,
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

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMsg {
    ConfigChanged,
    Ping,
    DeleteFile {
        filename: String,
        #[serde(default)]
        save_path: Option<String>,
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

// ── WsClient ─────────────────────────────────────────────

#[derive(Clone)]
pub struct WsClient {
    tx: mpsc::Sender<String>,
    config_notify: Arc<Notify>,
    connected: Arc<AtomicBool>,
}

impl WsClient {
    /// 启动 WebSocket 客户端（后台自动重连）
    pub fn spawn(node_id: Uuid, master: String, token: String, base_dir: PathBuf) -> Self {
        let (tx, rx) = mpsc::channel::<String>(128);
        let config_notify = Arc::new(Notify::new());
        let connected = Arc::new(AtomicBool::new(false));

        tokio::spawn(connection_loop(
            node_id,
            master,
            token,
            rx,
            config_notify.clone(),
            connected.clone(),
            base_dir,
        ));

        Self {
            tx,
            config_notify,
            connected,
        }
    }

    /// 等待 PK 推送 config_changed 通知
    pub async fn wait_config_change(&self) {
        self.config_notify.notified().await;
    }

    /// 主动触发一次 config 拉取（用于首次连接或重连后）
    pub fn notify_config_change(&self) {
        self.config_notify.notify_waiters();
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub async fn send_status(
        &self,
        active_tasks: u32,
        bytes_downloaded: u64,
        busy: bool,
        last_error: Option<&str>,
    ) {
        let msg = ClientMsg::Status {
            active_tasks,
            bytes_downloaded,
            busy,
            last_error,
        };
        self.send_json(&msg).await;
    }

    pub async fn send_task_started(&self, dispatch_id: Uuid) {
        let msg = ClientMsg::TaskStarted { dispatch_id };
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

async fn connection_loop(
    node_id: Uuid,
    master: String,
    token: String,
    mut rx: mpsc::Receiver<String>,
    config_notify: Arc<Notify>,
    connected: Arc<AtomicBool>,
    base_dir: PathBuf,
) {
    let ws_base = ws_base(&master);
    let ws_url = format!("{}/api/v1/agent/ws?node_id={}", ws_base, node_id);

    loop {
        match connect_ws(&ws_url, &token).await {
            Ok(ws_stream) => {
                connected.store(true, Ordering::SeqCst);
                eprintln!("[ws] connected to {}", ws_url);
                // 连接成功后通知拉一次 config
                config_notify.notify_waiters();

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
                                                eprintln!("[ws] config_changed received");
                                                config_notify.notify_waiters();
                                            }
                                            ServerMsg::Ping => {
                                                let pong = serde_json::to_string(&ClientMsg::Pong).unwrap_or_default();
                                                if write.send(Message::Text(pong.into())).await.is_err() {
                                                    break;
                                                }
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
                                                eprintln!("[ws] delete_file requested: {:?}", file_path);
                                                match tokio::fs::remove_file(&file_path).await {
                                                    Ok(_) => eprintln!("[ws] deleted: {:?}", file_path),
                                                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                                    Err(e) => eprintln!("[ws] delete failed: {e}"),
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
                eprintln!("[ws] disconnected, reconnecting in 3s");
            }
            Err(e) => {
                eprintln!("[ws] connect failed: {e}, retry in 3s");
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn connect_ws(ws_url: &str, _token: &str) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
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
