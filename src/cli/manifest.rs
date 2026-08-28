//! SPDE 自描述能力清单（Capability Manifest）
//! 遵循 PandaNetOS 标准：每个构建版本生成自己的说明书

use serde_json::{json, Value};

/// 生成 SPDE 的完整能力清单（说明书）
pub fn build_capability_manifest() -> Value {
    let build_timestamp = option_env!("SPDE_BUILD_TIMESTAMP").unwrap_or("0");
    let git_commit = option_env!("SPDE_GIT_COMMIT").unwrap_or("unknown");
    let rust_version = option_env!("SPDE_RUST_VERSION").unwrap_or("unknown");
    let target_triple = option_env!("SPDE_TARGET_TRIPLE").unwrap_or("unknown");

    // 复用已有的节点能力注册函数
    let capabilities = super::agent::build_node_capabilities();

    json!({
        "manifest_version": "1.0",
        "basic": {
            "name": "spde",
            "full_name": "Super-Download-Engine",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "SPDE — 统一下载中心，多协议抽象层，支持 HTTP/FTP/SFTP/BT/本地文件",
            "role": "download_agent",
            "license": env!("CARGO_PKG_LICENSE"),
        },
        "capabilities": capabilities,
        "run_modes": ["agent (接入PK主控)", "serve (本地配置下载)", "cli (命令行单任务)"],
        "communication": {
            "protocols": ["HTTP/1.1 (REST API)", "WebSocket (实时状态/任务进度)"],
            "data_format": "JSON",
            "auth": "Bearer Token (可选)",
            "heartbeat_interval": "5s (默认)",
            "realtime_report": "WebSocket，节点状态/任务进度/下载速度实时上报",
            "node_deleted_handling": "收到 PK 的 NodeDeleted 消息后，立即取消所有任务并重新注册",
        },
        "configurable_params": [
            {"name": "agent.master", "type": "string", "default": "", "description": "PK 主控地址"},
            {"name": "agent.node_id", "type": "string", "default": "", "description": "本节点 UUID"},
            {"name": "agent.heartbeat_interval_secs", "type": "integer", "default": 5, "min": 1, "unit": "seconds", "description": "心跳上报间隔"},
            {"name": "global.max_concurrent", "type": "integer", "default": 4, "min": 1, "max": 64, "description": "最大并发任务数"},
            {"name": "global.resume", "type": "boolean", "default": true, "description": "断点续传"},
            {"name": "global.retry_times", "type": "integer", "default": 3, "min": 0, "max": 20, "description": "失败重试次数"},
            {"name": "global.timeout", "type": "integer", "default": 1800, "min": 10, "unit": "seconds", "description": "任务超时时间"},
            {"name": "global.skip_tls_verify", "type": "boolean", "default": false, "description": "跳过 TLS 证书校验"},
            {"name": "global.connections_per_file", "type": "integer", "default": 8, "min": 1, "max": 64, "description": "单文件连接数"},
            {"name": "global.dry_run", "type": "boolean", "default": false, "description": "只解析不实际下载"},
            {"name": "output.save_path", "type": "string", "default": "./download", "description": "下载保存目录"},
            {"name": "proxy.http_proxy", "type": "string", "default": "", "description": "HTTP 代理"},
            {"name": "proxy.https_proxy", "type": "string", "default": "", "description": "HTTPS 代理"},
            {"name": "controller.url", "type": "string", "default": "", "description": "主控 URL（PK 地址）"},
        ],
        "api_interfaces": {
            "base_path": "/api/v1",
            "endpoints": [
                {"method": "GET", "path": "/overview", "description": "探测 PK 是否在线"},
                {"method": "POST", "path": "/agent/register", "description": "向 PK 注册为节点"},
                {"method": "POST", "path": "/agent/claim", "description": "领取待下发任务"},
                {"method": "GET", "path": "/nodes/{id}/config.yaml", "description": "拉取节点配置"},
                {"method": "GET", "path": "/agent/ws", "description": "WebSocket 实时状态通道"},
            ],
        },
        "status_report": {
            "node_level": [
                "node_id",
                "hostname",
                "platform",
                "arch",
                "version",
                "status",
                "active_tasks",
                "bytes_downloaded",
                "total_speed_bps",
            ],
            "task_level": [
                "dispatch_id",
                "task_name",
                "percent",
                "speed_bps",
                "downloaded_bytes",
                "total_bytes",
                "active_connections",
                "status",
                "error_message",
            ],
        },
        "build_info": {
            "rust_version": rust_version,
            "build_timestamp": build_timestamp,
            "git_commit": git_commit,
            "target_triple": target_triple,
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "compile_features": {
                "ftp": cfg!(feature = "ftp"),
                "torrent": cfg!(feature = "torrent"),
            },
        },
    })
}

/// 输出说明书到 stdout（--manifest 命令使用）
pub fn print_manifest() {
    let manifest = build_capability_manifest();
    println!("{}", serde_json::to_string_pretty(&manifest).unwrap());
}
