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
