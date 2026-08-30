//! SPDE 自描述能力清单（Capability Manifest）
//! 遵循 PandaNetOS 标准，基于共享库 [`pandanetos::capability`] 生成说明书

use pandanetos::capability::{
    ApiInterface, BasicInfo, BuildInfo, Capabilities, CapabilityManifest, Communication,
    ComponentRole, ConfigurableParam, StatusReport,
};
use pandanetos::protocol::paths;
use std::collections::BTreeMap;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 支持的全部协议（随编译特性变化）
fn supported_protocols() -> Vec<String> {
    [
        "http", "https", "ssh", "sftp", "file",
        #[cfg(feature = "ftp")]
        "ftp",
        #[cfg(feature = "torrent")]
        "torrent",
        #[cfg(feature = "torrent")]
        "magnet",
    ]
    .into_iter()
    .map(|s: &str| s.to_string())
    .collect()
}

/// 各协议的 URI 前缀格式（随编译特性变化）
fn uri_formats() -> Vec<String> {
    [
        "http://",
        "https://",
        "ssh://",
        "sftp://",
        "file://",
        #[cfg(feature = "ftp")]
        "ftp://",
        #[cfg(feature = "torrent")]
        "magnet:?xt=urn:btih:",
    ]
    .into_iter()
    .map(|s: &str| s.to_string())
    .collect()
}

/// 编译特性列表（随编译特性变化）
fn compile_features() -> Vec<String> {
    [
        #[cfg(feature = "ftp")]
        "ftp",
        #[cfg(feature = "torrent")]
        "torrent",
    ]
    .into_iter()
    .map(|s: &str| s.to_string())
    .collect()
}

/// 生成 SPDE 的完整能力清单（说明书）
pub fn build_capability_manifest() -> serde_json::Value {
    // ── 基本信息 ──
    let basic = BasicInfo::new(
        "spde",
        VERSION,
        "SPDE — 统一下载中心，多协议抽象层，支持 HTTP/FTP/SFTP/BT/本地文件",
        ComponentRole::DataPlane,
    )
    .with_mode("agent");

    // ── 能力清单 ──
    let capabilities = Capabilities {
        protocols: supported_protocols(),
        features: BTreeMap::from([
            ("resume".to_string(), true),
            ("multi_connection".to_string(), true),
            ("work_stealing".to_string(), true),
            ("chunked_download".to_string(), true),
            ("retry".to_string(), true),
            ("proxy".to_string(), true),
            ("tls_skip_verify".to_string(), true),
            ("dry_run".to_string(), true),
            ("speed_limit".to_string(), true),
            ("preallocate".to_string(), true),
            ("progress_callback".to_string(), true),
            ("realtime_progress".to_string(), true),
            ("auto_connections".to_string(), true),
            ("custom_headers".to_string(), true),
        ]),
        task_control: BTreeMap::from([
            ("pause".to_string(), true),
            ("resume".to_string(), true),
            ("cancel".to_string(), true),
            ("pause_all".to_string(), true),
            ("resume_all".to_string(), true),
            ("cancel_all".to_string(), true),
            ("controller".to_string(), true),
        ]),
        hardware: BTreeMap::from([
            ("cpu_cores".to_string(), serde_json::json!(available_cores())),
            ("os".to_string(), serde_json::json!(std::env::consts::OS)),
            ("arch".to_string(), serde_json::json!(std::env::consts::ARCH)),
            ("family".to_string(), serde_json::json!(std::env::consts::FAMILY)),
        ]),
        compile_features: compile_features(),
    };

    // ── 可配置参数 ──
    let configurable_params = BTreeMap::from([
        (
            "agent.master".to_string(),
            ConfigurableParam::string("", None, "PK 主控地址"),
        ),
        (
            "agent.node_id".to_string(),
            ConfigurableParam::string("", None, "本节点 UUID"),
        ),
        (
            "agent.heartbeat_interval_secs".to_string(),
            ConfigurableParam::number("u32", 5.0, Some(1.0), None, Some("seconds"), "心跳上报间隔"),
        ),
        (
            "global.max_concurrent".to_string(),
            ConfigurableParam::number("u32", 4.0, Some(1.0), Some(64.0), None, "最大并发任务数"),
        ),
        (
            "global.resume".to_string(),
            ConfigurableParam::boolean(true, "断点续传"),
        ),
        (
            "global.retry_times".to_string(),
            ConfigurableParam::number("u32", 3.0, Some(0.0), Some(20.0), None, "失败重试次数"),
        ),
        (
            "global.timeout".to_string(),
            ConfigurableParam::number("u32", 1800.0, Some(10.0), None, Some("seconds"), "任务超时时间"),
        ),
        (
            "global.skip_tls_verify".to_string(),
            ConfigurableParam::boolean(false, "跳过 TLS 证书校验"),
        ),
        (
            "global.connections_per_file".to_string(),
            ConfigurableParam::number("u32", 8.0, Some(1.0), Some(64.0), None, "单文件连接数"),
        ),
        (
            "global.dry_run".to_string(),
            ConfigurableParam::boolean(false, "只解析不实际下载"),
        ),
        (
            "output.save_path".to_string(),
            ConfigurableParam::string("./download", None, "下载保存目录"),
        ),
        (
            "proxy.http_proxy".to_string(),
            ConfigurableParam::string("", None, "HTTP 代理"),
        ),
        (
            "proxy.https_proxy".to_string(),
            ConfigurableParam::string("", None, "HTTPS 代理"),
        ),
        (
            "controller.url".to_string(),
            ConfigurableParam::string("", None, "主控 URL（PK 地址）"),
        ),
    ]);

    // ── API 接口 ──
    let api_interfaces = BTreeMap::from([
        (
            "overview".to_string(),
            ApiInterface::new("GET", paths::OVERVIEW, "探测 PK 是否在线"),
        ),
        (
            "agent_register".to_string(),
            ApiInterface::new("POST", paths::AGENT_REGISTER, "向 PK 注册为节点")
                .with_request_field("node_id", "uuid")
                .with_request_field("hostname", "string")
                .with_request_field("platform", "string")
                .with_request_field("arch", "string")
                .with_request_field("version", "string")
                .with_auth(),
        ),
        (
            "dispatch_claim".to_string(),
            ApiInterface::new("POST", paths::DISPATCH_CLAIM, "领取待下发任务"),
        ),
        (
            "fetch_config".to_string(),
            ApiInterface::new("GET", paths::NODE_CONFIG_YAML, "拉取节点配置"),
        ),
        (
            "agent_ws".to_string(),
            ApiInterface::new("GET", paths::AGENT_WS, "WebSocket 实时状态通道"),
        ),
    ]);

    // ── 状态上报字段 ──
    let status_report = StatusReport {
        node_level: [
            "node_id", "hostname", "platform", "arch", "version", "status", "active_tasks",
            "bytes_downloaded", "total_speed_bps",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
        task_level: [
            "dispatch_id", "task_name", "percent", "speed_bps", "downloaded_bytes", "total_size",
            "active_connections", "status", "error_message",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
    };

    // ── 通信能力 ──
    let communication = Communication {
        websocket: true,
        http_api: true,
        heartbeat: true,
        heartbeat_interval_secs: 5,
        websocket_reconnect_secs: 3,
    };

    // ── 组装 ──
    let mut manifest = CapabilityManifest::new(basic)
        .with_capabilities(capabilities)
        .with_status_report(status_report)
        .with_communication(communication);

    // 构建信息：本 crate 的 build.rs 已注入统一变量，在 spde 侧读取（共享库的
    // option_env! 在 pandanetos crate 内求值，看不到本项目注入的变量）
    manifest.build_info = build_info_from_env();

    for (name, param) in configurable_params {
        manifest = manifest.with_configurable_param(&name, param);
    }
    for (name, interface) in api_interfaces {
        manifest = manifest.with_api_interface(&name, interface);
    }

    pandanetos::capability::build_capability_manifest(&manifest)
}

/// 从本 crate 的 build.rs 注入变量构造构建信息（标准 2.8）
fn build_info_from_env() -> BuildInfo {
    let build_time = option_env!("BUILD_TIME")
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(|secs| chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0))
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "unknown".to_string());
    BuildInfo {
        rust_version: option_env!("RUSTC_VERSION").unwrap_or("unknown").to_string(),
        build_profile: option_env!("BUILD_PROFILE").unwrap_or("unknown").to_string(),
        build_time,
        git_commit: option_env!("GIT_COMMIT").unwrap_or("unknown").to_string(),
        git_branch: option_env!("GIT_BRANCH").unwrap_or("unknown").to_string(),
        target_triple: option_env!("TARGET_TRIPLE").unwrap_or("unknown").to_string(),
    }
}

/// 节点注册时上报的能力参数（标准 4.1：注册时上报完整能力清单）
///
/// 与 `--manifest` 输出同一份标准清单；另外保留兼容别名（supported_protocols 等），
/// 供旧版 PK 展示层透传使用，pk 对未知字段不做解析。
pub fn build_node_capabilities() -> serde_json::Value {
    let mut caps = build_capability_manifest();
    if let Some(obj) = caps.as_object_mut() {
        // 旧版 PK 展示层透传使用的兼容别名（pk 对未知字段不做解析）
        obj.insert("supported_protocols".to_string(), serde_json::json!(supported_protocols()));
        obj.insert("uri_formats".to_string(), serde_json::json!(uri_formats()));
        obj.insert("run_modes".to_string(), serde_json::json!(["agent", "standalone", "cli"]));
        obj.insert(
            "compile_features".to_string(),
            serde_json::json!({
                "ftp": cfg!(feature = "ftp"),
                "torrent": cfg!(feature = "torrent"),
            }),
        );
    }
    caps
}

/// 输出说明书到 stdout（--manifest 命令使用）
pub fn print_manifest() {
    let manifest = build_capability_manifest();
    println!("{}", serde_json::to_string_pretty(&manifest).unwrap()); // panda-allow: cli-output
}

fn available_cores() -> u64 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u64)
        .unwrap_or(1)
}