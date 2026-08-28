use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// 按 PandaNetOS 能力清单标准（3.1）注入统一构建信息环境变量。
/// 变量名与共享库 [`pandanetos::capability::BuildInfo`] 读取的完全一致。
fn main() {
    let build_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let git_commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let git_branch = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let rust_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());

    // 统一构建信息变量（与 capability-manifest.md 3.1 一致）
    println!("cargo:rustc-env=BUILD_TIME={}", build_timestamp);
    println!("cargo:rustc-env=GIT_COMMIT={}", git_commit);
    println!("cargo:rustc-env=GIT_BRANCH={}", git_branch);
    println!("cargo:rustc-env=RUSTC_VERSION={}", rust_version);
    println!("cargo:rustc-env=TARGET_TRIPLE={}", target);
    println!("cargo:rustc-env=BUILD_PROFILE={}", profile);

    println!("cargo:rerun-if-changed=build.rs");
}
