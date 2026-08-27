use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let build_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=SPDE_BUILD_TIMESTAMP={}", build_timestamp);

    let git_commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SPDE_GIT_COMMIT={}", git_commit);

    let rust_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SPDE_RUST_VERSION={}", rust_version);

    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=SPDE_TARGET_TRIPLE={}", target);

    println!("cargo:rerun-if-changed=build.rs");
}
