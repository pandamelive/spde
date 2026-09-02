//! SPDE CLI 子命令实现

macro_rules! log {
    ($($arg:tt)*) => {{
        let ts = pandanetos::time::now_rfc3339();
        std::eprint!("[{}] ", ts);
        std::eprintln!($($arg)*);
    }};
}

pub mod agent;
pub mod config;
pub mod discover;
pub mod history;
pub mod manifest;
pub mod new_download;
pub mod paths;
pub mod p2p;
pub mod ws_client;
