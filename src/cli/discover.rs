use anyhow::Result;
use std::net::{Ipv4Addr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// 扫描局域网内的 PK 服务端，返回第一个找到的 master URL（如 http://192.168.1.100:8080）
pub async fn discover_pk(scan_ports: &[u16]) -> Option<String> {
    let ips = local_ipv4s();
    if ips.is_empty() {
        eprintln!("[discover] no local IPv4 found, skip scan");
        return None;
    }

    eprintln!(
        "[discover] scanning {} subnet(s) on ports {:?} ...",
        ips.len(),
        scan_ports
    );

    let sem = Arc::new(Semaphore::new(64));
    let mut handles = Vec::new();

    for ip in &ips {
        let octets = ip.octets();
        for i in 1..255u8 {
            let target = Ipv4Addr::new(octets[0], octets[1], octets[2], i);
            for &port in scan_ports {
                let permit = sem.clone().acquire_owned().await.ok()?;
                handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    let addr = format!("{}:{}", target, port);
                    if tokio::time::timeout(
                        Duration::from_millis(400),
                        tokio::net::TcpStream::connect(&addr),
                    )
                    .await
                    .is_ok()
                    {
                        if verify_pk(&target, port).await {
                            return Some(format!("http://{}:{}", target, port));
                        }
                    }
                    None
                }));
            }
        }
    }

    for h in handles {
        if let Ok(Some(url)) = h.await {
            eprintln!("[discover] found PK at {}", url);
            return Some(url);
        }
    }

    eprintln!("[discover] no PK found on local network");
    None
}

/// 验证目标地址是否为 PK 服务端（GET /api/v1/overview 返回合法 JSON）
async fn verify_pk(ip: &Ipv4Addr, port: u16) -> bool {
    let url = format!("http://{}:{}/api/v1/overview", ip, port);
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(_) => return false,
    };
    if !resp.status().is_success() {
        return false;
    }
    let json: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return false,
    };
    json.get("nodes_total").is_some()
}

/// 获取本机所有非回环 IPv4 地址
fn local_ipv4s() -> Vec<Ipv4Addr> {
    let mut ips = Vec::new();

    // 方法1: UDP socket 连接技巧（拿到默认路由的出口 IP）
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = socket.local_addr() {
                if let std::net::IpAddr::V4(v4) = addr.ip() {
                    if !v4.is_loopback() && !ips.contains(&v4) {
                        ips.push(v4);
                    }
                }
            }
        }
    }

    // 方法2: 解析主机名
    if let Ok(host) = hostname::get() {
        if let Ok(name) = host.into_string() {
            if let Ok(addrs) = format!("{}:0", name).to_socket_addrs() {
                for addr in addrs {
                    if let std::net::IpAddr::V4(v4) = addr.ip() {
                        if !v4.is_loopback() && !ips.contains(&v4) {
                            ips.push(v4);
                        }
                    }
                }
            }
        }
    }

    ips
}

/// 持续扫描直到找到 PK 或用户中断
pub async fn discover_pk_wait(scan_ports: &[u16]) -> Result<String> {
    loop {
        if let Some(url) = discover_pk(scan_ports).await {
            return Ok(url);
        }
        eprintln!("[discover] retrying in 10s (Ctrl+C to exit) ...");
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {}
            _ = tokio::signal::ctrl_c() => {
                anyhow::bail!("interrupted during discovery");
            }
        }
    }
}
