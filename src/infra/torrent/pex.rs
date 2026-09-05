//! PEX (Peer Exchange) 协议实现
//!
//! BEP 11: Peer Exchange (PEX)
//!
//! PEX 允许 peer 之间交换已知的 peer 列表，减少对 tracker/DHT 的依赖。
//! 通过 Extension Protocol (BEP 10) 的 `ut_pex` 消息传输。
//!
//! 消息格式（bencode 字典）：
//! - added: 新增的 peer（compact format，IPv4 6B/个）
//! - added.f: 新增 peer 的标志位（1B/个，bit0=seeder, bit1=utp, bit2=outgoing）
//! - dropped: 离开的 peer（compact format）
//! - added6: 新增的 IPv6 peer（18B/个）
//! - added6.f: IPv6 peer 标志位
//! - dropped6: 离开的 IPv6 peer

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_bencode::value::Value;
use tracing::{debug, trace};

/// PEX 消息
#[derive(Debug, Clone, Default)]
pub struct PexMessage {
    /// 新增的 IPv4 peer
    pub added: Vec<SocketAddr>,
    /// 新增 peer 的标志位
    pub added_flags: Vec<PexPeerFlags>,
    /// 离开的 IPv4 peer
    pub dropped: Vec<SocketAddr>,
    /// 新增的 IPv6 peer
    pub added6: Vec<SocketAddr>,
    /// IPv6 peer 标志位
    pub added6_flags: Vec<PexPeerFlags>,
    /// 离开的 IPv6 peer
    pub dropped6: Vec<SocketAddr>,
}

/// PEX peer 标志位
#[derive(Debug, Clone, Copy, Default)]
pub struct PexPeerFlags {
    /// 是否为 seeder（已下载完成）
    pub seeder: bool,
    /// 是否使用 uTP 传输
    pub utp: bool,
    /// 是否为出站连接
    pub outgoing: bool,
}

impl PexPeerFlags {
    /// 从字节解析
    pub fn from_byte(b: u8) -> Self {
        PexPeerFlags {
            seeder: b & 0x01 != 0,
            utp: b & 0x02 != 0,
            outgoing: b & 0x04 != 0,
        }
    }

    /// 序列化为字节
    pub fn to_byte(&self) -> u8 {
        let mut b = 0u8;
        if self.seeder {
            b |= 0x01;
        }
        if self.utp {
            b |= 0x02;
        }
        if self.outgoing {
            b |= 0x04;
        }
        b
    }
}

impl PexMessage {
    /// 从 bencode 字节解析 PEX 消息
    pub fn from_bytes(data: &[u8]) -> anyhow::Result<Self> {
        let value: Value = serde_bencode::from_bytes(data)
            .map_err(|e| anyhow::anyhow!("PEX 消息解析失败: {}", e))?;

        let dict = match value {
            Value::Dict(d) => d,
            _ => return Err(anyhow::anyhow!("PEX 消息不是字典")),
        };

        let mut msg = PexMessage::default();

        // added (IPv4 compact peers)
        if let Some(Value::Bytes(b)) = dict.get(b"added".as_ref()) {
            msg.added = parse_compact_peers_v4(b);
        }

        // added.f (flags)
        if let Some(Value::Bytes(b)) = dict.get(b"added.f".as_ref()) {
            msg.added_flags = b.iter().map(|&x| PexPeerFlags::from_byte(x)).collect();
        }

        // dropped (IPv4 compact peers)
        if let Some(Value::Bytes(b)) = dict.get(b"dropped".as_ref()) {
            msg.dropped = parse_compact_peers_v4(b);
        }

        // added6 (IPv6 compact peers)
        if let Some(Value::Bytes(b)) = dict.get(b"added6".as_ref()) {
            msg.added6 = parse_compact_peers_v6(b);
        }

        // added6.f
        if let Some(Value::Bytes(b)) = dict.get(b"added6.f".as_ref()) {
            msg.added6_flags = b.iter().map(|&x| PexPeerFlags::from_byte(x)).collect();
        }

        // dropped6
        if let Some(Value::Bytes(b)) = dict.get(b"dropped6".as_ref()) {
            msg.dropped6 = parse_compact_peers_v6(b);
        }

        Ok(msg)
    }

    /// 序列化为 bencode 字节
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut dict = HashMap::new();

        // added
        if !self.added.is_empty() {
            dict.insert("added".to_string(), Value::Bytes(encode_compact_peers_v4(&self.added)));
        }

        // added.f
        if !self.added_flags.is_empty() {
            dict.insert(
                "added.f".to_string(),
                Value::Bytes(self.added_flags.iter().map(|f| f.to_byte()).collect()),
            );
        }

        // dropped
        if !self.dropped.is_empty() {
            dict.insert(
                "dropped".to_string(),
                Value::Bytes(encode_compact_peers_v4(&self.dropped)),
            );
        }

        // added6
        if !self.added6.is_empty() {
            dict.insert(
                "added6".to_string(),
                Value::Bytes(encode_compact_peers_v6(&self.added6)),
            );
        }

        // added6.f
        if !self.added6_flags.is_empty() {
            dict.insert(
                "added6.f".to_string(),
                Value::Bytes(self.added6_flags.iter().map(|f| f.to_byte()).collect()),
            );
        }

        // dropped6
        if !self.dropped6.is_empty() {
            dict.insert(
                "dropped6".to_string(),
                Value::Bytes(encode_compact_peers_v6(&self.dropped6)),
            );
        }

        let bytes = serde_bencode::to_bytes(&Value::Dict(
            dict.into_iter()
                .map(|(k, v)| (k.into_bytes(), v))
                .collect(),
        ))
        .map_err(|e| anyhow::anyhow!("PEX 消息序列化失败: {}", e))?;

        Ok(bytes)
    }
}

/// PEX 管理器
///
/// 管理与每个 peer 的 PEX 状态，包括：
/// - 已发送给该 peer 的 peer 列表（避免重复发送）
/// - 从该 peer 收到的 peer 列表
/// - 发送间隔限制（BEP 11: 最少 1 分钟）
pub struct PexManager {
    /// 每个 peer 的 PEX 状态
    peer_states: HashMap<String, PeerPexState>,
    /// 全局已知 peer 池
    known_peers: HashSet<SocketAddr>,
    /// 最大发送 peer 数（每次 PEX 消息）
    max_added: usize,
    /// 最小发送间隔
    min_interval: Duration,
}

/// 单个 peer 的 PEX 状态
struct PeerPexState {
    /// 已发送给该 peer 的 peer
    sent: HashSet<SocketAddr>,
    /// 上次发送时间
    last_sent: Option<Instant>,
    /// 从该 peer 收到的 peer
    received: HashSet<SocketAddr>,
}

impl PexManager {
    /// 创建新的 PEX 管理器
    pub fn new() -> Self {
        PexManager {
            peer_states: HashMap::new(),
            known_peers: HashSet::new(),
            max_added: 50,
            min_interval: Duration::from_secs(60),
        }
    }

    /// 添加已知 peer 到全局池
    pub fn add_known_peer(&mut self, addr: SocketAddr) {
        self.known_peers.insert(addr);
    }

    /// 批量添加已知 peer
    pub fn add_known_peers(&mut self, addrs: &[SocketAddr]) {
        for addr in addrs {
            self.known_peers.insert(*addr);
        }
    }

    /// 获取全局已知 peer 数量
    pub fn known_peer_count(&self) -> usize {
        self.known_peers.len()
    }

    /// 检查是否可以向某个 peer 发送 PEX 消息
    pub fn can_send(&self, peer_id: &str) -> bool {
        if let Some(state) = self.peer_states.get(peer_id) {
            if let Some(last) = state.last_sent {
                return last.elapsed() >= self.min_interval;
            }
        }
        true
    }

    /// 生成要发送给某个 peer 的 PEX 消息
    ///
    /// 返回 None 如果还不能发送（间隔未到）或没有新 peer
    pub fn build_message(&mut self, peer_id: &str, peer_addr: SocketAddr) -> Option<PexMessage> {
        // 检查间隔
        if !self.can_send(peer_id) {
            return None;
        }

        let state = self.peer_states.entry(peer_id.to_string()).or_insert(PeerPexState {
            sent: HashSet::new(),
            last_sent: None,
            received: HashSet::new(),
        });

        // 找出未发送过的 peer（排除 peer 自己）
        let new_peers: Vec<SocketAddr> = self
            .known_peers
            .iter()
            .filter(|&&addr| addr != peer_addr && !state.sent.contains(&addr))
            .take(self.max_added)
            .copied()
            .collect();

        if new_peers.is_empty() {
            return None;
        }

        // 标记为已发送
        for addr in &new_peers {
            state.sent.insert(*addr);
        }
        state.last_sent = Some(Instant::now());

        // 分离 IPv4 和 IPv6
        let (added_v4, added_v6): (Vec<_>, Vec<_>) = new_peers
            .into_iter()
            .partition(|addr| matches!(addr.ip(), IpAddr::V4(_)));

        let flags_v4: Vec<PexPeerFlags> = added_v4
            .iter()
            .map(|_| PexPeerFlags::default())
            .collect();
        let flags_v6: Vec<PexPeerFlags> = added_v6
            .iter()
            .map(|_| PexPeerFlags::default())
            .collect();

        Some(PexMessage {
            added: added_v4,
            added_flags: flags_v4,
            dropped: vec![],
            added6: added_v6,
            added6_flags: flags_v6,
            dropped6: vec![],
        })
    }

    /// 处理从某个 peer 收到的 PEX 消息
    ///
    /// 返回新发现的 peer 列表（之前全局池中没有的）
    pub fn handle_message(&mut self, peer_id: &str, msg: &PexMessage) -> Vec<SocketAddr> {
        let state = self.peer_states.entry(peer_id.to_string()).or_insert(PeerPexState {
            sent: HashSet::new(),
            last_sent: None,
            received: HashSet::new(),
        });

        let mut new_peers = vec![];

        // 处理 added
        for addr in &msg.added {
            if !self.known_peers.contains(addr) {
                new_peers.push(*addr);
            }
            self.known_peers.insert(*addr);
            state.received.insert(*addr);
        }

        // 处理 added6
        for addr in &msg.added6 {
            if !self.known_peers.contains(addr) {
                new_peers.push(*addr);
            }
            self.known_peers.insert(*addr);
            state.received.insert(*addr);
        }

        // 处理 dropped（从全局池移除）
        for addr in &msg.dropped {
            self.known_peers.remove(addr);
        }
        for addr in &msg.dropped6 {
            self.known_peers.remove(addr);
        }

        trace!(
            "[pex] 从 {} 收到 {} 个新 peer ({} IPv4, {} IPv6)",
            peer_id,
            new_peers.len(),
            msg.added.len(),
            msg.added6.len()
        );

        new_peers
    }

    /// 移除 peer 状态（断开连接时）
    pub fn remove_peer(&mut self, peer_id: &str) {
        self.peer_states.remove(peer_id);
    }
}

impl Default for PexManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 解析 IPv4 compact peer 列表（6B/个：4B IP + 2B port）
fn parse_compact_peers_v4(data: &[u8]) -> Vec<SocketAddr> {
    let mut peers = vec![];
    for chunk in data.chunks(6) {
        if chunk.len() < 6 {
            break;
        }
        let ip = IpAddr::V4(std::net::Ipv4Addr::new(
            chunk[0], chunk[1], chunk[2], chunk[3],
        ));
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(SocketAddr::new(ip, port));
    }
    peers
}

/// 解析 IPv6 compact peer 列表（18B/个：16B IP + 2B port）
fn parse_compact_peers_v6(data: &[u8]) -> Vec<SocketAddr> {
    let mut peers = vec![];
    for chunk in data.chunks(18) {
        if chunk.len() < 18 {
            break;
        }
        let mut ip_bytes = [0u8; 16];
        ip_bytes.copy_from_slice(&chunk[..16]);
        let ip = IpAddr::V6(std::net::Ipv6Addr::from(ip_bytes));
        let port = u16::from_be_bytes([chunk[16], chunk[17]]);
        peers.push(SocketAddr::new(ip, port));
    }
    peers
}

/// 编码 IPv4 compact peer 列表
fn encode_compact_peers_v4(peers: &[SocketAddr]) -> Vec<u8> {
    let mut data = vec![];
    for peer in peers {
        if let IpAddr::V4(ip) = peer.ip() {
            data.extend_from_slice(&ip.octets());
            data.extend_from_slice(&peer.port().to_be_bytes());
        }
    }
    data
}

/// 编码 IPv6 compact peer 列表
fn encode_compact_peers_v6(peers: &[SocketAddr]) -> Vec<u8> {
    let mut data = vec![];
    for peer in peers {
        if let IpAddr::V6(ip) = peer.ip() {
            data.extend_from_slice(&ip.octets());
            data.extend_from_slice(&peer.port().to_be_bytes());
        }
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4_addr(a: u8, b: u8, c: u8, d: u8, port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), port)
    }

    #[test]
    fn test_pex_flags() {
        let flags = PexPeerFlags {
            seeder: true,
            utp: false,
            outgoing: true,
        };
        let byte = flags.to_byte();
        assert_eq!(byte, 0x05); // 0b00000101

        let parsed = PexPeerFlags::from_byte(byte);
        assert!(parsed.seeder);
        assert!(!parsed.utp);
        assert!(parsed.outgoing);
    }

    #[test]
    fn test_compact_peers_v4() {
        let peers = vec![v4_addr(1, 2, 3, 4, 6881), v4_addr(5, 6, 7, 8, 6882)];
        let encoded = encode_compact_peers_v4(&peers);
        assert_eq!(encoded.len(), 12); // 2 * 6

        let decoded = parse_compact_peers_v4(&encoded);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], peers[0]);
        assert_eq!(decoded[1], peers[1]);
    }

    #[test]
    fn test_compact_peers_v6() {
        let addr = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
            6881,
        );
        let peers = vec![addr];
        let encoded = encode_compact_peers_v6(&peers);
        assert_eq!(encoded.len(), 18);

        let decoded = parse_compact_peers_v6(&encoded);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], addr);
    }

    #[test]
    fn test_pex_message_roundtrip() {
        let msg = PexMessage {
            added: vec![v4_addr(1, 2, 3, 4, 6881)],
            added_flags: vec![PexPeerFlags {
                seeder: true,
                ..Default::default()
            }],
            dropped: vec![v4_addr(5, 6, 7, 8, 6882)],
            added6: vec![],
            added6_flags: vec![],
            dropped6: vec![],
        };

        let bytes = msg.to_bytes().unwrap();
        let decoded = PexMessage::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.added.len(), 1);
        assert_eq!(decoded.added[0], v4_addr(1, 2, 3, 4, 6881));
        assert!(decoded.added_flags[0].seeder);
        assert_eq!(decoded.dropped.len(), 1);
    }

    #[test]
    fn test_pex_manager() {
        let mut manager = PexManager::new();

        // 添加已知 peer
        manager.add_known_peers(&[
            v4_addr(1, 1, 1, 1, 6881),
            v4_addr(2, 2, 2, 2, 6882),
            v4_addr(3, 3, 3, 3, 6883),
        ]);
        assert_eq!(manager.known_peer_count(), 3);

        // 构建消息（第一次应该包含所有 peer）
        let peer_addr = v4_addr(9, 9, 9, 9, 6889);
        let msg = manager.build_message("peer1", peer_addr);
        assert!(msg.is_some());
        let msg = msg.unwrap();
        assert_eq!(msg.added.len(), 3); // 不包含 peer 自己

        // 第二次应该返回 None（间隔未到）
        let msg2 = manager.build_message("peer1", peer_addr);
        assert!(msg2.is_none());
    }

    #[test]
    fn test_pex_manager_handle_message() {
        let mut manager = PexManager::new();

        let msg = PexMessage {
            added: vec![v4_addr(10, 0, 0, 1, 6881), v4_addr(10, 0, 0, 2, 6882)],
            added_flags: vec![],
            dropped: vec![],
            added6: vec![],
            added6_flags: vec![],
            dropped6: vec![],
        };

        let new_peers = manager.handle_message("peer1", &msg);
        assert_eq!(new_peers.len(), 2);
        assert_eq!(manager.known_peer_count(), 2);

        // 再次收到相同的 peer，不应该返回新的
        let new_peers2 = manager.handle_message("peer2", &msg);
        assert_eq!(new_peers2.len(), 0);
    }
}
