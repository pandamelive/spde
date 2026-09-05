//! BT 连接管理器
//!
//! 统一管理 BT 协议连接，整合：
//! - S1: BT 握手（BitTorrent protocol）
//! - S2: Extension 协议协商（BEP 10）
//! - S4: PEX 消息处理（BEP 11）
//! - S3: ut_metadata 下载（BEP 9）
//!
//! 供 MetadataDownloader 和主下载器共用，避免重复实现。

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_bencode::value::Value;
use tracing::{debug, trace};

use super::pex::{PexManager, PexMessage};

/// BT 协议头
const BT_PROTOCOL: &[u8] = b"BitTorrent protocol";

/// 保留字节（8 字节），设置 Extension 协议支持位
/// byte[5] bit 0x10 = Extension Protocol (BEP 10)
const RESERVED_BYTES: [u8; 8] = [0, 0, 0, 0, 0, 0x10, 0, 0];

/// peer ID 前缀（spde）
const PEER_ID_PREFIX: &[u8] = b"-SP0001-";

/// Extension 消息 ID
const EXTENDED_MESSAGE_ID: u8 = 20;

/// Extension 握手消息 ID
const EXTENSION_HANDSHAKE: u8 = 0;

/// BT 连接
pub struct BtConnection {
    /// TCP 流
    stream: TcpStream,
    /// 本端 peer ID
    peer_id: [u8; 20],
    /// 对端 peer ID
    remote_peer_id: Option<[u8; 20]>,
    /// 对端支持的 Extension 映射（name -> id）
    extension_map: HashMap<String, u8>,
    /// 本端 ut_metadata 消息 ID
    ut_metadata_id: Option<u8>,
    /// 对端 ut_metadata 消息 ID
    remote_ut_metadata_id: Option<u8>,
    /// metadata 大小（字节）
    metadata_size: Option<usize>,
    /// PEX 管理器
    pex: PexManager,
    /// 连接超时
    timeout: Duration,
}

impl BtConnection {
    /// 连接到 peer 并完成握手
    pub fn connect(addr: &str, infohash: &[u8; 20], timeout: Duration) -> Result<Self> {
        let stream =
            TcpStream::connect(addr).with_context(|| format!("连接 peer 失败: {}", addr))?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        stream.set_nodelay(true)?;

        let mut conn = BtConnection {
            stream,
            peer_id: generate_peer_id(),
            remote_peer_id: None,
            extension_map: HashMap::new(),
            ut_metadata_id: Some(1), // 本端分配 ut_metadata = 1
            remote_ut_metadata_id: None,
            metadata_size: None,
            pex: PexManager::new(),
            timeout,
        };

        // 执行 BT 握手
        conn.handshake(infohash)?;

        // 执行 Extension 握手
        conn.extension_handshake()?;

        Ok(conn)
    }

    /// BT 握手（S1）
    fn handshake(&mut self, infohash: &[u8; 20]) -> Result<()> {
        // 发送握手
        let mut handshake = Vec::with_capacity(68);
        handshake.push(19); // pstrlen
        handshake.extend_from_slice(BT_PROTOCOL);
        handshake.extend_from_slice(&RESERVED_BYTES);
        handshake.extend_from_slice(infohash);
        handshake.extend_from_slice(&self.peer_id);
        self.stream.write_all(&handshake).context("发送握手失败")?;

        // 接收握手
        let mut pstrlen = [0u8; 1];
        self.stream
            .read_exact(&mut pstrlen)
            .context("读取 pstrlen 失败")?;
        if pstrlen[0] != 19 {
            return Err(anyhow::anyhow!("无效的 pstrlen: {}", pstrlen[0]));
        }

        let mut pstr = [0u8; 19];
        self.stream
            .read_exact(&mut pstr)
            .context("读取 pstr 失败")?;
        if &pstr != BT_PROTOCOL {
            return Err(anyhow::anyhow!("无效的协议标识"));
        }

        let mut reserved = [0u8; 8];
        self.stream
            .read_exact(&mut reserved)
            .context("读取 reserved 失败")?;

        // 检查对端是否支持 Extension 协议
        let supports_extension = reserved[5] & 0x10 != 0;
        if !supports_extension {
            debug!("[bt] 对端不支持 Extension 协议");
        }

        let mut remote_infohash = [0u8; 20];
        self.stream
            .read_exact(&mut remote_infohash)
            .context("读取 infohash 失败")?;
        if &remote_infohash != infohash {
            return Err(anyhow::anyhow!("infohash 不匹配"));
        }

        let mut remote_peer_id = [0u8; 20];
        self.stream
            .read_exact(&mut remote_peer_id)
            .context("读取 peer_id 失败")?;
        self.remote_peer_id = Some(remote_peer_id);

        trace!("[bt] 握手完成: {:?}", hex::encode(&remote_peer_id[..8]));
        Ok(())
    }

    /// Extension 握手（S2）
    fn extension_handshake(&mut self) -> Result<()> {
        // 构建 Extension 握手消息
        let mut ext_dict: HashMap<Vec<u8>, Value> = HashMap::new();
        ext_dict.insert(b"e".to_vec(), Value::Int(0)); // 本地端口（占位）
        ext_dict.insert(b"v".to_vec(), Value::Bytes(b"spde 0.1.0".to_vec()));
        ext_dict.insert(b"reqq".to_vec(), Value::Int(250));

        let mut m_dict: HashMap<Vec<u8>, Value> = HashMap::new();
        m_dict.insert(b"ut_metadata".to_vec(), Value::Int(1));
        m_dict.insert(b"ut_pex".to_vec(), Value::Int(2));
        ext_dict.insert(b"m".to_vec(), Value::Dict(m_dict));

        let payload = serde_bencode::to_bytes(&Value::Dict(ext_dict))?;

        self.send_extended(EXTENSION_HANDSHAKE, &payload)?;

        // 接收 Extension 握手响应
        let (ext_id, payload) = self.read_extended()?;
        if ext_id != EXTENSION_HANDSHAKE {
            return Err(anyhow::anyhow!("期望 Extension 握手，收到: {}", ext_id));
        }

        // 解析对端的 Extension 映射
        let value: Value = serde_bencode::from_bytes(&payload)
            .map_err(|e| anyhow::anyhow!("解析 Extension 握手失败: {}", e))?;
        let dict = match value {
            Value::Dict(d) => d,
            _ => return Err(anyhow::anyhow!("Extension 握手不是字典")),
        };

        // 解析 metadata_size
        if let Some(Value::Int(size)) = dict.get(b"metadata_size".as_ref()) {
            self.metadata_size = Some(*size as usize);
        }

        // 解析 m 字典（Extension 映射）
        if let Some(Value::Dict(m)) = dict.get(b"m".as_ref()) {
            for (key, value) in m {
                if let Value::Int(id) = value {
                    let name = String::from_utf8_lossy(key).to_string();
                    self.extension_map.insert(name, *id as u8);
                }
            }
        }

        // 获取对端 ut_metadata ID
        self.remote_ut_metadata_id = self.extension_map.get("ut_metadata").copied();

        debug!(
            "[bt] Extension 握手完成, metadata_size={:?}, ut_metadata_id={:?}",
            self.metadata_size, self.remote_ut_metadata_id
        );
        Ok(())
    }

    /// 发送 Extension 消息
    pub fn send_extended(&mut self, ext_id: u8, payload: &[u8]) -> Result<()> {
        let mut msg = Vec::with_capacity(payload.len() + 5);
        // 消息长度（4 字节大端）= 1（消息ID） + 1（ext_id） + payload
        let length = 2 + payload.len() as u32;
        msg.extend_from_slice(&length.to_be_bytes());
        msg.push(EXTENDED_MESSAGE_ID);
        msg.push(ext_id);
        msg.extend_from_slice(payload);
        self.stream
            .write_all(&msg)
            .context("发送 Extension 消息失败")?;
        Ok(())
    }

    /// 读取 Extension 消息
    pub fn read_extended(&mut self) -> Result<(u8, Vec<u8>)> {
        // 读取消息长度
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let length = u32::from_be_bytes(len_buf) as usize;

        if length == 0 {
            // Keep-alive
            return self.read_extended();
        }

        let mut msg = vec![0u8; length];
        self.stream.read_exact(&mut msg)?;

        let msg_id = msg[0];
        if msg_id != EXTENDED_MESSAGE_ID {
            return Err(anyhow::anyhow!("期望 Extension 消息，收到: {}", msg_id));
        }

        let ext_id = msg[1];
        let payload = msg[2..].to_vec();

        Ok((ext_id, payload))
    }

    /// 读取普通 BT 消息（返回消息 ID 和 payload）
    pub fn read_message(&mut self) -> Result<(u8, Vec<u8>)> {
        loop {
            let mut len_buf = [0u8; 4];
            self.stream.read_exact(&mut len_buf)?;
            let length = u32::from_be_bytes(len_buf) as usize;

            if length == 0 {
                // Keep-alive，继续
                continue;
            }

            let mut msg = vec![0u8; length];
            self.stream.read_exact(&mut msg)?;

            let msg_id = msg[0];
            let payload = msg[1..].to_vec();
            return Ok((msg_id, payload));
        }
    }

    /// 发送 PEX 消息（S4）
    pub fn send_pex(&mut self, peer_addr: std::net::SocketAddr) -> Result<()> {
        let peer_id = self
            .remote_peer_id
            .map(|id| hex::encode(&id[..8]))
            .unwrap_or_else(|| "unknown".to_string());

        if let Some(msg) = self.pex.build_message(&peer_id, peer_addr) {
            let bytes = msg.to_bytes()?;
            let ut_pex_id = self.extension_map.get("ut_pex").copied().unwrap_or(2);
            self.send_extended(ut_pex_id, &bytes)?;
            trace!("[bt] 发送 PEX: {} 个 peer", msg.added.len());
        }
        Ok(())
    }

    /// 处理收到的 PEX 消息（S4）
    pub fn handle_pex(&mut self, payload: &[u8]) -> Result<Vec<std::net::SocketAddr>> {
        let msg = PexMessage::from_bytes(payload)?;
        let peer_id = self
            .remote_peer_id
            .map(|id| hex::encode(&id[..8]))
            .unwrap_or_else(|| "unknown".to_string());
        let new_peers = self.pex.handle_message(&peer_id, &msg);
        Ok(new_peers)
    }

    /// 获取 metadata 大小
    pub fn metadata_size(&self) -> Option<usize> {
        self.metadata_size
    }

    /// 获取对端 ut_metadata 消息 ID
    pub fn remote_ut_metadata_id(&self) -> Option<u8> {
        self.remote_ut_metadata_id
    }

    /// 获取 PEX 管理器引用
    pub fn pex(&self) -> &PexManager {
        &self.pex
    }

    /// 获取对端 peer ID
    pub fn remote_peer_id(&self) -> Option<[u8; 20]> {
        self.remote_peer_id
    }
}

/// 生成本端 peer ID
fn generate_peer_id() -> [u8; 20] {
    let mut peer_id = [0u8; 20];
    peer_id[..PEER_ID_PREFIX.len()].copy_from_slice(PEER_ID_PREFIX);
    use rand::Rng;
    let mut rng = rand::thread_rng();
    for byte in &mut peer_id[PEER_ID_PREFIX.len()..] {
        *byte = rng.gen();
    }
    peer_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_peer_id() {
        let id1 = generate_peer_id();
        let id2 = generate_peer_id();
        assert_eq!(&id1[..8], PEER_ID_PREFIX);
        assert_eq!(&id2[..8], PEER_ID_PREFIX);
        assert_ne!(id1, id2); // 随机部分应该不同
    }

    #[test]
    fn test_reserved_bytes() {
        // byte[5] 应该设置 0x10 位（Extension 协议）
        assert_eq!(RESERVED_BYTES[5] & 0x10, 0x10);
    }

    #[test]
    fn test_bt_protocol_constant() {
        assert_eq!(BT_PROTOCOL, b"BitTorrent protocol");
        assert_eq!(BT_PROTOCOL.len(), 19);
    }
}
