//! BitTorrent Metadata 下载（BEP 9 + BEP 10）
//!
//! 实现 BT 协议握手、Extension 协议协商、ut_metadata 扩展下载。
//! 用于从 peer 获取种子元数据（info 字典），无需下载 .torrent 文件。
//!
//! ## 协议流程
//! 1. TCP 连接到 peer
//! 2. 发送 BT 握手（协议名 + 8字节保留位 + infohash + peer_id）
//! 3. 收到握手响应后，发送 Extension 握手（BEP 10）
//! 4. 解析对方支持的扩展，确认 ut_metadata
//! 5. 发送 metadata 请求（ut_metadata request）
//! 6. 接收 metadata data 消息，拼接完整 metadata
//! 7. 验证 infohash，返回 MetadataInfo

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde_bencode::from_bytes;
use serde_bencode::value::Value as BencodeValue;
use sha1::{Digest, Sha1};
use tracing::{debug, info, warn};

use pandanetos::bittorrent::{Infohash, MetadataInfo};

/// BT 协议标识
const BT_PROTOCOL: &[u8] = b"BitTorrent protocol";

/// Extension 协议保留位（第 20 位，即字节 5 的 bit 0x10）
const EXTENSION_PROTOCOL_BIT: u8 = 0x10;

/// ut_metadata 扩展消息 ID
const UT_METADATA_ID: u8 = 3;

/// ut_metadata 消息类型
const UT_METADATA_REQUEST: i64 = 0;
const UT_METADATA_DATA: i64 = 1;
const UT_METADATA_REJECT: i64 = 2;

/// Metadata 分片大小（16KB）
const METADATA_PIECE_SIZE: usize = 16 * 1024;

/// 默认 peer_id 前缀（SPDE 标识）
const PEER_ID_PREFIX: &[u8] = b"-SP0001-";

/// Metadata 下载器
pub struct MetadataDownloader {
    /// 连接超时
    connect_timeout: Duration,
    /// 读取超时
    read_timeout: Duration,
    /// 本地 peer_id
    peer_id: [u8; 20],
}

impl MetadataDownloader {
    /// 创建新的 Metadata 下载器
    pub fn new() -> Self {
        let mut peer_id = [0u8; 20];
        peer_id[..PEER_ID_PREFIX.len()].copy_from_slice(PEER_ID_PREFIX);
        // 剩余字节随机
        use rand::Rng;
        let mut rng = rand::thread_rng();
        for byte in peer_id.iter_mut().skip(PEER_ID_PREFIX.len()) {
            *byte = rng.gen();
        }

        MetadataDownloader {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(15),
            peer_id,
        }
    }

    /// 从单个 peer 下载 metadata
    pub fn download_from_peer(&self, infohash: Infohash, peer_addr: &str) -> Result<MetadataInfo> {
        info!("[metadata] 从 {} 下载 infohash={}", peer_addr, infohash);

        // 1. 建立 TCP 连接
        let mut stream = TcpStream::connect_timeout(&peer_addr.parse()?, self.connect_timeout)
            .with_context(|| format!("连接 peer {} 失败", peer_addr))?;
        stream.set_read_timeout(Some(self.read_timeout))?;
        stream.set_write_timeout(Some(self.read_timeout))?;

        // 2. BT 握手
        self.handshake(&mut stream, &infohash)?;

        // 3. Extension 握手
        let (ut_metadata_id, metadata_size) = self.extension_handshake(&mut stream)?;
        debug!(
            "[metadata] Extension 握手成功: ut_metadata_id={}, metadata_size={}",
            ut_metadata_id, metadata_size
        );

        if metadata_size == 0 {
            return Err(anyhow!("peer 未提供 metadata 大小"));
        }

        // 4. 下载所有 metadata 分片
        let num_pieces = (metadata_size + METADATA_PIECE_SIZE - 1) / METADATA_PIECE_SIZE;
        let mut metadata = vec![0u8; metadata_size];

        for piece in 0..num_pieces {
            let offset = piece * METADATA_PIECE_SIZE;
            let piece_size = (metadata_size - offset).min(METADATA_PIECE_SIZE);

            // 发送 request
            self.send_ut_metadata_request(&mut stream, ut_metadata_id, piece)?;

            // 接收 data
            let (received_piece, data) = self.receive_ut_metadata_data(&mut stream)?;
            if received_piece != piece as i64 {
                return Err(anyhow!(
                    "metadata 分片不匹配: 期望 {}, 收到 {}",
                    piece,
                    received_piece
                ));
            }

            metadata[offset..offset + piece_size].copy_from_slice(&data[..piece_size]);
            debug!(
                "[metadata] 分片 {}/{} 下载完成 ({} bytes)",
                piece + 1,
                num_pieces,
                piece_size
            );
        }

        // 5. 验证 infohash
        let mut hasher = Sha1::new();
        hasher.update(&metadata);
        let computed_hash = hasher.finalize();
        if computed_hash.as_slice() != infohash.as_bytes() {
            return Err(anyhow!("metadata infohash 验证失败"));
        }

        // 6. 解析 metadata
        let info = Self::parse_metadata(&metadata, infohash)?;

        info!(
            "[metadata] 下载完成: name={}, size={}",
            info.name, info.total_length
        );
        Ok(info)
    }

    /// 从多个 peer 下载（尝试直到成功）
    pub fn download_from_peers(
        &self,
        infohash: Infohash,
        peers: &[String],
    ) -> Result<MetadataInfo> {
        let mut last_error = None;
        for peer in peers {
            match self.download_from_peer(infohash, peer) {
                Ok(meta) => return Ok(meta),
                Err(e) => {
                    warn!("[metadata] 从 {} 下载失败: {}", peer, e);
                    last_error = Some(e);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("所有 peer 下载失败")))
    }

    /// BT 握手
    fn handshake(&self, stream: &mut TcpStream, infohash: &Infohash) -> Result<()> {
        // 构造握手消息
        let mut handshake = Vec::with_capacity(68);
        handshake.push(BT_PROTOCOL.len() as u8); // 19
        handshake.extend_from_slice(BT_PROTOCOL);
        // 8 字节保留位，设置 Extension 协议位
        let mut reserved = [0u8; 8];
        reserved[5] |= EXTENSION_PROTOCOL_BIT;
        handshake.extend_from_slice(&reserved);
        handshake.extend_from_slice(infohash.as_bytes());
        handshake.extend_from_slice(&self.peer_id);

        // 发送握手
        stream.write_all(&handshake)?;
        stream.flush()?;

        // 接收握手响应
        let mut resp = [0u8; 68];
        stream.read_exact(&mut resp)?;

        // 验证协议名长度
        if resp[0] as usize != BT_PROTOCOL.len() {
            return Err(anyhow!("无效的 BT 握手响应: 协议名长度不匹配"));
        }

        // 验证 infohash
        if &resp[28..48] != infohash.as_bytes() {
            return Err(anyhow!("BT 握手响应 infohash 不匹配"));
        }

        // 检查是否支持 Extension 协议
        if resp[25] & EXTENSION_PROTOCOL_BIT == 0 {
            return Err(anyhow!("peer 不支持 Extension 协议"));
        }

        debug!("[metadata] BT 握手成功");
        Ok(())
    }

    /// Extension 握手（BEP 10）
    fn extension_handshake(&self, stream: &mut TcpStream) -> Result<(u8, usize)> {
        // 构造 Extension 握手消息
        // { m: { ut_metadata: 3 }, metadata_size: <size> }
        let ext_handshake = b"d1:md11:ut_metadatai3ee13:metadata_sizei0e1:pi6881e4:v4:spdee";

        // 发送 Extension 消息（消息类型 0 = handshake）
        self.send_extension_message(stream, 0, ext_handshake)?;

        // 接收响应
        let (msg_type, payload) = self.receive_extension_message(stream)?;
        if msg_type != 0 {
            return Err(anyhow!("期望 Extension 握手响应，收到类型 {}", msg_type));
        }

        // 解析响应
        let value: BencodeValue = from_bytes(&payload)?;
        let dict = value
            .as_dict()
            .ok_or_else(|| anyhow!("无效的 Extension 握手响应"))?;

        // 获取 ut_metadata 的消息 ID
        let m = dict
            .get(b"m".as_slice())
            .and_then(|v| v.as_dict())
            .ok_or_else(|| anyhow!("响应中无 m 字典"))?;

        let ut_metadata_id = m
            .get(b"ut_metadata".as_slice())
            .and_then(|v| v.as_int())
            .ok_or_else(|| anyhow!("peer 不支持 ut_metadata"))? as u8;

        // 获取 metadata 大小
        let metadata_size = dict
            .get(b"metadata_size".as_slice())
            .and_then(|v| v.as_int())
            .unwrap_or(0) as usize;

        Ok((ut_metadata_id, metadata_size))
    }

    /// 发送 ut_metadata request
    fn send_ut_metadata_request(
        &self,
        stream: &mut TcpStream,
        ut_metadata_id: u8,
        piece: usize,
    ) -> Result<()> {
        let msg = format!("d8:msg_typei0e5:piecei{}e", piece);
        self.send_extension_message(stream, ut_metadata_id, msg.as_bytes())
    }

    /// 接收 ut_metadata data
    fn receive_ut_metadata_data(&self, stream: &mut TcpStream) -> Result<(i64, Vec<u8>)> {
        let (msg_type, payload) = self.receive_extension_message(stream)?;

        // 解析消息头（bencode 字典）和数据
        // 格式: d8:msg_typei1e5:piecei0e<data>
        // 需要找到字典结束的位置
        let dict_end = Self::find_bencode_dict_end(&payload)
            .ok_or_else(|| anyhow!("无效的 ut_metadata 消息"))?;

        let header_bytes = &payload[..dict_end];
        let data = payload[dict_end..].to_vec();

        let value: BencodeValue = from_bytes(header_bytes)?;
        let dict = value
            .as_dict()
            .ok_or_else(|| anyhow!("无效的 ut_metadata 消息头"))?;

        let msg_type_val = dict
            .get(b"msg_type".as_slice())
            .and_then(|v| v.as_int())
            .ok_or_else(|| anyhow!("无 msg_type"))?;

        if msg_type_val == UT_METADATA_REJECT {
            return Err(anyhow!("peer 拒绝了 metadata 请求"));
        }

        if msg_type_val != UT_METADATA_DATA {
            return Err(anyhow!("期望 data 消息，收到类型 {}", msg_type_val));
        }

        let piece = dict
            .get(b"piece".as_slice())
            .and_then(|v| v.as_int())
            .ok_or_else(|| anyhow!("无 piece 字段"))?;

        // 忽略 msg_type 参数（与外层重复）
        let _ = msg_type;

        Ok((piece, data))
    }

    /// 发送 Extension 消息
    fn send_extension_message(
        &self,
        stream: &mut TcpStream,
        ext_id: u8,
        payload: &[u8],
    ) -> Result<()> {
        // BT 消息格式: <4字节长度><1字节消息类型=20><1字节扩展ID><payload>
        let msg_len = 2 + payload.len(); // 1字节类型 + 1字节扩展ID + payload
        let mut buf = Vec::with_capacity(4 + msg_len);
        buf.extend_from_slice(&(msg_len as u32).to_be_bytes());
        buf.push(20); // BT_EXTENSION
        buf.push(ext_id);
        buf.extend_from_slice(payload);

        stream.write_all(&buf)?;
        stream.flush()?;
        Ok(())
    }

    /// 接收 Extension 消息
    fn receive_extension_message(&self, stream: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
        // 读取 4 字节长度
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        if msg_len == 0 {
            return Err(anyhow!("收到 keep-alive 消息"));
        }

        // 读取消息体
        let mut msg_buf = vec![0u8; msg_len];
        stream.read_exact(&mut msg_buf)?;

        // 第一个字节是 BT 消息类型（应为 20 = Extension）
        let bt_msg_type = msg_buf[0];
        if bt_msg_type != 20 {
            return Err(anyhow!("期望 Extension 消息(20)，收到 {}", bt_msg_type));
        }

        // 第二个字节是扩展 ID
        let ext_id = msg_buf[1];
        // 剩余是 payload
        let payload = msg_buf[2..].to_vec();

        Ok((ext_id, payload))
    }

    /// 找到 bencode 字典的结束位置（用于分离 ut_metadata 的 header 和 data）
    fn find_bencode_dict_end(data: &[u8]) -> Option<usize> {
        if data.is_empty() || data[0] != b'd' {
            return None;
        }

        let mut depth = 0;
        let mut i = 0;
        while i < data.len() {
            match data[i] {
                b'd' | b'l' => {
                    depth += 1;
                    i += 1;
                }
                b'e' => {
                    depth -= 1;
                    i += 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                b'i' => {
                    // 整数: i<number>e
                    i += 1;
                    while i < data.len() && data[i] != b'e' {
                        i += 1;
                    }
                    i += 1; // skip 'e'
                }
                b'0'..=b'9' => {
                    // 字符串: <length>:<data>
                    let mut len_str = String::new();
                    while i < data.len() && data[i] != b':' {
                        len_str.push(data[i] as char);
                        i += 1;
                    }
                    i += 1; // skip ':'
                    let len: usize = len_str.parse().ok()?;
                    i += len;
                }
                _ => return None,
            }
        }
        None
    }

    /// 解析 metadata（info 字典）为 MetadataInfo
    fn parse_metadata(data: &[u8], infohash: Infohash) -> Result<MetadataInfo> {
        let value: BencodeValue = from_bytes(data)?;
        let dict = value.as_dict().ok_or_else(|| anyhow!("无效的 info 字典"))?;

        let name = dict
            .get(b"name".as_slice())
            .and_then(|v| v.as_bytes())
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default();

        let piece_length = dict
            .get(b"piece length".as_slice())
            .and_then(|v| v.as_int())
            .unwrap_or(0) as u64;

        // 计算总大小和文件列表
        let (total_length, files) =
            if let Some(file_list) = dict.get(b"files".as_slice()).and_then(|v| v.as_list()) {
                // 多文件模式
                let mut total = 0u64;
                let mut file_infos = vec![];
                for file in file_list {
                    if let Some(fdict) = file.as_dict() {
                        let length = fdict
                            .get(b"length".as_slice())
                            .and_then(|v| v.as_int())
                            .unwrap_or(0) as u64;
                        let path = fdict
                            .get(b"path".as_slice())
                            .and_then(|v| v.as_list())
                            .map(|parts| {
                                parts
                                    .iter()
                                    .filter_map(|p| p.as_bytes())
                                    .map(|b| String::from_utf8_lossy(b).to_string())
                                    .collect::<Vec<_>>()
                                    .join("/")
                            })
                            .unwrap_or_default();
                        total += length;
                        file_infos.push(pandanetos::bittorrent::FileInfo { path, length });
                    }
                }
                (total, file_infos)
            } else {
                // 单文件模式
                let length = dict
                    .get(b"length".as_slice())
                    .and_then(|v| v.as_int())
                    .unwrap_or(0) as u64;
                (
                    length,
                    vec![pandanetos::bittorrent::FileInfo {
                        path: name.clone(),
                        length,
                    }],
                )
            };

        let piece_count = if piece_length > 0 {
            ((total_length + piece_length - 1) / piece_length) as u32
        } else {
            0
        };

        let private = dict
            .get(b"private".as_slice())
            .and_then(|v| v.as_int())
            .map(|v| v == 1)
            .unwrap_or(false);

        Ok(MetadataInfo {
            infohash,
            name,
            total_length,
            piece_length,
            piece_count,
            files,
            created_by: None,
            creation_date: None,
            comment: None,
            private,
            trackers: vec![],
            fetched_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        })
    }
}

impl Default for MetadataDownloader {
    fn default() -> Self {
        Self::new()
    }
}

// BencodeValue 扩展 trait
trait BencodeExt {
    fn as_dict(&self) -> Option<&std::collections::HashMap<Vec<u8>, BencodeValue>>;
    fn as_list(&self) -> Option<&Vec<BencodeValue>>;
    fn as_bytes(&self) -> Option<&Vec<u8>>;
    fn as_int(&self) -> Option<i64>;
}

impl BencodeExt for BencodeValue {
    fn as_dict(&self) -> Option<&std::collections::HashMap<Vec<u8>, BencodeValue>> {
        match self {
            BencodeValue::Dict(d) => Some(d),
            _ => None,
        }
    }
    fn as_list(&self) -> Option<&Vec<BencodeValue>> {
        match self {
            BencodeValue::List(l) => Some(l),
            _ => None,
        }
    }
    fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            BencodeValue::Bytes(b) => Some(b),
            _ => None,
        }
    }
    fn as_int(&self) -> Option<i64> {
        match self {
            BencodeValue::Int(i) => Some(*i),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_bencode_dict_end() {
        // 简单字典
        let data = b"d3:key5:valuee";
        assert_eq!(
            MetadataDownloader::find_bencode_dict_end(data),
            Some(data.len())
        );

        // 嵌套字典
        let data = b"d1:ad3:keyi1eee";
        assert_eq!(
            MetadataDownloader::find_bencode_dict_end(data),
            Some(data.len())
        );

        // 字典后有数据
        let data = b"d3:key5:valueeEXTRADATA";
        assert_eq!(MetadataDownloader::find_bencode_dict_end(data), Some(14));

        // 非字典开头
        let data = b"i123e";
        assert_eq!(MetadataDownloader::find_bencode_dict_end(data), None);
    }

    #[test]
    fn test_peer_id_prefix() {
        let downloader = MetadataDownloader::new();
        assert_eq!(&downloader.peer_id[..8], PEER_ID_PREFIX);
    }

    #[test]
    fn test_parse_metadata_single_file() {
        // 构造一个简单的 info 字典
        let info =
            b"d6:lengthi1024e4:name8:test.txt12:piece lengthi256e6:pieces20:01234567890123456789e";
        let ih = Infohash::new([0u8; 20]);
        let meta = MetadataDownloader::parse_metadata(info, ih).unwrap();

        assert_eq!(meta.name, "test.txt");
        assert_eq!(meta.total_length, 1024);
        assert_eq!(meta.piece_length, 256);
        assert_eq!(meta.piece_count, 4);
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.files[0].path, "test.txt");
        assert_eq!(meta.files[0].length, 1024);
    }

    #[test]
    fn test_parse_metadata_multi_file() {
        let info = b"d5:filesld6:lengthi100e4:pathl1:aeed6:lengthi200e4:pathl1:beee4:name3:dir12:piece lengthi256e6:pieces20:01234567890123456789e";
        let ih = Infohash::new([0u8; 20]);
        let meta = MetadataDownloader::parse_metadata(info, ih).unwrap();

        assert_eq!(meta.name, "dir");
        assert_eq!(meta.total_length, 300);
        assert_eq!(meta.files.len(), 2);
    }
}
