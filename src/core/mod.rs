use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 事件元数据：标识来源节点、实例与时间戳
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMeta {
    pub node_id: Uuid,
    pub instance_id: Uuid,
    pub unix_ts: i64,
}

/// 统一下载引擎运行事件，持久化为 jsonl
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpdeEvent {
    pub meta: EventMeta,
    pub event_type: String,
    pub message: String,
}
