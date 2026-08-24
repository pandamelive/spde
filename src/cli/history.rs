use crate::lib::model::{EventMeta, SpdeEvent};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum HistoryError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
struct NodeIdStore {
    pub node_id: Uuid,
}

/// 获取或生成永久node_id
pub fn get_or_create_node_id(file_path: &Path) -> Result<Uuid, HistoryError> {
    if file_path.exists() {
        let text = std::fs::read_to_string(file_path)?;
        let store: NodeIdStore = serde_json::from_str(&text)?;
        return Ok(store.node_id);
    }
    let new_id = Uuid::new_v4();
    let store = NodeIdStore { node_id: new_id };
    let json = serde_json::to_string_pretty(&store)?;
    std::fs::write(file_path, json)?;
    Ok(new_id)
}

/// 追加一条事件到jsonl，强制flush落盘
pub fn append_event(file_path: &Path, event: &SpdeEvent) -> Result<(), HistoryError> {
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(file_path)?;
    let line = serde_json::to_string(event)?;
    writeln!(f, "{}", line)?;
    f.flush()?;
    Ok(())
}

/// 读取全部事件，跳过损坏行
pub fn read_all_events(file_path: &Path) -> Result<Vec<SpdeEvent>, HistoryError> {
    let mut res = Vec::new();
    if !file_path.exists() {
        return Ok(res);
    }
    let f = File::open(file_path)?;
    let reader = BufReader::new(f);
    for (line_no, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("warn: read line {} io error {}", line_no, e);
                continue;
            }
        };
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<SpdeEvent>(&line) {
            Ok(ev) => res.push(ev),
            Err(e) => eprintln!("warn: skip invalid json line {}: {}", line_no, e),
        }
    }
    Ok(res)
}

pub fn now_unix_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

pub fn make_meta(node_id: Uuid, instance_id: Uuid) -> EventMeta {
    EventMeta {
        node_id,
        instance_id,
        unix_ts: now_unix_ts(),
    }
}
