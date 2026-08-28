use serde::{Deserialize, Serialize};
use std::fs;
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
        let text = fs::read_to_string(file_path)?;
        let store: NodeIdStore = serde_json::from_str(&text)?;
        return Ok(store.node_id);
    }
    let new_id = Uuid::new_v4();
    let store = NodeIdStore { node_id: new_id };
    let json = serde_json::to_string_pretty(&store)?;
    fs::write(file_path, json)?;
    Ok(new_id)
}
