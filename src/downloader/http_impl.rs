use super::*;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::Client;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

pub struct HttpDownloader {
    client: Client,
}

impl HttpDownloader {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl DownloadBackend for HttpDownloader {
    fn support_uri(&self, uri: &str) -> bool {
        uri.starts_with("http://") || uri.starts_with("https://")
    }

    async fn run(&self, task: DownloadTask) -> Result<DownloadOutput> {
        let mut output = DownloadOutput {
            total_size: 0,
            downloaded_bytes: 0,
            success_chunks: 0,
            failed_chunks: 0,
            is_success: false,
            error_msg: None,
        };

        let local_size = tokio::fs::metadata(&task.save_path)
            .await
            .map(|meta| meta.len())
            .unwrap_or(0u64);

        let req_builder = self.client.get(&task.uri);
        let resp = if local_size > 0 {
            req_builder
                .header("Range", format!("bytes={}-", local_size))
                .send()
                .await
                .context("http 请求失败")?
        } else {
            req_builder.send().await.context("http 请求失败")?
        };

        if !resp.status().is_success() {
            output.error_msg = Some(format!("HTTP status: {}", resp.status()));
            return Ok(output);
        }

        output.total_size = resp.content_length().unwrap_or(0);

        let mut file = File::options()
            .create(true)
            .write(true)
            .read(true)
            .open(&task.save_path)
            .await
            .context("打开本地文件失败")?;

        file.seek(SeekFrom::Start(local_size)).await?;

        // reqwest0.12 使用 bytes_stream()，不要.stream()
        let mut stream = resp.bytes_stream();
        while let Some(chunk_res) = stream.next().await {
            match chunk_res {
                Ok(chunk) => {
                    file.write_all(&chunk).await?;
                    output.downloaded_bytes += chunk.len() as u64;
                    output.success_chunks += 1;
                }
                Err(e) => {
                    output.failed_chunks += 1;
                    output.error_msg = Some(e.to_string());
                    break;
                }
            }
        }

        file.flush().await?;
        output.is_success = output.error_msg.is_none();
        Ok(output)
    }

    async fn stop(&self, _task_id: &str) -> Result<()> {
        Ok(())
    }
}
