# SPDE (Super‑Download‑Engine)
PandaNetPL 生态统一下载中心，单点Node实现。

> 当前版本：v0.1.0
> 目标平台：Linux x86_64‑musl 静态单二进制
> Docker / Windows / macOS / HTTP‑Serve / 分布式 暂未实现

## 特性
‑ 单静态二进制，复制即可运行
‑ `‑‑base‑dir` 指定节点根目录，所有数据隔离
‑ 启动自动目录自检，缺失目录/配置自动生成最小模板
‑ `node‑id`：节点永久唯一标识
‑ `instance‑id`：每次进程启动生成，标记进程生命周期
‑ run‑history.jsonl 真相源，只追加不修改历史记录
‑ 事件：instance_start / task_run / instance_exit
‑ SIGINT/SIGTERM 捕获，尽可能写入退出事件
‑ CLI子命令：config / stats / list‑instances / inspect‑instance / serve(占位)

## 编译
```bash
rustup target add x86_64‑unknown‑linux‑musl
cargo build --release --target x86_64‑unknown‑linux‑musl
