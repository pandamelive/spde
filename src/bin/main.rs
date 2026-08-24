use clap::Parser;
use spde::cli::config::load_config;
use spde::cli::history::{append_event, make_meta, read_all_events, get_or_create_node_id};
use spde::cli::paths::SpdePaths;
use spde::cli::signal::setup_signal_handler;
use spde::lib::model::{SpdeEvent};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about = "SPDE download‑engine‑cli", long_about = None)]
struct Cli {
    #[arg(long)]
    base_dir: std::path::PathBuf,

    #[command(subcommand)]
    cmd: SubCommand,
}

#[derive(clap::Subcommand, Debug)]
enum SubCommand {
    Config,
    Stats,
    ListInstances,
    InspectInstance { instance_id: Uuid },
    Serve,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let paths = SpdePaths::new(&cli.base_dir);
    paths.check_and_prepare()?;

    let node_id = get_or_create_node_id(&paths.node_id_file)?;
    let instance_id = Uuid::new_v4();
    setup_signal_handler();

    // write instance start
    let meta_start = spde::cli::history::make_meta(node_id, instance_id);
    append_event(
        &paths.run_history_file,
        &SpdeEvent::InstanceStart {
            meta: meta_start,
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
    )?;

    let ret = run_subcommand(&cli.cmd, &paths, node_id, instance_id);

    // try write exit event
    let meta_exit = spde::cli::history::make_meta(node_id, instance_id);
    append_event(
        &paths.run_history_file,
        &SpdeEvent::InstanceExit {
            meta: meta_exit,
            normal_exit: ret.is_ok(),
        },
    )?;

    ret
}

fn run_subcommand(
    cmd: &SubCommand,
    paths: &SpdePaths,
    _node_id: Uuid,
    _instance_id: Uuid,
) -> anyhow::Result<()> {
    match cmd {
        SubCommand::Config => {
            let _cfg = load_config(&paths.config_file)?;
            println!("config loaded, tasks count: {}", _cfg.tasks.len());
            eprintln!("download kernel logic not implemented yet");
            Ok(())
        }
        SubCommand::Stats => {
            let events = read_all_events(&paths.run_history_file)?;
            println!("total events: {}", events.len());
            Ok(())
        }
        SubCommand::ListInstances => {
            let events = read_all_events(&paths.run_history_file)?;
            for e in events {
                match e {
                    SpdeEvent::InstanceStart { meta, .. } => {
                        println!("START instance_id={} ts={}", meta.instance_id, meta.unix_ts)
                    }
                    SpdeEvent::InstanceExit { meta, normal_exit, .. } => {
                        println!("EXIT  instance_id={} normal={}", meta.instance_id, normal_exit)
                    }
                    SpdeEvent::TaskRun { .. } => {}
                }
            }
            Ok(())
        }
        SubCommand::InspectInstance { instance_id } => {
            let events = read_all_events(&paths.run_history_file)?;
            for e in events {
                let mid = match &e {
                    SpdeEvent::InstanceStart { meta, .. } => meta.instance_id,
                    SpdeEvent::TaskRun { meta, .. } => meta.instance_id,
                    SpdeEvent::InstanceExit { meta, .. } => meta.instance_id,
                };
                if mid == *instance_id {
                    let s = serde_json::to_string_pretty(&e)?;
                    println!("{}", s);
                }
            }
            Ok(())
        }
        SubCommand::Serve => {
            eprintln!("serve subcommand: HTTP service not implemented (placeholder)");
            Ok(())
        }
    }
}
