mod assembly;

use clap::{Parser, Subcommand};
use orchestration_contract::{
    execution::{CallContext, NetworkPolicy},
    run::*,
};
use orchestration_run_manager::{
    config::{BackendRegistration, Configuration},
    service::Service,
    Result, RunManager,
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "mcts", about = "Run algorithms in durable background workers")]
struct Arguments {
    /// Explicit unified configuration; no legacy environment fallback.
    #[arg(long)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Accept a typed StartRequest JSON file and return immediately.
    Run {
        #[arg(long)]
        request: PathBuf,
    },
    Status {
        run_id: String,
        #[arg(long, default_value_t = 0)]
        after_sequence: u64,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    Cancel {
        run_id: String,
        #[arg(long, default_value = "user requested cancellation")]
        reason: String,
    },
    Result {
        run_id: String,
    },
    List {
        #[arg(long)]
        source_session_id: Option<String>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    Mcp,
    Install {
        #[arg(long)]
        target: String,
        #[arg(long)]
        config_dir: Option<PathBuf>,
        #[arg(long)]
        uninstall: bool,
    },
    #[command(hide = true)]
    Worker {
        run_id: String,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!(
            "{}",
            error
                .downcast_ref::<RunError>()
                .map_or_else(|| error.to_string(), RunError::json)
        );
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Arguments::parse();
    let path = match args.config {
        Some(path) => path,
        None => {
            let home = std::env::var_os("HOME")
                .ok_or("HOME is required to find ~/.config/mcts/config.json; or pass --config")?;
            PathBuf::from(home).join(".config/mcts/config.json")
        }
    };
    let config_path = std::fs::canonicalize(path)?;
    let config = Configuration::load(&config_path)?;
    if let Action::Install {
        target,
        config_dir,
        uninstall,
    } = args.action
    {
        if !matches!(target.as_str(), "opencode" | "xiaoo" | "dsh") {
            return Err("unsupported install target".into());
        }
        let root = config
            .package_directory
            .as_ref()
            .ok_or("package_directory is required for install")?;
        let mut command = std::process::Command::new(&config.python_executable);
        command.arg(root.join("plugins").join(&target).join("install.py"));
        if let Some(directory) = config_dir {
            command.arg("--config-dir").arg(directory);
        }
        if uninstall {
            command.arg("--uninstall");
        }
        if !command.status()?.success() {
            return Err(format!("{target} installation failed").into());
        }
        return Ok(());
    }
    let mut registrations = Vec::new();

    if matches!(args.action, Action::Run { .. } | Action::Mcp) {
        for descriptor in &config.backends {
            if registrations
                .iter()
                .any(|r: &BackendRegistration| r.kind == descriptor.kind)
            {
                return Err("duplicate backend registration".into());
            }
            let probe = tempfile::tempdir()?;
            let provider = assembly::assemble(
                descriptor,
                &config,
                probe.path(),
                CallContext {
                    plugin_id: "admission".into(),
                    run_id: "admission".into(),
                    timeout_ms: 1000,
                },
                NetworkPolicy::Offline,
                false,
            )?;
            registrations.push(BackendRegistration {
                kind: descriptor.kind,
                capabilities: provider.capabilities(),
            });
        }
    }
    let manager = if matches!(
        args.action,
        Action::Status { .. } | Action::Result { .. } | Action::List { .. }
    ) {
        RunManager::open_read_only(&config.runs_dir)?
    } else {
        RunManager::open(&config.runs_dir, registrations)?
    };
    let service = Service {
        manager,
        configuration: config_path,
        executable: std::env::current_exe()?,
    };
    let value = match args.action {
        Action::Run { request } => {
            let mut value: serde_json::Value = serde_json::from_slice(&std::fs::read(request)?)?;
            if let Some(config) = value["config"].as_object_mut() {
                config
                    .entry("backend")
                    .or_insert_with(|| serde_json::json!({"selection":"auto"}));
            }
            let request: StartRequest = serde_json::from_value(value)?;
            serde_json::to_value(service.start_run(request)?)?
        }
        Action::Status {
            run_id,
            after_sequence,
            limit,
        } => serde_json::to_value(service.manager.status_run(StatusRequest {
            run_id,
            after_sequence,
            limit,
        })?)?,
        Action::Cancel { run_id, reason } => serde_json::to_value(
            service
                .manager
                .cancel_run(CancelRequest { run_id, reason })?,
        )?,
        Action::Result { run_id } => {
            serde_json::to_value(service.manager.result_run(ResultRequest { run_id })?)?
        }
        Action::List {
            source_session_id,
            cursor,
            limit,
        } => serde_json::to_value(service.manager.list_runs(ListRequest {
            source_session_id,
            cursor,
            limit,
        })?)?,
        Action::Mcp => {
            xgovernor_mcp::McpServer::new(service)
                .serve(
                    tokio::io::BufReader::new(tokio::io::stdin()),
                    tokio::io::stdout(),
                )
                .await?;
            return Ok(());
        }
        Action::Worker { run_id } => {
            let selected = service
                .manager
                .result_run(ResultRequest {
                    run_id: run_id.clone(),
                })?
                .run
                .backend
                .ok_or("run has no selected backend")?;
            let descriptor = config
                .backends
                .iter()
                .find(|b| b.kind == selected)
                .ok_or("recorded backend is no longer configured")?;
            let request = service.manager.request(&run_id)?;
            let directory = service.manager.run_directory(&run_id)?;
            let provider = assembly::assemble(
                descriptor,
                &config,
                &directory,
                CallContext {
                    plugin_id: request.config.algorithm.namespace.clone(),
                    run_id: run_id.clone(),
                    timeout_ms: 1000,
                },
                request.config.network_policy,
                true,
            )?;
            orchestration_run_manager::worker::execute(
                &service.manager,
                &config,
                provider,
                &run_id,
            )
            .await?;
            return Ok(());
        }
        Action::Install { .. } => unreachable!(),
    };
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}
