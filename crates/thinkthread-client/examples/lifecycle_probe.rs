//! Manual native lifecycle probe; run against a pinned, isolated deployment.
#[path = "support/lifecycle.rs"]
mod lifecycle;

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;
use xgovernor_thinkthread_client::{sdk, ManagementConfig};

fn configuration(
    value: Value,
) -> Result<(ManagementConfig, sdk::ThinkThreadCreateRequest, u64), ()> {
    let context: sdk::ClientContext =
        serde_json::from_value(value["context"].clone()).map_err(|_| ())?;
    let host = value["host_executable"].as_str().ok_or(())?;
    let timeout = value["wait_timeout_ms"]
        .as_u64()
        .filter(|n| (1..=300_000).contains(n))
        .ok_or(())?;
    let mut environment = BTreeMap::new();
    for key in &context.environment_keys {
        environment.insert(key.clone(), std::env::var(key).map_err(|_| ())?);
    }
    let config = ManagementConfig {
        host_executable: PathBuf::from(host),
        state_root: context.state_root,
        runtime_root: context.runtime_root,
        config_root: context.config_root,
        cache_root: context.cache_root,
        working_directory: context.working_directory,
        fs_root: context.fs_root,
        home_directory: context.home_directory.ok_or(())?,
        executable_search_path: context.executable_search_path,
        base_environment: environment,
        connect_timeout: Duration::from_secs(10),
    };
    config.validate().map_err(|_| ())?;
    let create: sdk::ThinkThreadCreateRequest =
        serde_json::from_value(value["create"].clone()).map_err(|_| ())?;
    if create.fs.as_ref() != Some(&config.fs_root) {
        return Err(());
    }
    Ok((config, create, timeout))
}

#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 3 {
        eprintln!("usage: lifecycle_probe <protected-config.json> <new-journal.ndjson>");
        std::process::exit(2);
    }
    let input = std::fs::read(&args[1])
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    let Some((config, create, budget)) = input.and_then(|value| configuration(value).ok()) else {
        eprintln!("invalid or unavailable explicit deployment configuration");
        std::process::exit(2);
    };
    let mut output = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&args[2])
    {
        Ok(output) => output,
        Err(_) => {
            eprintln!("journal must be a new writable file");
            std::process::exit(2);
        }
    };
    let mut persist = |report: &Value| {
        let bytes = serde_json::to_vec(report).map_err(|_| "journal_encoding")?;
        // No native reply text, create payload or environment values are projected.
        // Refuse an unexpected credential projection rather than persist it.
        let text = std::str::from_utf8(&bytes).map_err(|_| "journal_encoding")?;
        for (key, value) in &config.base_environment {
            let key = key.to_ascii_uppercase();
            if (key.contains("KEY") || key.contains("TOKEN") || key.contains("SECRET"))
                && !value.is_empty()
                && text.contains(value)
            {
                return Err("journal_credential_projection");
            }
        }
        output
            .write_all(&bytes)
            .and_then(|_| output.write_all(b"\n"))
            .and_then(|_| output.sync_all())
            .map_err(|_| "journal_write")
    };
    let report = lifecycle::bounded_probe(&config, create, budget, &mut persist).await;
    if persist(&report).is_err() {
        eprintln!("evidence journal unavailable; inspect prior operation IDs");
        std::process::exit(1);
    }
    let passed = report["status"] == "passed_lifecycle_subset";
    println!(
        "Lifecycle subset {}; checkpoint and runtime readiness are not validated",
        if passed {
            "passed"
        } else {
            "failed or blocked"
        }
    );
    std::process::exit(if passed { 0 } else { 1 });
}
