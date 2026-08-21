#[tokio::main]
async fn main() {
    if let Err(error) = xgovernor_runtime_xiaoo::worker::run_worker_from_env().await {
        eprintln!("xiaoo worker failed: {error}");
        std::process::exit(1);
    }
}
