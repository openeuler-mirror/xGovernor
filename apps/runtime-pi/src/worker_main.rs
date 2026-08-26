#[tokio::main]
async fn main() {
    if let Err(error) = xgovernor_runtime_pi::run_worker_from_env().await {
        eprintln!("Pi worker failed: {error}");
        std::process::exit(1);
    }
}
