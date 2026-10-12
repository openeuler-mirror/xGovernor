use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};

const PREFIX: &[u8] = b"\0private-ready\0";
async fn start(argv: &[&str]) -> (Child, BufReader<ChildStdout>) {
    start_with_timeout(argv, 0).await
}
async fn start_with_timeout(argv: &[&str], timeout_ms: u64) -> (Child, BufReader<ChildStdout>) {
    let prefix: String = PREFIX.iter().map(|byte| format!("{byte:02x}")).collect();
    let mut child = Command::new("python3")
        .args([
            "-u",
            "-c",
            include_str!("process_guard.py"),
            &prefix,
            &timeout_ms.to_string(),
        ])
        .args(argv)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut ready = vec![0; PREFIX.len()];
    tokio::time::timeout(Duration::from_secs(5), output.read_exact(&mut ready))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready, PREFIX);
    (child, output)
}
async fn pid(path: &std::path::Path) -> u32 {
    for _ in 0..100 {
        if let Ok(text) = tokio::fs::read_to_string(path).await {
            if let Ok(pid) = text.trim().parse() {
                return pid;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("owned child did not start");
}
async fn stop(child: &mut Child) -> std::process::ExitStatus {
    let result = unsafe { libc::kill(child.id().unwrap() as i32, libc::SIGTERM) };
    assert_eq!(result, 0);
    tokio::time::timeout(Duration::from_secs(8), child.wait())
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn cancellation_reaps_child_and_detached_grandchild_without_touching_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pid");
    let detached = dir.path().join("detached");
    let script = format!("sleep 8 & echo $! > {}; setsid sh -c 'trap \"\" TERM; sleep 8 & echo $! > {}; wait' & wait", path.display(), detached.display());
    let (mut sibling, _) = start(&["sleep", "8"]).await;
    let (mut child, _) = start(&["/bin/sh", "-c", &script]).await;
    let owned = pid(&path).await;
    let escaped = pid(&detached).await;
    assert_eq!(stop(&mut child).await.code(), Some(143));
    assert!(!std::path::Path::new(&format!("/proc/{owned}")).exists());
    assert!(!std::path::Path::new(&format!("/proc/{escaped}")).exists());
    assert!(sibling.try_wait().unwrap().is_none());
    assert_eq!(stop(&mut sibling).await.code(), Some(143));
}
#[tokio::test]
async fn completed_command_keeps_binary_output_stderr_and_exit_code() {
    let script =
        "import os,sys;os.write(1,bytes(range(256)));os.write(2,b'diagnostic');sys.exit(7)";
    let (mut child, mut output) = start(&["python3", "-c", script]).await;
    let mut stdout = Vec::new();
    output.read_to_end(&mut stdout).await.unwrap();
    let mut stderr = Vec::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_end(&mut stderr)
        .await
        .unwrap();
    assert_eq!(child.wait().await.unwrap().code(), Some(7));
    assert_eq!(stdout, (0..=255).collect::<Vec<u8>>());
    assert_eq!(stderr, b"diagnostic");
}
#[tokio::test]
async fn normal_exit_also_reaps_background_children() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pid");
    let script = format!("sleep 8 & echo $! > {}; exit 0", path.display());
    let (mut child, _) = start(&["/bin/sh", "-c", &script]).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(8), child.wait())
            .await
            .unwrap()
            .unwrap()
            .code(),
        Some(0)
    );
    let owned = pid(&path).await;
    assert!(!std::path::Path::new(&format!("/proc/{owned}")).exists());
}
#[tokio::test]
async fn hard_timeout_reaps_term_ignoring_child_and_keeps_timeout_status() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pid");
    let script = format!(
        "sh -c 'trap \"\" TERM; sleep 8' & echo $! > {}; wait",
        path.display()
    );
    let (mut child, _) = start_with_timeout(&["/bin/sh", "-c", &script], 200).await;
    let owned = pid(&path).await;
    let status = tokio::time::timeout(Duration::from_secs(8), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(124));
    assert!(!std::path::Path::new(&format!("/proc/{owned}")).exists());
}
