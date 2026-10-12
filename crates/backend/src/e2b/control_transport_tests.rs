//! Exercise detached start/cleanup against a loopback envd transport with real processes.
use super::backend::{E2bBackendState, E2bLifecycle};
use super::exec::{start_process, E2bStartProcess};
use crate::execution::OPERATION_CONTEXT;
use base64::Engine;
use operation_protocol::{BackendPath, OperationContext};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

fn frame(value: Value) -> Vec<u8> {
    let body = serde_json::to_vec(&value).unwrap();
    let mut output = vec![0];
    output.extend((body.len() as u32).to_be_bytes());
    output.extend(body);
    output
}
async fn request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = Vec::new();
    let (end, length) = loop {
        let mut chunk = [0; 1024];
        let count = socket.read(&mut chunk).await.unwrap();
        assert!(count > 0);
        bytes.extend(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&bytes[..end]);
            let length = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .and_then(|value| value.parse::<usize>().ok())
                })
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < end + length {
        let mut chunk = [0; 1024];
        let count = socket.read(&mut chunk).await.unwrap();
        assert!(count > 0);
        bytes.extend(&chunk[..count]);
    }
    let header = String::from_utf8_lossy(&bytes[..end]);
    let path = header
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .to_string();
    let body = &bytes[end..end + length];
    let body = if path.ends_with("/Start") {
        &body[5..]
    } else {
        body
    };
    (path, serde_json::from_slice(body).unwrap())
}
async fn endpoint(
    lost_stream: bool,
) -> (
    Arc<E2bBackendState>,
    Arc<Notify>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let started = Arc::new(Notify::new());
    let processes = Arc::new(Mutex::new(HashMap::<String, u32>::new()));
    let worker = tokio::spawn({
        let started = started.clone();
        async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let processes = processes.clone();
                let started = started.clone();
                tokio::spawn(async move {
                    let (path, body) = request(&mut socket).await;
                    if path.ends_with("/SendSignal") {
                        let tag = body["process"]["tag"].as_str().unwrap();
                        let pid = *processes.lock().unwrap().get(tag).unwrap();
                        assert_eq!(body["signal"], "SIGNAL_SIGTERM");
                        unsafe {
                            libc::kill(pid as i32, libc::SIGTERM);
                        }
                        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
                        return;
                    }
                    assert!(path.ends_with("/Start"));
                    started.notify_one();
                    // Cancellation must tolerate a Start which has not returned a PID yet.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let tag = body["tag"].as_str().unwrap().to_owned();
                    let args: Vec<_> = body["process"]["args"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|value| value.as_str().unwrap())
                        .collect();
                    let mut child =
                        tokio::process::Command::new(body["process"]["cmd"].as_str().unwrap())
                            .args(&args)
                            .stdout(std::process::Stdio::piped())
                            .stderr(std::process::Stdio::piped())
                            .kill_on_drop(true)
                            .spawn()
                            .unwrap();
                    let pid = child.id().unwrap();
                    processes.lock().unwrap().insert(tag.clone(), pid);
                    let mut stdout = child.stdout.take().unwrap();
                    let mut stderr = child.stderr.take().unwrap();
                    let mut prefix = vec![0; args[3].len() / 2];
                    stdout.read_exact(&mut prefix).await.unwrap();
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/connect+json\r\nConnection: close\r\n\r\n").await.unwrap();
                    socket
                        .write_all(&frame(json!({"event":{"start":{"pid":pid}}})))
                        .await
                        .unwrap();
                    let encoded = base64::engine::general_purpose::STANDARD.encode(prefix);
                    socket
                        .write_all(&frame(json!({"event":{"data":{"stdout":encoded}}})))
                        .await
                        .unwrap();
                    if lost_stream {
                        socket.shutdown().await.unwrap();
                        drop(socket);
                        // Explicitly retire this test-owned supervisor after observing failure.
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        unsafe {
                            libc::kill(pid as i32, libc::SIGTERM);
                        }
                        let status = child.wait().await.unwrap();
                        assert_eq!(status.code(), Some(143));
                        processes.lock().unwrap().remove(&tag);
                        return;
                    }
                    let mut bytes = Vec::new();
                    let mut errors = Vec::new();
                    let (status, _, _) = tokio::join!(
                        child.wait(),
                        stdout.read_to_end(&mut bytes),
                        stderr.read_to_end(&mut errors)
                    );
                    let code = status.unwrap().code().unwrap_or(-1);
                    if !bytes.is_empty() {
                        socket.write_all(&frame(json!({"event":{"data":{"stdout":base64::engine::general_purpose::STANDARD.encode(bytes)}}}))).await.unwrap();
                    }
                    socket
                        .write_all(&frame(json!({"event":{"end":{"exitCode":code}}})))
                        .await
                        .unwrap();
                    processes.lock().unwrap().remove(&tag);
                });
            }
        }
    });
    let http = reqwest::Client::builder()
        .no_proxy()
        .resolve("49983-owned.e2b.test", address)
        .build()
        .unwrap();
    let state = Arc::new_cyclic(|weak| E2bBackendState {
        backend_id: "owned".into(),
        api_base: "http://unused".into(),
        api_key: "test".into(),
        sandbox_id: "owned".into(),
        sandbox_domain: "e2b.test".into(),
        envd_access_token: None,
        envd_port: 49983,
        envd_scheme: "http".into(),
        workspace_root: BackendPath("/tmp".into()),
        home_dir: None,
        temp_root: BackendPath("/tmp".into()),
        default_shell: None,
        username: None,
        envd_file_upload_multipart: false,
        http,
        lifecycle: Mutex::new(E2bLifecycle::Active),
        execution: Default::default(),
        timeout_secs: 3600,
        activity_refresh_throttle: Duration::from_secs(60),
        last_refresh: Mutex::new(Instant::now()),
        self_weak: weak.clone(),
    });
    (state, started, worker)
}
fn operation() -> OperationContext {
    OperationContext {
        turn_id: "turn".into(),
        operation_id: "operation".into(),
    }
}
fn command() -> E2bStartProcess {
    E2bStartProcess {
        cmd: "sleep".into(),
        args: vec!["3".into()],
        env: HashMap::new(),
        cwd: None,
        connect_timeout_ms: None,
        execution_timeout_ms: None,
    }
}
#[tokio::test]
async fn aborted_caller_and_pending_start_are_drained_before_next_turn() {
    let (state, started, endpoint) = endpoint(false).await;
    state.execution.begin_turn("turn").unwrap();
    let caller = tokio::spawn({
        let state = state.clone();
        async move {
            OPERATION_CONTEXT
                .scope(operation(), start_process(&state, command()))
                .await
        }
    });
    started.notified().await;
    caller.abort();
    state.execution.drain("turn", false).await.unwrap();
    assert!(state.execution.begin_turn("next").is_err());
    state.execution.drain("turn", true).await.unwrap();
    state.execution.begin_turn("next").unwrap();
    endpoint.abort();
}
#[tokio::test]
async fn broken_remote_stream_is_not_confirmed_cleanup() {
    let (state, _, endpoint) = endpoint(true).await;
    state.execution.begin_turn("turn").unwrap();
    let _ = OPERATION_CONTEXT
        .scope(operation(), start_process(&state, command()))
        .await;
    assert!(state.execution.drain("turn", true).await.is_err());
    assert!(state.execution.begin_turn("next").is_err());
    tokio::time::sleep(Duration::from_millis(500)).await;
    endpoint.abort();
}
