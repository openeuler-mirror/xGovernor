use mcts_plugin_host::PluginProcess;
use mcts_protocol::messages::JSONRPC_VERSION;
use mcts_protocol::{MethodName, Request, RpcEnvelope, RpcId};
use serde_json::json;
use tokio::process::Command;

fn initialize_request() -> RpcEnvelope {
    RpcEnvelope::Request(Request {
        jsonrpc: JSONRPC_VERSION.to_string(),
        id: RpcId::String("process-init".to_string()),
        method: MethodName::PluginInitialize,
        params: json!({
            "plugin_id": "mcts",
            "supported_api_versions": ["mcts.rpc.v1"],
            "host_version": "xgovernor-dev",
            "available_capabilities": []
        }),
    })
}

fn stub_command() -> Command {
    let test_binary = std::env::current_exe().expect("current test binary");
    let deps_dir = test_binary.parent().expect("target/deps directory");
    let target_dir = deps_dir.parent().expect("target directory");
    let binary = target_dir.join("stub-plugin");
    assert!(binary.exists(), "stub-plugin binary was not built: {binary:?}");
    Command::new(binary)
}

#[tokio::test]
async fn sends_json_and_reads_response() {
    let command = stub_command();
    let process = PluginProcess::spawn(command, &[]).await.unwrap();

    process.send(&initialize_request()).await.unwrap();
    let message = process.next_message().await.unwrap();
    match message {
        RpcEnvelope::Response(response) => {
            assert_eq!(response.id, RpcId::String("process-init".to_string()));
            assert!(response.result.is_some());
        }
        other => panic!("expected response, got {other:?}"),
    }

    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn eof_returns_error() {
    let mut command = Command::new("sh");
    command.arg("-c").arg("exit 0");
    let process = PluginProcess::spawn(command, &[]).await.unwrap();
    assert!(process.next_message().await.is_err());
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn non_json_stdout_is_rejected() {
    let mut command = Command::new("sh");
    command.arg("-c").arg("echo not-json; sleep 2");
    let process = PluginProcess::spawn(command, &[]).await.unwrap();
    assert!(process.next_message().await.is_err());
    process.shutdown().await.unwrap();
}

#[tokio::test]
async fn two_plugins_do_not_cross_talk() {
    let first = PluginProcess::spawn(stub_command(), &[])
        .await
        .unwrap();
    let second = PluginProcess::spawn(stub_command(), &[])
        .await
        .unwrap();

    let mut first_request = initialize_request();
    if let RpcEnvelope::Request(request) = &mut first_request {
        request.id = RpcId::String("first".to_string());
    }
    let mut second_request = initialize_request();
    if let RpcEnvelope::Request(request) = &mut second_request {
        request.id = RpcId::String("second".to_string());
    }

    first.send(&first_request).await.unwrap();
    second.send(&second_request).await.unwrap();
    let (first_reply, second_reply) = tokio::join!(
        first.next_message(),
        second.next_message()
    );

    let RpcEnvelope::Response(first_reply) = first_reply.unwrap() else {
        panic!("first plugin did not respond");
    };
    let RpcEnvelope::Response(second_reply) = second_reply.unwrap() else {
        panic!("second plugin did not respond");
    };
    assert_eq!(first_reply.id, RpcId::String("first".to_string()));
    assert_eq!(second_reply.id, RpcId::String("second".to_string()));

    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}
