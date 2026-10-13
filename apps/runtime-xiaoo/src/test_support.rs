use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn mock(
    status: u16,
    content_type: &str,
    body: String,
) -> (String, tokio::task::JoinHandle<(String, Value)>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let content_type = content_type.to_string();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0; 4096];
        let (headers, body_start) = loop {
            let n = socket.read(&mut buf).await.unwrap();
            assert_ne!(n, 0);
            request.extend_from_slice(&buf[..n]);
            if let Some(end) = request.windows(4).position(|v| v == b"\r\n\r\n") {
                break (String::from_utf8(request[..end].to_vec()).unwrap(), end + 4);
            }
        };
        let length: usize = headers
            .lines()
            .find_map(|line| {
                line.to_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().parse().unwrap())
            })
            .unwrap();
        while request.len() - body_start < length {
            let n = socket.read(&mut buf).await.unwrap();
            assert_ne!(n, 0);
            request.extend_from_slice(&buf[..n]);
        }
        let value = serde_json::from_slice(&request[body_start..body_start + length]).unwrap();
        let reply=format!("HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
        socket.write_all(reply.as_bytes()).await.unwrap();
        (headers, value)
    });
    (format!("http://{address}/v1"), task)
}
