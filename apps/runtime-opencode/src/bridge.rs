use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use backend::execution::OPERATION_CONTEXT;
use futures_util::StreamExt;
use operation_protocol::{
    capability::{
        exec::ExecRequest,
        filesystem::{ReadBytesRequest, WriteBytesRequest, WriteMode},
        path::{ResolveBase, ResolvePathRequest},
        search::{GlobRequest, GrepMode, GrepRequest},
    },
    BackendPath, OperationBackend, OperationContext, PathKind,
};
use serde_json::{json, Value};
use session_protocol::SessionUsage;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use tokio::sync::{Mutex, RwLock};

pub(crate) const TOOLS: &[&str] = &["shell", "read", "write", "edit", "glob", "grep"];

pub(crate) struct Bridge {
    pub backend: Arc<dyn OperationBackend>,
    pub token: String,
    pub session: RwLock<String>,
    pub turn: RwLock<Option<String>>,
    pub activity: RwLock<()>,
    pub model_activity: Arc<RwLock<()>>,
    pub usage: Mutex<SessionUsage>,
    pub usage_complete: AtomicBool,
    mutations: Mutex<()>,
    pub ready: AtomicBool,
    pub model_key: String,
    control_password: String,
    #[cfg(test)]
    pub model_tools: Mutex<Vec<String>>,
    pub max_requests: AtomicU32,
    pub model_requests: AtomicU32,
    pub limit_reached: AtomicBool,
    pub limit_notify: tokio::sync::Notify,
    pub model_base: String,
    pub client: reqwest::Client,
}

impl Bridge {
    pub fn new(
        backend: Arc<dyn OperationBackend>,
        key: String,
        base: String,
        control_password: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend,
            token: uuid::Uuid::new_v4().to_string(),
            session: RwLock::new(String::new()),
            turn: RwLock::new(None),
            activity: RwLock::new(()),
            model_activity: Arc::new(RwLock::new(())),
            usage: Mutex::new(SessionUsage::default()),
            usage_complete: AtomicBool::new(true),
            mutations: Mutex::new(()),
            ready: AtomicBool::new(false),
            model_key: key,
            control_password,
            #[cfg(test)]
            model_tools: Mutex::new(Vec::new()),
            max_requests: AtomicU32::new(30),
            model_requests: AtomicU32::new(0),
            limit_reached: AtomicBool::new(false),
            limit_notify: tokio::sync::Notify::new(),
            model_base: base,
            client: reqwest::Client::new(),
        })
    }
    pub async fn listen(
        self: &Arc<Self>,
    ) -> std::io::Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let router = Router::new()
            .route("/binding", post(binding))
            .route("/tool", post(tool))
            .route("/ready", post(ready))
            .route("/model/chat/completions", post(model))
            .layer(axum::extract::DefaultBodyLimit::max(8 * 1024 * 1024))
            .with_state(self.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok((url, task))
    }
    pub fn authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get(header::AUTHORIZATION)
            .is_some_and(|v| v.as_bytes() == format!("Bearer {}", self.token).as_bytes())
    }
    pub async fn validate(&self, session: &str, turn: Option<&str>) -> bool {
        session == self.session.read().await.as_str()
            && self
                .turn
                .read()
                .await
                .as_deref()
                .is_some_and(|t| turn.is_none() || turn == Some(t))
    }
    async fn path(&self, raw: &str) -> Result<BackendPath, String> {
        self.backend
            .paths()
            .resolve_path(ResolvePathRequest {
                raw_path: raw.into(),
                base: ResolveBase::WorkspaceRoot,
            })
            .await
            .map_err(|_| "Sandbox path could not be resolved".into())
    }
    async fn execute(&self, name: &str, args: &Value) -> Result<String, String> {
        let text = |k: &str| {
            args[k]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("Missing {k}"))
        };
        let path = args["path"].as_str().unwrap_or(".");
        let result: Result<String, operation_protocol::OperationError> = match name {
            "shell" => {
                if args["background"].as_bool() == Some(true) {
                    return Err("Background commands are disabled".into());
                }
                let cwd = self.path(args["workdir"].as_str().unwrap_or(".")).await?;
                self.backend
                    .exec()
                    .exec(ExecRequest {
                        command: text("command")?,
                        shell: Some("/bin/bash".into()),
                        cwd: Some(cwd),
                        timeout_ms: Some(
                            args["timeout"].as_u64().unwrap_or(60_000).clamp(1, 120_000),
                        ),
                        ..Default::default()
                    })
                    .await
                    .map(|r| {
                        format!(
                            "exit={:?} timeout={}\n{}{}",
                            r.exit_code,
                            r.timed_out,
                            String::from_utf8_lossy(&r.stdout),
                            String::from_utf8_lossy(&r.stderr)
                        )
                    })
            }
            "read" => {
                let path = self.path(&text("path")?).await?;
                if self
                    .backend
                    .files()
                    .stat(&path)
                    .await
                    .map_err(|_| "Sandbox stat failed")?
                    .kind
                    == Some(PathKind::Directory)
                {
                    self.backend
                        .search()
                        .glob(GlobRequest {
                            pattern: "*".into(),
                            base_dir: Some(path),
                            limit: Some(200),
                        })
                        .await
                        .map(|p| p.into_iter().map(|p| p.0).collect::<Vec<_>>().join("\n"))
                } else {
                    self.backend
                        .files()
                        .read_bytes(ReadBytesRequest { path })
                        .await
                        .map(|b| {
                            let content = String::from_utf8_lossy(&b);
                            content
                                .lines()
                                .enumerate()
                                .skip(
                                    args["offset"].as_u64().unwrap_or(1).saturating_sub(1) as usize
                                )
                                .take(args["limit"].as_u64().unwrap_or(2000).min(2000) as usize)
                                .map(|(i, l)| format!("{}: {l}", i + 1))
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                }
            }
            "write" | "edit" => {
                let _mutation = self.mutations.lock().await;
                let path = self.path(&text("path")?).await?;
                let content = if name == "write" {
                    text("content")?
                } else {
                    let bytes = self
                        .backend
                        .files()
                        .read_bytes(ReadBytesRequest { path: path.clone() })
                        .await
                        .map_err(|_| "Sandbox read failed")?;
                    let source = String::from_utf8(bytes).map_err(|_| "Edit requires UTF-8")?;
                    let old = text("oldString")?;
                    let new = text("newString")?;
                    if old.is_empty() || old == new || !source.contains(&old) {
                        return Err("Edit must match existing nonempty text and change it".into());
                    }
                    if args["replaceAll"].as_bool() != Some(true)
                        && source.matches(&old).count() != 1
                    {
                        return Err("Edit text is not unique".into());
                    }
                    if args["replaceAll"].as_bool() == Some(true) {
                        source.replace(&old, &new)
                    } else {
                        source.replacen(&old, &new, 1)
                    }
                };
                self.backend
                    .files()
                    .write_bytes(WriteBytesRequest {
                        path,
                        content: content.into_bytes(),
                        mode: if self.backend.capabilities().supports_atomic_write {
                            WriteMode::AtomicOverwrite
                        } else {
                            WriteMode::Overwrite
                        },
                    })
                    .await
                    .map(|_| "File updated".into())
            }
            "glob" => self
                .backend
                .search()
                .glob(GlobRequest {
                    pattern: text("pattern")?,
                    base_dir: Some(self.path(path).await?),
                    limit: Some(args["limit"].as_u64().unwrap_or(200).min(200) as usize),
                })
                .await
                .map(|v| v.into_iter().map(|p| p.0).collect::<Vec<_>>().join("\n")),
            "grep" => self
                .backend
                .search()
                .grep(GrepRequest {
                    query: text("pattern")?,
                    base_dir: self.path(path).await?,
                    include: args["include"].as_str().map(str::to_owned),
                    mode: GrepMode::Content,
                    head_limit: Some(args["limit"].as_u64().unwrap_or(200).min(200) as usize),
                })
                .await
                .map(|v| v.entries.join("\n")),
            _ => return Err("Tool is disabled".into()),
        };
        let output = result.map_err(|e| {
            format!(
                "Sandbox operation failed ({})",
                match e {
                    operation_protocol::OperationError::NotFound { .. } => "not_found",
                    operation_protocol::OperationError::Unsupported { .. } => "unsupported",
                    _ => "backend_error",
                }
            )
        })?;
        Ok(output.chars().take(32_000).collect())
    }
}

async fn ready(State(b): State<Arc<Bridge>>, h: HeaderMap, Json(v): Json<Value>) -> StatusCode {
    if !b.authorized(&h) {
        return StatusCode::UNAUTHORIZED;
    }
    let tools = v["tools"].as_array();
    if tools.is_none_or(|ts| {
        !TOOLS
            .iter()
            .all(|t| ts.iter().any(|v| v.as_str() == Some(t)))
            || ts
                .iter()
                .any(|v| !TOOLS.contains(&v.as_str().unwrap_or("")))
    }) {
        return StatusCode::CONFLICT;
    }
    b.ready.store(true, Ordering::Release);
    StatusCode::NO_CONTENT
}
async fn binding(State(b): State<Arc<Bridge>>, h: HeaderMap, Json(v): Json<Value>) -> Response {
    if !b.authorized(&h) || !b.validate(v["session"].as_str().unwrap_or(""), None).await {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(json!({"turn":b.turn.read().await.clone()})).into_response()
}
async fn tool(State(b): State<Arc<Bridge>>, h: HeaderMap, Json(v): Json<Value>) -> Response {
    let _activity = b.activity.read().await;
    let turn = v["turn"].as_str().unwrap_or("");
    let op = v["operation"].as_str().unwrap_or("");
    if !b.authorized(&h)
        || turn.is_empty()
        || op.is_empty()
        || !b
            .validate(v["session"].as_str().unwrap_or(""), Some(turn))
            .await
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let result = OPERATION_CONTEXT
        .scope(
            OperationContext {
                turn_id: turn.into(),
                operation_id: op.into(),
            },
            b.execute(v["tool"].as_str().unwrap_or(""), &v["args"]),
        )
        .await;
    match result {
        Ok(output) => Json(json!({"output":output})).into_response(),
        Err(message) => Json(json!({"error":message})).into_response(),
    }
}
async fn model(State(b): State<Arc<Bridge>>, h: HeaderMap, body: Bytes) -> Response {
    let activity = b.model_activity.clone().read_owned().await;
    if !b.authorized(&h) || b.turn.read().await.is_none() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(mut value) = serde_json::from_slice::<Value>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if [&b.token, &b.control_password, &b.model_key]
        .iter()
        .any(|s| !s.is_empty() && body.windows(s.len()).any(|w| w == s.as_bytes()))
    {
        return (
            StatusCode::FORBIDDEN,
            "Model context failed credential boundary",
        )
            .into_response();
    }
    let names: Vec<&str> = value["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|t| t["function"]["name"].as_str())
        .collect();
    if names.iter().any(|name| !TOOLS.contains(name)) {
        return (StatusCode::FORBIDDEN, "Model tool is disabled").into_response();
    }
    #[cfg(test)]
    {
        *b.model_tools.lock().await = names.iter().map(|s| s.to_string()).collect();
    }
    // Count logical requests, not rate-limit retries. The native final step may
    // request continuations after truncated output; those must not extend a turn.
    let count = b.model_requests.fetch_add(1, Ordering::AcqRel) + 1;
    let max = b.max_requests.load(Ordering::Acquire);
    if count >= max {
        b.limit_reached.store(true, Ordering::Release);
    }
    if count > max {
        b.limit_notify.notify_one();
        return (StatusCode::CONFLICT, "Turn model step limit reached").into_response();
    }
    if value["stream"] == true {
        value["stream_options"] = json!({"include_usage":true});
    }
    let body = serde_json::to_vec(&value).unwrap();
    let mut attempt = 0;
    let response = loop {
        let result = b
            .client
            .post(format!(
                "{}/chat/completions",
                b.model_base.trim_end_matches('/')
            ))
            .bearer_auth(&b.model_key)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.clone())
            .timeout(std::time::Duration::from_secs(180))
            .send()
            .await;
        if let Ok(r) = &result {
            if r.status() == StatusCode::TOO_MANY_REQUESTS && attempt < 3 {
                let delay = r
                    .headers()
                    .get(header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(60)
                    .max(1);
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                if b.turn.read().await.is_none() {
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                attempt += 1;
                continue;
            }
        }
        break result;
    };
    match response {
        Ok(r) if r.status().is_success() => {
            let content_type = r.headers().get(header::CONTENT_TYPE).cloned();
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            tokio::spawn(async move {
                let _activity = activity;
                let mut stream = r.bytes_stream();
                let mut pending = Vec::new();
                let mut reported = None;
                let mut forwarding = Some(tx);
                while let Some(chunk) = stream.next().await {
                    if let Ok(bytes) = &chunk {
                        pending.extend_from_slice(bytes);
                        while let Some(end) = pending.iter().position(|c| *c == b'\n') {
                            let line: Vec<u8> = pending.drain(..=end).collect();
                            let data = line.strip_prefix(b"data: ").unwrap_or(&line);
                            if let Ok(v) = serde_json::from_slice::<Value>(data) {
                                if let Some(u) = provider_usage(&v) {
                                    reported = Some(u);
                                }
                            }
                        }
                        if pending.len() > 8 * 1024 * 1024 {
                            pending.clear();
                        }
                    }
                    // Keep draining after the consumer disconnects, including a final usage frame.
                    // Bounded backpressure must not strand metering behind a stopped consumer.
                    if let Some(sender) = &forwarding {
                        if !matches!(
                            tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                sender.send(chunk)
                            )
                            .await,
                            Ok(Ok(()))
                        ) {
                            forwarding = None;
                        }
                    }
                }
                if let Ok(v) = serde_json::from_slice::<Value>(&pending) {
                    if let Some(u) = provider_usage(&v) {
                        reported = Some(u);
                    }
                }
                if let Some(u) = reported {
                    let mut total = b.usage.lock().await;
                    total.input_tokens += u.input_tokens;
                    total.output_tokens += u.output_tokens;
                    total.total_tokens += u.total_tokens;
                } else {
                    b.usage_complete.store(false, Ordering::Release);
                }
            });
            let stream = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|chunk| (chunk, rx))
            });
            let mut out = Response::new(Body::from_stream(stream));
            if let Some(t) = content_type {
                out.headers_mut().insert(header::CONTENT_TYPE, t);
            }
            out
        }
        _ => {
            b.usage_complete.store(false, Ordering::Release);
            (StatusCode::BAD_GATEWAY, "Model request failed").into_response()
        }
    }
}

// OpenAI-compatible usage includes cached input and reasoning output in these totals.
fn provider_usage(value: &Value) -> Option<SessionUsage> {
    let u = &value["usage"];
    Some(SessionUsage {
        input_tokens: u["prompt_tokens"].as_u64()?,
        output_tokens: u["completion_tokens"].as_u64()?,
        total_tokens: u["total_tokens"].as_u64()?,
    })
}
