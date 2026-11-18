mod bridge;

use agent_runtime_protocol::*;
use async_trait::async_trait;
use backend::{execution::OperationExecutionExt, process_group};
use bridge::{Bridge, TOOLS};
use futures_util::StreamExt;
use serde_json::{json, Value};
use session_protocol::{SessionTurnOutcome, SessionUsage};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::{mpsc, watch, Mutex, RwLock},
};

pub const OPENCODE_VERSION: &str = "2.3.0";

#[derive(Clone, Copy, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TurnLimits {
    steps: u32,
    output_tokens: u32,
    turn_timeout_s: u32,
}
impl Default for TurnLimits {
    fn default() -> Self {
        Self {
            steps: 30,
            output_tokens: 2048,
            turn_timeout_s: 1800,
        }
    }
}
const ACTIVE_PLUGINS: &[&str] = &[
    "opencode.provider.opencode",
    "opencode.config.policy",
    "opencode.agent",
    "opencode.config.provider",
    "opencode.config.agent",
    "opencode.tool.shell",
    "opencode.tool.read",
    "opencode.tool.write",
    "opencode.tool.edit",
    "opencode.tool.glob",
    "opencode.tool.grep",
    "xgovernor.operation-bridge",
];
const DISABLED_PLUGINS: &[&str] = &[
    "opencode.mcp.codemode.defaults",
    "opencode.tool.webfetch",
    "opencode.tool.websearch",
    "opencode.tool.patch",
    "opencode.tool.subagent",
    "opencode.tool.skill",
    "opencode.tool.schedule",
    "opencode.tool.question",
    "opencode.browser",
    "opencode.provider.dynamic",
    "opencode.config.shell",
    "opencode.config.mcp",
    "opencode.skill",
    "opencode.config.skill",
    "opencode.vcs.git",
    "opencode.vcs.hg",
    "opencode.config.worktree",
    "opencode.tools",
    "opencode.tools.mcp-events",
    "opencode.tools.mcp-resources",
    "opencode.config.instruction",
    "opencode.config.reference",
];
const PLUGIN: &str = include_str!("../plugin/bridge.js");

/// Each runtime owns a separate OpenCode server, database and operation binding.
#[derive(Clone)]
pub struct OpenCodeRuntime {
    executable: PathBuf,
    sessions: Arc<RwLock<HashMap<String, Arc<Session>>>>,
    starting: Arc<Mutex<HashSet<String>>>,
}
struct Session {
    child: Mutex<ManagedChild>,
    _background: Background,
    output_leaked: Arc<std::sync::atomic::AtomicBool>,
    root: tempfile::TempDir,
    api: Api,
    bridge: Arc<Bridge>,
    gate: Arc<Mutex<()>>,
    cancel: watch::Sender<bool>,
    model: Value,
    poisoned: std::sync::atomic::AtomicBool,
    cancellation: Mutex<()>,
    turn_timeout: Duration,
}
struct Background(Vec<tokio::task::JoinHandle<()>>);
impl Drop for Background {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}
struct ManagedChild {
    child: Child,
    pgid: i32,
    stopped: bool,
}
impl ManagedChild {
    async fn terminate(&mut self) {
        if self.stopped {
            return;
        }
        let pgid = self.pgid;
        let _ =
            tokio::task::spawn_blocking(move || process_group::terminate_process_group(pgid)).await;
        let _ = self.child.wait().await;
        self.stopped = true;
    }
}
impl Drop for ManagedChild {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = process_group::terminate_process_group(self.pgid);
        }
    }
}

fn capture_output<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
    reader: R,
    secrets: Vec<String>,
    leaked: Arc<std::sync::atomic::AtomicBool>,
    announcement: mpsc::Sender<String>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut reader = BufReader::new(reader);
        loop {
            let mut line = Vec::new();
            // Bound individual lines without retaining raw process output.
            use tokio::io::AsyncReadExt;
            match (&mut reader)
                .take(65_537)
                .read_until(b'\n', &mut line)
                .await
            {
                Ok(0) => break,
                Ok(_) if line.len() <= 65_536 => {}
                _ => {
                    leaked.store(true, Ordering::Release);
                    break;
                }
            }
            let line = String::from_utf8_lossy(&line);
            if secrets.iter().any(|s| !s.is_empty() && line.contains(s)) {
                leaked.store(true, Ordering::Release);
            }
            if let Some(url) = line.trim().strip_prefix("server listening on ") {
                let _ = announcement.try_send(url.to_owned());
            }
        }
    })
}

fn plugins_ready(value: &Value) -> Result<bool, RuntimeError> {
    let plugins = value["data"]
        .as_array()
        .ok_or_else(|| failure("OpenCode omitted plugin inventory"))?;
    let mut active = BTreeSet::new();
    for plugin in plugins {
        if plugin["state"]["status"] == "active" {
            let id = plugin["id"]
                .as_str()
                .ok_or_else(|| failure("Invalid plugin identity"))?;
            if !ACTIVE_PLUGINS.contains(&id) {
                return Err(invalid("Unexpected active OpenCode plugin"));
            }
            active.insert(id);
        } else if plugin["state"]["status"] == "failed" {
            return Err(failure("OpenCode plugin activation failed"));
        }
    }
    Ok(ACTIVE_PLUGINS.iter().all(|id| active.contains(id)))
}
fn validate_plugins(value: &Value) -> Result<(), RuntimeError> {
    if !plugins_ready(value)? {
        return Err(failure("Required controlled plugin is not active"));
    }
    Ok(())
}

#[derive(Clone)]
struct Api {
    client: reqwest::Client,
    url: String,
    password: String,
}
fn failure(message: &str) -> RuntimeError {
    RuntimeError::WorkerUnavailable {
        message: message.into(),
        retryable: false,
    }
}
fn invalid(message: &str) -> RuntimeError {
    RuntimeError::InvalidRequest {
        code: "invalid_opencode_configuration".into(),
        message: message.into(),
    }
}
fn busy() -> RuntimeError {
    RuntimeError::Conflict {
        code: "runtime_busy".into(),
        message: "OpenCode session is busy".into(),
    }
}
impl Api {
    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, RuntimeError> {
        let mut r = self
            .client
            .request(method.clone(), format!("{}{path}", self.url))
            .basic_auth("opencode", Some(&self.password))
            .timeout(Duration::from_secs(if path.ends_with("/export") {
                60
            } else {
                15
            }));
        if let Some(b) = body {
            r = r.json(&b);
        }
        let r = r.send().await.map_err(|e| {
            failure(&format!(
                "OpenCode {method} {path}: {}",
                if e.is_timeout() {
                    "request timed out"
                } else if e.is_connect() {
                    "connection failed"
                } else {
                    "transport failed"
                }
            ))
        })?;
        if !r.status().is_success() {
            return Err(failure(&format!(
                "OpenCode {method} {path}: HTTP {}",
                r.status()
            )));
        }
        if r.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(Value::Null);
        }
        r.json().await.map_err(|e| {
            failure(&format!(
                "OpenCode {method} {path}: {}",
                if e.is_timeout() {
                    "response body timed out"
                } else {
                    "invalid response body"
                }
            ))
        })
    }
    async fn get(&self, path: &str) -> Result<Value, RuntimeError> {
        self.call(reqwest::Method::GET, path, None).await
    }
    async fn post(&self, path: &str, body: Value) -> Result<Value, RuntimeError> {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }
}
impl OpenCodeRuntime {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            starting: Arc::new(Mutex::new(HashSet::new())),
        }
    }
    async fn session(&self, id: &str) -> Result<Arc<Session>, RuntimeError> {
        self.sessions
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound {
                runtime_id: id.into(),
            })
    }
    async fn launch(
        &self,
        request: &RuntimeStartRequest,
        context: RuntimeExecutionContext,
        plugin_source: &str,
    ) -> Result<Arc<Session>, RuntimeError> {
        self.validate_llm_config(request.llm.as_ref())?;
        let limits: TurnLimits = serde_json::from_value(
            request
                .ext
                .get("opencode")
                .cloned()
                .unwrap_or_else(|| json!({})),
        )
        .map_err(|_| invalid("Invalid OpenCode turn limits"))?;
        if limits.steps == 0 || limits.output_tokens == 0 || limits.turn_timeout_s == 0 {
            return Err(invalid("OpenCode turn limits must be positive"));
        }
        let llm = request
            .llm
            .as_ref()
            .ok_or_else(|| invalid("Explicit model configuration is required"))?;
        let model = llm
            .model
            .clone()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("Model is required"))?;
        let key = llm
            .api_key
            .clone()
            .or_else(|| llm.api_key_env.as_ref().and_then(|k| std::env::var(k).ok()))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("Model credential is required"))?;
        let base = llm
            .api_base
            .clone()
            .ok_or_else(|| invalid("Model endpoint is required"))?;
        let parsed = reqwest::Url::parse(&base).map_err(|_| invalid("Invalid model endpoint"))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid(
                "Model endpoint must not contain credentials, queries or fragments",
            ));
        }
        if std::path::Path::new("/etc/opencode").exists() {
            return Err(invalid(
                "System OpenCode configuration must be absent in controlled mode",
            ));
        }
        let version = Command::new(&self.executable)
            .arg("--version")
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .kill_on_drop(true)
            .output();
        let version = tokio::time::timeout(Duration::from_secs(10), version)
            .await
            .map_err(|_| failure("OpenCode version check timed out"))?
            .map_err(|_| failure("OpenCode executable unavailable"))?;
        if !version.status.success()
            || String::from_utf8_lossy(&version.stdout).trim()
                != format!("opencode v{OPENCODE_VERSION}")
        {
            return Err(invalid("Only OpenCode 2.3.0 is supported"));
        }
        let root = tempfile::Builder::new()
            .prefix("opencode-")
            .tempdir()
            .map_err(|_| failure("Cannot create private runtime directory"))?;
        let config_dir = root.path().join("config");
        tokio::fs::create_dir_all(config_dir.join("plugins"))
            .await
            .map_err(|_| failure("Cannot prepare bundled plugin"))?;
        tokio::fs::create_dir_all(root.path().join("workspace"))
            .await
            .map_err(|_| failure("Cannot prepare runtime directory"))?;
        for name in ["home", "xdg-config", "data", "cache", "state", "tmp"] {
            tokio::fs::create_dir_all(root.path().join(name))
                .await
                .map_err(|_| failure("Cannot prepare private runtime directories"))?;
        }
        tokio::fs::write(config_dir.join("plugins/bridge.js"), plugin_source)
            .await
            .map_err(|_| failure("Cannot prepare bundled plugin"))?;
        let password = uuid::Uuid::new_v4().to_string();
        let bridge = Bridge::new(
            context.operation_backend,
            key,
            base.clone(),
            password.clone(),
        );
        bridge.max_requests.store(limits.steps, Ordering::Release);
        let (bridge_url, bridge_task) = bridge
            .listen()
            .await
            .map_err(|_| failure("Cannot start operation bridge"))?;
        let mut background = Background(vec![bridge_task]);
        let config = controlled_config(
            &model,
            &bridge_url,
            &config_dir.join("plugins/bridge.js").to_string_lossy(),
            limits,
        );
        let mut command = Command::new(&self.executable);
        command
            .args(["serve", "--hostname", "127.0.0.1", "--port", "0"])
            .current_dir(root.path().join("workspace"))
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .env("HOME", root.path().join("home"))
            .env("XDG_CONFIG_HOME", root.path().join("xdg-config"))
            .env("XDG_DATA_HOME", root.path().join("data"))
            .env("XDG_CACHE_HOME", root.path().join("cache"))
            .env("XDG_STATE_HOME", root.path().join("state"))
            .env("TMPDIR", root.path().join("tmp"))
            .env("OPENCODE_CONFIG_DIR", &config_dir)
            .env("OPENCODE_CONFIG_CONTENT", config.to_string())
            .env("OPENCODE_PASSWORD", &password)
            .env("OPENCODE_DISABLE_MODELS_FETCH", "1")
            .env("OPENCODE_CONFIG_PROJECT_DISABLE", "1")
            .env("OPENCODE_DISABLE_CLAUDE_CODE", "1")
            .env("OPENCODE_FILEWATCHER_DISABLE", "1")
            .env("OPENCODE_DISABLE_FREE_MODELS", "1")
            .env("OPENCODE_FLAGS", "-all")
            .env("XG_OPERATION_URL", &bridge_url)
            .env("XG_OPERATION_TOKEN", &bridge.token)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .process_group(0)
            .kill_on_drop(true);
        let mut child = match command.spawn() {
            Ok(c) => c,
            Err(_) => {
                return Err(failure("Cannot start OpenCode"));
            }
        };
        let pgid = child
            .id()
            .ok_or_else(|| failure("OpenCode process identity unavailable"))?
            as i32;
        process_group::register_pgid(pgid);
        let output_leaked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (announcement, mut announcements) = mpsc::channel(2);
        let secrets = vec![
            password.clone(),
            bridge.token.clone(),
            bridge.model_key.clone(),
        ];
        background.0.extend([
            capture_output(
                child.stdout.take().unwrap(),
                secrets.clone(),
                output_leaked.clone(),
                announcement.clone(),
            ),
            capture_output(
                child.stderr.take().unwrap(),
                secrets,
                output_leaked.clone(),
                announcement,
            ),
        ]);
        let child = ManagedChild {
            child,
            pgid,
            stopped: false,
        };
        let url = match tokio::time::timeout(Duration::from_secs(25), announcements.recv()).await {
            Ok(Some(url)) => url,
            _ => {
                return Err(failure("OpenCode listen announcement unavailable"));
            }
        };
        let parsed =
            reqwest::Url::parse(&url).map_err(|_| failure("Invalid OpenCode listen address"))?;
        if parsed.scheme() != "http"
            || parsed.host_str() != Some("127.0.0.1")
            || parsed.port().is_none_or(|p| p == 0)
            || parsed.path() != "/"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid("OpenCode must listen on a private loopback port"));
        }
        let (cancel, _) = watch::channel(false);
        let session = Arc::new(Session {
            child: Mutex::new(child),
            _background: background,
            output_leaked,
            root,
            api: Api {
                client: reqwest::Client::new(),
                url,
                password,
            },
            bridge,
            gate: Arc::new(Mutex::new(())),
            cancel,
            poisoned: std::sync::atomic::AtomicBool::new(false),
            cancellation: Mutex::new(()),
            turn_timeout: Duration::from_secs(limits.turn_timeout_s.into()),
            model: json!({"provider":llm.provider.clone().unwrap_or_else(||"openai-compatible".into()),"model":model,"api_base":base}),
        });
        let result=async {
            session.wait_ready().await?;
            let unauthenticated = session.api.client.get(format!("{}/api/info", session.api.url)).send().await.map_err(|_| failure("Cannot check OpenCode authentication"))?;
            if unauthenticated.status() != reqwest::StatusCode::UNAUTHORIZED { return Err(invalid("OpenCode control plane requires authentication")); }
            session.check_logs().await?;
            let agent = session.api.get("/api/agent/build").await?;
            if agent["data"]["steps"].as_u64() != Some(limits.steps as u64) { return Err(invalid("OpenCode agent step limit did not take effect")); }
            let created=session.api.post("/api/session",json!({"location":{"directory":session.root.path().join("workspace")},"model":{"providerID":"xgovernor","id":session.model["model"]},"title":"Workspace session"})).await?;
            let id=created["data"]["id"].as_str().ok_or_else(||failure("OpenCode omitted session identity"))?.to_owned();
            *session.bridge.session.write().await=id;
            if let Some(snapshot)=&request.state {session.restore(snapshot.clone()).await?;}
            session.wait_ready().await?;
            Ok(())
        }.await;
        if let Err(e) = result {
            session.child.lock().await.terminate().await;
            return Err(e);
        }
        Ok(session)
    }
    /// Authenticated local attach arguments for a retained selected branch.
    /// This live handle contains a control-plane credential; do not persist it in a checkpoint or log it.
    pub async fn continuation(&self, id: &str) -> Result<Continuation, RuntimeError> {
        let s = self.session(id).await?;
        let sid = s.bridge.session.read().await.clone();
        Ok(Continuation {
            url: s.api.url.clone(),
            session_id: sid,
            password: s.api.password.clone(),
        })
    }
}
/// A live control-plane handle, deliberately without Debug or Serialize.
pub struct Continuation {
    pub url: String,
    pub session_id: String,
    pub password: String,
}

fn controlled_config(model: &str, bridge: &str, plugin: &str, limits: TurnLimits) -> Value {
    let mut permissions = vec![json!({"action":"*","resource":"*","effect":"deny"})];
    for t in TOOLS {
        permissions.push(json!({"action":t,"resource":"*","effect":"allow"}));
    }
    json!({"update":"disable","share":"disabled","snapshots":false,"lsp":false,"formatter":false,"warming":false,
        "permissions":permissions,"model":format!("xgovernor/{model}"),
        "plugins":std::iter::once("-*".to_owned()).chain(DISABLED_PLUGINS.iter().map(|id|format!("-{id}"))).chain(ACTIVE_PLUGINS.iter().map(|id|id.to_string())).collect::<Vec<_>>(),
        "policies":[{"action":"plugin.use","resource":"*","effect":"deny"},{"action":"plugin.use","resource":plugin,"effect":"allow"},{"action":"mcp.use","resource":"*","effect":"deny"},{"action":"skill.use","resource":"*","effect":"deny"},{"action":"integration.connect","resource":"*","effect":"deny"},{"action":"integration.use","resource":"*","effect":"deny"},{"action":"integration.use","resource":"xgovernor","effect":"allow"},{"action":"websearch.use","resource":"*","effect":"deny"}],
        "default_agent":"build","agents":{"build":{"steps":limits.steps,"system":"Work only in the remote sandbox through the provided tools. Paths are sandbox paths. No user interaction or background execution is available."}},
        "providers":{"xgovernor":{"package":"@ai-sdk/openai-compatible","settings":{"baseURL":format!("{bridge}/model"),"apiKey":"local-proxy"},"models":{model:{"name":model,"limit":{"context":65536,"output":limits.output_tokens},"capabilities":{"tools":true}}}}}})
}
impl Session {
    async fn wait_ready(&self) -> Result<(), RuntimeError> {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if self.child.lock().await.child.try_wait().map_err(|_| failure("Cannot inspect OpenCode"))?.is_some() {
                    return Err(failure("OpenCode exited before controlled plugin activation"));
                }
                if let Ok(info) = self.api.get("/api/info").await {
                    self.validate_info(&info).await?;
                    // /ready is sent inside setup; inventory is published only after setup returns.
                    let plugins = self.api.get("/api/plugin").await?;
                    if plugins_ready(&plugins)? && self.bridge.ready.load(Ordering::Acquire) {
                        return validate_plugins(&plugins);
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.map_err(|_| failure("OpenCode controlled plugin activation timed out: handshake and required active inventory were not both ready"))?
    }
    async fn validate_info(&self, info: &Value) -> Result<(), RuntimeError> {
        if info["version"] != OPENCODE_VERSION
            || !info["capabilities"]["persistentPty"].is_boolean()
        {
            return Err(invalid("OpenCode server identity or capability mismatch"));
        }
        if info["urls"]
            .as_array()
            .is_none_or(|urls| urls.len() != 1 || urls[0].as_str() != Some(&self.api.url))
        {
            return Err(invalid(
                "OpenCode listener does not match private loopback address",
            ));
        }
        let root = self
            .root
            .path()
            .canonicalize()
            .map_err(|_| failure("Cannot inspect private root"))?;
        let tmp = info["paths"]["tmp"]
            .as_str()
            .ok_or_else(|| failure("OpenCode omitted temporary directory"))?;
        for path in [PathBuf::from(tmp), self.root.path().join("data/opencode")] {
            let path = path
                .canonicalize()
                .map_err(|_| failure("Cannot inspect OpenCode private directory"))?;
            if !path.starts_with(&root) || path == root {
                return Err(invalid("OpenCode directory escaped private root"));
            }
        }
        Ok(())
    }
    async fn check_logs(&self) -> Result<(), RuntimeError> {
        if self.output_leaked.load(Ordering::Acquire) {
            return Err(failure(
                "OpenCode process output failed credential boundary",
            ));
        }
        let mut pending = vec![self.root.path().join("data")];
        let secrets = [
            &self.api.password,
            &self.bridge.token,
            &self.bridge.model_key,
        ];
        while let Some(path) = pending.pop() {
            let mut entries = tokio::fs::read_dir(path)
                .await
                .map_err(|_| failure("Cannot inspect OpenCode logs"))?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|_| failure("Cannot inspect OpenCode logs"))?
            {
                let kind = entry
                    .file_type()
                    .await
                    .map_err(|_| failure("Cannot inspect OpenCode logs"))?;
                if kind.is_symlink() {
                    return Err(invalid("Unexpected link in OpenCode data directory"));
                }
                if kind.is_dir() {
                    pending.push(entry.path());
                } else if entry.path().extension().is_some_and(|e| e == "log") {
                    if entry
                        .metadata()
                        .await
                        .map_err(|_| failure("Cannot inspect OpenCode logs"))?
                        .len()
                        > 8 * 1024 * 1024
                    {
                        return Err(failure("OpenCode log exceeded inspection limit"));
                    }
                    let bytes = tokio::fs::read(entry.path())
                        .await
                        .map_err(|_| failure("Cannot inspect OpenCode logs"))?;
                    if secrets
                        .iter()
                        .any(|s| !s.is_empty() && bytes.windows(s.len()).any(|w| w == s.as_bytes()))
                    {
                        return Err(failure("OpenCode log failed credential boundary"));
                    }
                }
            }
        }
        Ok(())
    }

    async fn snapshot(&self) -> Result<RuntimeStateSnapshot, RuntimeError> {
        let id = self.bridge.session.read().await.clone();
        let value = self
            .api
            .get(&format!("/api/experimental/session/{id}/export"))
            .await
            .map_err(|error| failure(&format!("OpenCode state export failed: {error:?}")))?;
        let mut export = value["data"].clone();
        if !export["messages"].is_array() {
            return Err(failure("OpenCode export omitted history"));
        }
        // Identity, local locations, permissions and arbitrary metadata are never checkpoint authority.
        let info = export["info"]
            .as_object_mut()
            .ok_or_else(|| failure("OpenCode export omitted session"))?;
        info.retain(|k, _| {
            matches!(
                k.as_str(),
                "id" | "projectID" | "model" | "cost" | "tokens" | "time" | "title"
            )
        });
        export["info"]["location"] = json!({"directory":"/workspace"});
        let mut state = json!({"export":export,"model":self.model});
        sanitize(
            &mut state,
            &[
                &self.bridge.model_key,
                &self.bridge.token,
                &self.api.password,
                &self.root.path().to_string_lossy(),
            ],
        );
        RuntimeStateSnapshot::try_new("opencode", 1, &state)
    }
    async fn restore(&self, snapshot: RuntimeStateSnapshot) -> Result<(), RuntimeError> {
        snapshot.validate("opencode", 1)?;
        if snapshot.state["model"] != self.model {
            return Err(invalid(
                "Restore requires the checkpoint model configuration",
            ));
        }
        let mut export = snapshot.state["export"].clone();
        if !export["messages"].is_array() {
            return Err(RuntimeError::StateCorrupt {
                message: "OpenCode history is missing".into(),
            });
        }
        let id = self.bridge.session.read().await.clone();
        let old = self.api.get(&format!("/api/session/{id}")).await?;
        export["info"] = old["data"].clone();
        export["location"] = json!({"directory":self.root.path().join("workspace")});
        self.api
            .call(reqwest::Method::DELETE, &format!("/api/session/{id}"), None)
            .await?;
        let imported = self
            .api
            .post("/api/experimental/session/import", export)
            .await?;
        if imported["data"]["id"].as_str() != Some(&id) {
            return Err(failure("Imported session identity mismatch"));
        }
        *self.bridge.session.write().await = id;
        Ok(())
    }
}
fn sanitize(value: &mut Value, secrets: &[&str]) {
    match value {
        Value::String(s) => {
            for secret in secrets {
                if !secret.is_empty() {
                    *s = s.replace(secret, "[redacted]");
                }
            }
        }
        Value::Array(v) => {
            for x in v {
                sanitize(x, secrets);
            }
        }
        Value::Object(m) => {
            m.retain(|k, _| {
                !matches!(
                    k.as_str(),
                    "apiKey" | "api_key" | "authorization" | "password" | "credential" | "headers"
                )
            });
            for v in m.values_mut() {
                sanitize(v, secrets);
            }
        }
        _ => {}
    }
}
#[async_trait]
impl AgentRuntime for OpenCodeRuntime {
    fn runtime_kind(&self) -> &str {
        "opencode"
    }
    fn capabilities(&self) -> BTreeSet<RuntimeCapability> {
        [RuntimeCapability::StateExport].into_iter().collect()
    }
    async fn start(
        &self,
        request: RuntimeStartRequest,
        context: RuntimeExecutionContext,
    ) -> Result<(), RuntimeError> {
        let mut starting = self.starting.lock().await;
        if self.sessions.read().await.contains_key(&request.runtime_id)
            || !starting.insert(request.runtime_id.clone())
        {
            return Err(busy());
        }
        drop(starting);
        let result = self.launch(&request, context, PLUGIN).await;
        self.starting.lock().await.remove(&request.runtime_id);
        let session = result?;
        self.sessions
            .write()
            .await
            .insert(request.runtime_id, session);
        Ok(())
    }
    async fn attach(&self, id: &str, context: RuntimeExecutionContext) -> Result<(), RuntimeError> {
        let s = self.session(id).await?;
        if !Arc::ptr_eq(&s.bridge.backend, &context.operation_backend) {
            return Err(invalid("Execution backend cannot be rebound on attach"));
        }
        Ok(())
    }
    async fn check_alive(&self, id: &str) -> Result<bool, RuntimeError> {
        match self.sessions.read().await.get(id).cloned() {
            Some(s) => Ok(s
                .child
                .lock()
                .await
                .child
                .try_wait()
                .map_err(|_| failure("Cannot inspect OpenCode"))?
                .is_none()),
            None => Ok(false),
        }
    }
    async fn stop(&self, id: &str) -> Result<(), RuntimeError> {
        let s = self.session(id).await?;
        self.cancel(RuntimeCancelRequest {
            runtime_id: id.into(),
            turn_id: None,
        })
        .await?;
        let _gate = s.gate.lock().await;
        s.child.lock().await.terminate().await;
        self.sessions.write().await.remove(id);
        Ok(())
    }
    async fn submit_turn(
        &self,
        request: RuntimeTurnRequest,
    ) -> Result<RuntimeEventReceiver, RuntimeError> {
        if request.llm.is_some() || request.reasoning_effort.is_some() {
            return Err(invalid(
                "Model and reasoning overrides are not supported during a turn",
            ));
        }
        let s = self.session(&request.runtime_id).await?;
        let gate = s.gate.clone().try_lock_owned().map_err(|_| busy())?;
        if s.poisoned.load(Ordering::Acquire) {
            return Err(failure("OpenCode session requires replacement"));
        }
        s.bridge
            .backend
            .begin_turn(&request.turn_id)
            .await
            .map_err(|_| failure("Sandbox turn could not begin"))?;
        *s.bridge.usage.lock().await = SessionUsage::default();
        s.bridge.usage_complete.store(true, Ordering::Release);
        s.bridge.model_requests.store(0, Ordering::Release);
        s.bridge.limit_reached.store(false, Ordering::Release);
        *s.bridge.turn.write().await = Some(request.turn_id.clone());
        s.cancel.send_replace(false);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            let _gate = gate;
            let driven = tokio::time::timeout(s.turn_timeout, drive_turn(&s, &request, &tx)).await;
            let timed_out = driven.is_err();
            let mut result = driven.unwrap_or(Ok(SessionTurnOutcome::BudgetExhausted));
            let _cancel = s.cancellation.lock().await;
            if timed_out || result.is_err() {
                if !timed_out {
                    s.poisoned.store(true, Ordering::Release);
                }
                // Retire tool admission before interrupting; completed writes remain in the sandbox.
                *s.bridge.turn.write().await = None;
                let id = s.bridge.session.read().await.clone();
                if let Err(interrupt) = s
                    .api
                    .post(&format!("/api/session/{id}/interrupt"), json!({}))
                    .await
                {
                    s.poisoned.store(true, Ordering::Release);
                    result = Err(failure(&format!(
                        "{result:?}; interrupt failed: {interrupt:?}"
                    )));
                }
                if let Err(cleanup) = s.bridge.backend.cancel_turn(&request.turn_id).await {
                    s.poisoned.store(true, Ordering::Release);
                    result = Err(failure(&format!(
                        "{result:?}; sandbox cancellation failed: {cleanup}"
                    )));
                }
            }
            // Fence new calls before draining existing backend work and publishing a terminal event.
            *s.bridge.turn.write().await = None;
            let cleanup = s.bridge.backend.finish_turn(&request.turn_id).await;
            let _activity = s.bridge.activity.write().await;
            if cleanup.is_err() {
                s.poisoned.store(true, Ordering::Release);
            }
            // A disconnected OpenCode client must not discard the provider's final usage.
            let metering =
                tokio::time::timeout(Duration::from_secs(180), s.bridge.model_activity.write())
                    .await;
            let usage = *s.bridge.usage.lock().await;
            let result = if metering.is_err() || !s.bridge.usage_complete.load(Ordering::Acquire) {
                s.poisoned.store(true, Ordering::Release);
                Err(failure(&format!(
                    "{:?}; provider usage is incomplete",
                    result.err()
                )))
            } else {
                result
            };
            let event = match (result, cleanup) {
                (Ok(outcome), Ok(())) => RuntimeEvent::Completed { outcome, usage },
                (result, _) => RuntimeEvent::Failed {
                    error: RuntimeFailure {
                        code: "opencode_turn_failed".into(),
                        message: result
                            .err()
                            .map(|e| format!("{e:?}"))
                            .unwrap_or_else(|| "Sandbox cleanup failed".into()),
                        retryable: false,
                        details: json!({"usage_complete": metering.is_ok() && s.bridge.usage_complete.load(Ordering::Acquire)}),
                    },
                    usage,
                },
            };
            drop(metering);
            drop(_activity);
            drop(_gate);
            let _ = tx.send(event).await;
        });
        Ok(rx)
    }
    async fn answer_interaction(&self, _: RuntimeInteractionRequest) -> Result<(), RuntimeError> {
        Err(RuntimeError::UnsupportedCapability {
            capability: "interaction".into(),
        })
    }
    async fn cancel(&self, request: RuntimeCancelRequest) -> Result<(), RuntimeError> {
        let s = self.session(&request.runtime_id).await?;
        let _cancel = s.cancellation.lock().await;
        let mut turn = s.bridge.turn.write().await;
        let Some(active) = turn.clone() else {
            return Ok(());
        };
        if request.turn_id.as_ref().is_some_and(|id| id != &active) {
            return Err(invalid("Cancel turn identity mismatch"));
        }
        s.bridge
            .backend
            .block_turn(&active)
            .map_err(|_| failure("Sandbox turn could not be fenced"))?;
        *turn = None;
        drop(turn);
        let id = s.bridge.session.read().await.clone();
        let interrupt = s
            .api
            .post(&format!("/api/session/{id}/interrupt"), json!({}))
            .await;
        let cleanup = s.bridge.backend.cancel_turn(&active).await;
        let _activity = s.bridge.activity.write().await;
        s.cancel.send_replace(true);
        s.poisoned.store(true, Ordering::Release);
        interrupt?;
        cleanup.map_err(|_| failure("Sandbox cancellation cleanup is unconfirmed"))?;
        Ok(())
    }
    async fn export_state(&self, id: &str) -> Result<RuntimeStateSnapshot, RuntimeError> {
        let s = self.session(id).await?;
        let _gate = s.gate.try_lock().map_err(|_| busy())?;
        let _activity = s.bridge.activity.write().await;
        s.snapshot().await
    }
}

async fn drive_turn(
    s: &Session,
    r: &RuntimeTurnRequest,
    tx: &mpsc::Sender<RuntimeEvent>,
) -> Result<SessionTurnOutcome, RuntimeError> {
    let id = s.bridge.session.read().await.clone();
    let response = s
        .api
        .client
        .get(format!("{}/api/event", s.api.url))
        .basic_auth("opencode", Some(&s.api.password))
        .send()
        .await
        .map_err(|_| failure("OpenCode event stream unavailable"))?;
    if !response.status().is_success() {
        return Err(failure("OpenCode event stream rejected"));
    }
    let mut stream = response.bytes_stream();
    s.api
        .post(&format!("/api/session/{id}/prompt"), json!({"text":r.text}))
        .await?;
    let mut cancelled = s.cancel.subscribe();
    let mut limit_interrupted = false;
    if *cancelled.borrow() {
        return Ok(SessionTurnOutcome::Cancelled);
    }
    let mut buffer = Vec::new();
    let mut sequence = 0;
    let mut names = HashMap::<String, String>::new();
    let mut pending_text = String::new();
    let outcome = 'events: loop {
        let chunk = tokio::select! {
            chunk = stream.next() => chunk,
            _ = s.bridge.limit_notify.notified(), if !limit_interrupted => {
                if s.bridge.model_requests.load(Ordering::Acquire) > s.bridge.max_requests.load(Ordering::Acquire) {
                    limit_interrupted = true;
                    s.api.post(&format!("/api/session/{id}/interrupt"), json!({})).await?;
                }
                continue;
            }
            _ = cancelled.changed() => {
                if *cancelled.borrow() { break 'events SessionTurnOutcome::Cancelled; }
                continue;
            }
        }
        .ok_or_else(|| failure("OpenCode event stream ended before terminal"))?
        .map_err(|_| failure("OpenCode event stream failed"))?;
        buffer.extend_from_slice(&chunk);
        if buffer.len() > 8 * 1024 * 1024 {
            return Err(failure("OpenCode event exceeded size limit"));
        }
        while let Some(end) = buffer.windows(2).position(|w| w == b"\n\n") {
            let frame: Vec<u8> = buffer.drain(..end + 2).collect();
            let text =
                std::str::from_utf8(&frame).map_err(|_| failure("Invalid OpenCode event"))?;
            for line in text.lines().filter_map(|l| l.strip_prefix("data: ")) {
                let mut event: Value =
                    serde_json::from_str(line).map_err(|_| failure("Invalid OpenCode event"))?;
                let relevant = event["data"]["sessionID"]
                    .as_str()
                    .or_else(|| event["data"]["form"]["sessionID"].as_str())
                    == Some(&id);
                if !relevant {
                    continue;
                }
                if matches!(
                    event["type"].as_str(),
                    Some("form.created" | "permission.asked" | "question.asked")
                ) {
                    return Err(failure("OpenCode requested unsupported user interaction"));
                }
                if event["type"] == "session.text.delta" {
                    pending_text.push_str(event["data"]["delta"].as_str().unwrap_or(""));
                    let secrets = [&s.bridge.model_key, &s.bridge.token, &s.api.password];
                    for secret in secrets {
                        if !secret.is_empty() {
                            pending_text = pending_text.replace(secret, "[redacted]");
                        }
                    }
                    let keep = secrets.iter().map(|s| s.len()).max().unwrap_or(0);
                    let mut end = pending_text.len().saturating_sub(keep);
                    while !pending_text.is_char_boundary(end) {
                        end -= 1;
                    }
                    event["data"]["delta"] = pending_text.drain(..end).collect::<String>().into();
                }
                sanitize(
                    &mut event,
                    &[&s.bridge.model_key, &s.bridge.token, &s.api.password],
                );
                let data = &event["data"];
                if data["sessionID"].as_str() != Some(&id) {
                    continue;
                }
                match event["type"].as_str().unwrap_or("") {
                    "session.text.delta" => {
                        sequence += 1;
                        tx.send(RuntimeEvent::OutputDelta {
                            stream_id: r.turn_id.clone(),
                            sequence,
                            delta: data["delta"].as_str().unwrap_or("").into(),
                        })
                        .await
                        .map_err(|_| failure("Runtime receiver closed"))?;
                    }
                    "session.tool.input.started"
                    | "session.tool.success"
                    | "session.tool.failed" => {
                        use session_protocol::{
                            SessionToolActivityPhase as Phase, SessionToolActivityStatus as Status,
                        };
                        let tool_id = data["id"].as_str().unwrap_or("").to_owned();
                        let begin = event["type"] == "session.tool.input.started";
                        if begin {
                            names.insert(
                                tool_id.clone(),
                                data["name"].as_str().unwrap_or("").into(),
                            );
                        }
                        tx.send(RuntimeEvent::ToolActivity {
                            activity_id: tool_id.clone(),
                            phase: if begin { Phase::Begin } else { Phase::End },
                            name: names.get(&tool_id).cloned().unwrap_or_default(),
                            status: if begin {
                                Status::Running
                            } else if event["type"] == "session.tool.success" {
                                Status::Succeeded
                            } else {
                                Status::Failed
                            },
                            summary: None,
                            ext: Default::default(),
                        })
                        .await
                        .map_err(|_| failure("Runtime receiver closed"))?;
                    }
                    "session.execution.succeeded" => break 'events SessionTurnOutcome::Complete,
                    "session.execution.interrupted" => break 'events SessionTurnOutcome::Cancelled,
                    "session.execution.failed"
                        if s.bridge.model_requests.load(Ordering::Acquire)
                            > s.bridge.max_requests.load(Ordering::Acquire) =>
                    {
                        break 'events SessionTurnOutcome::MaxTurns;
                    }
                    "session.execution.failed" => {
                        let mut error = data["error"].clone();
                        sanitize(
                            &mut error,
                            &[&s.bridge.model_key, &s.bridge.token, &s.api.password],
                        );
                        return Err(failure(&format!("OpenCode execution failed: {error}")));
                    }
                    _ => {}
                }
            }
        }
    };
    if !pending_text.is_empty() {
        let mut delta = Value::String(pending_text);
        sanitize(
            &mut delta,
            &[&s.bridge.model_key, &s.bridge.token, &s.api.password],
        );
        sequence += 1;
        tx.send(RuntimeEvent::OutputDelta {
            stream_id: r.turn_id.clone(),
            sequence,
            delta: delta.as_str().unwrap_or("").into(),
        })
        .await
        .map_err(|_| failure("Runtime receiver closed"))?;
    }
    let outcome = if limit_interrupted
        || (outcome == SessionTurnOutcome::Complete
            && s.bridge.limit_reached.load(Ordering::Acquire))
    {
        SessionTurnOutcome::MaxTurns
    } else {
        outcome
    };
    if outcome == SessionTurnOutcome::Cancelled {
        return Ok(outcome);
    }
    s.check_logs().await?;
    Ok(outcome)
}

#[cfg(test)]
mod tests;
