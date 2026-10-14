//! Experimental, administrator-only Docker tool backend. No host mounts; opt-in idle filesystem snapshots.
mod bootstrap;
mod operations;
use bootstrap::{bootstrapped, BOOTSTRAP};
mod snapshots;

use crate::OperationAttach;
use async_trait::async_trait;
use operation_protocol::OperationBackend;
use provider_protocol::*;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const LABEL: &str = "io.xgovernor.docker-prototype";
const OWNER: &str = "io.xgovernor.owner";
const REQUEST: &str = "io.xgovernor.request";
const WORKSPACE: &str = "/workspace";

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DockerNetworkMode {
    None,
    Default,
    Bridge,
}

impl Default for DockerNetworkMode {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DockerProviderOptions {
    #[serde(default)]
    pub workspace_root: Option<String>,
    #[serde(default)]
    pub network_mode: DockerNetworkMode,
    /// Install the bundled helper and prepare workspace ownership in the container.
    #[serde(default)]
    pub bootstrap: bool,
}

impl DockerProviderOptions {
    /// Parse the shared admission/provider contract. Paths refer to the container,
    /// never to a host mount, and remain unchanged through attach and restore.
    pub fn parse(value: &Value) -> Result<Self, ProviderControlError> {
        let options: Self = serde_json::from_value(if value.is_null() {
            json!({})
        } else {
            value.clone()
        })
        .map_err(|error| invalid(format!("invalid Docker provider_options: {error}")))?;
        validate_workspace_root(options.workspace_root())?;
        if options.bootstrap && options.workspace_root() == "/" {
            return Err(invalid("Docker bootstrap cannot use / as workspace_root"));
        }
        Ok(options)
    }

    pub fn workspace_root(&self) -> &str {
        self.workspace_root.as_deref().unwrap_or(WORKSPACE)
    }
}

fn validate_workspace_root(root: &str) -> Result<(), ProviderControlError> {
    if !root.starts_with('/')
        || root.chars().any(char::is_control)
        || (root != "/"
            && root[1..]
                .split('/')
                .any(|part| matches!(part, "" | "." | "..")))
    {
        return Err(invalid(
            "Docker workspace_root must be a normalized absolute container directory",
        ));
    }
    Ok(())
}

fn validate_requested_workspace(value: &Value, root: &str) -> Result<(), ProviderControlError> {
    if value["Config"]["WorkingDir"].as_str() != Some(root) {
        return Err(invalid(
            "existing container workspace differs from the requested Docker workspace_root",
        ));
    }
    Ok(())
}

mod config;
mod control;
pub use config::{DockerConfig, DockerLimits};
use control::{Control, Journal};

fn invalid(message: impl ToString) -> ProviderControlError {
    ProviderControlError::InvalidRequest {
        message: message.to_string(),
    }
}
fn transport(message: impl ToString) -> ProviderControlError {
    ProviderControlError::Transport {
        message: message.to_string(),
    }
}
fn unsupported(capability: &str) -> ProviderControlError {
    ProviderControlError::UnsupportedCapability {
        provider: ProviderKind("docker".into()),
        capability: capability.into(),
    }
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|s| s.to_string()).collect()
}

fn capabilities(snapshots: bool) -> ProviderCapabilities {
    ProviderCapabilities {
        lifecycle: [ProviderCapability::ResourceLimits]
            .into_iter()
            .chain(snapshots.then_some(ProviderCapability::Snapshot))
            .chain(snapshots.then_some(ProviderCapability::Pause))
            .collect(),
        operation_plane: ProviderOperationCapabilities {
            exec: true,
            file_read: true,
            file_write: true,
            search: true,
            export_file: true,
            network: true,
            ..Default::default()
        },
    }
}

#[derive(Clone)]
pub struct DockerProvider {
    config: Arc<DockerConfig>,
    kind: ProviderKind,
    creation_lock: Arc<tokio::sync::Mutex<()>>,
    journal: Arc<Journal>,
    snapshots: Arc<snapshots::SnapshotStore>,
    snapshot_lock: Arc<tokio::sync::Mutex<()>>,
    controls: Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<Control>>>>,
}
impl DockerProvider {
    pub fn new(config: DockerConfig) -> Result<Self, ProviderControlError> {
        config.validate()?;
        let journal = Arc::new(Journal::open(&config)?);
        Ok(Self {
            snapshots: Arc::new(snapshots::SnapshotStore::open(&config)?),
            snapshot_lock: Default::default(),
            journal,
            controls: Default::default(),
            config: Arc::new(config),
            kind: ProviderKind("docker".into()),
            creation_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    fn control(&self, id: &str) -> Result<Arc<Control>, ProviderControlError> {
        let mut controls = self.controls.lock().unwrap();
        if let Some(control) = controls.get(id) {
            return Ok(control.clone());
        }
        let control = Arc::new(Control::new(
            self.config.clone(),
            self.journal.clone(),
            id.into(),
        )?);
        controls.insert(id.into(), control.clone());
        Ok(control)
    }

    fn validate_container(
        &self,
        value: &Value,
        expected_image: Option<&Value>,
    ) -> Result<(), ProviderControlError> {
        validate_workspace_root(value["Config"]["WorkingDir"].as_str().unwrap_or(""))?;
        let h = &value["HostConfig"];
        let c = &value["Config"];
        let l = &self.config.limits;
        let has = |v: &Value, s: &str| v.as_array().is_some_and(|a| a.iter().any(|x| x == s));
        if expected_image.is_some_and(|image| image != &value["Image"])
            || c["User"] != "10001:10001"
            || h["Privileged"] != false
            || h["Init"] != true
            || !has(&h["CapDrop"], "ALL")
            || !has(&h["SecurityOpt"], "no-new-privileges:true")
            || if bootstrapped(value) {
                h["CapAdd"] != json!(["CHOWN"])
            } else {
                h["CapAdd"].as_array().is_some_and(|a| !a.is_empty())
            }
            || value["Mounts"].as_array().is_none_or(|a| !a.is_empty())
            || h["PortBindings"].as_object().is_some_and(|a| !a.is_empty())
            || !matches!(
                h["NetworkMode"].as_str(),
                Some("none" | "default" | "bridge")
            )
            || h["PidMode"] != ""
            || h["IpcMode"] != "private"
            || h["Devices"].as_array().is_some_and(|a| !a.is_empty())
            || h["DeviceRequests"]
                .as_array()
                .is_some_and(|a| !a.is_empty())
            || h["NanoCpus"].as_u64() != Some(l.cpus as u64 * 1_000_000_000)
            || h["Memory"].as_u64() != Some(l.memory_mb * 1024 * 1024)
            || h["MemorySwap"] != h["Memory"]
            || h["PidsLimit"].as_u64() != Some(l.pids as u64)
            || h["LogConfig"]["Type"] != "local"
            || h["LogConfig"]["Config"]["max-size"] != "5m"
            || h["LogConfig"]["Config"]["max-file"] != "2"
        {
            return Err(invalid(
                "container configuration differs from the controlled Docker policy",
            ));
        }
        Ok(())
    }

    /// Called by the assembly layer every monitor interval. Never touches another deployment.
    pub async fn maintenance(&self) -> Result<(), ProviderControlError> {
        let low_disk = self.config.free_bytes().await? < self.config.limits.reserve_bytes;
        for instance in self.list_instances().await? {
            let id = &instance.instance_id.0;
            let control = self.control(id)?;
            let state = control.state()?;
            if state == "closing" {
                self.config.checked(args(&["rm", "-f", id])).await?;
                control.set("closed")?;
                continue;
            }
            let output = self
                .config
                .checked(args(&["container", "inspect", "--size", id]))
                .await?;
            let values: Vec<Value> = serde_json::from_slice(&output.stdout).map_err(transport)?;
            let value = &values[0];
            self.validate_container(value, instance.metadata.get("image_id"))?;
            if low_disk
                || value["SizeRw"].as_u64().unwrap_or(u64::MAX) > self.config.limits.disk_bytes
            {
                control.set("resource_stopped")?;
                control.invalidate();
                let _guard = control.gate.lock().await;
                self.config
                    .checked(args(&["stop", "--time", "0", id]))
                    .await?;
                continue;
            }
            if state == "resource_stopped" {
                continue;
            }
            if state == "ready" && value["State"]["Running"] != true {
                let _guard = control.gate.lock().await;
                if control.state()? == "ready" {
                    self.config.checked(args(&["start", id])).await?;
                }
            }
            // Do not interfere with live requests; crash leftovers are retired at attach.
            if state == "cleanup" {
                let _guard = control.gate.lock().await;
                control.recover().await?;
            }
        }
        self.reconcile_snapshots().await?;
        Ok(())
    }

    /// Only reclaim an old, journalled create if the authoritative provider ledger
    /// has no owner for it. Never infer deletion from a Docker transport error.
    pub async fn cleanup_unbound(&self, bound: &[String]) -> Result<(), ProviderControlError> {
        let _creation = self.creation_lock.lock().await;
        for instance in self.list_instances().await? {
            if bound.contains(&instance.instance_id.0) {
                continue;
            }
            let name = instance.metadata["container_name"]
                .as_str()
                .unwrap_or("")
                .trim_start_matches('/');
            if self
                .journal
                .abandoned_create(name, now_ms().saturating_sub(300_000))?
            {
                self.delete(ProviderDeleteRequest {
                    backend_id: instance.backend_id,
                    instance_id: Some(instance.instance_id),
                    snapshot_id: None,
                    reason: ProviderLifecycleReason::ErrorCleanup,
                    correlation: json!(null),
                })
                .await?;
                self.journal.set(name, "closed", 0, json!({}))?;
            }
        }
        Ok(())
    }

    pub async fn preflight(&self) -> Result<(), ProviderControlError> {
        if self.config.limits.snapshot_enabled
            && !std::path::Path::new("/usr/bin/python3").is_file()
        {
            return Err(invalid(
                "Docker snapshots require host /usr/bin/python3 (Linux Python 3)",
            ));
        }
        let out = self
            .config
            .checked(args(&["info", "--format", "{{json .}}"]))
            .await?;
        let info: Value = serde_json::from_slice(&out.stdout).map_err(transport)?;
        if info["OSType"] != "linux"
            || info["CgroupVersion"] != "2"
            || info["MemoryLimit"] != true
            || info["PidsLimit"] != true
            || info["CpuCfsQuota"] != true
        {
            return Err(invalid(
                "Docker prototype requires Linux, cgroup v2, memory and PID limits",
            ));
        }
        self.config
            .checked(args(&["image", "inspect", &self.config.image]))
            .await?;
        Ok(())
    }

    async fn raw_inspect(&self, id: &str) -> Result<Value, ProviderControlError> {
        if id.is_empty() || id.starts_with('-') {
            return Err(invalid("invalid container identity"));
        }
        let out = self
            .config
            .run(&args(&["container", "inspect", id]), None, 30_000)
            .await?;
        if !out.status.success() {
            let message = String::from_utf8_lossy(&out.stderr);
            if message.contains("No such container:") || message.contains("No such object:") {
                return Err(ProviderControlError::NotFound {
                    resource_ref: id.into(),
                });
            }
            return Err(transport(message));
        }
        let values: Vec<Value> = serde_json::from_slice(&out.stdout).map_err(transport)?;
        let value = values
            .into_iter()
            .next()
            .ok_or_else(|| transport("empty Docker inspect"))?;
        if value["Config"]["Labels"][LABEL].as_str() != Some(&self.config.deployment) {
            return Err(invalid("container belongs to another deployment"));
        }
        Ok(value)
    }

    fn instance(&self, value: &Value) -> Result<ProviderInstance, ProviderControlError> {
        let id = value["Id"]
            .as_str()
            .ok_or_else(|| transport("missing container ID"))?;
        Ok(ProviderInstance {
            backend_id: BackendId("docker".into()),
            provider: self.kind.clone(),
            instance_id: ProviderInstanceId(id.into()),
            state: if value["State"]["Running"] == true {
                ProviderLifecycleState::Active
            } else if self.control(id)?.state()? == "paused" {
                ProviderLifecycleState::Paused
            } else {
                ProviderLifecycleState::Failed
            },
            endpoint: None,
            snapshot: None,
            capabilities: capabilities(self.config.limits.snapshot_enabled),
            resources: ProviderResourceAllocation {
                vcpu_count: Some(self.config.limits.cpus),
                memory_mb: Some(self.config.limits.memory_mb),
                disk_mb: None,
            },
            metadata: json!({"workspace_root": value["Config"]["WorkingDir"], "image_id": value["Image"], "deployment": self.config.deployment,
                "owner_ref": value["Config"]["Labels"][OWNER], "request_id": value["Config"]["Labels"][REQUEST],
                "pids_limit": self.config.limits.pids, "container_name": value["Name"], "effective_limits": self.config.limits, "helper_protocol": 2}),
            created_at_ms: now_ms(),
            updated_at_ms: now_ms(),
        })
    }
}

impl DockerProvider {
    fn validate_request_options(
        provider_options: &Value,
        resource_limits: &ProviderResourceLimits,
    ) -> Result<DockerProviderOptions, ProviderControlError> {
        let options = DockerProviderOptions::parse(provider_options)?;
        if *resource_limits != ProviderResourceLimits::default() {
            return Err(invalid("prototype resources are fixed by the server"));
        }
        Ok(options)
    }
    async fn create_container(
        &self,
        request: ProviderCreateRequest,
        source_image: Option<&str>,
    ) -> Result<ProviderInstance, ProviderControlError> {
        if request.backend_id.0 != "docker" {
            return Err(invalid("Docker backend_id must be docker"));
        }
        let options =
            Self::validate_request_options(&request.provider_options, &request.resource_limits)?;
        let network_mode = options.network_mode;
        let workspace = options.workspace_root();
        if self.config.free_bytes().await? < self.config.limits.reserve_bytes {
            return Err(transport("Docker filesystem reserve exhausted"));
        }
        let request_id = request
            .correlation
            .get("request_id")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("stable request_id required"))?;
        let hash = format!(
            "{:x}",
            Sha256::digest(format!(
                "{}:{}:{}",
                self.config.deployment, request.owner_ref, request_id
            ))
        );
        let name = format!("xg-{}-{}", self.config.deployment, &hash[..24]);
        match self.raw_inspect(&name).await {
            Ok(value) => {
                if value["Config"]["Labels"][OWNER] != request.owner_ref
                    || value["Config"]["Labels"][REQUEST] != request_id
                {
                    return Err(invalid("creation identity mismatch"));
                }
                self.validate_container(&value, None)?;
                validate_requested_network_mode(&value, network_mode)?;
                validate_requested_workspace(&value, workspace)?;
                if bootstrapped(&value) != options.bootstrap {
                    return Err(invalid("existing container bootstrap differs from request"));
                }
                let id = value["Id"]
                    .as_str()
                    .ok_or_else(|| transport("missing container ID"))?;
                if matches!(
                    self.control(id)?.state()?.as_str(),
                    "resource_stopped" | "closing" | "closed" | "paused" | "snapshotting"
                ) {
                    return Err(transport("container is stopped by lifecycle protection"));
                }
                if value["State"]["Running"] != true {
                    self.config.checked(args(&["start", &name])).await?;
                }
                if options.bootstrap && self.journal.state(&name)? != "created" {
                    self.bootstrap_container(&name, workspace).await?;
                    self.journal.set(&name, "created", 0, json!({"id":id}))?;
                }
                return self.instance(&self.raw_inspect(&name).await?);
            }
            Err(ProviderControlError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        let existing = self.list_instances().await?;
        if existing.len() >= self.config.limits.max_containers {
            return Err(ProviderControlError::ResourceLimitExceeded {
                provider: self.kind.clone(),
                owner_ref: request.owner_ref,
                current: existing.len(),
                max: self.config.limits.max_containers,
            });
        }
        let image = self
            .config
            .checked(args(&[
                "image",
                "inspect",
                "--format",
                "{{.Id}}",
                source_image.unwrap_or(&self.config.image),
            ]))
            .await?;
        let image_id = String::from_utf8_lossy(&image.stdout).trim().to_string();
        self.journal.set(&name,"creating",0,json!({"owner":request.owner_ref,"request_id":request_id,"image_id":image_id,"limits":self.config.limits}))?;
        let mut create = args(&[
            "create",
            "--name",
            &name,
            "--label",
            &format!("{LABEL}={}", self.config.deployment),
            "--label",
            &format!("{OWNER}={}", request.owner_ref),
            "--label",
            &format!("{REQUEST}={request_id}"),
            "--label",
            &format!(
                "{BOOTSTRAP}={}",
                if options.bootstrap { "v1" } else { "none" }
            ),
            "--cpus",
            &self.config.limits.cpus.to_string(),
            "--memory",
            &format!("{}m", self.config.limits.memory_mb),
            "--memory-swap",
            &format!("{}m", self.config.limits.memory_mb),
            "--pids-limit",
            &self.config.limits.pids.to_string(),
            "--init",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--user",
            "10001:10001",
            "--workdir",
            workspace,
            "--log-driver",
            "local",
            "--log-opt",
            "max-size=5m",
            "--log-opt",
            "max-file=2",
            "--entrypoint",
            "/usr/bin/sleep",
            "--env",
            "HOME=/home/agent",
            &image_id,
            "infinity",
        ]);
        if options.bootstrap {
            let insert_at = create.iter().position(|arg| arg == "--cpus").unwrap();
            create.splice(insert_at..insert_at, ["--cap-add".into(), "CHOWN".into()]);
        }
        if network_mode != DockerNetworkMode::Default {
            let network_arg = match network_mode {
                DockerNetworkMode::None => "none",
                DockerNetworkMode::Bridge => "bridge",
                DockerNetworkMode::Default => unreachable!(),
            };
            let insert_at = create
                .iter()
                .position(|arg| arg == "--cpus")
                .expect("create command always has resource flags");
            create.splice(
                insert_at..insert_at,
                ["--network".into(), network_arg.into()],
            );
        }
        if let Err(error) = self.config.checked(create).await {
            // A lost create response is resolved by the same stable name on retry.
            match self.raw_inspect(&name).await {
                Ok(_) => {}
                Err(_) => return Err(error),
            }
        }
        if let Err(error) = self.config.checked(args(&["start", &name])).await {
            let _ = self.config.checked(args(&["rm", "-f", &name])).await;
            return Err(error);
        }
        let value = self.raw_inspect(&name).await?;
        if let Err(error) = self.validate_container(&value, Some(&Value::String(image_id))) {
            let _ = self.config.checked(args(&["rm", "-f", &name])).await;
            return Err(error);
        }
        if let Err(error) = validate_requested_network_mode(&value, network_mode)
            .and_then(|_| validate_requested_workspace(&value, workspace))
        {
            let _ = self.config.checked(args(&["rm", "-f", &name])).await;
            return Err(error);
        }
        if options.bootstrap && source_image.is_none() {
            if let Err(error) = self.bootstrap_container(&name, workspace).await {
                let _ = self.config.checked(args(&["rm", "-f", &name])).await;
                return Err(error);
            }
        }
        self.journal
            .set(&name, "created", 0, json!({"id":value["Id"]}))?;
        self.instance(&value)
    }
}

fn validate_requested_network_mode(
    value: &Value,
    requested: DockerNetworkMode,
) -> Result<(), ProviderControlError> {
    let actual = value["HostConfig"]["NetworkMode"].as_str();
    let matches = match requested {
        DockerNetworkMode::None => actual == Some("none"),
        // Docker's daemon default is represented as `default` or `bridge`
        // depending on the engine version and API path.
        DockerNetworkMode::Default => matches!(actual, Some("default" | "bridge")),
        DockerNetworkMode::Bridge => actual == Some("bridge"),
    };
    if matches {
        Ok(())
    } else {
        Err(invalid(
            "existing container network mode differs from the requested Docker network_mode",
        ))
    }
}

impl Provider for DockerProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }
    fn lifecycle(&self) -> &dyn ProviderLifecycle {
        self
    }
}

#[async_trait]
impl ProviderLifecycle for DockerProvider {
    async fn create(
        &self,
        request: ProviderCreateRequest,
    ) -> Result<ProviderInstance, ProviderControlError> {
        let _creation = self.creation_lock.lock().await;
        self.create_container(request, None).await
    }
    async fn load(
        &self,
        request: ProviderLoadRequest,
    ) -> Result<ProviderInstance, ProviderControlError> {
        self.load_snapshot(request).await
    }
    async fn pause(
        &self,
        request: ProviderPauseRequest,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        if request.mode != ProviderPauseMode::Snapshot {
            return Err(unsupported("Suspend"));
        }
        self.capture_snapshot(
            request.backend_id,
            request.instance_id,
            request.correlation,
            true,
        )
        .await
    }
    async fn checkpoint(
        &self,
        request: ProviderCheckpointRequest,
    ) -> Result<ProviderSnapshot, ProviderControlError> {
        self.capture_snapshot(
            request.backend_id,
            request.instance_id,
            request.correlation,
            false,
        )
        .await
    }
    async fn delete(
        &self,
        request: ProviderDeleteRequest,
    ) -> Result<ProviderDeleteOutcome, ProviderControlError> {
        if request.instance_id.is_none() && request.snapshot_id.is_none() {
            if let Some(id) = request.correlation["abort_load_request"].as_str() {
                self.abort_snapshot_load(id).await?;
                return Ok(ProviderDeleteOutcome {
                    backend_id: request.backend_id,
                    provider: self.kind.clone(),
                    instance_id: None,
                    deleted: true,
                    retained_snapshots: vec![],
                    deleted_snapshots: vec![],
                    correlation: request.correlation,
                });
            }
        }
        if let Some(snapshot) = &request.snapshot_id {
            if request.instance_id.is_some() {
                return Err(invalid("delete one resource at a time"));
            }
            self.retire_snapshot(&snapshot.0).await?;
            return Ok(ProviderDeleteOutcome {
                backend_id: request.backend_id,
                provider: self.kind.clone(),
                instance_id: None,
                deleted: true,
                retained_snapshots: vec![],
                deleted_snapshots: vec![snapshot.clone()],
                correlation: request.correlation,
            });
        }
        let id = request
            .instance_id
            .as_ref()
            .ok_or_else(|| invalid("instance_id required"))?;
        let control = self.control(&id.0)?;
        control.set("closing")?;
        control.invalidate();
        let _guard = control.gate.lock().await;
        let deleted = match self.raw_inspect(&id.0).await {
            Ok(_) => {
                self.config.checked(args(&["rm", "-f", &id.0])).await?;
                true
            }
            Err(ProviderControlError::NotFound { .. }) => false,
            Err(error) => return Err(error),
        };
        control.set("closed")?;
        Ok(ProviderDeleteOutcome {
            backend_id: request.backend_id,
            provider: self.kind.clone(),
            instance_id: request.instance_id,
            deleted,
            retained_snapshots: vec![],
            deleted_snapshots: vec![],
            correlation: request.correlation,
        })
    }
    async fn inspect(
        &self,
        request: ProviderInspectRequest,
    ) -> Result<ProviderInstanceStatus, ProviderControlError> {
        let id = request
            .instance_id
            .ok_or_else(|| invalid("instance_id required"))?;
        let i = self.instance(&self.raw_inspect(&id.0).await?)?;
        Ok(ProviderInstanceStatus {
            backend_id: i.backend_id,
            provider: i.provider,
            instance_id: Some(i.instance_id),
            state: i.state,
            endpoint: i.endpoint,
            snapshot: i.snapshot,
            capabilities: i.capabilities,
            resources: i.resources,
            last_error: None,
            metadata: i.metadata,
            updated_at_ms: now_ms(),
        })
    }
    async fn list_instances(&self) -> Result<Vec<ProviderInstance>, ProviderControlError> {
        let out = self
            .config
            .checked(args(&[
                "ps",
                "-aq",
                "--filter",
                &format!("label={LABEL}={}", self.config.deployment),
            ]))
            .await?;
        let mut result = vec![];
        for id in String::from_utf8_lossy(&out.stdout).lines() {
            match self.raw_inspect(id).await {
                Ok(value) => result.push(self.instance(&value)?),
                Err(ProviderControlError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }
}

#[async_trait]
impl OperationAttach for DockerProvider {
    async fn attach(
        &self,
        instance: &ProviderInstance,
    ) -> Result<Arc<dyn OperationBackend>, ProviderControlError> {
        let value = self.raw_inspect(&instance.instance_id.0).await?;
        self.validate_container(&value, instance.metadata.get("image_id"))?;
        let workspace = instance.metadata["workspace_root"]
            .as_str()
            .unwrap_or(WORKSPACE);
        validate_requested_workspace(&value, workspace)?;
        let control = self.control(&instance.instance_id.0)?;
        let _guard = control.gate.lock().await;
        let state = control.state()?;
        if matches!(
            state.as_str(),
            "closing" | "closed" | "resource_stopped" | "paused" | "snapshotting"
        ) {
            return Err(transport(format!("container is {state}")));
        }
        if matches!(state.as_str(), "executing" | "cleanup") {
            control.invalidate();
            control.recover().await?;
        } else if value["State"]["Running"] != true {
            self.config
                .checked(args(&["start", &instance.instance_id.0]))
                .await?;
        }
        drop(_guard);
        let backend = operations::DockerBackend::new(
            self.config.clone(),
            instance.instance_id.0.clone(),
            workspace.into(),
            control,
        );
        backend.health().await.map_err(transport)?;
        Ok(Arc::new(backend))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn workspace_options_validate_container_paths_and_keep_default() {
        assert_eq!(
            DockerProviderOptions::parse(&Value::Null)
                .unwrap()
                .workspace_root(),
            "/workspace"
        );
        for root in ["/", "/testbed", "/opt/project with spaces", "/代码"] {
            let options = DockerProviderOptions::parse(
                &json!({"workspace_root": root, "network_mode": "none"}),
            )
            .unwrap();
            assert_eq!(options.workspace_root(), root);
            assert_eq!(options.network_mode, DockerNetworkMode::None);
        }
        for root in [
            "",
            "relative",
            "~/code",
            "/tmp/../code",
            "/tmp/./code",
            "//code",
            "/code/",
            "/code\n",
            "/code\0",
        ] {
            assert!(
                DockerProviderOptions::parse(&json!({"workspace_root":root})).is_err(),
                "{root:?}"
            );
        }
        for options in [
            json!({"workspace_root":1}),
            json!({"privileged":true}),
            json!({"network_mode":false}),
        ] {
            assert!(DockerProviderOptions::parse(&options).is_err());
        }
        assert!(
            DockerProviderOptions::parse(&json!({"workspace_root":"/", "bootstrap":true})).is_err()
        );
        assert!(DockerProviderOptions::parse(&json!({"bootstrap":"true"})).is_err());
        assert!(!DockerProviderOptions::parse(&json!({})).unwrap().bootstrap);
        assert!(
            DockerProviderOptions::parse(&json!({"bootstrap":true}))
                .unwrap()
                .bootstrap
        );
        let value = json!({"Config":{"WorkingDir":"/testbed"}});
        assert!(validate_requested_workspace(&value, "/testbed").is_ok());
        assert!(validate_requested_workspace(&value, "/workspace").is_err());
    }

    #[test]
    fn rejects_remote_socket_and_unsafe_deployment() {
        let mut c = DockerConfig {
            executable: "docker".into(),
            socket: "tcp://host".into(),
            image: "test:v1".into(),
            deployment: "test".into(),
            limits: DockerLimits::default(),
            state_dir: std::env::temp_dir().join("xg-config-test"),
        };
        assert!(c.validate().is_err());
        c.socket = "/var/run/docker.sock".into();
        assert!(c.validate().is_ok());
        c.deployment = "../other".into();
        assert!(c.validate().is_err());
    }
}
