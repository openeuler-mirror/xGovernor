//! Experimental, administrator-only Docker tool backend. No host mounts; opt-in idle filesystem snapshots.
mod operations;
mod snapshots;

use crate::OperationAttach;
use async_trait::async_trait;
use operation_protocol::OperationBackend;
use provider_protocol::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const LABEL: &str = "io.xgovernor.docker-prototype";
const OWNER: &str = "io.xgovernor.owner";
const REQUEST: &str = "io.xgovernor.request";
const WORKSPACE: &str = "/workspace";

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
        let h = &value["HostConfig"];
        let c = &value["Config"];
        let l = &self.config.limits;
        let has = |v: &Value, s: &str| v.as_array().is_some_and(|a| a.iter().any(|x| x == s));
        if expected_image.is_some_and(|image| image != &value["Image"])
            || c["User"] != "10001:10001"
            || c["WorkingDir"] != WORKSPACE
            || h["Privileged"] != false
            || h["Init"] != true
            || !has(&h["CapDrop"], "ALL")
            || !has(&h["SecurityOpt"], "no-new-privileges:true")
            || h["CapAdd"].as_array().is_some_and(|a| !a.is_empty())
            || value["Mounts"].as_array().is_none_or(|a| !a.is_empty())
            || h["PortBindings"].as_object().is_some_and(|a| !a.is_empty())
            || !matches!(h["NetworkMode"].as_str(), Some("default" | "bridge"))
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
            metadata: json!({"workspace_root": WORKSPACE, "image_id": value["Image"], "deployment": self.config.deployment,
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
    ) -> Result<(), ProviderControlError> {
        if let Some(options) = provider_options.as_object() {
            if options.keys().any(|key| key != "workspace_root")
                || options
                    .get("workspace_root")
                    .is_some_and(|root| root != WORKSPACE)
            {
                return Err(invalid("Docker prototype accepts only the fixed /workspace root; runtime Docker options are forbidden"));
            }
        } else if !provider_options.is_null() {
            return Err(invalid("provider_options must be an object"));
        }
        if *resource_limits != ProviderResourceLimits::default() {
            return Err(invalid("prototype resources are fixed by the server"));
        }
        Ok(())
    }
    async fn create_container(
        &self,
        request: ProviderCreateRequest,
        source_image: Option<&str>,
    ) -> Result<ProviderInstance, ProviderControlError> {
        if request.backend_id.0 != "docker" {
            return Err(invalid("Docker backend_id must be docker"));
        }
        Self::validate_request_options(&request.provider_options, &request.resource_limits)?;
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
        let create = args(&[
            "create",
            "--name",
            &name,
            "--label",
            &format!("{LABEL}={}", self.config.deployment),
            "--label",
            &format!("{OWNER}={}", request.owner_ref),
            "--label",
            &format!("{REQUEST}={request_id}"),
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
            WORKSPACE,
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
        self.journal
            .set(&name, "created", 0, json!({"id":value["Id"]}))?;
        self.instance(&value)
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
