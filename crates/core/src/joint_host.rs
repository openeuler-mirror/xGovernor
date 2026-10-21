//! Request-bound Host access and durable, backend-neutral bookkeeping.
use crate::{
    JointAdmission, JointBinding, JointNativeIntent, JointOperationContext, NativeOperationState,
    SessionDomainError,
};
use async_trait::async_trait;
use operation_protocol::{
    capability::{exec::ExecRequest, filesystem::WriteMode},
    OperationBackend,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub fn host_digest(value: &impl Serialize) -> String {
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).expect("Host serializable value"))
    )
}
pub fn host_bytes_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn host_invalid(message: &str) -> SessionDomainError {
    SessionDomainError::InvalidRequest {
        message: message.into(),
    }
}
pub fn host_unavailable(message: &str) -> SessionDomainError {
    SessionDomainError::Unavailable {
        message: message.into(),
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostPathPolicyV1 {
    pub workspace_backend_root: String,
    pub fs_relative_workspace: String,
    pub home_relative: Option<String>,
    pub mapping_version: u32,
}
impl HostPathPolicyV1 {
    pub fn resolve(&self, raw: &str) -> Result<String, SessionDomainError> {
        fn relative(path: &str) -> bool {
            !path.starts_with('/')
                && !path.contains('\0')
                && !path.split('/').any(|p| p == ".." || p.starts_with('~'))
        }
        if self.mapping_version != 1
            || !self.workspace_backend_root.starts_with('/')
            || self.workspace_backend_root.contains('\0')
            || self.workspace_backend_root.split('/').any(|p| p == "..")
            || !relative(&self.fs_relative_workspace)
            || self.home_relative.as_deref().is_some_and(|p| !relative(p))
        {
            return Err(host_invalid("invalid fixed workspace mapping"));
        }
        if let Some(home) = &self.home_relative {
            let workspace = self.fs_relative_workspace.trim_end_matches('/');
            if workspace != "."
                && !workspace.is_empty()
                && home != workspace
                && !home.starts_with(&format!("{workspace}/"))
            {
                return Err(crate::unsupported_joint("home_outside_workspace_mapping"));
            }
        }
        let root = self.workspace_backend_root.trim_end_matches('/');
        let (base, suffix) = if raw.starts_with('/') {
            (
                &self.fs_relative_workspace,
                raw.strip_prefix(root)
                    .filter(|s| s.is_empty() || s.starts_with('/'))
                    .ok_or_else(|| host_invalid("path is outside workspace mapping"))?
                    .trim_start_matches('/'),
            )
        } else if raw == "~" || raw.starts_with("~/") {
            (
                self.home_relative
                    .as_ref()
                    .ok_or_else(|| host_invalid("HOME is not configured"))?,
                raw.strip_prefix('~').unwrap().trim_start_matches('/'),
            )
        } else {
            (&self.fs_relative_workspace, raw)
        };
        if !relative(suffix) {
            return Err(host_invalid("invalid logical path"));
        }
        let joined = format!("{base}/{suffix}")
            .split('/')
            .filter(|s| !s.is_empty() && *s != ".")
            .collect::<Vec<_>>()
            .join("/");
        Ok(if joined.is_empty() {
            ".".into()
        } else {
            joined
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostCapabilityPolicyV1 {
    pub schema_version: u32,
    pub configuration_revision: String,
    pub deployment_id: String,
    pub backend_release_ref: String,
    pub sdk_fingerprint: String,
    pub frozen_host_config_digest: String,
    pub evidence_refs: Vec<String>,
    pub paths: HostPathPolicyV1,
    pub exec: bool,
    pub read: bool,
    pub overwrite: bool,
    pub max_timeout_ms: u64,
    pub max_output_bytes: u32,
    pub max_argv_items: usize,
    pub max_argv_bytes: usize,
    pub max_file_bytes: u32,
    pub max_inflight: u32,
    pub max_reserved_bytes: u64,
    pub mkdir_timeout_ms: u64,
    pub exited_settlement_declared: bool,
    pub timeout_settlement_declared: bool,
}
impl HostCapabilityPolicyV1 {
    pub fn validate(&self) -> Result<(), SessionDomainError> {
        if self.schema_version != 1
            || self.configuration_revision.is_empty()
            || self.deployment_id.is_empty()
            || self.backend_release_ref.is_empty()
            || self.sdk_fingerprint.is_empty()
            || self.evidence_refs.is_empty()
            || !(1..=300_000).contains(&self.max_timeout_ms)
            || !(1..=65536).contains(&self.max_output_bytes)
            || !(1..=256).contains(&self.max_argv_items)
            || !(1..=32768).contains(&self.max_argv_bytes)
            || !(1..=67_108_864).contains(&self.max_file_bytes)
            || self.max_inflight == 0
            || self.max_reserved_bytes == 0
        {
            return Err(host_invalid("unsupported Host policy or limits"));
        }
        self.paths.resolve("")?;
        Ok(())
    }
    pub fn preflight(
        &self,
        request: &JointHostRequest,
    ) -> Result<HostCanonicalRequestV1, SessionDomainError> {
        self.validate()?;
        let unsupported = || crate::unsupported_joint("host_request");
        match request {
            JointHostRequest::Exec {
                request,
                required_output_bytes,
            } => {
                if !self.exec
                    || request.env.as_ref().is_some_and(|e| !e.is_empty())
                    || request.extra.is_some()
                    || request.shell.is_some()
                    || required_output_bytes.is_some_and(|n| n > self.max_output_bytes)
                {
                    return Err(unsupported());
                }
                let argv: Vec<String> = std::iter::once(request.command.clone())
                    .chain(request.args.clone())
                    .collect();
                let timeout_ms = request.timeout_ms.unwrap_or(30_000);
                if argv[0].trim().is_empty()
                    || argv.iter().any(|a| a.contains('\0'))
                    || argv.len() > self.max_argv_items
                    || argv.iter().map(String::len).sum::<usize>() > self.max_argv_bytes
                    || !(1..=self.max_timeout_ms).contains(&timeout_ms)
                {
                    return Err(unsupported());
                }
                let cwd = self
                    .paths
                    .resolve(request.cwd.as_ref().map_or("", |p| &p.0))?;
                Ok(HostCanonicalRequestV1::Exec {
                    argv,
                    cwd,
                    timeout_ms,
                    max_output_bytes: self.max_output_bytes,
                })
            }
            JointHostRequest::Read { path } if self.read => Ok(HostCanonicalRequestV1::Read {
                path: self.paths.resolve(path)?,
                max_bytes: self.max_file_bytes,
            }),
            JointHostRequest::Write {
                path,
                content,
                mode,
                create_parents,
            } if self.overwrite && *mode == WriteMode::Overwrite => {
                if content.len() > self.max_file_bytes as usize {
                    return Err(unsupported());
                }
                let path = self.paths.resolve(path)?;
                let mkdir = if *create_parents {
                    let parent = path.rsplit_once('/').map_or(".", |(p, _)| p);
                    let argv = vec!["mkdir".into(), "-p".into(), "--".into(), parent.into()];
                    if !self.exec
                        || !(1..=self.max_timeout_ms).contains(&self.mkdir_timeout_ms)
                        || argv.len() > self.max_argv_items
                        || argv.iter().map(String::len).sum::<usize>() > self.max_argv_bytes
                    {
                        return Err(unsupported());
                    }
                    Some(HostMkdirV1 {
                        argv,
                        cwd: ".".into(),
                        timeout_ms: self.mkdir_timeout_ms,
                        max_output_bytes: self.max_output_bytes,
                    })
                } else {
                    None
                };
                Ok(HostCanonicalRequestV1::Write {
                    path,
                    size: content.len() as u64,
                    sha256: host_bytes_digest(content),
                    max_bytes: self.max_file_bytes,
                    mkdir,
                })
            }
            _ => Err(unsupported()),
        }
    }
}
#[derive(Debug, Clone)]
pub enum JointHostRequest {
    Exec {
        request: ExecRequest,
        required_output_bytes: Option<u32>,
    },
    Read {
        path: String,
    },
    Write {
        path: String,
        content: Vec<u8>,
        mode: WriteMode,
        create_parents: bool,
    },
    Export {
        path: String,
        preferred_name: Option<String>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostMkdirV1 {
    pub argv: Vec<String>,
    pub cwd: String,
    pub timeout_ms: u64,
    pub max_output_bytes: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostCanonicalRequestV1 {
    Exec {
        argv: Vec<String>,
        cwd: String,
        timeout_ms: u64,
        max_output_bytes: u32,
    },
    Read {
        path: String,
        max_bytes: u32,
    },
    Write {
        path: String,
        size: u64,
        sha256: String,
        max_bytes: u32,
        mkdir: Option<HostMkdirV1>,
    },
}
impl HostCanonicalRequestV1 {
    pub fn operation(&self) -> &'static str {
        match self {
            Self::Exec { .. } => "host_exec",
            Self::Read { .. } => "host_read",
            Self::Write { .. } => "host_write",
        }
    }
    pub fn reserved_bytes(&self) -> u64 {
        match self {
            Self::Exec {
                max_output_bytes, ..
            } => u64::from(*max_output_bytes),
            Self::Read { max_bytes, .. } => u64::from(*max_bytes),
            Self::Write { size, .. } => *size,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostTargetV1 {
    pub schema_version: u32,
    pub public_runtime_id: String,
    pub live_binding: JointBinding,
    pub tenant_key: String,
    pub owner_ref: String,
    pub deployment_id: String,
    pub root_binding_id: String,
    pub root_id: String,
    pub native_child_id: String,
    pub branch_id: String,
    pub binding_generation: u64,
    /// Local recovery epoch; a reconnect or SDK Ready view cannot establish it.
    pub root_runtime_generation: u64,
    pub workspace_mapping_digest: String,
    pub source_checkpoint_id: Option<String>,
    pub source_pair_ref: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostPreparedV1 {
    pub context: JointOperationContext,
    pub target: HostTargetV1,
    pub policy: HostCapabilityPolicyV1,
    pub request: HostCanonicalRequestV1,
    pub native: Vec<JointNativeIntent>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostAccessTokenV1 {
    pub admission: JointAdmission,
    pub target_digest: String,
    pub request_digest: String,
    pub policy_digest: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostFileTransferV1 {
    pub relative_path: String,
    pub expected_size: Option<u64>,
    pub expected_sha256: Option<String>,
    pub observed_size: Option<u64>,
    pub observed_sha256: Option<String>,
    pub state: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostExecOutcomeV1 {
    pub reason: String,
    pub code: Option<i32>,
    pub stdout_len: u64,
    pub stderr_len: u64,
    pub stdout_digest: String,
    pub stderr_digest: String,
    pub timed_out: bool,
    pub completeness: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostObservationV1 {
    pub state: NativeOperationState,
    pub settled: bool,
    pub result: Value,
    pub transfer: Option<HostFileTransferV1>,
    pub exec: Option<HostExecOutcomeV1>,
    pub error: Option<String>,
    pub native_outcome: Option<Value>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostStepV1 {
    pub step_no: usize,
    pub method: String,
    pub original_uuid: Option<String>,
    pub canonical_params_digest: String,
    pub sdk_operation_digest: Option<String>,
    pub target_digest: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactEvidenceV1 {
    pub artifact_id: String,
    pub read_intent_id: String,
    pub public_runtime_id: String,
    pub live_binding_id: String,
    pub native_child_id: String,
    pub branch_id: String,
    pub binding_generation: u64,
    pub root_runtime_generation: u64,
    pub configuration_revision: String,
    pub content_revision_before: String,
    pub content_revision_after: String,
    pub workspace_epoch: u64,
    pub relative_path: String,
    pub byte_length: u64,
    pub sha256: String,
    pub completeness: bool,
    pub source_checkpoint_id: Option<String>,
    pub source_pair_ref: Option<String>,
    pub load_intent_id: Option<String>,
    pub bytes_ref: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostIntentV1 {
    pub schema_version: u32,
    pub prepared: HostPreparedV1,
    pub access: HostAccessTokenV1,
    pub state: String,
    pub observations: Vec<Option<HostObservationV1>>,
    pub steps: Vec<HostStepV1>,
    pub recovery_evidence: Vec<Value>,
    pub result: Option<Value>,
    pub cleanup_state: String,
    #[serde(default)]
    pub artifact_evidence: Option<ArtifactEvidenceV1>,
    #[serde(default)]
    pub blocker_id: Option<String>,
    #[serde(default)]
    pub suspended_cleanup_plan_id: Option<String>,
    #[serde(default)]
    pub resume_joint_stage: Option<String>,
}
pub struct JointHostAccess {
    pub access: HostAccessTokenV1,
    pub target: HostTargetV1,
    pub backend: Arc<dyn JointHostBackend>,
}
#[async_trait]
pub trait JointHostBackend: OperationBackend {
    async fn write_host(&self, request: JointHostRequest) -> Result<u64, SessionDomainError>;
}

/// Every optional repository entry point fails closed when not implemented.
#[async_trait]
pub trait HostRepository: Send + Sync {
    async fn register_host_target(
        &self,
        _target: HostTargetV1,
        _policy: HostCapabilityPolicyV1,
    ) -> Result<(), SessionDomainError> {
        Err(crate::unsupported_joint("host_target"))
    }
    async fn host_target(
        &self,
        _runtime_id: String,
    ) -> Result<(HostTargetV1, HostCapabilityPolicyV1), SessionDomainError> {
        Err(crate::unsupported_joint("host_target"))
    }
    async fn prepare_host(
        &self,
        _prepared: HostPreparedV1,
    ) -> Result<HostAccessTokenV1, SessionDomainError> {
        Err(crate::unsupported_joint("host_prepare"))
    }
    async fn host_intent(&self, _id: String) -> Result<HostIntentV1, SessionDomainError> {
        Err(crate::unsupported_joint("host_intent"))
    }
    async fn claim_host_step(
        &self,
        _access: HostAccessTokenV1,
        _step: usize,
    ) -> Result<bool, SessionDomainError> {
        Err(crate::unsupported_joint("host_claim"))
    }
    async fn record_host_step(
        &self,
        _access: HostAccessTokenV1,
        _step: usize,
        _observation: HostObservationV1,
    ) -> Result<(), SessionDomainError> {
        Err(crate::unsupported_joint("host_record"))
    }
    async fn record_host_recovery(
        &self,
        _access: HostAccessTokenV1,
        _evidence: Value,
    ) -> Result<(), SessionDomainError> {
        Err(crate::unsupported_joint("host_recovery"))
    }
    async fn finish_host(
        &self,
        _access: HostAccessTokenV1,
        _result: Value,
    ) -> Result<Value, SessionDomainError> {
        Err(crate::unsupported_joint("host_finish"))
    }
    async fn finish_host_read(
        &self,
        _access: HostAccessTokenV1,
        _result: Value,
        _revision_before: String,
        _revision_after: String,
    ) -> Result<Value, SessionDomainError> {
        Err(crate::unsupported_joint("host_read_evidence"))
    }
    async fn pending_hosts(&self) -> Result<Vec<HostIntentV1>, SessionDomainError> {
        Err(crate::unsupported_joint("host_pending"))
    }
    async fn prepare_host_cleanup(
        &self,
        _access: HostAccessTokenV1,
        _child_id: String,
        _dependencies: Vec<String>,
    ) -> Result<(), SessionDomainError> {
        Err(crate::unsupported_joint("host_cleanup"))
    }
    async fn suspend_host_cleanup(
        &self,
        _access: HostAccessTokenV1,
    ) -> Result<(), SessionDomainError> {
        Err(crate::unsupported_joint("host_suspend"))
    }
    async fn release_host_dependency(
        &self,
        _access: HostAccessTokenV1,
        _dependency: String,
    ) -> Result<(), SessionDomainError> {
        Err(crate::unsupported_joint("host_dependency"))
    }
    async fn resume_host_cleanup(
        &self,
        _access: HostAccessTokenV1,
    ) -> Result<HostAccessTokenV1, SessionDomainError> {
        Err(crate::unsupported_joint("host_resume"))
    }
    async fn finish_host_cleanup(
        &self,
        _access: HostAccessTokenV1,
    ) -> Result<(), SessionDomainError> {
        Err(crate::unsupported_joint("host_cleanup_finish"))
    }
}
