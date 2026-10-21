//! Immutable server-side release references and fail-closed execution admission.
use crate::{
    host::{management_config_digest, CandidateHostOwner},
    root_owner::*,
    sdk, ManagementConfig, EXPECTED_CONTRACT_FINGERPRINT,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use xgovernor_core::*;

pub const SDK_COMMIT: &str = "881ff5d49395bfe53904c2da2f8d6403db1b42fd";
pub const RPM_SHA256: &str = "5e9347d0ba04a6ee1781cf0e05e4fc396e1401d4e11904ca050993779b549d59";
fn denied() -> SessionDomainError {
    unsupported_joint("execution_profile")
}
fn safe_ref(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/._:@+-".contains(&b))
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseBinding {
    pub public_sdk_commit: String,
    pub release_version: String,
    pub architecture: String,
    pub rpm_sha256: String,
    pub management_fingerprint: String,
    pub builtin_extension_version_ref: String,
    pub endside_version_ref: String,
    pub session_format_ref: String,
    pub compatibility_evidence_ref: String,
}
impl ReleaseBinding {
    fn validate(&self) -> Result<(), SessionDomainError> {
        if self.public_sdk_commit != SDK_COMMIT
            || self.release_version != "alpha11/0.1.0-25"
            || self.architecture != "aarch64-unknown-linux-gnu"
            || self.rpm_sha256 != RPM_SHA256
            || self.management_fingerprint != EXPECTED_CONTRACT_FINGERPRINT
            || self.builtin_extension_version_ref != "builtin-xiaoo/v7"
            || !safe_ref(&self.endside_version_ref)
            || !safe_ref(&self.session_format_ref)
            || !safe_ref(&self.compatibility_evidence_ref)
        {
            return Err(denied());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionMode {
    #[default]
    Disabled,
    Experiment,
    ProductionCandidate,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Message,
    HostRead,
    HostWrite,
    HostExec,
    OrdinaryCancel,
    TrustedUsage,
    TokenFuse,
    HardPolicy,
    Isolation,
    ControlProtection,
    TaskImage,
    StructuredEvents,
    NewIdentity,
    DurableStorage,
    StorageUsage,
    StorageLimit,
    CancelContinuation,
    CompleteReply,
}
impl Capability {
    fn supported(&self) -> bool {
        matches!(
            self,
            Self::Message
                | Self::HostRead
                | Self::HostWrite
                | Self::HostExec
                | Self::OrdinaryCancel
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityDeclaration {
    pub declaration_id: String,
    pub schema_version: u32,
    pub release_ref: String,
    pub public_contract_refs: BTreeSet<String>,
    pub operation_subset: BTreeSet<Capability>,
    pub usage_mode: UsageMode,
    pub policy_subset: PolicySubset,
    pub cancel_mode: CancelMode,
    pub observation_mode: ObservationMode,
    pub evidence_refs: BTreeMap<Capability, String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageMode {
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicySubset {
    Empty,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelMode {
    OrdinaryCheckpointRequired,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationMode {
    TerminalOnly,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Workload {
    Research,
    DefaultMcts,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionRole {
    Author,
    Solver,
    Research,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolePolicy {
    #[serde(default)]
    pub init_limits: Option<PolicyLimits>,
    #[serde(default)]
    pub step_limits: Option<PolicyLimits>,
    #[serde(default)]
    pub tools_disabled: Option<bool>,
    #[serde(default)]
    pub unattended: Option<bool>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyLimits {
    pub max_tokens: u64,
    pub timeout_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchLimits {
    pub token_fuse_enabled: bool,
    pub wall_timeout_ms: u64,
    pub expansion_bound: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentPermit {
    pub approval_ref: String,
    pub principal: String,
    pub tenant_key: String,
    pub profile_revision: String,
    pub release_ref: String,
    pub workload: Workload,
    pub capability_subset: BTreeSet<Capability>,
    pub expires_at_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionProfile {
    pub profile_id: String,
    pub revision: String,
    pub owner_kind: String,
    pub runtime_kind: String,
    pub backing_schema: String,
    pub deployment_id: String,
    pub context_ref: String,
    pub context_digest: String,
    pub tt_profile_ref: String,
    pub model_refs: BTreeSet<String>,
    pub secret_version_refs: BTreeMap<String, String>,
    pub release_ref: String,
    pub release: ReleaseBinding,
    pub declaration: CapabilityDeclaration,
    pub workload: Workload,
    pub role: ExecutionRole,
    pub policy: RolePolicy,
    pub required_capabilities: BTreeSet<Capability>,
    pub research_limits: ResearchLimits,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub admission_mode: AdmissionMode,
    #[serde(default)]
    pub experiment_allowlist: Vec<ExperimentPermit>,
}
/// Separate trusted inputs supplied by deployment provisioning, never by a Session request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentManifest {
    pub host_sha256: String,
    pub owner_kind: String,
    pub runtime_kind: String,
    pub backing_schema: String,
    pub deployment_id: String,
    pub context_ref: String,
    pub context_digest: String,
    pub tt_profile_ref: String,
    pub model_refs: BTreeSet<String>,
    pub secret_version_refs: BTreeMap<String, String>,
    pub release_ref: String,
    pub release: ReleaseBinding,
    pub declaration: CapabilityDeclaration,
}
impl ExecutionProfile {
    /// Configuration parse errors intentionally exclude caller-controlled strings.
    pub fn parse(input: &str) -> Result<Self, SessionDomainError> {
        serde_json::from_str(input).map_err(|_| denied())
    }
    pub fn validate(
        &self,
        manifest: &DeploymentManifest,
        management: &ManagementConfig,
    ) -> Result<(), SessionDomainError> {
        self.release.validate()?;
        management.validate().map_err(|_| denied())?;
        if host_bytes_digest(&std::fs::read(&management.host_executable).map_err(|_| denied())?)
            != manifest.host_sha256
        {
            return Err(denied());
        }
        let declaration = &self.declaration;
        if self.release != manifest.release
            || self.release_ref != manifest.release_ref
            || self.deployment_id != manifest.deployment_id
            || self.context_ref != manifest.context_ref
            || self.context_digest != manifest.context_digest
            || self.context_digest != management_config_digest(management)
            || self.tt_profile_ref != manifest.tt_profile_ref
            || self.model_refs != manifest.model_refs
            || self.secret_version_refs != manifest.secret_version_refs
            || self.declaration != manifest.declaration
            || self.owner_kind != manifest.owner_kind
            || self.runtime_kind != manifest.runtime_kind
            || self.backing_schema != manifest.backing_schema
            || !safe_ref(&self.owner_kind)
            || !safe_ref(&self.runtime_kind)
            || self.backing_schema != "joint_v1"
            || declaration.schema_version != 1
            || declaration.release_ref != self.release_ref
            || !safe_ref(&self.profile_id)
            || !safe_ref(&self.revision)
            || !safe_ref(&self.deployment_id)
            || !safe_ref(&self.context_ref)
            || !safe_ref(&self.tt_profile_ref)
            || self.release_ref != "alpha11"
            || !safe_ref(&declaration.declaration_id)
            || declaration.public_contract_refs.is_empty()
            || !declaration.public_contract_refs.iter().all(|v| safe_ref(v))
            || self.secret_version_refs.keys().any(|key| {
                management
                    .base_environment
                    .get(key)
                    .is_none_or(|v| v.is_empty())
            })
            || self.model_refs.is_empty()
            || !self.model_refs.iter().all(|v| safe_ref(v))
            || !self
                .secret_version_refs
                .iter()
                .all(|(key, version)| safe_ref(key) && safe_ref(version))
            || declaration.operation_subset.is_empty()
            || declaration
                .operation_subset
                .iter()
                .any(|cap| !cap.supported())
            || declaration.evidence_refs.keys().collect::<BTreeSet<_>>()
                != declaration.operation_subset.iter().collect::<BTreeSet<_>>()
            || declaration
                .evidence_refs
                .values()
                .any(|v| !safe_ref(v) || !v.starts_with("alpha11/"))
        {
            return Err(denied());
        }
        Ok(())
    }
    fn admit(
        &self,
        principal: &str,
        tenant: &str,
        now_ms: u64,
        new_open: bool,
        operation: &Capability,
    ) -> Result<(), SessionDomainError> {
        if self.workload == Workload::DefaultMcts {
            return Err(unsupported_joint(
                "default_workload_requires_trusted_usage_and_hard_policy",
            ));
        }
        if new_open && !self.enabled
            || self.admission_mode != AdmissionMode::Experiment
            || self.policy != RolePolicy::default()
            || self.research_limits.token_fuse_enabled
            || self.research_limits.wall_timeout_ms == 0
            || self.research_limits.wall_timeout_ms > 300_000
            || self.research_limits.expansion_bound == 0
            || self.research_limits.expansion_bound > u32::MAX as u64
            || !self.declaration.operation_subset.contains(operation)
            || !self
                .required_capabilities
                .is_subset(&self.declaration.operation_subset)
        {
            return Err(denied());
        }
        if !self.experiment_allowlist.iter().any(|permit| {
            permit.capability_subset.contains(operation)
                && safe_ref(&permit.approval_ref)
                && permit.principal == principal
                && permit.tenant_key == tenant
                && permit.profile_revision == self.revision
                && permit.release_ref == self.release_ref
                && permit.workload == self.workload
                && now_ms < permit.expires_at_ms
                && permit
                    .capability_subset
                    .is_subset(&self.declaration.operation_subset)
                && self
                    .required_capabilities
                    .is_subset(&permit.capability_subset)
        }) {
            return Err(denied());
        }
        Ok(())
    }
    fn normalize_request(
        &self,
        request: &session_protocol::SessionOpenRequest,
    ) -> Result<(), SessionDomainError> {
        if request.llm.is_some()
            || !request.ext.is_empty()
            || request.deployment != Default::default()
            || !request.requested_capabilities.runtime.is_empty()
            || !request.requested_capabilities.sandbox.is_empty()
        {
            return Err(denied());
        }
        Ok(())
    }
}

/// One frozen adapter and its safe server-side references.
pub struct ProfileRevision {
    profile: ExecutionProfile,
    digest: String,
    management: ManagementConfig,
    owner: Arc<dyn JointSessionLifecycle>,
    can_open: bool,
    host_sha256: String,
}
impl ProfileRevision {
    pub fn root(
        profile: ExecutionProfile,
        manifest: &DeploymentManifest,
        management: ManagementConfig,
        create: sdk::ThinkThreadCreateRequest,
        timeout_ms: u64,
    ) -> Result<Self, SessionDomainError> {
        profile.validate(manifest, &management)?;
        if profile.owner_kind != ROOT_OWNER_KIND || profile.runtime_kind != ROOT_RUNTIME_KIND {
            return Err(denied());
        }
        let template = serde_json::to_value(&create).map_err(|_| denied())?;
        if template["agent"] != serde_json::json!({"kind":"profile","name":profile.tt_profile_ref})
            || create
                .models
                .as_ref()
                .is_some_and(|models| !models.is_empty())
            || create.allow_shared_fs
            || create.fs.as_ref() != Some(&management.fs_root)
        {
            return Err(denied());
        }
        let mut frozen = profile.clone();
        frozen.enabled = false;
        let digest = host_digest(&serde_json::json!({"profile":frozen,"create":template}));
        let owner = RootSessionOwner::new(
            management.clone(),
            create,
            profile.revision.clone(),
            0,
            0,
            timeout_ms,
        )?;
        Ok(Self {
            profile,
            digest,
            management,
            owner: Arc::new(owner),
            can_open: true,
            host_sha256: manifest.host_sha256.clone(),
        })
    }
    pub fn host(
        profile: ExecutionProfile,
        manifest: &DeploymentManifest,
        management: ManagementConfig,
        timeout_ms: u64,
    ) -> Result<Self, SessionDomainError> {
        profile.validate(manifest, &management)?;
        let mut frozen = profile.clone();
        frozen.enabled = false;
        let digest = host_digest(&serde_json::json!({"profile":frozen,"adapter":"host_v1"}));
        let owner = CandidateHostOwner::new(
            management.clone(),
            profile.owner_kind.clone(),
            profile.revision.clone(),
            std::time::Duration::from_millis(timeout_ms),
        )?;
        Ok(Self {
            profile,
            digest,
            management,
            owner: Arc::new(owner),
            can_open: false,
            host_sha256: manifest.host_sha256.clone(),
        })
    }
    fn context(
        &self,
        context: &JointOperationContext,
        now_ms: u64,
        new_open: bool,
        operation: &Capability,
    ) -> Result<(), SessionDomainError> {
        if context.configuration_revision != self.profile.revision
            || context.owner_ref
                != context
                    .tenant_id
                    .as_ref()
                    .map(|id| format!("tenant/{id}"))
                    .unwrap_or_else(|| ADMIN_OWNER_REF.into())
            || context.actor_role != Role::Admin
            || context.runtime_kind != self.profile.runtime_kind
        {
            return Err(denied());
        }
        self.profile.admit(
            &context.principal,
            &context.owner_ref,
            now_ms,
            new_open,
            operation,
        )
    }
}
/// Explicit test/research assembly. It does not register a production backend.
pub struct ProfileRouter {
    guard: Arc<DeploymentGuard>,
    revisions: BTreeMap<String, ProfileRevision>,
    default_revision: String,
    repository: Arc<dyn SessionRepository>,
    clock: Arc<dyn Clock>,
}
impl ProfileRouter {
    pub fn assemble(
        guard: Arc<DeploymentGuard>,
        budget: JointBudgetPolicy,
        revisions: Vec<ProfileRevision>,
        default_revision: String,
        joint_transactions: bool,
        clock: Arc<dyn Clock>,
    ) -> Result<Arc<Self>, SessionDomainError> {
        let mut map = BTreeMap::new();
        for revision in revisions {
            let profile = &revision.profile;
            if profile.deployment_id != guard.deployment_id() || map.contains_key(&profile.revision)
            {
                return Err(denied());
            }
            for path in [
                &revision.management.state_root,
                &revision.management.runtime_root,
                &revision.management.config_root,
                &revision.management.cache_root,
                &revision.management.fs_root,
                &revision.management.working_directory,
                &revision.management.home_directory,
            ] {
                guard.protects_directory(path)?;
            }
            guard.validate_host_environment(&revision.management.base_environment)?;
            guard.protect_values(
                profile
                    .secret_version_refs
                    .keys()
                    .filter_map(|key| revision.management.base_environment.get(key))
                    .cloned(),
            )?;
            guard.validate_bytes(&serde_json::to_vec(profile).map_err(|_| denied())?)?;
            let references = serde_json::json!({
                "profile_id":profile.profile_id,"revision":profile.revision,
                "owner_kind":profile.owner_kind,"runtime_kind":profile.runtime_kind,"backing_schema":profile.backing_schema,
                "deployment_id":profile.deployment_id,"database_identity":guard.database_identity(),
                "context_ref":profile.context_ref,"context_digest":profile.context_digest,
                "tt_profile_ref":profile.tt_profile_ref,"model_refs":profile.model_refs,
                "secret_version_refs":profile.secret_version_refs,"release_ref":profile.release_ref,
                "role":profile.role,"policy_digest":host_digest(&serde_json::to_value(&profile.policy).map_err(|_|denied())?),"usage_requirement":"unknown", "required_capabilities":profile.required_capabilities,
                "release":profile.release,"declaration":profile.declaration,"host_sha256":revision.host_sha256,
                "state_root":revision.management.state_root,"runtime_root":revision.management.runtime_root,
                "config_root":revision.management.config_root,"cache_root":revision.management.cache_root,
                "working_directory":revision.management.working_directory,"fs_root":revision.management.fs_root,
                "home_directory":revision.management.home_directory,"executable_search_path":revision.management.executable_search_path,
                "environment_keys":revision.management.base_environment.keys().collect::<Vec<_>>(),
            });
            guard.register_configuration(&profile.revision, &revision.digest, &references)?;
            map.insert(profile.revision.clone(), revision);
        }
        let selected = map.get(&default_revision).ok_or_else(denied)?;
        if map.values().any(|r| {
            r.profile.owner_kind != selected.profile.owner_kind
                || r.profile.runtime_kind != selected.profile.runtime_kind
        }) {
            return Err(denied());
        }
        let repository = guard.repository(budget)?;
        guard.activate_default(
            &selected.profile.owner_kind,
            &selected.profile.runtime_kind,
            &selected.profile.revision,
            &selected.digest,
            selected.profile.enabled,
        )?;
        let repository = if joint_transactions {
            repository.with_joint_transactions()
        } else {
            repository
        };
        Ok(Arc::new(Self {
            guard,
            revisions: map,
            default_revision,
            repository: Arc::new(repository),
            clock,
        }))
    }
    pub fn repository(&self) -> Arc<dyn SessionRepository> {
        self.repository.clone()
    }
    fn revision(&self, revision: &str) -> Result<&ProfileRevision, SessionDomainError> {
        let selected = self.revisions.get(revision).ok_or_else(denied)?;
        self.guard.check_configuration(revision, &selected.digest)?;
        if host_bytes_digest(
            &std::fs::read(&selected.management.host_executable).map_err(|_| denied())?,
        ) != selected.host_sha256
        {
            return Err(denied());
        }
        for path in [
            &selected.management.state_root,
            &selected.management.runtime_root,
            &selected.management.config_root,
            &selected.management.cache_root,
            &selected.management.fs_root,
            &selected.management.working_directory,
            &selected.management.home_directory,
        ] {
            self.guard.protects_directory(path)?;
        }
        Ok(selected)
    }
    fn check_repository(
        &self,
        repository: &Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        self.guard.validate()?;
        if !Arc::ptr_eq(repository, &self.repository) || !repository.supports_joint_transactions() {
            return Err(denied());
        }
        Ok(())
    }
    fn record<'a>(
        &'a self,
        session: &SessionRecord,
    ) -> Result<&'a ProfileRevision, SessionDomainError> {
        let binding = session.execution_backing.joint().ok_or_else(denied)?;
        let selected = self.revision(&binding.configuration_revision)?;
        if binding.owner_kind != selected.profile.owner_kind
            || session.runtime.runtime_kind != selected.profile.runtime_kind
        {
            return Err(denied());
        }
        self.revision(&binding.configuration_revision)
    }
}
#[async_trait]
impl SessionEnvironmentNormalizer for ProfileRouter {
    async fn normalize(
        &self,
        context: &SecurityContext,
        request: &session_protocol::SessionOpenRequest,
    ) -> Result<NormalizedSessionEnvironment, SessionDomainError> {
        if !self.repository.supports_joint_transactions() || !context.is_admin() {
            return Err(denied());
        }
        let selected = self.revision(&self.default_revision)?;
        self.guard.check_default(
            &selected.profile.owner_kind,
            &selected.profile.runtime_kind,
            &selected.profile.revision,
            &selected.digest,
        )?;
        if !selected.can_open {
            return Err(denied());
        }
        selected.profile.admit(
            &context.principal,
            &context.owner_ref(),
            self.clock.now_ms(),
            true,
            &Capability::Message,
        )?;
        self.guard
            .validate_bytes(&serde_json::to_vec(request).map_err(|_| denied())?)?;
        selected.profile.normalize_request(request)?;
        if selected
            .profile
            .required_capabilities
            .iter()
            .any(|c| !matches!(c, Capability::Message))
        {
            return Err(denied());
        }
        match &request.workspace {
            session_protocol::WorkspaceSpec::LocalPath { path }
                if std::path::Path::new(path) == selected.management.fs_root => {}
            session_protocol::WorkspaceSpec::DaemonDefault => {}
            _ => return Err(denied()),
        }
        Ok(NormalizedSessionEnvironment {
            workspace: WorkspaceFacts {
                workspace_id: selected.profile.context_ref.clone(),
                root: selected.management.fs_root.to_string_lossy().into(),
                access: WorkspaceAccess::ReadWrite,
                revision: None,
                metadata: serde_json::json!({}),
            },
            isolation: IsolationFacts {
                boundary: IsolationBoundary::Host,
                workspace_access: WorkspaceAccess::ReadWrite,
                network: NetworkIsolation::None,
                metadata: serde_json::json!({}),
            },
            sandbox_capabilities: BTreeSet::new(),
            llm: None,
            lease: None,
        })
    }
}
#[async_trait]
impl JointSessionLifecycle for ProfileRouter {
    fn owner_kind(&self) -> &str {
        &self.revisions[&self.default_revision].profile.owner_kind
    }
    fn configuration_revision(&self) -> &str {
        &self.default_revision
    }
    async fn open(
        &self,
        context: JointOperationContext,
        input: JointOpenInput,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<SessionRecord, SessionDomainError> {
        self.check_repository(&repository)?;
        let selected = self.revision(&context.configuration_revision)?;
        if context.configuration_revision != self.default_revision {
            return Err(denied());
        }
        self.guard.check_default(
            &selected.profile.owner_kind,
            &selected.profile.runtime_kind,
            &selected.profile.revision,
            &selected.digest,
        )?;
        selected.context(&context, self.clock.now_ms(), true, &Capability::Message)?;
        self.guard
            .validate_bytes(&serde_json::to_vec(&input.request).map_err(|_| denied())?)?;
        selected.profile.normalize_request(&input.request)?;
        if selected
            .profile
            .required_capabilities
            .iter()
            .any(|c| !matches!(c, Capability::Message))
        {
            return Err(denied());
        }
        selected.owner.open(context, input, repository).await
    }
    async fn submit_turn(
        &self,
        context: JointOperationContext,
        session: SessionRecord,
        request: session_protocol::SessionTurnRequest,
        turn_id: String,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<JointTurnSubmission, SessionDomainError> {
        self.check_repository(&repository)?;
        let selected = self.record(&session)?;
        selected.context(&context, self.clock.now_ms(), false, &Capability::Message)?;
        if request.llm.is_some()
            || request.reasoning_effort.is_some()
            || !request.ext.is_empty()
            || request.entry != Default::default()
            || selected
                .profile
                .required_capabilities
                .iter()
                .any(|c| !matches!(c, Capability::Message))
        {
            return Err(denied());
        }
        self.guard
            .validate_bytes(&serde_json::to_vec(&request).map_err(|_| denied())?)?;
        selected
            .owner
            .submit_turn(context, session, request, turn_id, repository)
            .await
    }
    async fn cancel_turn(
        &self,
        context: JointOperationContext,
        session: SessionRecord,
        turn_id: Option<String>,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        self.check_repository(&repository)?;
        let selected = self.record(&session)?;
        selected.context(
            &context,
            self.clock.now_ms(),
            false,
            &Capability::OrdinaryCancel,
        )?;
        selected
            .owner
            .cancel_turn(context, session, turn_id, repository)
            .await
    }
    async fn prepare_host_operation(
        &self,
        context: JointOperationContext,
        session: SessionRecord,
        request: JointHostRequest,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<JointHostAccess, SessionDomainError> {
        self.check_repository(&repository)?;
        let selected = self.record(&session)?;
        let capability = match &request {
            JointHostRequest::Exec { .. } => Capability::HostExec,
            JointHostRequest::Read { .. } => Capability::HostRead,
            JointHostRequest::Write { .. } => Capability::HostWrite,
            JointHostRequest::Export { .. } => return Err(denied()),
        };
        selected.context(&context, self.clock.now_ms(), false, &capability)?;
        match &request {
            JointHostRequest::Write { content, path, .. } => {
                self.guard.validate_bytes(content)?;
                self.guard.validate_bytes(path.as_bytes())?;
            }
            JointHostRequest::Read { path } | JointHostRequest::Export { path, .. } => {
                self.guard.validate_bytes(path.as_bytes())?
            }
            JointHostRequest::Exec { request, .. } => {
                self.guard.validate_bytes(request.command.as_bytes())?;
                for argument in &request.args {
                    self.guard.validate_bytes(argument.as_bytes())?;
                }
                if let Some(path) = &request.cwd {
                    self.guard.validate_bytes(path.0.as_bytes())?;
                }
                if let Some(environment) = &request.env {
                    for (key, value) in environment {
                        self.guard.validate_bytes(key.as_bytes())?;
                        self.guard.validate_bytes(value.as_bytes())?;
                    }
                }
                if let Some(extra) = &request.extra {
                    self.guard
                        .validate_bytes(&serde_json::to_vec(extra).map_err(|_| denied())?)?;
                }
            }
        }
        selected
            .owner
            .prepare_host_operation(context, session, request, repository)
            .await
    }
    async fn ensure_attached(
        &self,
        session: &SessionRecord,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        self.check_repository(&repository)?;
        self.record(session)?
            .owner
            .ensure_attached(session, repository)
            .await
    }
    async fn check_alive(
        &self,
        session: &SessionRecord,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<bool, SessionDomainError> {
        self.check_repository(&repository)?;
        self.record(session)?
            .owner
            .check_alive(session, repository)
            .await
    }
    async fn reconcile(
        &self,
        pending: JointPendingOperation,
        repository: Arc<dyn SessionRepository>,
    ) -> Result<(), SessionDomainError> {
        self.check_repository(&repository)?;
        let selected = self.revision(&pending.admission.binding.configuration_revision)?;
        selected.owner.reconcile(pending, repository).await
    }
    // Candidate capture/load and public close retain the lifecycle's strict refusal.
}
