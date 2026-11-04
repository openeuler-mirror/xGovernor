//! Real MCTS host application backed by xGovernor's `SessionApplication`.
//!
//! The plugin talks only stdio. Every `host.*` method is translated here into
//! an existing session-control or session-operation call, preserving the
//! caller's `SecurityContext` and the run's lease identity.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde_json::json;
use session_protocol::{
    LlmApi, LlmOverrideRequest, SessionCapabilityRequest, SessionCheckpointRequest,
    SessionCheckpointDeleteRequest, SessionCheckpointScope, SessionEntryContext, SessionEvent,
    SessionExecRequest, SessionExtensions, SessionFileReadRequest, SessionFileWriteRequest,
    SessionLeaseClaim, SessionLoadRequest, SessionRuntimeCapability, SessionSandboxCapability,
    SessionTurnRequest, SessionUsageReport,
    DeploymentProfile,
};
use xgovernor_core::{SecurityContext, SessionApplication, SessionDomainError};

use mcts_plugin_host::{
    Failure, HostApplication, HostJobOutcome, HostSnapshot, JobStatus, LlmPolicy, PluginHostError,
    PluginManifest, ProfileRegistry,
};

const EXEC_TIMEOUT_MS: u64 = 10 * 60 * 1000;
const JOB_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Phase 2 uses a long-lived checkpoint lease. Real expiry metadata still has
/// to be loaded from the session repository before `ApplyPlanValidator` can
/// enforce it; this value keeps the wire contract valid in the meantime.
const STATE_EXPIRES_AT: &str = "2099-12-31T23:59:59Z";

pub struct RealSessionHostApplication {
    session: SessionApplication,
    ctx: SecurityContext,
    mode: String,
    client_id: String,
    profile_registry: ProfileRegistry,
    llm_policy: LlmPolicy,
    jobs: Arc<Mutex<HashMap<String, HostJobOutcome>>>,
    next_job: AtomicU64,
    cancel: Arc<AtomicBool>,
}

impl RealSessionHostApplication {
    pub fn new(
        session: SessionApplication,
        ctx: SecurityContext,
        mode: String,
        run_id: &str,
        repo_root: &Path,
        llm_model: Option<&str>,
        cancel: Arc<AtomicBool>,
    ) -> Result<Self, PluginHostError> {
        let manifest_rel = std::env::var("MCTS_PLUGIN_MANIFEST")
            .unwrap_or_else(|_| "src/plugin/plugin_manifest.json".to_string());
        let manifest_path = repo_root.join(manifest_rel);
        let manifest = PluginManifest::load(manifest_path)?;
        let profile_registry = ProfileRegistry::new(manifest, repo_root);
        let llm_policy = LlmPolicy::new(llm_model.unwrap_or("DeepSeek-V4-Pro"));
        Ok(Self {
            session,
            ctx,
            mode,
            client_id: format!("mcts-run:{run_id}"),
            profile_registry,
            llm_policy,
            jobs: Arc::new(Mutex::new(HashMap::new())),
            next_job: AtomicU64::new(1),
            cancel,
        })
    }

    fn lease(&self) -> SessionLeaseClaim {
        SessionLeaseClaim {
            client_id: Some(self.client_id.clone()),
            client_pid: None,
            client_hostname: None,
        }
    }

    fn capabilities() -> SessionCapabilityRequest {
        SessionCapabilityRequest {
            sandbox: [
                SessionSandboxCapability::Exec,
                SessionSandboxCapability::FileRead,
                SessionSandboxCapability::FileWrite,
                SessionSandboxCapability::Snapshot,
            ]
            .into_iter()
            .collect(),
            runtime: [
                SessionRuntimeCapability::StateExport,
                SessionRuntimeCapability::Fork,
                SessionRuntimeCapability::Checkpoint,
            ]
            .into_iter()
            .collect(),
        }
    }

    fn session_error(error: SessionDomainError) -> PluginHostError {
        PluginHostError::Manifest(format!("xg session operation failed: {error}"))
    }

    fn next_job_id(&self) -> String {
        format!("mcts-job-{}", self.next_job.fetch_add(1, Ordering::Relaxed))
    }

    fn store_job(&self, job_id: &str, outcome: HostJobOutcome) {
        if let Ok(mut jobs) = self.jobs.lock() {
            jobs.insert(job_id.to_string(), outcome);
        }
    }

    fn load_request(&self, checkpoint_id: &str, offline: bool) -> SessionLoadRequest {
        let mut deployment = DeploymentProfile::default();
        deployment.options = json!({
            "provider_options": {"allow_internet_access": !offline}
        });
        SessionLoadRequest {
            checkpoint_id: checkpoint_id.to_string(),
            runtime_id: None,
            conversation_id: Some(format!("mcts:{}", self.client_id)),
            sender_id: Some("mcts".to_string()),
            requested_capabilities: Self::capabilities(),
            deployment,
            lease: self.lease(),
            llm: self.load_llm(),
        }
    }

    fn load_llm(&self) -> Option<LlmOverrideRequest> {
        let env = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let provider = env("XGOVERNOR_LLM_PROVIDER").unwrap_or_else(|| "deepseek".to_string());
        let model = env("XGOVERNOR_LLM_MODEL").unwrap_or_else(|| self.llm_policy.resolve(None));
        let api_base = env("XGOVERNOR_LLM_API_BASE");
        Some(LlmOverrideRequest {
            provider: Some(provider),
            model: Some(model),
            api: api_base.as_ref().map(|_| LlmApi::OpenaiCompletions),
            api_base,
            capabilities: None,
            api_key_env: env("XGOVERNOR_LLM_KEY_ENV"),
            api_key: None,
        })
    }

    fn turn_request(
        &self,
        branch_id: &str,
        input: &str,
        profile: Option<&str>,
    ) -> Result<SessionTurnRequest, PluginHostError> {
        let mut ext = SessionExtensions::new();
        if let Some(profile) = profile {
            let profile_manifest = self.profile_registry.profile(&self.mode, profile)?;
            let system_prompt = self
                .profile_registry
                .resolve(&self.mode, profile, &json!({}))?;
            ext.insert(
                "runtime_pi".to_string(),
                json!({"system_prompt": system_prompt, "tools_enabled": profile_manifest.tools_enabled}),
            );
        }
        Ok(SessionTurnRequest {
            runtime_id: branch_id.to_string(),
            text: input.to_string(),
            entry: SessionEntryContext::default(),
            llm: None,
            reasoning_effort: None,
            client_request_id: None,
            ext,
            lease: self.lease(),
        })
    }

    async fn drain_turn_events(
        mut events: tokio::sync::mpsc::Receiver<SessionEvent>,
        jobs: Arc<Mutex<HashMap<String, HostJobOutcome>>>,
        job_id: String,
    ) {
        let mut output = String::new();
        while let Some(event) = events.recv().await {
            match event {
                SessionEvent::OutputDelta { delta, .. } => {
                    output.push_str(&delta);
                }
                SessionEvent::ToolActivity { summary, .. } => {
                    if let Some(summary) = summary {
                        output.push_str(&summary);
                        output.push('\n');
                    }
                }
                SessionEvent::TurnCompleted { usage, .. } => {
                    let outcome = HostJobOutcome {
                        status: JobStatus::Completed,
                        output,
                        exit_code: Some(0),
                        usage: usage_to_map(usage),
                        failure: None,
                    };
                    if let Ok(mut jobs) = jobs.lock() {
                        jobs.insert(job_id, outcome);
                    }
                    return;
                }
                SessionEvent::TurnFailed { error, usage, .. } => {
                    let outcome = HostJobOutcome {
                        status: JobStatus::Failed,
                        output,
                        exit_code: None,
                        usage: usage_to_map(usage),
                        failure: Some(Failure {
                            code: "turn-failed".to_string(),
                            message: error.message,
                        }),
                    };
                    if let Ok(mut jobs) = jobs.lock() {
                        jobs.insert(job_id, outcome);
                    }
                    return;
                }
                SessionEvent::InteractionRequested { prompt, .. } => {
                    output.push_str(&format!("interaction requested: {prompt}\n"));
                }
                SessionEvent::Extension { .. } => {}
            }
        }
        let outcome = HostJobOutcome {
            status: JobStatus::Failed,
            output,
            exit_code: None,
            usage: BTreeMap::new(),
            failure: Some(Failure {
                code: "event-stream-closed".to_string(),
                message: "runtime event stream closed before terminal event".to_string(),
            }),
        };
        if let Ok(mut jobs) = jobs.lock() {
            jobs.insert(job_id, outcome);
        }
    }
}

fn rfc3339_from_unix_ms(ms: u64) -> String {
    let seconds = ms / 1_000;
    let millis = ms % 1_000;
    let days = (seconds / 86_400) as i64;
    let rem_secs = seconds % 86_400;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    if month <= 2 {
        year += 1;
    }
    let hour = rem_secs / 3_600;
    let minute = (rem_secs % 3_600) / 60;
    let second = rem_secs % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

fn usage_to_map(usage: SessionUsageReport) -> BTreeMap<String, u64> {
    let SessionUsageReport::Known(usage) = usage else {
        return BTreeMap::new();
    };
    let mut map = BTreeMap::new();
    map.insert("input_tokens".to_string(), usage.input_tokens);
    map.insert("output_tokens".to_string(), usage.output_tokens);
    map.insert("total_tokens".to_string(), usage.total_tokens);
    map
}

#[async_trait]
impl HostApplication for RealSessionHostApplication {
    async fn snapshot(
        &self,
        _run_id: &str,
        branch_id: &str,
    ) -> Result<HostSnapshot, PluginHostError> {
        let lease_expires_at_ms = self
            .session
            .heartbeat(&self.ctx, branch_id, self.lease())
            .await
            .map_err(Self::session_error)?
            .lease_expires_at_ms;
        let checkpoint = self
            .session
            .checkpoint(
                &self.ctx,
                SessionCheckpointRequest {
                    runtime_id: branch_id.to_string(),
                    name: None,
                    requested_scope: Some(SessionCheckpointScope::Full),
                    lease: self.lease(),
                },
            )
            .await
            .map_err(Self::session_error)?;
        Ok(HostSnapshot {
            state_id: checkpoint.checkpoint_id,
            expires_at: lease_expires_at_ms
                .map(rfc3339_from_unix_ms)
                .unwrap_or_else(|| STATE_EXPIRES_AT.to_string()),
            expires_at_ms: lease_expires_at_ms,
            tenant_id: self.ctx.tenant_id().map(str::to_string),
        })
    }

    async fn fork(&self, _run_id: &str, parent_state_id: &str, offline: bool) -> Result<String, PluginHostError> {
        let opened = self
            .session
            .load_checkpoint(&self.ctx, self.load_request(parent_state_id, offline))
            .await
            .map_err(Self::session_error)?;
        Ok(opened.runtime_id)
    }

    async fn agent_turn(
        &self,
        _run_id: &str,
        branch_id: &str,
        input: &str,
        profile: Option<&str>,
    ) -> Result<String, PluginHostError> {
        let request = self.turn_request(branch_id, input, profile)?;
        let submission = self
            .session
            .submit_turn(&self.ctx, request)
            .await
            .map_err(Self::session_error)?;
        let job_id = self.next_job_id();
        let events = submission.events.ok_or_else(|| {
            PluginHostError::InvalidResponse(
                "duplicate turn submission has no event stream".to_string(),
            )
        })?;
        tokio::spawn(Self::drain_turn_events(
            events,
            Arc::clone(&self.jobs),
            job_id.clone(),
        ));
        self.store_job(&job_id, running_job());
        Ok(job_id)
    }

    async fn inspect(
        &self,
        _run_id: &str,
        branch_id: &str,
        view: &str,
    ) -> Result<String, PluginHostError> {
        // zsoft does not depend on host.inspect; keep a minimal stable summary
        // until a session-level inspect projection exists.
        Ok(format!("{view}:{branch_id}"))
    }

    async fn exec(
        &self,
        _run_id: &str,
        branch_id: &str,
        argv: &[String],
        cwd: &str,
    ) -> Result<String, PluginHostError> {
        let job_id = self.next_job_id();
        let request = SessionExecRequest {
            runtime_id: branch_id.to_string(),
            command: argv.to_vec(),
            cwd: Some(cwd.to_string()),
            env: BTreeMap::new(),
            timeout_ms: Some(EXEC_TIMEOUT_MS),
            lease: self.lease(),
        };
        let session = self.session.clone();
        let jobs = Arc::clone(&self.jobs);
        let ctx = self.ctx.clone();
        let spawn_job_id = job_id.clone();
        tokio::spawn(async move {
            let outcome = match session.exec(&ctx, request).await {
                Ok(result) => HostJobOutcome {
                    status: if result.timed_out {
                        JobStatus::Failed
                    } else {
                        JobStatus::Completed
                    },
                    output: format!("{}{}", result.stdout, result.stderr),
                    exit_code: result.exit_code.map(i64::from),
                    usage: BTreeMap::new(),
                    failure: if result.timed_out {
                        Some(Failure {
                            code: "exec-timed-out".to_string(),
                            message: "mcts exec timed out".to_string(),
                        })
                    } else {
                        None
                    },
                },
                Err(error) => HostJobOutcome {
                    status: JobStatus::Failed,
                    output: error.to_string(),
                    exit_code: None,
                    usage: BTreeMap::new(),
                    failure: Some(Failure {
                        code: "exec-failed".to_string(),
                        message: error.to_string(),
                    }),
                },
            };
            if let Ok(mut jobs) = jobs.lock() {
                jobs.insert(spawn_job_id.clone(), outcome);
            }
        });
        Ok(job_id)
    }

    async fn file_read(
        &self,
        _run_id: &str,
        branch_id: &str,
        path: &str,
    ) -> Result<Vec<u8>, PluginHostError> {
        let result = self
            .session
            .read_file(
                &self.ctx,
                SessionFileReadRequest {
                    runtime_id: branch_id.to_string(),
                    path: path.to_string(),
                    lease: self.lease(),
                },
            )
            .await
            .map_err(Self::session_error)?;
        BASE64.decode(&result.content_base64).map_err(|error| {
            PluginHostError::Manifest(format!(
                "session returned invalid base64 file content: {error}"
            ))
        })
    }

    async fn file_write(
        &self,
        _run_id: &str,
        branch_id: &str,
        path: &str,
        data: &[u8],
    ) -> Result<u64, PluginHostError> {
        let result = self
            .session
            .write_file(
                &self.ctx,
                SessionFileWriteRequest {
                    runtime_id: branch_id.to_string(),
                    path: path.to_string(),
                    content_base64: BASE64.encode(data),
                    create_parents: true,
                    lease: self.lease(),
                },
            )
            .await
            .map_err(Self::session_error)?;
        Ok(result.bytes_written)
    }

    async fn heartbeat(
        &self,
        _run_id: &str,
        branch_id: &str,
        _client_id: &str,
    ) -> Result<(), PluginHostError> {
        self.session
            .heartbeat(&self.ctx, branch_id, self.lease())
            .await
            .map_err(Self::session_error)?;
        Ok(())
    }

    async fn delete_snapshot(
        &self,
        _run_id: &str,
        state_id: &str,
    ) -> Result<(), PluginHostError> {
        self.session
            .delete_checkpoint(
                &self.ctx,
                SessionCheckpointDeleteRequest {
                    checkpoint_id: state_id.to_string(),
                    lease: self.lease(),
                },
            )
            .await
            .map_err(Self::session_error)?;
        Ok(())
    }

    async fn diff(
        &self,
        _run_id: &str,
        branch_id: &str,
        _base_state_id: &str,
    ) -> Result<String, PluginHostError> {
        let request = SessionExecRequest {
            runtime_id: branch_id.to_string(),
            command: vec![
                "bash".to_string(),
                "-lc".to_string(),
                "git diff --no-ext-diff --binary HEAD --".to_string(),
            ],
            cwd: Some("/home/user/workspace/repo".to_string()),
            env: BTreeMap::new(),
            timeout_ms: Some(EXEC_TIMEOUT_MS),
            lease: self.lease(),
        };
        let result = self
            .session
            .exec(&self.ctx, request)
            .await
            .map_err(Self::session_error)?;
        if result.timed_out {
            return Err(PluginHostError::CapabilityDenied(
                "host.diff timed out".to_string(),
            ));
        }
        Ok(format!("{}{}", result.stdout, result.stderr))
    }

    async fn release(&self, _run_id: &str, branch_id: &str) -> Result<(), PluginHostError> {
        self.session
            .close(&self.ctx, branch_id, self.lease())
            .await
            .map_err(Self::session_error)?;
        Ok(())
    }

    async fn wait_job(
        &self,
        run_id: &str,
        job_id: &str,
    ) -> Result<HostJobOutcome, PluginHostError> {
        loop {
            if self.cancel.load(Ordering::SeqCst) {
                return Err(PluginHostError::Cancelled(format!(
                    "mcts run {run_id} cancelled while waiting for job {job_id}"
                )));
            }
            if let Some(outcome) = self
                .jobs
                .lock()
                .ok()
                .and_then(|jobs| jobs.get(job_id).cloned())
            {
                if matches!(outcome.status, JobStatus::Completed | JobStatus::Failed) {
                    return Ok(outcome);
                }
            }
            tokio::time::sleep(JOB_POLL_INTERVAL).await;
        }
    }
}

fn running_job() -> HostJobOutcome {
    HostJobOutcome {
        status: JobStatus::Running,
        output: String::new(),
        exit_code: None,
        usage: BTreeMap::new(),
        failure: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_epoch_as_rfc3339() {
        assert_eq!(rfc3339_from_unix_ms(0), "1970-01-01T00:00:00.000Z");
    }
}
