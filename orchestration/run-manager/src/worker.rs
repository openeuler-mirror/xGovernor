//! Run-owned stdio algorithm process and execution dispatch.
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orchestration_contract::event::{EventPayload, EventSource};
use orchestration_contract::execution::*;
use orchestration_contract::rpc::*;
use orchestration_contract::run::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::os::unix::process::CommandExt;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};

use crate::config::Configuration;
use crate::store::{atomic_json, atomic_write};
use crate::{Result, RunManager, Usage};

const CALL_MS: u64 = 1000;
const MAX_LINE: u64 = 1024 * 1024;

struct EventValidator {
    _child: Child,
    input: ChildStdin,
    output: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
}

impl EventValidator {
    fn start(python: &std::path::Path) -> Result<Self> {
        let mut child = Command::new(python)
            .args(["-u", "-m", "xgovernor_mcts.bridge.event_validator"])
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().ok_or("validator stdin unavailable")?;
        let output =
            BufReader::new(child.stdout.take().ok_or("validator stdout unavailable")?).lines();
        Ok(Self {
            _child: child,
            input,
            output,
        })
    }

    async fn validate(&mut self, value: &Value) -> Result<()> {
        send(&mut self.input, value.clone()).await?;
        let line = tokio::time::timeout(Duration::from_secs(5), self.output.next_line())
            .await??
            .ok_or("event validator exited")?;
        let response: Value = serde_json::from_str(&line)?;
        if response["valid"] != true {
            return Err("algorithm event does not match MCTS events schema".into());
        }
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize)]
struct Resources {
    states: BTreeSet<String>,
    branches: BTreeSet<String>,
    jobs: BTreeMap<String, bool>,
    operations: BTreeMap<String, OperationIdentity>,
}

struct Worker<'a> {
    manager: &'a RunManager,
    config: &'a Configuration,
    request: StartRequest,
    backend: Arc<dyn ExecutionBackend>,
    run_id: String,
    directory: PathBuf,
    resources: Resources,
    usage: Usage,
    sequence: u64,
    next_key: u64,
    last_renewal: Instant,
    root: String,
    notified_jobs: BTreeSet<String>,
    started: Instant,
}

/// Called only in the detached worker. A failed durable claim means another
/// worker already owns this run; no backend resources have been created yet.
pub async fn execute(
    manager: &RunManager,
    config: &Configuration,
    backend: Arc<dyn ExecutionBackend>,
    run_id: &str,
) -> Result<()> {
    let request = match manager.claim(run_id) {
        Ok(request) => request,
        Err(_) => return Ok(()),
    };
    let mut worker = Worker {
        manager,
        config,
        request,
        backend,
        run_id: run_id.into(),
        directory: manager.run_directory(run_id)?,
        resources: Resources::default(),
        usage: Usage::default(),
        sequence: 0,
        next_key: 0,
        last_renewal: Instant::now(),
        root: String::new(),
        notified_jobs: BTreeSet::new(),
        started: Instant::now(),
    };
    let outcome = worker.run().await;
    let mut result = None;
    let mut status = RunStatus::Failed;
    match outcome {
        Ok((state, value)) => {
            status = state;
            result = Some(value);
        }
        Err(error) => {
            manager.fail(run_id, error.to_string())?;
        }
    }
    // No algorithm process remains at this point. Backend reclamation is the
    // authority for job stop and resource release, including unknown mutations.
    let cleanup = worker.cleanup().await;
    if manager
        .result_run(ResultRequest {
            run_id: run_id.into(),
        })?
        .run
        .status
        == RunStatus::Cancelling
    {
        if result.is_none() {
            result = Some(stopped_result("user_cancelled"));
        }
        status = if cleanup == CleanupStatus::Complete {
            RunStatus::Cancelled
        } else {
            RunStatus::Failed
        };
    }
    manager.finish(run_id, status, cleanup, result)
}

impl Worker<'_> {
    fn context(&self) -> CallContext {
        CallContext {
            plugin_id: self.request.config.algorithm.namespace.clone(),
            run_id: self.run_id.clone(),
            timeout_ms: CALL_MS,
        }
    }

    fn mutation(&mut self, name: &str) -> MutationContext {
        self.next_key += 1;
        let mut call = self.context();
        if name == "reclaim" {
            call.timeout_ms = 120_000;
        } else if matches!(name, "selected" | "export") {
            call.timeout_ms = self.delivery_timeout();
        }
        MutationContext {
            call,
            idempotency_key: format!("manager-{name}-{}", self.next_key),
        }
    }

    fn persist(&self) -> Result<()> {
        atomic_json(&self.directory.join("execution.json"), &self.resources)
    }

    fn event(&mut self, payload: EventPayload) -> Result<()> {
        self.sequence += 1;
        self.manager.record_event(
            &self.run_id,
            EventSource {
                source: "executor".into(),
                source_sequence: self.sequence,
            },
            payload,
        )
    }

    async fn mutation_call<T: serde::Serialize, F: Future<Output = BackendResult<T>>>(
        &mut self,
        method: &str,
        context: &MutationContext,
        call: F,
    ) -> Result<Value> {
        let identity = OperationIdentity {
            run_id: self.run_id.clone(),
            method: method.into(),
            idempotency_key: context.idempotency_key.clone(),
        };
        let key = format!("{method}:{}", context.idempotency_key);
        self.resources
            .operations
            .insert(key.clone(), identity.clone());
        self.persist()?;
        let outcome = if context.idempotency_key.starts_with("manager-selected-")
            || context.idempotency_key.starts_with("manager-export-")
        {
            self.delivery_call(context.call.timeout_ms, call).await
        } else {
            tokio::time::timeout(Duration::from_millis(context.call.timeout_ms), call)
                .await
                .map_err(Into::into)
        };
        let result = match outcome {
            Ok(Ok(value)) => Ok(serde_json::to_value(value)?),
            Ok(Err(BackendError::Rejected { code, message })) => {
                Err(BackendError::Rejected { code, message })
            }
            unknown => {
                let operation = match unknown {
                    Ok(Err(BackendError::OutcomeUnknown { operation, .. }))
                        if operation == identity =>
                    {
                        operation
                    }
                    _ => identity.clone(),
                };
                let resolution = tokio::time::timeout(
                    Duration::from_millis(CALL_MS),
                    self.backend.reconcile(self.context(), operation.clone()),
                )
                .await;
                match resolution {
                    Ok(Ok(OperationResolution::Succeeded(value))) => {
                        // MutationResult's content is the original typed payload.
                        Ok(serde_json::to_value(value)?["result"].clone())
                    }
                    Ok(Ok(OperationResolution::Rejected { code, message })) => {
                        Err(BackendError::Rejected { code, message })
                    }
                    _ => Err(BackendError::OutcomeUnknown {
                        operation,
                        message: "mutation remains unresolved".into(),
                    }),
                }
            }
        };
        if !matches!(result, Err(BackendError::OutcomeUnknown { .. })) {
            self.resources.operations.remove(&key);
        }
        // Record new resource identities in the same journal update that removes
        // the completed operation, before any reply is visible to the algorithm.
        if let Ok(value) = &result {
            if let Some(id) = value["state_id"].as_str() {
                self.resources.states.insert(id.into());
            }
            if let Some(id) = value["branch_id"].as_str() {
                self.resources.branches.insert(id.into());
            }
            if let Some(id) = value["job_id"].as_str() {
                self.resources.jobs.entry(id.into()).or_insert(false);
            }
        }
        self.persist()?;
        result.map_err(Into::into)
    }

    async fn renew(&mut self) -> Result<()> {
        if !self
            .backend
            .capabilities()
            .capabilities
            .contains(&Capability::RenewRunLease)
        {
            return Ok(());
        }
        let request = RenewRunLeaseRequest {
            context: self.mutation("lease"),
            ttl_ms: self.config.lease_ttl_ms,
        };
        let backend = self.backend.clone();
        self.mutation_call(
            "host.renew_run_lease",
            &request.context.clone(),
            backend.renew_run_lease(request),
        )
        .await?;
        self.last_renewal = Instant::now();
        Ok(())
    }

    async fn cleanup(&mut self) -> CleanupStatus {
        let request = ReclaimRunRequest {
            context: self.mutation("reclaim"),
        };
        let backend = self.backend.clone();
        let outcome = self
            .mutation_call(
                "host.reclaim_run",
                &request.context.clone(),
                backend.reclaim_run(request),
            )
            .await;
        let pending: Vec<_> = self
            .resources
            .jobs
            .iter()
            .filter(|(_, done)| !**done)
            .map(|(id, _)| id.clone())
            .collect();
        for id in pending {
            let state = tokio::time::timeout(
                Duration::from_millis(100),
                self.backend.wait_job(self.context(), &id),
            )
            .await;
            let usage = match state {
                Ok(Ok(JobState::Completed(value))) => Some(value.usage),
                Ok(Ok(JobState::Failed(value))) => Some(value.usage),
                _ => None,
            };
            if let Some(usage) = usage {
                self.add_usage(&usage);
                self.resources.jobs.insert(id, true);
            }
        }
        let _ = self.manager.account_usage(&self.run_id, self.usage.clone());
        match outcome.and_then(|value| Ok(serde_json::from_value::<RunReclaimed>(value)?)) {
            Ok(reclaimed) => {
                let _ = atomic_json(&self.directory.join("cleanup.json"), &reclaimed);
                match reclaimed.status {
                    ReclaimStatus::Complete if reclaimed.remaining.is_empty() => {
                        self.resources = Resources::default();
                        let _ = self.persist();
                        CleanupStatus::Complete
                    }
                    ReclaimStatus::Failed => CleanupStatus::Failed,
                    _ => CleanupStatus::Unknown,
                }
            }
            Err(error) => {
                let _ = atomic_json(
                    &self.directory.join("cleanup.json"),
                    &json!({"status":"unknown","error":error.to_string()}),
                );
                CleanupStatus::Unknown
            }
        }
    }

    async fn run(&mut self) -> Result<(RunStatus, RunResult)> {
        self.renew().await?;
        let mut root = PrepareRootRequest {
            context: self.mutation("root"),
            root: self.config.root_description(&self.request)?,
        };
        root.context.call.timeout_ms = self.call_timeout("host.prepare_root");
        let backend = self.backend.clone();
        let snapshot: Snapshot = serde_json::from_value(
            self.mutation_call(
                "host.prepare_root",
                &root.context.clone(),
                backend.prepare_root(root),
            )
            .await?,
        )?;
        self.root = snapshot.state_id;
        let entry = self
            .config
            .algorithm_entry(&self.request.config.algorithm.namespace)?;
        atomic_json(
            &self.directory.join("task-runtime.json"),
            &json!({"swebench_python":self.config.swebench_python}),
        )?;
        let mut command = Command::new(&self.config.python_executable);
        command
            .arg("-u")
            .arg(entry)
            .current_dir(&self.directory)
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        // Kill the algorithm if its owning worker disappears unexpectedly.
        let parent = std::process::id();
        unsafe {
            command.as_std_mut().pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() as u32 != parent {
                    return Err(std::io::Error::other("worker disappeared during spawn"));
                }
                Ok(())
            });
        }
        let mut child = command.spawn()?;
        let outcome = self.algorithm(&mut child).await;
        // Terminate the complete algorithm process group, including descendants.
        if let Some(pid) = child.id() {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
        let _ = child.wait().await;
        outcome
    }

    async fn algorithm(&mut self, child: &mut Child) -> Result<(RunStatus, RunResult)> {
        let mut input = child.stdin.take().ok_or("algorithm stdin unavailable")?;
        let output = child.stdout.take().ok_or("algorithm stdout unavailable")?;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
        let reader = tokio::spawn(async move {
            let mut output = BufReader::new(output);
            loop {
                let mut bytes = Vec::new();
                let count = (&mut output)
                    .take(MAX_LINE + 1)
                    .read_until(b'\n', &mut bytes)
                    .await?;
                if count == 0 {
                    return Ok::<_, std::io::Error>(());
                }
                if bytes.len() as u64 > MAX_LINE {
                    return Err(std::io::Error::other("algorithm frame exceeds limit"));
                }
                if sender.send(bytes).await.is_err() {
                    return Ok(());
                }
            }
        });
        let outcome = self.conversation(&mut input, &mut receiver).await;
        let _ = send(&mut input, json!({"jsonrpc":"2.0","id":"manager-shutdown","method":"plugin.shutdown","params":ShutdownParams {plugin_id:self.context().plugin_id,grace_period_ms:100}})).await;
        let _ = tokio::time::timeout(Duration::from_millis(100), child.wait()).await;
        reader.abort();
        outcome
    }

    fn remaining_time_ms(&self) -> u64 {
        self.request
            .config
            .budget
            .max_time_ms
            .saturating_sub(self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64)
    }

    fn call_timeout(&self, method: &str) -> u64 {
        self.config
            .operation_timeout_ms(method, self.remaining_time_ms())
    }

    fn non_token_budget_available(&self) -> bool {
        self.started.elapsed().as_millis() < u128::from(self.request.config.budget.max_time_ms)
            && self
                .request
                .config
                .budget
                .max_cost_microunits
                .is_none_or(|limit| self.usage.cost_microunits < limit)
    }

    async fn conversation(
        &mut self,
        input: &mut ChildStdin,
        receiver: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> Result<(RunStatus, RunResult)> {
        let capabilities: BTreeSet<_> = self
            .backend
            .capabilities()
            .capabilities
            .into_iter()
            .filter_map(WireCapability::from_backend)
            .collect();
        send(input, json!({"jsonrpc":"2.0","id":"manager-initialize","method":"plugin.initialize","params":InitializeParams {
            plugin_id: self.context().plugin_id, supported_api_versions: vec![API_VERSION.into()], host_version: env!("CARGO_PKG_VERSION").into(), available_capabilities: capabilities.clone()
        }})).await?;
        let mut validator = if self.request.config.algorithm.namespace == "mcts" {
            Some(EventValidator::start(&self.config.python_executable)?)
        } else {
            None
        };
        let mut started = false;
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        let mut deadline = Instant::now() + Duration::from_secs(10);
        let mut stop: Option<(&str, Instant)> = None;
        loop {
            tokio::select! {
                bytes = receiver.recv() => {
                    let bytes = match bytes {
                        Some(bytes) => bytes,
                        None if stop.is_some() => {
                            let reason = stop.unwrap().0;
                            return Ok((if reason == "user_cancelled" { RunStatus::Cancelled } else { RunStatus::Failed }, stopped_result(reason)));
                        }
                        None => return Err("algorithm exited without a confirmed terminal".into()),
                    };
                    let value: Value = serde_json::from_slice(&bytes)?;
                    if value["jsonrpc"] != "2.0" { return Err("invalid algorithm JSON-RPC version".into()); }
                    if value["id"] == "manager-initialize" && value.get("method").is_none() {
                        let initialized: InitializeResult = serde_json::from_value(value["result"].clone())?;
                        if initialized.api_version != API_VERSION || !initialized.required_host_capabilities.is_subset(&capabilities) { return Err("algorithm protocol/capability mismatch".into()); }
                        let parameters = self.request.config.algorithm.parameters.clone();
                        if parameters.contains_key("max_time_ms") || parameters.contains_key("max_tokens") { return Err("algorithm parameters cannot override common budget".into()); }
                        let start = StartParams {
                            plugin_id: self.context().plugin_id, run_id: self.run_id.clone(), session_id: self.request.source_session.as_ref().map_or_else(|| self.run_id.clone(), |s| s.session_id.clone()),
                            turn_id: self.request.source_session.as_ref().and_then(|s| s.turn_id.clone()).unwrap_or_else(|| self.run_id.clone()),
                            root_state_id: self.root.clone(), goal: self.request.goal.clone(), content: self.request.content.clone(),
                            budget: { let mut budget = WireBudget::from_config(&self.request.config)?; budget.max_time_ms = self.remaining_time_ms(); budget },
                            granted_capabilities: capabilities.clone(),
                        };
                        send(input, json!({"jsonrpc":"2.0","id":"manager-start","method":"plugin.start","params":start})).await?;
                        started = true;
                        deadline = Instant::now() + Duration::from_secs(10);
                    } else if value["id"] == "manager-start" && value.get("method").is_none() {
                        let accepted: StartResult = serde_json::from_value(value["result"].clone())?;
                        if accepted.run_id != self.run_id { return Err("start receipt run mismatch".into()); }
                    } else if value["id"] == "manager-cancel" && value.get("method").is_none() {
                        let cancelled: CancelResult = serde_json::from_value(value["result"].clone())?;
                        if cancelled.run_id != self.run_id { return Err("cancel receipt run mismatch".into()); }
                    } else if value.get("method").is_some() && value.get("id").is_some() {
                        let id = value["id"].clone();
                        let response = self.dispatch(&value).await;
                        let envelope = match response {
                            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
                            Err(error) => {
                                if matches!(error.downcast_ref::<BackendError>(), Some(BackendError::OutcomeUnknown { .. })) { return Err(error); }
                                if stop.is_none() && !self.non_token_budget_available() && matches!(error.downcast_ref::<BackendError>(), Some(BackendError::Rejected { code: ErrorCode::BudgetExhausted, .. })) {
                                    let cancelling = self.manager.result_run(ResultRequest { run_id: self.run_id.clone() })?.run.status == RunStatus::Cancelling;
                                    let reason = if cancelling { "user_cancelled" } else { "budget_exhausted" };
                                    stop = Some((reason, Instant::now()));
                                    send(input, json!({"jsonrpc":"2.0","id":"manager-cancel","method":"plugin.cancel","params":CancelParams {plugin_id:self.context().plugin_id,run_id:self.run_id.clone(),reason:reason.into()}})).await?;
                                }
                                let rpc = error.downcast_ref::<BackendError>().cloned().and_then(|value| RpcError::try_from(value).ok()).unwrap_or(RpcError { code: -32602, message: error.to_string(), data: Value::Null });
                                json!({"jsonrpc":"2.0","id":id,"error":rpc})
                            }
                        };
                        let job = envelope["result"]["job_id"].as_str().map(str::to_owned);
                        send(input, envelope).await?;
                        if let Some(job) = job {
                            if self.notified_jobs.insert(job.clone()) {
                                send(input, json!({"jsonrpc":"2.0","method":"host.job.started","params":{"run_id":self.run_id,"job_id":job,"sequence":1}})).await?;
                            }
                        }
                    } else if let Some(method) = value["method"].as_str() {
                        if !method.starts_with("plugin.") { return Err("unsupported algorithm notification".into()); }
                        let event: AlgorithmEvent = serde_json::from_value(value["params"].clone())?;
                        if event.run_id != self.run_id || event.plugin_id != self.context().plugin_id { return Err("algorithm notification ownership mismatch".into()); }
                        let structured = event.payload.get("summary").and_then(Value::as_str)
                            .and_then(|summary| summary.strip_prefix("mcts.event.v1 "));
                        let payload = if self.request.config.algorithm.namespace == "mcts" {
                            if let Some(encoded) = structured {
                                let raw: Value = serde_json::from_str(encoded)?;
                                validator.as_mut().ok_or("event validator unavailable")?.validate(&raw).await?;
                                let payload: EventPayload = serde_json::from_value(raw)?;
                                if !matches!(payload, EventPayload::Node { .. } | EventPayload::Score { .. }
                                    | EventPayload::Selection { .. } | EventPayload::Artifact { .. }
                                    | EventPayload::Extension { .. }) {
                                    return Err("algorithm cannot emit lifecycle or execution events".into());
                                }
                                payload
                            } else {
                                EventPayload::Extension {
                                    namespace: self.request.config.algorithm.namespace.clone(), version: "1".into(),
                                    schema: "urn:xgovernor:mcts:events:1".into(),
                                    name: method.into(), data: serde_json::to_value(&event.payload)?,
                                }
                            }
                        } else {
                            EventPayload::Extension {
                                namespace: self.request.config.algorithm.namespace.clone(), version: self.request.config.algorithm.version.clone(), schema: self.request.config.algorithm.schema.clone(),
                                name: method.into(), data: serde_json::to_value(&event.payload)?,
                            }
                        };
                        if structured.is_none() {
                            if let Some(validator) = validator.as_mut() {
                                validator.validate(&serde_json::to_value(&payload)?).await?;
                            }
                        }
                        self.manager.record_event(&self.run_id, EventSource { source: "algorithm".into(), source_sequence: event.sequence }, payload)?;
                        match method {
                            "plugin.completed" if stop.is_none() => {
                                let result = value["params"]["result"].clone();
                                return self.deliver(result).await;
                            }
                            "plugin.cancelled" => return Ok((if stop.is_some_and(|s| s.0 == "budget_exhausted") { RunStatus::Failed } else { RunStatus::Cancelled }, stopped_result(stop.map_or("algorithm_cancelled", |s| s.0)))),
                            "plugin.failed" => return Err(format!("algorithm failed: {}", value["params"]["failure"]).into()),
                            _ => {}
                        }
                    } else if value.get("error").is_some() { return Err(format!("algorithm lifecycle rejected: {}", value["error"]).into()); }
                }
                _ = tick.tick() => {
                    if self.last_renewal.elapsed().as_millis() >= u128::from(self.config.lease_ttl_ms / 3) { self.renew().await?; }
                    let state = self.manager.result_run(ResultRequest { run_id: self.run_id.clone() })?.run.status;
                    self.manager.account_usage(&self.run_id, self.usage.clone())?;
                    let within_budget = self.non_token_budget_available();
                    if stop.is_none() && (state == RunStatus::Cancelling || !within_budget) {
                        let reason = if state == RunStatus::Cancelling { "user_cancelled" } else { "budget_exhausted" };
                        stop = Some((reason, Instant::now()));
                        send(input, json!({"jsonrpc":"2.0","id":"manager-cancel","method":"plugin.cancel","params":CancelParams {plugin_id:self.context().plugin_id,run_id:self.run_id.clone(),reason:reason.into()}})).await?;
                    }
                    if let Some((reason, since)) = stop {
                        if since.elapsed() >= Duration::from_secs(2) { return Ok((if reason == "user_cancelled" { RunStatus::Cancelled } else { RunStatus::Failed }, stopped_result(reason))); }
                    }
                    self.jobs(input).await?;
                    if !started && Instant::now() > deadline { return Err("algorithm initialization timed out".into()); }
                }
            }
        }
    }

    fn add_usage(&mut self, usage: &BTreeMap<String, u64>) {
        self.usage.tokens = self.usage.tokens.saturating_add(
            usage
                .get("total_tokens")
                .copied()
                .or_else(|| usage.get("tokens").copied())
                .unwrap_or_else(|| {
                    usage
                        .get("input_tokens")
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(usage.get("output_tokens").copied().unwrap_or(0))
                }),
        );
        self.usage.cost_microunits = self
            .usage
            .cost_microunits
            .saturating_add(usage.get("cost_microunits").copied().unwrap_or(0));
    }

    async fn jobs(&mut self, input: &mut ChildStdin) -> Result<()> {
        let jobs: Vec<_> = self
            .resources
            .jobs
            .iter()
            .filter(|(_, done)| !**done)
            .map(|(id, _)| id.clone())
            .collect();
        for job in jobs {
            let mut context = self.context();
            context.timeout_ms = 100;
            let state = tokio::time::timeout(
                Duration::from_millis(100),
                self.backend.wait_job(context, &job),
            )
            .await;
            let (method, payload, usage) = match state {
                Ok(Ok(JobState::Completed(value))) => (
                    "host.job.completed",
                    serde_json::to_value(&value)?,
                    value.usage,
                ),
                Ok(Ok(JobState::Failed(value))) => (
                    "host.job.failed",
                    serde_json::to_value(&value)?,
                    value.usage,
                ),
                Ok(Err(error)) => return Err(error.into()),
                _ => continue,
            };
            self.add_usage(&usage);
            self.manager
                .account_usage(&self.run_id, self.usage.clone())?;
            self.resources.jobs.insert(job.clone(), true);
            self.persist()?;
            let mut params = payload.as_object().ok_or("invalid job payload")?.clone();
            params.insert("run_id".into(), json!(self.run_id));
            params.insert("job_id".into(), json!(job));
            params.insert("sequence".into(), json!(2));
            self.event(EventPayload::Execution {
                method: method.into(),
                branch_id: None,
                job_id: Some(job),
                status: "terminal".into(),
            })?;
            send(
                input,
                json!({"jsonrpc":"2.0","method":method,"params":params}),
            )
            .await?;
        }
        Ok(())
    }

    async fn dispatch(&mut self, value: &Value) -> Result<Value> {
        let method = value["method"].as_str().ok_or("missing host method")?;
        let mut params = value["params"].clone();
        let mut context: CallContext = serde_json::from_value(params.clone())?;
        if context.run_id != self.run_id
            || context.plugin_id != self.context().plugin_id
            || context.timeout_ms == 0
        {
            return Err("host context is not authorized or deadline is zero".into());
        }
        context.timeout_ms = context.timeout_ms.min(self.call_timeout(method));
        params["timeout_ms"] = json!(context.timeout_ms);
        if let Some(branch) = params["branch_id"].as_str() {
            if !self.resources.branches.contains(branch) {
                return Err("branch is not owned by this run".into());
            }
        }
        if let Some(state) = params["parent_state_id"]
            .as_str()
            .or_else(|| params["base_state_id"].as_str())
        {
            if !self.resources.states.contains(state) {
                return Err("state is not owned by this run".into());
            }
        }
        if let Some(job) = params["job_id"].as_str() {
            if !self.resources.jobs.contains_key(job) {
                return Err("job is not owned by this run".into());
            }
        }
        let capability: Capability = serde_json::from_value(json!(method
            .strip_prefix("host.")
            .ok_or("not a host method")?))?;
        if !self
            .backend
            .capabilities()
            .capabilities
            .contains(&capability)
        {
            return Err("host capability unavailable".into());
        }
        self.manager
            .account_usage(&self.run_id, self.usage.clone())?;
        let running = self
            .manager
            .result_run(ResultRequest {
                run_id: self.run_id.clone(),
            })?
            .run
            .status
            == RunStatus::Running;
        if matches!(
            capability,
            Capability::AgentTurn
                | Capability::Exec
                | Capability::Fork
                | Capability::FileWrite
                | Capability::Snapshot
        ) && (!running
            || !self.non_token_budget_available()
            || (capability == Capability::AgentTurn
                && self.usage.tokens >= self.request.config.budget.max_tokens))
        {
            return Err(BackendError::Rejected {
                code: ErrorCode::BudgetExhausted,
                message: "run is no longer accepting this execution request".into(),
            }
            .into());
        }
        let backend = self.backend.clone();
        macro_rules! mutation {
            ($ty:ty, $call:ident) => {{
                let request: $ty = serde_json::from_value(params.clone())?;
                self.mutation_call(method, &request.context.clone(), backend.$call(request))
                    .await
            }};
        }
        macro_rules! query {
            ($ty:ty, $call:ident) => {{
                let request: $ty = serde_json::from_value(params.clone())?;
                Ok(serde_json::to_value(
                    tokio::time::timeout(
                        Duration::from_millis(context.timeout_ms),
                        backend.$call(request),
                    )
                    .await??,
                )?)
            }};
        }
        let result = match method {
            "host.fork" => {
                let params: HostForkParams = serde_json::from_value(params.clone())?;
                let request = self.manager.fork_request(params)?;
                self.mutation_call(method, &request.context.clone(), backend.fork(request))
                    .await
            }
            "host.snapshot" => mutation!(SnapshotRequest, snapshot),
            "host.agent_turn" => mutation!(AgentTurnRequest, agent_turn),
            "host.cancel_job" => {
                let value = mutation!(CancelJobRequest, cancel_job)?;
                let backend_result: CancelJobResponse = serde_json::from_value(value)?;
                Ok(serde_json::to_value(WireCancelJobResponse::from(
                    backend_result,
                ))?)
            }
            "host.exec" => mutation!(ExecRequest, exec),
            "host.file_write" => mutation!(FileWriteRequest, file_write),
            "host.release" => mutation!(ReleaseRequest, release),
            "host.inspect" => query!(InspectRequest, inspect),
            "host.file_read" => query!(FileReadRequest, file_read),
            "host.diff" => query!(DiffRequest, diff),
            _ => Err("unknown host method".into()),
        };
        if result.is_ok() {
            self.event(EventPayload::Execution {
                method: method.into(),
                branch_id: params["branch_id"].as_str().map(Into::into),
                job_id: result
                    .as_ref()
                    .ok()
                    .and_then(|v| v["job_id"].as_str())
                    .map(Into::into),
                status: "returned".into(),
            })?;
        }
        result
    }

    fn delivery_timeout(&self) -> u64 {
        self.remaining_time_ms()
            .min(self.config.job_timeout_ms)
            .max(1)
    }

    async fn delivery_call<T, F: Future<Output = BackendResult<T>>>(
        &mut self,
        timeout_ms: u64,
        call: F,
    ) -> Result<BackendResult<T>> {
        let call = tokio::time::timeout(Duration::from_millis(timeout_ms), call);
        tokio::pin!(call);
        let mut heartbeat =
            tokio::time::interval(Duration::from_millis(self.config.lease_ttl_ms / 3));
        loop {
            tokio::select! {
                result = &mut call => return result.map_err(Into::into),
                _ = heartbeat.tick() => Box::pin(self.renew()).await?,
            }
        }
    }

    async fn deliver(&mut self, value: Value) -> Result<(RunStatus, RunResult)> {
        atomic_json(&self.directory.join("selection.json"), &value)?;
        let mut result = RunResult {
            summary: value["summary"]
                .as_str()
                .unwrap_or("algorithm completed")
                .into(),
            answer: value["answer"].as_str().unwrap_or("").into(),
            stop_reason: value["stop_reason"].as_str().unwrap_or("completed").into(),
            selected_candidate_id: value["selected_candidate_id"].as_str().map(Into::into),
            artifacts: vec![
                artifact("selection.json", "selection", "application/json"),
                artifact("events.jsonl", "record", "application/x-ndjson"),
            ],
            continuation: None,
            limitations: serde_json::from_value(value["limitations"].clone()).unwrap_or_default(),
        };
        let status = match self.deliver_artifacts(&value, &mut result).await {
            Ok(()) => RunStatus::Completed,
            Err(error) => {
                let message = format!("selected-result delivery failed: {error}");
                self.manager.fail(&self.run_id, message.clone())?;
                result.limitations.push(message);
                RunStatus::Failed
            }
        };
        for reference in result.artifacts.clone() {
            self.event(EventPayload::Artifact { reference })?;
        }
        Ok((status, result))
    }

    async fn deliver_artifacts(&mut self, value: &Value, result: &mut RunResult) -> Result<()> {
        let branch = value["branch_id"]
            .as_str()
            .or_else(|| value["apply_plan"]["source_branch_id"].as_str());
        let state = value["state_id"]
            .as_str()
            .or_else(|| value["apply_plan"]["state_id"].as_str());
        let selected = if let Some(branch) = branch {
            if !self.resources.branches.contains(branch) {
                return Err("selected branch is not owned by run".into());
            }
            Some(branch.to_owned())
        } else if let Some(state) = state {
            if !self.resources.states.contains(state) {
                return Err("selected snapshot is not owned by run".into());
            }
            let context = self.mutation("selected");
            let request = self
                .request
                .config
                .fork_request(context, state.into(), true);
            let backend = self.backend.clone();
            let branch: Branch = serde_json::from_value(
                self.mutation_call("host.fork", &request.context.clone(), backend.fork(request))
                    .await?,
            )?;
            Some(branch.branch_id)
        } else {
            None
        };
        if let Some(branch_id) = selected {
            let timeout_ms = self.delivery_timeout();
            let backend = self.backend.clone();
            let patch = self
                .delivery_call(
                    timeout_ms,
                    backend.diff(DiffRequest {
                        context: CallContext {
                            timeout_ms,
                            ..self.context()
                        },
                        branch_id: branch_id.clone(),
                        base_state_id: self.root.clone(),
                    }),
                )
                .await??;
            atomic_write(
                &self.directory.join("selected.patch"),
                patch.content.as_bytes(),
            )?;
            result
                .artifacts
                .push(artifact("selected.patch", "patch", "text/x-diff"));
            let source = WorkspaceRef::Branch(branch_id);
            let request = ExportWorkspaceRequest {
                context: self.mutation("export"),
                source,
                run_directory: self.directory.to_string_lossy().into(),
                path: "workspace.tar".into(),
                max_bytes: self.config.export_max_bytes,
                include_vcs_metadata: false,
            };
            let backend = self.backend.clone();
            let summary = self
                .mutation_call(
                    "host.export_workspace",
                    &request.context.clone(),
                    backend.export_workspace(request),
                )
                .await?;
            atomic_json(&self.directory.join("workspace-summary.json"), &summary)?;
            result
                .artifacts
                .push(artifact("workspace.tar", "workspace", "application/x-tar"));
        }
        Ok(())
    }
}

fn artifact(id: &str, kind: &str, media_type: &str) -> ArtifactRef {
    ArtifactRef {
        artifact_id: id.into(),
        kind: kind.into(),
        media_type: media_type.into(),
        expires_at: None,
    }
}
fn stopped_result(reason: &str) -> RunResult {
    RunResult {
        summary: reason.into(),
        answer: String::new(),
        stop_reason: reason.into(),
        selected_candidate_id: None,
        artifacts: vec![],
        continuation: None,
        limitations: vec![],
    }
}
async fn send(input: &mut ChildStdin, value: Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(1), input.write_all(&bytes)).await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackendRegistration;

    struct ExportTimeout;

    #[async_trait::async_trait]
    impl ExecutionBackend for ExportTimeout {
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities {
                capabilities: BTreeSet::new(),
                network_policies: [NetworkPolicy::Offline].into(),
                isolation: Isolation::None,
                joint_snapshots: false,
                independent_branches: false,
                can_reset_history: false,
            }
        }
        async fn snapshot(&self, _: SnapshotRequest) -> BackendResult<Snapshot> {
            unreachable!()
        }
        async fn fork(&self, _: ForkRequest) -> BackendResult<Branch> {
            unreachable!()
        }
        async fn agent_turn(&self, _: AgentTurnRequest) -> BackendResult<JobReceipt> {
            unreachable!()
        }
        async fn cancel_job(&self, _: CancelJobRequest) -> BackendResult<CancelJobResponse> {
            unreachable!()
        }
        async fn inspect(&self, _: InspectRequest) -> BackendResult<Content> {
            unreachable!()
        }
        async fn exec(&self, _: ExecRequest) -> BackendResult<JobReceipt> {
            unreachable!()
        }
        async fn file_read(&self, _: FileReadRequest) -> BackendResult<FileContent> {
            unreachable!()
        }
        async fn file_write(&self, _: FileWriteRequest) -> BackendResult<FileWritten> {
            unreachable!()
        }
        async fn release(&self, _: ReleaseRequest) -> BackendResult<Released> {
            unreachable!()
        }
        async fn wait_job(&self, _: CallContext, _: &str) -> BackendResult<JobState> {
            unreachable!()
        }
        async fn reconcile(
            &self,
            _: CallContext,
            _: OperationIdentity,
        ) -> BackendResult<OperationResolution> {
            Ok(OperationResolution::Unknown)
        }
        async fn diff(&self, r: DiffRequest) -> BackendResult<Content> {
            assert!(r.context.timeout_ms > 30_000 && r.context.timeout_ms <= 90_000);
            Ok(Content {
                content: "selected patch".into(),
            })
        }
        async fn export_workspace(
            &self,
            r: ExportWorkspaceRequest,
        ) -> BackendResult<WorkspaceExported> {
            assert!(r.context.call.timeout_ms > 30_000 && r.context.call.timeout_ms <= 90_000);
            Err(BackendError::Rejected {
                code: ErrorCode::DeadlineExceeded,
                message: "export timed out".into(),
            })
        }
    }

    #[tokio::test]
    async fn delivery_timeout_preserves_selection_and_published_patch() {
        let directory = tempfile::tempdir().unwrap();
        let backend = Arc::new(ExportTimeout);
        let manager = RunManager::open(
            directory.path(),
            vec![BackendRegistration {
                kind: BackendKind::E2b,
                capabilities: backend.capabilities(),
            }],
        )
        .unwrap();
        let request = serde_json::from_value(json!({
            "start_key":"delivery", "goal":"repair", "content":"",
            "code_source":{"kind":"snapshot","state_id":"root"},
            "config":{"budget":{"max_tokens":1,"max_time_ms":90_000},"network_policy":"offline",
                "backend":{"selection":"auto"}, "requirements":{"capabilities":[],"isolation":"none","joint_snapshots":false,"independent_branches":false},
                "algorithm":{"namespace":"mcts","version":"1","schema":"v1","parameters":{}}},
            "approval":{"budget":{"max_tokens":1,"max_time_ms":90_000}}
        })).unwrap();
        let run_id = manager.start_run(request).unwrap().run_id;
        let config: Configuration = serde_json::from_value(json!({
            "runs_dir":directory.path(), "algorithms_dir":"unused", "models":{}
        }))
        .unwrap();
        let mut worker = Worker {
            manager: &manager,
            config: &config,
            request: manager.claim(&run_id).unwrap(),
            backend,
            run_id: run_id.clone(),
            directory: manager.run_directory(&run_id).unwrap(),
            resources: Resources::default(),
            usage: Usage::default(),
            sequence: 0,
            next_key: 0,
            last_renewal: Instant::now(),
            root: "root".into(),
            notified_jobs: BTreeSet::new(),
            started: Instant::now(),
        };
        worker.resources.branches.insert("selected".into());
        assert!(worker.mutation("selected").call.timeout_ms > 30_000);
        let selection = json!({"summary":"selected", "answer":"answer", "stop_reason":"budget_exhausted",
            "selected_candidate_id":"node", "branch_id":"selected", "official_report":{"resolved":true}});
        let (status, result) = worker.deliver(selection.clone()).await.unwrap();
        manager
            .finish(&run_id, status, CleanupStatus::Unknown, Some(result))
            .unwrap();
        let terminal = manager.result_run(ResultRequest { run_id }).unwrap();
        assert_eq!(terminal.run.status, RunStatus::Failed);
        assert!(terminal
            .failure
            .unwrap()
            .message
            .contains("export timed out"));
        let result = terminal.result.unwrap();
        assert_eq!(result.selected_candidate_id.as_deref(), Some("node"));
        assert_eq!(result.answer, "answer");
        assert_eq!(result.stop_reason, "budget_exhausted");
        assert_eq!(
            result
                .artifacts
                .iter()
                .map(|a| a.artifact_id.as_str())
                .collect::<Vec<_>>(),
            ["selection.json", "events.jsonl", "selected.patch"]
        );
        assert!(result
            .limitations
            .iter()
            .any(|s| s.contains("export timed out")));
        assert_eq!(
            serde_json::from_slice::<Value>(
                &std::fs::read(worker.directory.join("selection.json")).unwrap()
            )
            .unwrap(),
            selection
        );
    }
}
