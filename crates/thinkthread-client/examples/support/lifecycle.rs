//! Standalone, run-owned lifecycle probe using only the pinned public SDK.

use std::time::Duration;

use serde_json::{json, Value};
use xgovernor_thinkthread_client::{sdk, ManagementConfig};

pub struct Probe {
    pub report: Value,
    root: Option<String>,
    message_ids: Vec<String>,
    sequence: u64,
}

impl Probe {
    pub fn new() -> Self {
        Self {
            report: json!({
                "scope": "lifecycle_only", "status": "not_run",
                "contract_fingerprint": sdk::MANAGEMENT_CONTRACT_FINGERPRINT,
                "operations": [], "checks": [], "cleanup": "not_started",
            }),
            root: None,
            message_ids: Vec::new(),
            sequence: 0,
        }
    }

    // Caller journal persists the exact UUID before the SDK can send a mutation.
    fn operation(
        &mut self,
        method: &str,
        context: &sdk::ClientContext,
        params: Value,
    ) -> Result<String, &'static str> {
        let digest =
            sdk::operation_digest(context, method, &params).map_err(|_| "operation_digest")?;
        self.sequence += 1;
        let id = uuid::Uuid::new_v4().to_string();
        self.report["operations"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "method": method, "operation_id": id, "delivery": "pending",
                "owned_target": self.root, "sequence": self.sequence, "expected_digest": digest
            }));
        Ok(id)
    }

    fn accepted(
        &mut self,
        handle: &sdk::OperationHandle,
        expected: &str,
    ) -> Result<(), &'static str> {
        if handle.operation_id != expected || handle.scope != sdk::OperationScope::ThinkThread {
            return Err("operation_identity_mismatch");
        }
        let row = self.report["operations"]
            .as_array_mut()
            .unwrap()
            .last_mut()
            .unwrap();
        if row["expected_digest"] != handle.operation_digest {
            return Err("operation_digest_mismatch");
        }
        row["delivery"] = json!("accepted");
        row["operation_digest"] = json!(handle.operation_digest);
        Ok(())
    }

    fn check(&mut self, name: &str) {
        self.report["checks"]
            .as_array_mut()
            .unwrap()
            .push(json!(name));
    }

    fn sdk_error(&mut self, error: sdk::ManagementError) -> &'static str {
        let (code, delivery) = match error {
            sdk::ManagementError::Transport { delivery, .. } => ("transport", Some(delivery)),
            sdk::ManagementError::Rejected(body) => ("rejected", Some(body.delivery)),
            sdk::ManagementError::Host(_) => ("host", None),
            sdk::ManagementError::Protocol(_) => ("protocol", None),
            sdk::ManagementError::Configuration(_) => ("configuration", None),
        };
        self.report["failure"] = json!({"category": code, "delivery": delivery});
        code
    }

    fn target(&self) -> sdk::ThinkThreadTargetRequest {
        sdk::ThinkThreadTargetRequest {
            thinkthread_id: self.root.clone().unwrap(),
        }
    }

    pub async fn run(
        &mut self,
        client: &sdk::ThinkThreadClient,
        create: sdk::ThinkThreadCreateRequest,
        timeout_ms: u64,
        persist: &mut impl FnMut(&Value) -> Result<(), &'static str>,
    ) -> Result<(), &'static str> {
        let op = self.operation(
            "thinkthread.create",
            client.context(),
            serde_json::to_value(&create).map_err(|_| "request_encoding")?,
        )?;
        persist(&self.report)?;
        let created = client
            .thinkthreads()
            .create_with_operation(op.clone(), create)
            .await
            .map_err(|error| self.sdk_error(error))?;
        self.accepted(&created.operation, &op)?;
        if created.thinkthread_id.is_empty() || !created.changed {
            return Err("create_did_not_publish_owned_root");
        }
        self.root = Some(created.thinkthread_id.clone());
        self.report["owned_root"] = json!(created.thinkthread_id);
        persist(&self.report)?;
        let initial = client
            .thinkthreads()
            .get(self.target())
            .await
            .map_err(|error| self.sdk_error(error))?;
        self.validate_view(&initial, "Ready")?;
        self.check("create_publishes_inactive_root");

        for attempt in 0..2 {
            let op = self.operation(
                "thinkthread.start",
                client.context(),
                serde_json::to_value(self.target()).map_err(|_| "request_encoding")?,
            )?;
            persist(&self.report)?;
            let started = client
                .thinkthreads()
                .start_with_operation(op.clone(), self.target())
                .await
                .map_err(|error| self.sdk_error(error))?;
            self.accepted(&started.operation, &op)?;
            if Some(&started.thinkthread_id) != self.root.as_ref()
                || (attempt == 1 && started.changed)
            {
                return Err("start_identity_or_repeat_changed");
            }
            let view = client
                .thinkthreads()
                .get(self.target())
                .await
                .map_err(|error| self.sdk_error(error))?;
            self.validate_view(&view, "Running")?;
        }
        self.check("start_and_repeated_start");
        for turn in 1..=2 {
            let marker = format!("message-{}", uuid::Uuid::new_v4());
            let message_request = sdk::MessageSendRequest {
                recipient_thinkthread_id: self.root.clone().unwrap(),
                text: format!("Reply with exactly {marker}. Do not call any tools."),
            };
            let op = self.operation(
                "message.send",
                client.context(),
                serde_json::to_value(&message_request).map_err(|_| "request_encoding")?,
            )?;
            persist(&self.report)?;
            let sent = client
                .messages()
                .send_with_operation(op.clone(), message_request)
                .await
                .map_err(|error| self.sdk_error(error))?;
            self.accepted(&sent.operation, &op)?;
            self.validate_message_identity(&sent.message, None)?;
            if sent.message.truncated || self.message_ids.contains(&sent.message.message_id) {
                return Err("message_admission_truncated_or_duplicate");
            }
            let id = sent.message.message_id;
            self.message_ids.push(id.clone());
            self.report["message_ids"] = json!(self.message_ids);
            persist(&self.report)?;
            let terminal = client
                .messages()
                .wait(sdk::MessageWaitRequest {
                    recipient_thinkthread_id: self.root.clone().unwrap(),
                    message_id: id.clone(),
                    timeout_ms: timeout_ms.into(),
                })
                .await
                .map_err(|error| self.sdk_error(error))?;
            self.validate_message_identity(&terminal, Some(&id))?;
            if terminal.state != sdk::MessageState::Completed
                || terminal.truncated
                || terminal.reply_truncated
                || terminal.error.is_some()
                || terminal.reply_text.as_deref().map(str::trim) != Some(marker.as_str())
            {
                return Err("message_not_complete_exact_untruncated_reply");
            }
            let reread = client
                .messages()
                .get(sdk::MessageTargetRequest {
                    recipient_thinkthread_id: self.root.clone().unwrap(),
                    message_id: id,
                })
                .await
                .map_err(|error| self.sdk_error(error))?;
            if reread != terminal {
                return Err("terminal_projection_changed");
            }
            self.check(&format!("managed_turn_{turn}_complete_and_stable"));
        }
        Ok(())
    }

    fn validate_view(&self, view: &sdk::ThinkThreadView, state: &str) -> Result<(), &'static str> {
        if Some(&view.thinkthread_id) != self.root.as_ref()
            || Some(&view.root_id) != self.root.as_ref()
            || view.parent_id.is_some()
            || view.needs_recovery
            || !view.state.eq_ignore_ascii_case(state)
        {
            return Err("native_root_identity_state_or_recovery_mismatch");
        }
        Ok(())
    }

    fn validate_message_identity(
        &self,
        view: &sdk::MessageView,
        id: Option<&str>,
    ) -> Result<(), &'static str> {
        if Some(&view.recipient_thinkthread_id) != self.root.as_ref()
            || view.message_id.is_empty()
            || id.is_some_and(|id| view.message_id != id)
        {
            return Err("message_identity_mismatch");
        }
        Ok(())
    }

    pub async fn cleanup(
        &mut self,
        client: &sdk::ThinkThreadClient,
        timeout_ms: u64,
        persist: &mut impl FnMut(&Value) -> Result<(), &'static str>,
    ) -> Result<(), &'static str> {
        let Some(root) = self.root.clone() else {
            self.report["cleanup"] =
                json!("no_known_root; inspect pending create operation before reclaiming");
            return Ok(());
        };
        self.report["cleanup"] = json!("pending");
        for repeat in 0..2 {
            let op = self.operation(
                "thinkthread.stop",
                client.context(),
                json!({"thinkthreadIds": [root], "force": false}),
            )?;
            persist(&self.report)?;
            let stopped = client
                .thinkthreads()
                .stop_with_operation(
                    op.clone(),
                    sdk::ThinkThreadStopRequest {
                        thinkthread_ids: vec![root.clone()],
                        force: false,
                    },
                )
                .await
                .map_err(|error| self.sdk_error(error))?;
            self.accepted(&stopped.operation, &op)?;
            if stopped.targets.len() != 1 {
                return Err("stop_batch_target_not_proven");
            }
            let target = &stopped.targets[0];
            let stopped_or_noop = (target.status == "stopped" && target.accepted)
                || (target.status == "already_stopped" && !target.accepted && !target.changed);
            if target.thinkthread_id != root || !stopped_or_noop || (repeat == 1 && target.changed)
            {
                return Err("stop_batch_target_not_proven");
            }
            // A success envelope can contain per-target failures. Require the
            // actual stopped projection before proceeding to destructive remove.
            let waited = client
                .thinkthreads()
                .wait_for_state(sdk::ThinkThreadWaitRequest {
                    thinkthread_ids: vec![root.clone()],
                    target: sdk::ThinkThreadWaitTarget::Stopped,
                    timeout_ms: timeout_ms.into(),
                })
                .await
                .map_err(|error| self.sdk_error(error))?;
            if !waited.reached || waited.thinkthreads.len() != 1 {
                return Err("stop_postcondition_not_proven");
            }
            self.validate_view(&waited.thinkthreads[0], "Stopped")?;
        }
        self.check("ordinary_stop_and_repeated_stop");
        let op = self.operation(
            "thinkthread.remove",
            client.context(),
            json!({"thinkthreadIds": [root], "force": false}),
        )?;
        persist(&self.report)?;
        let removed = client
            .thinkthreads()
            .remove_with_operation(
                op.clone(),
                sdk::ThinkThreadRemoveRequest {
                    thinkthread_ids: vec![root.clone()],
                    force: false,
                },
            )
            .await
            .map_err(|error| self.sdk_error(error))?;
        self.accepted(&removed.operation, &op)?;
        if removed.targets.len() != 1
            || removed.targets[0].thinkthread_id != root
            || !removed.targets[0].accepted
        {
            return Err("remove_batch_target_not_proven");
        }
        let waited = client
            .thinkthreads()
            .wait_for_state(sdk::ThinkThreadWaitRequest {
                thinkthread_ids: vec![root],
                target: sdk::ThinkThreadWaitTarget::Absent,
                timeout_ms: timeout_ms.into(),
            })
            .await
            .map_err(|error| self.sdk_error(error))?;
        if !waited.reached || !waited.thinkthreads.is_empty() {
            return Err("remove_not_absent");
        }
        self.report["cleanup"] = json!("owned_root_absent");
        self.check("run_owned_root_removed");
        persist(&self.report)?;
        Ok(())
    }
}

pub async fn bounded_probe(
    config: &ManagementConfig,
    create: sdk::ThinkThreadCreateRequest,
    timeout_ms: u64,
    persist: &mut impl FnMut(&Value) -> Result<(), &'static str>,
) -> Value {
    let mut probe = Probe::new();
    if !(1..=300_000).contains(&timeout_ms) {
        probe.report["failure"] = json!({"category":"invalid_wait_budget"});
        probe.report["status"] = json!("blocked");
        return probe.report;
    }
    let client = match config.connect().await {
        Ok(client) => client,
        Err(_) => {
            probe.report["failure"] = json!({"category":"connect"});
            probe.report["status"] = json!("blocked");
            return probe.report;
        }
    };
    let budget = Duration::from_millis(timeout_ms + 5_000);
    let result =
        tokio::time::timeout(budget * 8, probe.run(&client, create, timeout_ms, persist)).await;
    probe.report["status"] = json!("failed");
    match result {
        Ok(Ok(())) => {
            match tokio::time::timeout(budget * 4, probe.cleanup(&client, timeout_ms, persist))
                .await
            {
                Ok(Ok(())) => probe.report["status"] = json!("passed_lifecycle_subset"),
                Ok(Err(code)) => probe.report["cleanup_failure"] = json!(code),
                Err(_) => probe.report["cleanup_failure"] = json!("timeout_unknown"),
            }
        }
        Ok(Err(code)) => probe.report["check_failure"] = json!(code),
        Err(_) => probe.report["check_failure"] = json!("timeout_unknown"),
    }
    // Failure does not trigger force-stop or unknown mutation replay. The
    // journal retains exact run-owned IDs for explicit public-SDK reconciliation.
    match tokio::time::timeout(Duration::from_secs(5), client.close()).await {
        Ok(Ok(())) => (),
        _ => {
            probe.report["connection_close"] = json!("unconfirmed");
            probe.report["status"] = json!("failed");
        }
    }
    let _ = persist(&probe.report);
    probe.report
}
