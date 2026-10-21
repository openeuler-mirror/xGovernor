//! Atomic cancellation control and persistent workspace boundary bookkeeping.
use crate::{sqlite_joint::*, *};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn validate_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.prepare(
        "SELECT intent_id,context_json,native_json,outcome_json FROM joint_turn_cancels LIMIT 0",
    )?;
    conn.prepare("SELECT binding_id,generation,cancel_epoch,checkpoint_epoch FROM joint_cancel_boundaries LIMIT 0")?;
    conn.prepare("SELECT operation_id,body,checkpoint_id FROM joint_cancel_checkpoints LIMIT 0")?;
    Ok(())
}

impl SqliteSessionRepository {
    pub async fn prepare_joint_cancel(
        &self,
        context: JointOperationContext,
        session: SessionRecord,
        turn_id: Option<String>,
    ) -> std::result::Result<Option<JointTurnRecord>, SessionDomainError> {
        self.joint_transaction(move |tx| {
            let binding = session.execution_backing.joint().ok_or_else(|| conflict("cancel requires joint binding"))?;
            if context.operation != "cancel"
                || context.source_runtime_id.as_deref() != Some(&session.runtime_id)
                || context.runtime_kind != session.runtime.runtime_kind
                || context.configuration_revision != binding.configuration_revision
                || context.owner_ref != tenant_key(context.tenant_id.as_deref())
                || context.tenant_id != session.tenant_id
            {
                return Err(conflict("cancel context mismatch"));
            }
            let rows = {
                let mut stmt = tx.prepare("SELECT o.request_json,o.admission_json,t.terminal_json FROM joint_turns t JOIN joint_operations o ON t.intent_id=o.id WHERE t.runtime_id=?1 AND ((?2 IS NOT NULL AND t.turn_id=?2) OR (?2 IS NULL AND t.terminal_json IS NULL))")?;
                let rows = stmt.query_map(params![session.runtime_id,turn_id], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<String>>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
                rows
            };
            if rows.is_empty() && turn_id.is_none() { return Ok(None); }
            if rows.len()!=1 {
                return Err(SessionDomainError::NotFound {runtime_id:session.runtime_id.clone()}.into());
            }
            let (raw, admission, terminal) = &rows[0];
            let request: JointPrepareRequest = decode(raw)?;
            let admission: JointAdmission = decode(admission)?;
            let current: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_sessions s JOIN joint_bindings b ON b.id=s.binding_id WHERE s.runtime_id=?1 AND s.binding_id=?2 AND b.generation=?3 AND b.state='active')",params![session.runtime_id,binding.binding_id,admission.generation],|r|r.get(0))?;
            if !current || request.binding!=*binding || admission.binding!=*binding
                || request.context.owner_ref!=context.owner_ref
                || request.context.tenant_id!=context.tenant_id
                || request.context.runtime_kind!=context.runtime_kind
                || request.context.configuration_revision!=context.configuration_revision
                || request.context.source_runtime_id!=context.source_runtime_id
            { return Err(conflict("cancel original ownership mismatch")); }
            if terminal.is_none() {
                check_fences(tx,&request,&admission)?;
                tx.execute("INSERT OR IGNORE INTO joint_turn_cancels(intent_id,context_json) VALUES(?1,?2)",params![admission.intent_id,encode(&context)?])?;
            }
            Ok(Some(JointTurnRecord {
                runtime_id:session.runtime_id,
                identity:request.turn.ok_or_else(||conflict("cancel turn identity absent"))?,
                admission,
                terminal:terminal.as_deref().map(decode).transpose()?,
            }))
        }).await
    }

    pub async fn joint_cancel_control(
        &self,
        admission: JointAdmission,
    ) -> std::result::Result<Option<JointCancelControl>, SessionDomainError> {
        self.joint_transaction(move |tx| {
            operation(tx, &admission)?;
            let row: Option<(Option<String>, Option<String>)> = tx
                .query_row(
                    "SELECT native_json,outcome_json FROM joint_turn_cancels WHERE intent_id=?1",
                    [&admission.intent_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            row.map(|(native, outcome)| {
                Ok(JointCancelControl {
                    admission,
                    native: native.as_deref().map(decode).transpose()?,
                    outcome: outcome.as_deref().map(decode).transpose()?,
                })
            })
            .transpose()
        })
        .await
    }

    pub async fn prepare_joint_cancel_native(
        &self,
        admission: JointAdmission,
        native: JointNativeIntent,
    ) -> std::result::Result<JointCancelControl, SessionDomainError> {
        validate_native(&native).map_err(|_| SessionDomainError::InvalidRequest {
            message: "invalid cancel operation".into(),
        })?;
        self.joint_transaction(move |tx| {
            let (request, stage, _) = operation(tx, &admission)?;
            if stage == "settled" {
                return Err(conflict("turn already settled"));
            }
            check_fences(tx, &request, &admission)?;
            let original = &request.native[0];
            let outcome: String = tx.query_row(
                "SELECT outcome_json FROM joint_native_steps WHERE intent_id=?1 AND step=?2",
                params![admission.intent_id, original.step],
                |r| r.get(0),
            )?;
            let outcome: JointNativeOutcome = decode(&outcome)?;
            if request.context.operation != "turn"
                || native.method != "message.cancel"
                || native.effect != NativeEffect::Mutate
                || native.compensation
                || native.params["recipientThinkthreadId"]
                    != original.params["recipientThinkthreadId"]
                || !outcome
                    .resources
                    .iter()
                    .any(|o| o.kind == "message" && native.params["messageId"] == o.id)
                || native.params.as_object().is_none_or(|o| o.len() != 2)
            {
                return Err(conflict("cancel target differs from original Message"));
            }
            let (old, outcome): (Option<String>, Option<String>) = tx.query_row(
                "SELECT native_json,outcome_json FROM joint_turn_cancels WHERE intent_id=?1",
                [&admission.intent_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let native = if let Some(old) = old {
                decode::<JointNativeIntent>(&old)?
            } else {
                tx.execute(
                    "UPDATE joint_turn_cancels SET native_json=?1 WHERE intent_id=?2",
                    params![encode(&native)?, admission.intent_id],
                )?;
                native
            };
            Ok(JointCancelControl {
                admission,
                native: Some(native),
                outcome: outcome.as_deref().map(decode).transpose()?,
            })
        })
        .await
    }

    pub async fn claim_joint_cancel_send(
        &self,
        admission: JointAdmission,
    ) -> std::result::Result<bool, SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (request,stage,_)=operation(tx,&admission)?;
            if stage=="settled" {return Ok(false);}
            check_fences(tx,&request,&admission)?;
            let unknown=JointNativeOutcome {state:NativeOperationState::Unknown,resources:vec![],payload:serde_json::json!({"delivery":"sent_unknown"})};
            Ok(tx.execute("UPDATE joint_turn_cancels SET outcome_json=?1 WHERE intent_id=?2 AND native_json IS NOT NULL AND outcome_json IS NULL",params![encode(&unknown)?,admission.intent_id])?==1)
        }).await
    }

    pub async fn record_joint_cancel_result(
        &self,
        admission: JointAdmission,
        native: JointNativeIntent,
        outcome: JointNativeOutcome,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move |tx| {
            operation(tx, &admission)?;
            let (old_native, old): (String, Option<String>) = tx.query_row(
                "SELECT native_json,outcome_json FROM joint_turn_cancels WHERE intent_id=?1",
                [&admission.intent_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if decode::<JointNativeIntent>(&old_native)? != native {
                return Err(conflict("cancel result identity changed"));
            }
            if let Some(old) = old {
                let old: JointNativeOutcome = decode(&old)?;
                if old == outcome {
                    return Ok(());
                }
                if matches!(
                    old.state,
                    NativeOperationState::Completed
                        | NativeOperationState::Failed
                        | NativeOperationState::NotSent
                ) {
                    return Err(conflict("cancel operation terminal changed"));
                }
            }
            tx.execute(
                "UPDATE joint_turn_cancels SET outcome_json=?1 WHERE intent_id=?2",
                params![encode(&outcome)?, admission.intent_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn joint_workspace_boundary(
        &self,
        binding: JointBinding,
    ) -> std::result::Result<JointWorkspaceBoundary, SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (generation,owner,revision):(u64,String,String)=tx.query_row("SELECT generation,owner_kind,revision FROM joint_bindings WHERE id=?1",[&binding.binding_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
            if owner!=binding.owner_kind || revision!=binding.configuration_revision {return Err(conflict("workspace boundary ownership mismatch"));}
            let (cancel_epoch,checkpoint_epoch)=tx.query_row("SELECT cancel_epoch,checkpoint_epoch FROM joint_cancel_boundaries WHERE binding_id=?1 AND generation=?2",params![binding.binding_id,generation],|r|Ok((r.get(0)?,r.get(1)?))).optional()?.unwrap_or((0,0));
            Ok(JointWorkspaceBoundary {binding,generation,cancel_epoch,checkpoint_epoch})
        }).await
    }
}

pub(crate) fn require_stable_boundary(tx: &Connection, binding_id: &str) -> Result<()> {
    let unsettled: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM joint_cancel_boundaries WHERE binding_id=?1 AND cancel_epoch>checkpoint_epoch)",
        [binding_id], |r| r.get(0),
    )?;
    if unsettled {
        return Err(conflict(
            "workspace awaits a verified checkpoint after cancellation",
        ));
    }
    Ok(())
}

impl SqliteSessionRepository {
    pub async fn prepare_joint_checkpoint_boundary(
        &self,
        binding: JointBinding,
        native: JointNativeIntent,
    ) -> std::result::Result<JointCheckpointBoundaryObservation, SessionDomainError> {
        validate_native(&native).map_err(|_| SessionDomainError::InvalidRequest {
            message: "invalid checkpoint observation".into(),
        })?;
        self.joint_transaction(move |tx| {
            let generation:u64=tx.query_row("SELECT generation FROM joint_bindings WHERE id=?1 AND owner_kind=?2 AND revision=?3 AND state='active'",params![binding.binding_id,binding.owner_kind,binding.configuration_revision],|r|r.get(0))?;
            let source=native.params["thinkthreadId"].as_str().ok_or_else(||conflict("checkpoint source absent"))?;
            let owned:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_objects WHERE binding_id=?1 AND generation=?2 AND native_id=?3 AND kind='root' AND absent=0)",params![binding.binding_id,generation,source],|r|r.get(0))?;
            if !owned || native.method!="checkpoint.create" || native.params["runtimeThinkthreadId"]!=source || native.compensation || native.effect!=NativeEffect::Mutate {
                return Err(conflict("checkpoint observation source mismatch"));
            }
            if let Some(body)=tx.query_row("SELECT body FROM joint_cancel_checkpoints WHERE operation_id=?1",[&native.operation_id],|r|r.get::<_,String>(0)).optional()? {
                let old:JointCheckpointBoundaryObservation=decode(&body)?;
                if old.native!=native || old.boundary.binding!=binding || old.boundary.generation!=generation {return Err(conflict("checkpoint observation identity changed"));}
                return Ok(old);
            }
            let (cancel_epoch,checkpoint_epoch)=tx.query_row("SELECT cancel_epoch,checkpoint_epoch FROM joint_cancel_boundaries WHERE binding_id=?1 AND generation=?2",params![binding.binding_id,generation],|r|Ok((r.get(0)?,r.get(1)?))).optional()?.unwrap_or((0,0));
            let ticket=JointCheckpointBoundaryObservation {boundary:JointWorkspaceBoundary {binding,generation,cancel_epoch,checkpoint_epoch},native};
            tx.execute("INSERT INTO joint_cancel_checkpoints(operation_id,body) VALUES(?1,?2)",params![ticket.native.operation_id,encode(&ticket)?])?;
            Ok(ticket)
        }).await
    }

    pub async fn confirm_joint_checkpoint_boundary(
        &self,
        ticket: JointCheckpointBoundaryObservation,
        checkpoint_id: String,
    ) -> std::result::Result<bool, SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (body,old):(String,Option<String>)=tx.query_row("SELECT body,checkpoint_id FROM joint_cancel_checkpoints WHERE operation_id=?1",[&ticket.native.operation_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if decode::<JointCheckpointBoundaryObservation>(&body)?!=ticket || checkpoint_id.trim().is_empty() || old.is_some_and(|v|v!=checkpoint_id) {return Err(conflict("checkpoint success observation changed"));}
            let b=&ticket.boundary;
            let generation:u64=tx.query_row("SELECT generation FROM joint_bindings WHERE id=?1 AND state='active'",[&b.binding.binding_id],|r|r.get(0))?;
            if generation!=b.generation {return Err(conflict("checkpoint binding generation changed"));}
            tx.execute("UPDATE joint_cancel_checkpoints SET checkpoint_id=?1 WHERE operation_id=?2",params![checkpoint_id,ticket.native.operation_id])?;
            let updated=tx.execute("UPDATE joint_cancel_boundaries SET checkpoint_epoch=?1 WHERE binding_id=?2 AND generation=?3 AND cancel_epoch=?1",params![b.cancel_epoch,b.binding.binding_id,b.generation])?;
            Ok(updated==1)
        }).await
    }
}

impl SqliteSessionRepository {
    pub async fn invalidate_joint_turn(
        &self,
        admission: JointAdmission,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (request, stage, _) = operation(tx, &admission)?;
            if stage == "settled" {
                return Ok(());
            }
            check_fences(tx, &request, &admission)?;
            if request.context.operation != "turn" {
                return Err(conflict("invalidation requires a turn"));
            }
            let body: String = tx.query_row(
                "SELECT body FROM joint_sessions WHERE runtime_id=?1 AND binding_id=?2",
                params![request.source_id, admission.binding.binding_id],
                |r| r.get(0),
            )?;
            let mut record: SessionRecord = decode(&body)?;
            record.status = SessionStatus::Failed;
            record.last_error = Some("message_execution_invalidated".into());
            tx.execute(
                "UPDATE joint_sessions SET status='failed',body=?1 WHERE runtime_id=?2",
                params![encode(&record)?, request.source_id],
            )?;
            Ok(())
        })
        .await
    }
}
