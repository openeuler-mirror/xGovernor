//! Host bookkeeping uses the same immediate transaction and writer fences as turns.
use crate::{
    joint_host::*,
    sqlite_joint::{
        check_fences, conflict, decode, encode, operation, release_fences, tenant_key,
        validate_native, Result,
    },
    *,
};
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::{json, Value};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS joint_host_roots(binding_id TEXT PRIMARY KEY REFERENCES joint_bindings(id), generation INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS joint_host_targets(runtime_id TEXT PRIMARY KEY REFERENCES joint_sessions(runtime_id), target_json TEXT NOT NULL, policy_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS joint_host_intents(intent_id TEXT PRIMARY KEY REFERENCES joint_operations(id), body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS joint_host_allocations(intent_id TEXT PRIMARY KEY REFERENCES joint_operations(id), owner TEXT NOT NULL, bytes INTEGER NOT NULL, state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS joint_host_workspace(binding_id TEXT PRIMARY KEY REFERENCES joint_bindings(id), generation INTEGER NOT NULL, epoch INTEGER NOT NULL, dirty INTEGER NOT NULL, active_intent TEXT, last_settled TEXT);
CREATE TABLE IF NOT EXISTS joint_host_cleanup(parent_id TEXT PRIMARY KEY REFERENCES joint_operations(id), child_id TEXT UNIQUE REFERENCES joint_operations(id), state TEXT NOT NULL, delegated INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS joint_host_dependencies(parent_id TEXT NOT NULL REFERENCES joint_operations(id), dependency TEXT NOT NULL, released INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(parent_id,dependency));
";
pub(crate) fn init(conn: &Connection) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch("CREATE TABLE IF NOT EXISTS joint_host_schema(version INTEGER PRIMARY KEY, checksum TEXT NOT NULL)")?;
    let versions: u32 = tx.query_row("SELECT count(*) FROM joint_host_schema", [], |r| r.get(0))?;
    if versions > 1 {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let checksum = host_bytes_digest(SCHEMA.as_bytes());
    let old: Option<(u32, String)> = tx
        .query_row("SELECT version,checksum FROM joint_host_schema", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .optional()?;
    if old.as_ref().is_some_and(|v| v != &(1, checksum.clone())) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    tx.execute_batch(SCHEMA)?;
    if old.is_none() {
        tx.execute("INSERT INTO joint_host_schema VALUES(1,?1)", [&checksum])?;
    }
    tx.commit()
}
fn validate_target(
    tx: &Transaction<'_>,
    t: &HostTargetV1,
    p: &HostCapabilityPolicyV1,
) -> Result<()> {
    p.validate()?;
    if t.schema_version != 1
        || t.binding_generation == 0
        || t.root_runtime_generation == 0
        || t.native_child_id.is_empty()
        || t.branch_id.is_empty()
        || t.native_child_id == t.root_id
        || t.owner_ref != t.tenant_key
        || t.deployment_id != p.deployment_id
        || t.live_binding.configuration_revision != p.configuration_revision
        || t.workspace_mapping_digest != host_digest(&p.paths)
        || t.source_checkpoint_id.is_some()
        || t.source_pair_ref.is_some()
    {
        return Err(conflict("invalid or unsupported exact Host target"));
    }
    let good: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_sessions s JOIN joint_bindings b ON s.binding_id=b.id JOIN joint_roots r ON r.id=b.root_id JOIN joint_bindings rb ON rb.id=r.binding_id JOIN joint_host_roots hr ON hr.binding_id=rb.id WHERE s.runtime_id=?1 AND s.binding_id=?2 AND b.state='active' AND b.generation=?3 AND b.tenant_key=?4 AND b.owner_kind=?5 AND b.revision=?6 AND b.root_id=?7 AND r.binding_id=?8 AND rb.state='active' AND hr.generation=?9 AND r.owner_kind=b.owner_kind AND r.revision=b.revision AND r.tenant_key=b.tenant_key AND s.status IN ('idle','failed'))",
        params![t.public_runtime_id,t.live_binding.binding_id,t.binding_generation,t.tenant_key,t.live_binding.owner_kind,t.live_binding.configuration_revision,t.root_id,t.root_binding_id,t.root_runtime_generation], |r| r.get(0))?;
    if !good {
        return Err(conflict("Host target is not an active owned live binding"));
    }
    let root_binding_generation: u64 = tx.query_row(
        "SELECT generation FROM joint_bindings WHERE id=?1",
        [&t.root_binding_id],
        |r| r.get(0),
    )?;
    for (binding, generation, kind, id) in [
        (
            &t.root_binding_id,
            root_binding_generation,
            "root",
            &t.root_id,
        ),
        (
            &t.live_binding.binding_id,
            t.binding_generation,
            "child",
            &t.native_child_id,
        ),
        (
            &t.live_binding.binding_id,
            t.binding_generation,
            "branch",
            &t.branch_id,
        ),
    ] {
        let good: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_objects WHERE binding_id=?1 AND generation=?2 AND root_id=?3 AND kind=?4 AND native_id=?5 AND absent=0)",params![binding,generation,t.root_id,kind,id],|r|r.get(0))?;
        if !good {
            return Err(conflict("Host target object ownership mismatch"));
        }
    }
    let body: String = tx.query_row(
        "SELECT body FROM joint_sessions WHERE runtime_id=?1",
        [&t.public_runtime_id],
        |r| r.get(0),
    )?;
    let record: SessionRecord = decode(&body)?;
    if record.execution_backing.joint() != Some(&t.live_binding)
        || tenant_key(record.tenant_id.as_deref()) != t.tenant_key
        || record.workspace.root != p.paths.workspace_backend_root
    {
        return Err(conflict("Host Session mapping mismatch"));
    }
    Ok(())
}
fn load(tx: &Transaction<'_>, id: &str) -> Result<HostIntentV1> {
    let body: String = tx
        .query_row(
            "SELECT body FROM joint_host_intents WHERE intent_id=?1",
            [id],
            |r| r.get(0),
        )
        .optional()?
        .ok_or_else(|| conflict("Host parent record not found"))?;
    let h: HostIntentV1 = decode(&body)?;
    verify_request(&h.prepared)?;
    let expected = native_params(&h.prepared.target, &h.prepared.request);
    if h.observations.len() != expected.len().max(1) || h.steps.len() != h.observations.len() {
        return Err(conflict("Host step journal shape changed"));
    }
    for (i, step) in h.steps.iter().enumerate() {
        if step.step_no != i || step.target_digest != h.access.target_digest {
            return Err(conflict("Host step identity changed"));
        }
        if let Some(n) = h.prepared.native.get(i) {
            if step.original_uuid.as_ref() != Some(&n.operation_id)
                || step.canonical_params_digest != host_digest(&n.params)
                || step.sdk_operation_digest.as_ref() != Some(&n.digest)
                || step.method != n.method
            {
                return Err(conflict("Host original step changed"));
            }
        } else if step.original_uuid.is_some()
            || step.sdk_operation_digest.is_some()
            || step.method != "thinkthread.read"
            || step.canonical_params_digest != host_digest(&h.prepared.request)
        {
            return Err(conflict("read cannot have a native mutation identity"));
        }
    }

    if h.schema_version != 1
        || h.access.target_digest != host_digest(&h.prepared.target)
        || h.access.policy_digest != host_digest(&h.prepared.policy)
        || h.access.request_digest != host_digest(&h.prepared.request)
    {
        return Err(conflict("Host record schema or digest mismatch"));
    }
    Ok(h)
}
fn save(tx: &Transaction<'_>, h: &HostIntentV1) -> Result<()> {
    tx.execute(
        "UPDATE joint_host_intents SET body=?1 WHERE intent_id=?2",
        params![encode(h)?, h.access.admission.intent_id],
    )?;
    Ok(())
}
fn check(tx: &Transaction<'_>, a: &HostAccessTokenV1, active: bool) -> Result<HostIntentV1> {
    let h = load(tx, &a.admission.intent_id)?;
    if h.access != *a {
        return Err(conflict("Host request-bound token changed"));
    }
    if active {
        let (request, stage, _) = operation(tx, &a.admission)?;
        if !["prepared", "accepted", "unknown"].contains(&stage.as_str())
            || h.state == "suspended_waiting_dependency"
        {
            return Err(conflict("Host operation is not active"));
        }
        check_fences(tx, &request, &a.admission)?;
        validate_target(tx, &h.prepared.target, &h.prepared.policy)?;
        let (target, policy): (String, String) = tx.query_row(
            "SELECT target_json,policy_json FROM joint_host_targets WHERE runtime_id=?1",
            [&h.prepared.target.public_runtime_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if target != encode(&h.prepared.target)? || policy != encode(&h.prepared.policy)? {
            return Err(conflict("Host assembled configuration changed"));
        }
    }
    Ok(h)
}
fn all_settled(h: &HostIntentV1) -> bool {
    h.observations.iter().all(|o| {
        o.as_ref().is_some_and(|o| {
            o.settled
                && matches!(
                    o.state,
                    NativeOperationState::Completed
                        | NativeOperationState::Failed
                        | NativeOperationState::NotSent
                )
        })
    })
}
fn native_params(t: &HostTargetV1, r: &HostCanonicalRequestV1) -> Vec<(String, Value)> {
    match r {
        HostCanonicalRequestV1::Read { .. } => vec![],
        HostCanonicalRequestV1::Exec {
            argv,
            cwd,
            timeout_ms,
            max_output_bytes,
        } => vec![(
            "thinkthread.exec".into(),
            json!({"thinkthreadId":t.native_child_id,"argv":argv,"cwd":cwd,"timeoutMs":timeout_ms.to_string(),"maxOutputBytes":max_output_bytes}),
        )],
        HostCanonicalRequestV1::Write {
            path,
            size,
            sha256,
            max_bytes,
            mkdir,
        } => {
            let mut steps = Vec::new();
            if let Some(m) = mkdir {
                steps.push(("thinkthread.exec".into(),json!({"thinkthreadId":t.native_child_id,"argv":m.argv,"cwd":m.cwd,"timeoutMs":m.timeout_ms.to_string(),"maxOutputBytes":m.max_output_bytes})));
            }
            steps.push(("thinkthread.write".into(),json!({"thinkthreadId":t.native_child_id,"path":path,"maxBytes":max_bytes,"byteLength":size,"sha256":sha256})));
            steps
        }
    }
}
fn verify_request(p: &HostPreparedV1) -> Result<()> {
    let c = &p.context;
    let t = &p.target;
    if c.intent_id.is_empty()
        || c.operation != p.request.operation()
        || c.source_runtime_id.as_deref() != Some(&t.public_runtime_id)
        || c.target_runtime_id.is_some()
        || c.checkpoint_id.is_some()
        || c.principal.is_empty()
        || c.owner_ref != t.owner_ref
        || tenant_key(c.tenant_id.as_deref()) != t.tenant_key
        || c.configuration_revision != p.policy.configuration_revision
    {
        return Err(conflict("Host context mismatch"));
    }
    let expected = native_params(t, &p.request);
    if expected.len() != p.native.len() {
        return Err(conflict("Host step count mismatch"));
    }
    for ((method, params), n) in expected.iter().zip(&p.native) {
        validate_native(n)?;
        if &n.method != method
            || &n.params != params
            || n.effect != NativeEffect::Mutate
            || n.compensation
        {
            return Err(conflict("Host canonical step mismatch"));
        }
    }
    // Reconstruct the request to apply the same trusted limits at the storage boundary.
    let request = match &p.request {
        HostCanonicalRequestV1::Exec {
            argv,
            cwd,
            timeout_ms,
            max_output_bytes,
        } => {
            if !p.policy.exec
                || argv.is_empty()
                || argv.len() > p.policy.max_argv_items
                || argv.iter().any(|v| v.contains('\0'))
                || argv[0].trim().is_empty()
                || argv.iter().map(String::len).sum::<usize>() > p.policy.max_argv_bytes
                || !(1..=p.policy.max_timeout_ms).contains(timeout_ms)
                || *max_output_bytes != p.policy.max_output_bytes
                || cwd.starts_with('/')
                || cwd.split('/').any(|v| v == "..")
            {
                return Err(conflict("Host exec exceeds policy"));
            }
            None
        }
        HostCanonicalRequestV1::Read { path, max_bytes } => {
            if !p.policy.read || *max_bytes != p.policy.max_file_bytes {
                return Err(conflict("Host read exceeds policy"));
            }
            Some(path)
        }
        HostCanonicalRequestV1::Write {
            path,
            size,
            sha256,
            max_bytes,
            mkdir,
        } => {
            if !p.policy.overwrite
                || *size > u64::from(p.policy.max_file_bytes)
                || *max_bytes != p.policy.max_file_bytes
                || sha256.len() != 64
                || !sha256.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(conflict("Host write exceeds policy"));
            }
            if let Some(m) = mkdir {
                let parent = path.rsplit_once('/').map_or(".", |(p, _)| p);
                if !p.policy.exec
                    || m.argv != ["mkdir", "-p", "--", parent]
                    || m.argv.len() > p.policy.max_argv_items
                    || m.argv.iter().map(String::len).sum::<usize>() > p.policy.max_argv_bytes
                    || !(1..=p.policy.max_timeout_ms).contains(&m.timeout_ms)
                    || m.max_output_bytes != p.policy.max_output_bytes
                    || m.cwd != "."
                {
                    return Err(conflict("Host mkdir exceeds policy"));
                }
            }
            Some(path)
        }
    };
    if request.is_some_and(|v| {
        v.starts_with('/')
            || v.contains('\0')
            || v.split('/').any(|c| c == ".." || c.starts_with('~'))
    }) {
        return Err(conflict("Host path exceeds mapping"));
    }
    Ok(())
}
#[async_trait]
impl HostRepository for SqliteSessionRepository {
    async fn register_host_target(
        &self,
        target: HostTargetV1,
        policy: HostCapabilityPolicyV1,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move |tx| {
            tx.execute(
                "INSERT OR IGNORE INTO joint_host_roots VALUES(?1,1)",
                [&target.root_binding_id],
            )?;
            validate_target(tx, &target, &policy)?;
            let old: Option<(String, String)> = tx
                .query_row(
                    "SELECT target_json,policy_json FROM joint_host_targets WHERE runtime_id=?1",
                    [&target.public_runtime_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some(old) = old {
                return if old == (encode(&target)?, encode(&policy)?) {
                    Ok(())
                } else {
                    Err(conflict("Host target registration is immutable"))
                };
            }
            tx.execute(
                "INSERT INTO joint_host_targets VALUES(?1,?2,?3)",
                params![target.public_runtime_id, encode(&target)?, encode(&policy)?],
            )?;
            tx.execute(
                "INSERT INTO joint_host_workspace VALUES(?1,?2,0,0,NULL,NULL)",
                params![target.live_binding.binding_id, target.binding_generation],
            )?;
            Ok(())
        })
        .await
    }
    async fn host_target(
        &self,
        runtime_id: String,
    ) -> std::result::Result<(HostTargetV1, HostCapabilityPolicyV1), SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (t, p): (String, String) = tx.query_row(
                "SELECT target_json,policy_json FROM joint_host_targets WHERE runtime_id=?1",
                [runtime_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let (t, p) = (decode(&t)?, decode(&p)?);
            validate_target(tx, &t, &p)?;
            Ok((t, p))
        })
        .await
    }
    async fn prepare_host(
        &self,
        prepared: HostPreparedV1,
    ) -> std::result::Result<HostAccessTokenV1, SessionDomainError> {
        self.joint_transaction(move |tx| {
            verify_request(&prepared)?; validate_target(tx,&prepared.target,&prepared.policy)?;
            let t=&prepared.target; let c=&prepared.context;
            let configured:(String,String)=tx.query_row("SELECT target_json,policy_json FROM joint_host_targets WHERE runtime_id=?1",[&t.public_runtime_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if configured!=(encode(t)?,encode(&prepared.policy)?) {return Err(conflict("Host configuration differs from registration"));}
            let exists: bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_operations WHERE id=?1)",[&c.intent_id],|r|r.get(0))?;
            if exists {let old=load(tx,&c.intent_id)?; return if encode(&old.prepared)?==encode(&prepared)? {Ok(old.access)}else{Err(conflict("Host replay input changed"))};}
            let (count,bytes):(u32,u64)=tx.query_row("SELECT count(*),coalesce(sum(bytes),0) FROM joint_host_allocations WHERE owner=?1 AND state!='released'",[&t.owner_ref],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if count>=prepared.policy.max_inflight || bytes.checked_add(prepared.request.reserved_bytes()).is_none_or(|n|n>prepared.policy.max_reserved_bytes) {return Err(SessionDomainError::QuotaExceeded {scope:"host_resources".into(),limit:prepared.policy.max_inflight}.into());}
            let (root_epoch,holder):(u64,Option<String>)=tx.query_row("SELECT epoch,holder FROM joint_fences WHERE kind='root' AND id=?1",[&t.root_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if holder.is_some() {return Err(conflict("Host Root is fenced"));}
            tx.execute("INSERT OR IGNORE INTO joint_fences VALUES('source',?1,?2,?3,0,NULL)",params![t.public_runtime_id,t.root_id,t.tenant_key])?;
            let changed=tx.execute("UPDATE joint_fences SET epoch=epoch+1,holder=?1 WHERE kind='source' AND id=?2 AND root_id=?3 AND tenant_key=?4 AND holder IS NULL",params![c.intent_id,t.public_runtime_id,t.root_id,t.tenant_key])?;
            if changed!=1 {return Err(conflict("Host source is fenced"));}
            let epoch=tx.query_row("SELECT epoch FROM joint_fences WHERE kind='source' AND id=?1",[&t.public_runtime_id],|r|r.get(0))?;
            let admission=JointAdmission {intent_id:c.intent_id.clone(),binding:t.live_binding.clone(),generation:t.binding_generation,root_epoch,source_epoch:Some(epoch)};
            let request=JointPrepareRequest {context:c.clone(),binding:t.live_binding.clone(),root_id:t.root_id.clone(),source_id:t.public_runtime_id.clone(),exclusive_root:false,parent_binding:None,reservations:vec![],native:prepared.native.clone(),publication_input:None,turn:None};
            tx.execute("INSERT INTO joint_operations VALUES(?1,?2,?3,?4,?5,'prepared',NULL)",params![c.intent_id,encode(&request)?,encode(&admission)?,t.live_binding.binding_id,t.tenant_key])?;
            for n in &prepared.native {tx.execute("INSERT INTO joint_native_steps VALUES(?1,?2,?3,?4,?5,NULL)",params![c.intent_id,n.step,n.operation_id,n.digest,encode(n)?])?;}
            tx.execute("INSERT INTO joint_host_allocations VALUES(?1,?2,?3,'reserved')",params![c.intent_id,t.owner_ref,prepared.request.reserved_bytes()])?;
            tx.execute("INSERT INTO joint_refs VALUES('host_hold',?1,?2)",params![c.intent_id,t.root_binding_id])?;
            let access=HostAccessTokenV1 {admission,target_digest:host_digest(t),request_digest:host_digest(&prepared.request),policy_digest:host_digest(&prepared.policy)};
            let count=prepared.native.len().max(1);
            let steps = if prepared.native.is_empty() {
                vec![HostStepV1 { step_no:0, method:"thinkthread.read".into(), original_uuid:None, canonical_params_digest:host_digest(&prepared.request), sdk_operation_digest:None, target_digest:access.target_digest.clone() }]
            } else { prepared.native.iter().enumerate().map(|(step_no,n)| HostStepV1 {step_no,method:n.method.clone(),original_uuid:Some(n.operation_id.clone()),canonical_params_digest:host_digest(&n.params),sdk_operation_digest:Some(n.digest.clone()),target_digest:access.target_digest.clone()}).collect() };

            let h=HostIntentV1 {schema_version:1,prepared,access:access.clone(),state:"prepared".into(),observations:vec![None;count],steps,recovery_evidence:vec![],result:None,cleanup_state:"none".into(),blocker_id:None,suspended_cleanup_plan_id:None,resume_joint_stage:None};
            tx.execute("INSERT INTO joint_host_intents VALUES(?1,?2)",params![access.admission.intent_id,encode(&h)?])?;Ok(access)
        }).await
    }
    async fn host_intent(
        &self,
        id: String,
    ) -> std::result::Result<HostIntentV1, SessionDomainError> {
        self.joint_transaction(move |tx| load(tx, &id)).await
    }
    async fn claim_host_step(
        &self,
        access: HostAccessTokenV1,
        step: usize,
    ) -> std::result::Result<bool, SessionDomainError> {
        self.joint_transaction(move|tx|{
            let mut h=check(tx,&access,true)?;
            if step>=h.observations.len() || (step>0 && !h.observations[step-1].as_ref().is_some_and(|v|v.settled && v.state==NativeOperationState::Completed)) {return Err(conflict("Host previous step has not succeeded"));}
            if h.observations[step].is_some() {return Ok(false);}
            h.observations[step]=Some(HostObservationV1 {state:NativeOperationState::Unknown,settled:false,result:Value::Null,transfer:None,exec:None,error:Some("claimed; delivery unknown".into()),native_outcome: None,
});
            h.state="executing".into();
            h.blocker_id=Some(format!("host:{}:unsettled",access.admission.intent_id));
            if !matches!(h.prepared.request,HostCanonicalRequestV1::Read{..}) {
                let n=&h.prepared.native[step];let unknown=JointNativeOutcome {state:NativeOperationState::Unknown,resources:vec![],payload:json!({"delivery":"sent_unknown"})};
                tx.execute("UPDATE joint_native_steps SET outcome_json=?1 WHERE intent_id=?2 AND step=?3 AND outcome_json IS NULL",params![encode(&unknown)?,access.admission.intent_id,n.step])?;
                tx.execute("UPDATE joint_host_workspace SET epoch=epoch+1,dirty=1,active_intent=?1 WHERE binding_id=?2 AND generation=?3",params![access.admission.intent_id,access.admission.binding.binding_id,access.admission.generation])?;
            }
            tx.execute("UPDATE joint_operations SET stage='unknown' WHERE id=?1",[&access.admission.intent_id])?;save(tx,&h)?;Ok(true)
        }).await
    }
    async fn record_host_step(
        &self,
        access: HostAccessTokenV1,
        step: usize,
        observation: HostObservationV1,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move |tx| {
            let mut h = check(tx, &access, true)?;
            let old = h
                .observations
                .get(step)
                .and_then(Option::as_ref)
                .ok_or_else(|| conflict("Host step was not claimed"))?;
            if old == &observation {
                return Ok(());
            }
            if old.settled {
                return Err(conflict("Host terminal observation changed"));
            }
            if observation.settled
                && !matches!(
                    observation.state,
                    NativeOperationState::Completed
                        | NativeOperationState::Failed
                        | NativeOperationState::NotSent
                )
            {
                return Err(conflict("unsettled native result cannot settle Host"));
            }
            if let Some(n) = h.prepared.native.get(step) {
                let raw: String = tx.query_row(
                    "SELECT outcome_json FROM joint_native_steps WHERE intent_id=?1 AND step=?2",
                    params![access.admission.intent_id, n.step],
                    |r| r.get(0),
                )?;
                let old: JointNativeOutcome = decode(&raw)?;
                if matches!(
                    old.state,
                    NativeOperationState::Completed
                        | NativeOperationState::Failed
                        | NativeOperationState::NotSent
                ) && (old.state != observation.state
                    || old.payload
                        != observation
                            .native_outcome
                            .clone()
                            .unwrap_or_else(|| observation.result.clone()))
                {
                    return Err(conflict("native history is immutable"));
                }
                let native = JointNativeOutcome {
                    state: observation.state.clone(),
                    resources: vec![],
                    payload: observation
                        .native_outcome
                        .clone()
                        .unwrap_or_else(|| observation.result.clone()),
                };
                tx.execute(
                    "UPDATE joint_native_steps SET outcome_json=?1 WHERE intent_id=?2 AND step=?3",
                    params![encode(&native)?, access.admission.intent_id, n.step],
                )?;
            }
            let settled = observation.settled;
            h.observations[step] = Some(observation);
            h.blocker_id = (!all_settled(&h))
                .then(|| format!("host:{}:unsettled", access.admission.intent_id));
            h.state = if settled { "executing" } else { "unknown" }.into();
            tx.execute(
                "UPDATE joint_operations SET stage=?1 WHERE id=?2",
                params![
                    if all_settled(&h) {
                        "accepted"
                    } else {
                        "unknown"
                    },
                    access.admission.intent_id
                ],
            )?;
            save(tx, &h)
        })
        .await
    }
    async fn record_host_recovery(
        &self,
        access: HostAccessTokenV1,
        evidence: Value,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move |tx| {
            let mut h = check(tx, &access, true)?;
            if h.recovery_evidence.len() >= 64 || encode(&evidence)?.len() > 262144 {
                return Err(conflict("Host recovery evidence budget exhausted"));
            }
            h.recovery_evidence.push(evidence);
            save(tx, &h)
        })
        .await
    }
    async fn finish_host(
        &self,
        access: HostAccessTokenV1,
        result: Value,
    ) -> std::result::Result<Value, SessionDomainError> {
        self.joint_transaction(move|tx|{
            let old=check(tx,&access,false)?;
            if old.result.is_some() {return if old.result==Some(result.clone()){Ok(result)}else{Err(conflict("Host result replay changed"))};}
            let mut h=check(tx,&access,true)?;
            let unsent=h.observations.iter().all(Option::is_none);
            if !all_settled(&h) && !(unsent && result.get("error").is_some()) {return Err(conflict("Host effects or execution are not settled"));}
            let waiting:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_host_cleanup WHERE parent_id=?1 AND state!='done')",[&access.admission.intent_id],|r|r.get(0))?;
            if waiting {return Err(conflict("Host cleanup delegation is pending"));}
            if !unsent && result.get("error").is_none() && h.observations.iter().any(|o|o.as_ref().is_none_or(|v|v.state!=NativeOperationState::Completed || v.error.is_some())) {return Err(conflict("failed Host cannot publish success"));}
            h.result=Some(result.clone());h.state=if result.get("error").is_some(){"failed"}else{"completed"}.into();h.cleanup_state=if unsent {"done"}else{"pending"}.into();h.blocker_id=None;
            if unsent {
                tx.execute("UPDATE joint_host_allocations SET state='released' WHERE intent_id=?1",[&access.admission.intent_id])?;
                tx.execute("DELETE FROM joint_refs WHERE kind='host_hold' AND public_id=?1",[&access.admission.intent_id])?;
            }
            tx.execute("UPDATE joint_operations SET stage='settled',publication_json=?1 WHERE id=?2",params![encode(&result)?,access.admission.intent_id])?;
            tx.execute("UPDATE joint_host_workspace SET dirty=0,active_intent=NULL,last_settled=?1 WHERE binding_id=?2 AND (active_intent=?1 OR active_intent IS NULL)",params![access.admission.intent_id,access.admission.binding.binding_id])?;
            release_fences(tx,&access.admission.intent_id)?;save(tx,&h)?;Ok(result)
        }).await
    }
    async fn pending_hosts(&self) -> std::result::Result<Vec<HostIntentV1>, SessionDomainError> {
        self.joint_transaction(|tx| {
            let ids = tx
                .prepare("SELECT intent_id FROM joint_host_intents ORDER BY intent_id")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            ids.into_iter()
                .map(|id| load(tx, &id))
                .filter(|r| {
                    r.as_ref()
                        .map_or(true, |h| h.result.is_none() || h.cleanup_state == "pending")
                })
                .collect()
        })
        .await
    }
    async fn prepare_host_cleanup(
        &self,
        access: HostAccessTokenV1,
        child_id: String,
        dependencies: Vec<String>,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move |tx| {
            let mut h = check(tx, &access, true)?;
            if !all_settled(&h) || child_id.is_empty() || dependencies.is_empty() {
                return Err(conflict(
                    "cleanup requires settled execution and owned dependencies",
                ));
            }
            let mut request = operation(tx, &access.admission)?.0;
            request.context.intent_id = child_id.clone();
            request.context.operation = "host_cleanup".into();
            request.native.clear();
            let mut a = access.admission.clone();
            a.intent_id = child_id.clone();
            tx.execute(
                "INSERT INTO joint_operations VALUES(?1,?2,?3,?4,?5,'accepted',NULL)",
                params![
                    child_id,
                    encode(&request)?,
                    encode(&a)?,
                    a.binding.binding_id,
                    h.prepared.target.tenant_key
                ],
            )?;
            tx.execute(
                "INSERT INTO joint_host_cleanup VALUES(?1,?2,'prepared',1)",
                params![access.admission.intent_id, child_id],
            )?;
            for d in dependencies {
                if d.is_empty() {
                    return Err(conflict("empty dependency"));
                }
                tx.execute(
                    "INSERT INTO joint_host_dependencies VALUES(?1,?2,0)",
                    params![access.admission.intent_id, d],
                )?;
            }
            h.cleanup_state = "delegated".into();
            save(tx, &h)
        })
        .await
    }
    async fn suspend_host_cleanup(
        &self,
        access: HostAccessTokenV1,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move|tx|{
            let mut h=check(tx,&access,true)?;if !all_settled(&h) {return Err(conflict("unknown execution cannot suspend"));}
            let (child,state):(String,String)=tx.query_row("SELECT child_id,state FROM joint_host_cleanup WHERE parent_id=?1",[&access.admission.intent_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if state!="prepared" {return Err(conflict("cleanup cannot suspend"));}
            let waiting:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_host_dependencies WHERE parent_id=?1 AND released=0)",[&access.admission.intent_id],|r|r.get(0))?;
            if !waiting {return Err(conflict("cleanup has no dependency wait"));}
            let stage: String=tx.query_row("SELECT stage FROM joint_operations WHERE id=?1",[&access.admission.intent_id],|r|r.get(0))?;
            h.suspended_cleanup_plan_id=Some(child.clone());h.resume_joint_stage=Some(stage);
            h.blocker_id=Some(format!("host:{}:dependency",access.admission.intent_id));
            tx.execute("UPDATE joint_operations SET stage='suspended_waiting_dependency' WHERE id IN (?1,?2)",params![access.admission.intent_id,child])?;
            tx.execute("UPDATE joint_host_cleanup SET state='waiting_dependency',delegated=0 WHERE parent_id=?1",[&access.admission.intent_id])?;
            release_fences(tx,&access.admission.intent_id)?;h.state="suspended_waiting_dependency".into();save(tx,&h)
        }).await
    }
    async fn release_host_dependency(
        &self,
        access: HostAccessTokenV1,
        dependency: String,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move|tx|{
            check(tx,&access,false)?;
            let changed=tx.execute("UPDATE joint_host_dependencies SET released=1 WHERE parent_id=?1 AND dependency=?2",params![access.admission.intent_id,dependency])?;
            if changed!=1 {return Err(conflict("dependency is not owned"));}Ok(())
        }).await
    }
    async fn resume_host_cleanup(
        &self,
        access: HostAccessTokenV1,
    ) -> std::result::Result<HostAccessTokenV1, SessionDomainError> {
        self.joint_transaction(move|tx|{
            let mut h=check(tx,&access,false)?;
            if h.state!="suspended_waiting_dependency" {return Err(conflict("Host cleanup is not suspended"));}
            validate_target(tx,&h.prepared.target,&h.prepared.policy)?;
            let configured:(String,String)=tx.query_row("SELECT target_json,policy_json FROM joint_host_targets WHERE runtime_id=?1",[&h.prepared.target.public_runtime_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if configured!=(encode(&h.prepared.target)?,encode(&h.prepared.policy)?) {return Err(conflict("Host configuration changed during dependency wait"));}

            let (child,state):(String,String)=tx.query_row("SELECT child_id,state FROM joint_host_cleanup WHERE parent_id=?1",[&access.admission.intent_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if h.suspended_cleanup_plan_id.as_deref()!=Some(child.as_str()) || h.resume_joint_stage.as_deref()!=Some("accepted") {return Err(conflict("Host suspended cleanup plan changed"));}
            let waiting:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_host_dependencies WHERE parent_id=?1 AND released=0)",[&access.admission.intent_id],|r|r.get(0))?;
            let allocation:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_host_allocations WHERE intent_id=?1 AND owner=?2 AND state!='released')",params![access.admission.intent_id,h.prepared.target.owner_ref],|r|r.get(0))?;
            let hold:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_refs WHERE kind='host_hold' AND public_id=?1 AND binding_id=?2)",params![access.admission.intent_id,h.prepared.target.root_binding_id],|r|r.get(0))?;
            if waiting || !allocation || !hold || state!="waiting_dependency" {return Err(conflict("Host dependency, owner or allocation changed"));}
            let (root_epoch,holder):(u64,Option<String>)=tx.query_row("SELECT epoch,holder FROM joint_fences WHERE kind='root' AND id=?1",[&h.prepared.target.root_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if holder.is_some() {return Err(conflict("Root is fenced"));}
            if tx.execute("UPDATE joint_fences SET epoch=epoch+1,holder=?1 WHERE kind='source' AND id=?2 AND holder IS NULL",params![access.admission.intent_id,h.prepared.target.public_runtime_id])?!=1 {return Err(conflict("source is fenced"));}
            h.access.admission.source_epoch=Some(tx.query_row("SELECT epoch FROM joint_fences WHERE kind='source' AND id=?1",[&h.prepared.target.public_runtime_id],|r|r.get(0))?);h.access.admission.root_epoch=root_epoch;
            let mut child_token=h.access.admission.clone();child_token.intent_id=child.clone();
            for a in [&h.access.admission,&child_token] {if tx.execute("UPDATE joint_operations SET stage='accepted',admission_json=?1 WHERE id=?2 AND stage='suspended_waiting_dependency'",params![encode(a)?,a.intent_id])?!=1 {return Err(conflict("parent/child phase changed"));}}
            tx.execute("UPDATE joint_host_cleanup SET state='prepared',delegated=1 WHERE parent_id=?1",[&access.admission.intent_id])?;
            h.state="executing".into();h.blocker_id=None;h.suspended_cleanup_plan_id=None;h.resume_joint_stage=None;save(tx,&h)?;Ok(h.access)
        }).await
    }
    async fn finish_host_cleanup(
        &self,
        access: HostAccessTokenV1,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move|tx|{
            let mut h=check(tx,&access,false)?;
            if h.cleanup_state=="done" {return Ok(());}
            if h.result.is_none() {check(tx,&access,true)?;if !all_settled(&h) || h.cleanup_state!="delegated" {return Err(conflict("cleanup cannot release unknown execution or an absent delegation"));}
                let delegated:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_host_cleanup WHERE parent_id=?1 AND state='prepared' AND delegated=1)",[&access.admission.intent_id],|r|r.get(0))?;
                if !delegated {return Err(conflict("cleanup delegation is not active"));}
let waiting:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_host_dependencies WHERE parent_id=?1 AND released=0)",[&access.admission.intent_id],|r|r.get(0))?;if waiting {return Err(conflict("cleanup has surviving dependencies"));}
                tx.execute("UPDATE joint_operations SET stage='settled' WHERE id=(SELECT child_id FROM joint_host_cleanup WHERE parent_id=?1)",[&access.admission.intent_id])?;
                tx.execute("UPDATE joint_host_cleanup SET state='done',delegated=0 WHERE parent_id=?1",[&access.admission.intent_id])?;
                h.cleanup_state="none".into();
            } else {
                tx.execute("UPDATE joint_host_allocations SET state='released' WHERE intent_id=?1 AND state!='released'",[&access.admission.intent_id])?;
                tx.execute("DELETE FROM joint_refs WHERE kind='host_hold' AND public_id=?1",[&access.admission.intent_id])?;h.cleanup_state="done".into();
            }
            save(tx,&h)
        }).await
    }
}
