//! SQLite transactions shared with the public Session repository.
use crate::{
    joint_store::*, JointCheckpointRecord, SessionDomainError, SessionRecord, SessionStatus,
    SqliteSessionRepository,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Domain(#[from] SessionDomainError),
}
type Result<T> = std::result::Result<T, Error>;
fn conflict(message: &str) -> Error {
    SessionDomainError::Conflict {
        message: message.into(),
    }
    .into()
}
fn encode<T: Serialize>(value: &T) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}
fn decode<T: DeserializeOwned>(value: &str) -> Result<T> {
    Ok(serde_json::from_str(value)?)
}
fn tenant_key(tenant: Option<&str>) -> String {
    tenant
        .map(|v| format!("tenant/{v}"))
        .unwrap_or_else(|| "admin".into())
}
fn nonempty(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 4096
}
fn no_credentials(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(values) => values.iter().all(|(key, value)| {
            ![
                "api_key",
                "apikey",
                "password",
                "authorization",
                "access_token",
                "secret_key",
            ]
            .contains(&key.to_ascii_lowercase().as_str())
                && no_credentials(value)
        }),
        serde_json::Value::Array(values) => values.iter().all(no_credentials),
        _ => true,
    }
}

pub(crate) fn init(conn: &Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS joint_bindings (
          id TEXT PRIMARY KEY, owner_kind TEXT NOT NULL, revision TEXT NOT NULL,
          runtime_kind TEXT NOT NULL, tenant_key TEXT NOT NULL, root_id TEXT NOT NULL,
          generation INTEGER NOT NULL, state TEXT NOT NULL, creation_intent TEXT NOT NULL,
          parent_id TEXT REFERENCES joint_bindings(id));
        CREATE TABLE IF NOT EXISTS joint_operations (
          id TEXT PRIMARY KEY, request_json TEXT NOT NULL, admission_json TEXT NOT NULL,
          binding_id TEXT NOT NULL REFERENCES joint_bindings(id), tenant_key TEXT NOT NULL,
          stage TEXT NOT NULL, publication_json TEXT);
        CREATE TABLE IF NOT EXISTS joint_reservations (
          intent_id TEXT NOT NULL REFERENCES joint_operations(id), kind TEXT NOT NULL,
          units INTEGER NOT NULL CHECK(units>0), state TEXT NOT NULL,
          PRIMARY KEY(intent_id,kind));
        CREATE TABLE IF NOT EXISTS joint_native_steps (
          intent_id TEXT NOT NULL REFERENCES joint_operations(id), step TEXT NOT NULL,
          operation_id TEXT UNIQUE NOT NULL, digest TEXT NOT NULL, intent_json TEXT NOT NULL,
          outcome_json TEXT, PRIMARY KEY(intent_id,step));
        CREATE TABLE IF NOT EXISTS joint_objects (
          binding_id TEXT NOT NULL REFERENCES joint_bindings(id), generation INTEGER NOT NULL,
          root_id TEXT NOT NULL, kind TEXT NOT NULL, native_id TEXT NOT NULL, absent INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(binding_id,generation,kind,native_id));
        CREATE UNIQUE INDEX IF NOT EXISTS joint_object_owner ON joint_objects(root_id,kind,native_id) WHERE absent=0;
        CREATE TABLE IF NOT EXISTS joint_roots (
          id TEXT PRIMARY KEY, owner_kind TEXT NOT NULL, revision TEXT NOT NULL,
          tenant_key TEXT NOT NULL, binding_id TEXT NOT NULL REFERENCES joint_bindings(id));
        CREATE TABLE IF NOT EXISTS joint_fences (
          kind TEXT NOT NULL, id TEXT NOT NULL, root_id TEXT NOT NULL, tenant_key TEXT NOT NULL,
          epoch INTEGER NOT NULL, holder TEXT,
          PRIMARY KEY(kind,id));
        CREATE TABLE IF NOT EXISTS joint_public_claims (
          kind TEXT NOT NULL, public_id TEXT NOT NULL, intent_id TEXT NOT NULL UNIQUE,
          PRIMARY KEY(kind,public_id));
        CREATE TABLE IF NOT EXISTS joint_refs (
          kind TEXT NOT NULL, public_id TEXT NOT NULL,
          binding_id TEXT NOT NULL REFERENCES joint_bindings(id), PRIMARY KEY(kind,public_id));
        CREATE TABLE IF NOT EXISTS joint_sessions (
          runtime_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL REFERENCES joint_bindings(id),
          tenant_id TEXT, status TEXT NOT NULL, updated_at_ms INTEGER NOT NULL, body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS joint_checkpoints (
          checkpoint_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL REFERENCES joint_bindings(id),
          tenant_id TEXT, source_runtime_id TEXT NOT NULL, created_by TEXT,
          created_at_ms INTEGER NOT NULL, body TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS split_session_identity_guard BEFORE INSERT ON sessions
          WHEN EXISTS(SELECT 1 FROM joint_sessions WHERE runtime_id=NEW.runtime_id) OR EXISTS(SELECT 1 FROM joint_public_claims WHERE kind='session' AND public_id=NEW.runtime_id)
          BEGIN SELECT RAISE(ABORT,'public identity belongs to joint execution'); END;
        CREATE TRIGGER IF NOT EXISTS joint_session_identity_guard BEFORE INSERT ON joint_sessions
          WHEN EXISTS(SELECT 1 FROM sessions WHERE runtime_id=NEW.runtime_id)
          BEGIN SELECT RAISE(ABORT,'public identity belongs to split execution'); END;
        CREATE TRIGGER IF NOT EXISTS split_checkpoint_identity_guard BEFORE INSERT ON checkpoints
          WHEN EXISTS(SELECT 1 FROM joint_checkpoints WHERE checkpoint_id=NEW.checkpoint_id) OR EXISTS(SELECT 1 FROM joint_public_claims WHERE kind='checkpoint' AND public_id=NEW.checkpoint_id)
          BEGIN SELECT RAISE(ABORT,'checkpoint identity belongs to joint execution'); END;
        CREATE TRIGGER IF NOT EXISTS joint_checkpoint_identity_guard BEFORE INSERT ON joint_checkpoints
          WHEN EXISTS(SELECT 1 FROM checkpoints WHERE checkpoint_id=NEW.checkpoint_id)
          BEGIN SELECT RAISE(ABORT,'checkpoint identity belongs to split execution'); END;
        CREATE TRIGGER IF NOT EXISTS sessions_update_identity_guard BEFORE UPDATE OF runtime_id ON sessions
          WHEN EXISTS(SELECT 1 FROM joint_sessions WHERE runtime_id=NEW.runtime_id) OR EXISTS(SELECT 1 FROM joint_public_claims WHERE kind='session' AND public_id=NEW.runtime_id)
          BEGIN SELECT RAISE(ABORT,'public execution identity conflict'); END;
        CREATE TRIGGER IF NOT EXISTS joint_sessions_update_identity_guard BEFORE UPDATE OF runtime_id ON joint_sessions
          WHEN EXISTS(SELECT 1 FROM sessions WHERE runtime_id=NEW.runtime_id)
          BEGIN SELECT RAISE(ABORT,'public execution identity conflict'); END;
        CREATE TRIGGER IF NOT EXISTS checkpoints_update_identity_guard BEFORE UPDATE OF checkpoint_id ON checkpoints
          WHEN EXISTS(SELECT 1 FROM joint_checkpoints WHERE checkpoint_id=NEW.checkpoint_id) OR EXISTS(SELECT 1 FROM joint_public_claims WHERE kind='checkpoint' AND public_id=NEW.checkpoint_id)
          BEGIN SELECT RAISE(ABORT,'public execution identity conflict'); END;
        CREATE TRIGGER IF NOT EXISTS joint_checkpoints_update_identity_guard BEFORE UPDATE OF checkpoint_id ON joint_checkpoints
          WHEN EXISTS(SELECT 1 FROM checkpoints WHERE checkpoint_id=NEW.checkpoint_id)
          BEGIN SELECT RAISE(ABORT,'public execution identity conflict'); END;
    ")
}

fn validate_request(request: &JointPrepareRequest) -> Result<()> {
    let c = &request.context;
    request.binding.validate()?;
    if ![
        &c.intent_id,
        &c.operation,
        &c.runtime_kind,
        &c.configuration_revision,
        &c.owner_ref,
        &c.principal,
        &request.root_id,
        &request.source_id,
    ]
    .iter()
    .all(|v| nonempty(v))
        || c.owner_ref != tenant_key(c.tenant_id.as_deref())
        || c.configuration_revision != request.binding.configuration_revision
        || !["open", "load", "checkpoint", "close", "delete_checkpoint"]
            .contains(&c.operation.as_str())
        || (c.actor_role == crate::Role::Tenant && c.tenant_id.is_none())
        || request.native.is_empty()
    {
        return Err(conflict("invalid joint admission identity"));
    }
    let (source, reservation, parent) = match c.operation.as_str() {
        "open" => (c.target_runtime_id.as_deref(), Some("session"), false),
        "load" => (c.checkpoint_id.as_deref(), Some("session"), true),
        "checkpoint" => (c.source_runtime_id.as_deref(), Some("checkpoint"), true),
        "close" => (c.source_runtime_id.as_deref(), None, false),
        "delete_checkpoint" => (c.checkpoint_id.as_deref(), None, false),
        _ => unreachable!(),
    };
    if source != Some(request.source_id.as_str())
        || request.parent_binding.is_some() != parent
        || reservation.is_some_and(|kind| {
            !request
                .reservations
                .iter()
                .any(|v| v.kind == kind && v.units == 1)
        })
        || (reservation.is_none() && !request.reservations.is_empty())
    {
        return Err(conflict(
            "joint source or reservation does not match operation",
        ));
    }
    if ["open", "load"].contains(&c.operation.as_str())
        && !c.target_runtime_id.as_deref().is_some_and(nonempty)
        || ["checkpoint", "load", "delete_checkpoint"].contains(&c.operation.as_str())
            && !c.checkpoint_id.as_deref().is_some_and(nonempty)
        || ["checkpoint", "close"].contains(&c.operation.as_str())
            && !c.source_runtime_id.as_deref().is_some_and(nonempty)
    {
        return Err(conflict("incomplete joint public identity"));
    }
    if c.operation == "open"
        && !request
            .reservations
            .iter()
            .any(|r| r.kind == "root" && r.units == 1)
    {
        return Err(conflict("new Root requires a resource reservation"));
    }
    let mut kinds = BTreeSet::new();
    for reservation in &request.reservations {
        if !["session", "root", "replica", "checkpoint", "repair", "disk"]
            .contains(&reservation.kind.as_str())
            || reservation.units == 0
            || !kinds.insert(reservation.kind.clone())
        {
            return Err(conflict("invalid joint reservation"));
        }
    }
    let mut steps = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for native in &request.native {
        validate_native(native)?;
        if !steps.insert(native.step.clone()) || !ids.insert(native.operation_id.clone()) {
            return Err(conflict("duplicate joint native operation"));
        }
    }
    Ok(())
}
fn validate_native(native: &JointNativeIntent) -> Result<()> {
    if ![&native.step, &native.operation_id, &native.method]
        .iter()
        .all(|v| nonempty(v))
        || native.digest.len() != 64
        || !native.digest.bytes().all(|b| b.is_ascii_hexdigit())
        || native.operation_id.len() != 36
        || !native.operation_id.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
        || !no_credentials(&native.params)
    {
        return Err(conflict("invalid or credential-bearing native intent"));
    }
    Ok(())
}

fn check_fences(
    tx: &Transaction<'_>,
    request: &JointPrepareRequest,
    admission: &JointAdmission,
) -> Result<()> {
    let root: (u64, Option<String>) = tx.query_row(
        "SELECT epoch,holder FROM joint_fences WHERE kind='root' AND id=?1",
        [&request.root_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if root.0 != admission.root_epoch
        || (request.exclusive_root && root.1.as_deref() != Some(&admission.intent_id))
        || (!request.exclusive_root && root.1.is_some())
    {
        return Err(conflict("joint Root fence changed"));
    }
    if let Some(epoch) = admission.source_epoch {
        let source: (u64, Option<String>) = tx.query_row(
            "SELECT epoch,holder FROM joint_fences WHERE kind='source' AND id=?1",
            [&request.source_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if source.0 != epoch || source.1.as_deref() != Some(&admission.intent_id) {
            return Err(conflict("joint source fence changed"));
        }
    }
    let binding: (u64, String) = tx.query_row(
        "SELECT generation,state FROM joint_bindings WHERE id=?1",
        [&admission.binding.binding_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if binding.0 != admission.generation || binding.1 == "removed" {
        return Err(conflict("joint binding generation changed"));
    }
    Ok(())
}
fn operation(
    tx: &Transaction<'_>,
    admission: &JointAdmission,
) -> Result<(JointPrepareRequest, String, Option<String>)> {
    let (request,stored,stage,publication):(String,String,String,Option<String>)=tx.query_row(
        "SELECT request_json,admission_json,stage,publication_json FROM joint_operations WHERE id=?1",
        [&admission.intent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    if decode::<JointAdmission>(&stored)? != *admission {
        return Err(conflict("joint admission token mismatch"));
    }
    Ok((decode(&request)?, stage, publication))
}
fn release_fences(tx: &Transaction<'_>, id: &str) -> Result<()> {
    tx.execute("UPDATE joint_fences SET holder=NULL WHERE holder=?1", [id])?;
    Ok(())
}
fn completed_native(tx: &Transaction<'_>, id: &str) -> Result<()> {
    let outcomes = tx
        .prepare("SELECT outcome_json FROM joint_native_steps WHERE intent_id=?1")?
        .query_map([id], |r| r.get::<_, Option<String>>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if outcomes.is_empty()
        || outcomes.iter().any(|v| {
            v.as_deref()
                .and_then(|s| decode::<JointNativeOutcome>(s).ok())
                .is_none_or(|v| v.state != NativeOperationState::Completed)
        })
    {
        return Err(conflict("joint native result is not settled"));
    }
    Ok(())
}

fn settled_native(tx: &Transaction<'_>, id: &str) -> Result<()> {
    let unsettled:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_native_steps WHERE intent_id=?1 AND (outcome_json IS NULL OR json_extract(outcome_json,'$.state') NOT IN ('completed','failed','not_sent')))",[id],|r|r.get(0))?;
    if unsettled {
        return Err(conflict("joint native operation is not settled"));
    }
    Ok(())
}

impl SqliteSessionRepository {
    async fn joint_transaction<T: Send + 'static, F>(
        &self,
        action: F,
    ) -> std::result::Result<T, SessionDomainError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T> + Send + 'static,
    {
        let connection = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connection
                .lock()
                .map_err(|_| conflict("joint database mutex poisoned"))?;
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let result = action(&tx)?;
            tx.commit()?;
            Ok(result)
        })
        .await
        .map_err(|_| SessionDomainError::Unavailable {
            message: "joint database task failed".into(),
        })?
        .map_err(|error: Error| match error {
            Error::Domain(error) => error,
            _ => SessionDomainError::Internal {
                message: format!("joint repository failed: {error}"),
                source: None,
            },
        })
    }

    pub async fn prepare_joint_operation(
        &self,
        request: JointPrepareRequest,
    ) -> std::result::Result<JointAdmission, SessionDomainError> {
        validate_request(&request).map_err(|e| SessionDomainError::InvalidRequest {
            message: e.to_string(),
        })?;
        self.joint_transaction(move |tx| {
            let encoded=encode(&request)?;let id=&request.context.intent_id;
            if let Some((old,admission))=tx.query_row("SELECT request_json,admission_json FROM joint_operations WHERE id=?1",
                [id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional()? {
                if old!=encoded {return Err(conflict("joint operation identity conflict"));} return decode(&admission);
            }
            if ["open","load","checkpoint"].contains(&request.context.operation.as_str()) {
                let (kind,id,split,joint,column)=if request.context.operation=="checkpoint" {
                    ("checkpoint",request.context.checkpoint_id.as_deref().unwrap(),"checkpoints","joint_checkpoints","checkpoint_id")
                } else {("session",request.context.target_runtime_id.as_deref().unwrap(),"sessions","joint_sessions","runtime_id")};
                let exists:bool=tx.query_row(&format!("SELECT EXISTS(SELECT 1 FROM {split} WHERE {column}=?1 UNION ALL SELECT 1 FROM {joint} WHERE {column}=?1)"),[id],|r|r.get(0))?;
                if exists {return Err(conflict("joint public identity already exists"));}
                tx.execute("INSERT INTO joint_public_claims VALUES(?1,?2,?3)",params![kind,id,request.context.intent_id])?;
            }
            let tenant=tenant_key(request.context.tenant_id.as_deref());
            let binding=&request.binding;
            if let Some((owner,revision,runtime,stored_tenant,root,generation,state))=tx.query_row(
                "SELECT owner_kind,revision,runtime_kind,tenant_key,root_id,generation,state FROM joint_bindings WHERE id=?1",
                [&binding.binding_id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,u64>(5)?,r.get::<_,String>(6)?))).optional()? {
                if owner!=binding.owner_kind || revision!=binding.configuration_revision || runtime!=request.context.runtime_kind
                    || stored_tenant!=tenant || root!=request.root_id || generation!=1 || state!="active"
                    || !["close","delete_checkpoint"].contains(&request.context.operation.as_str()) {
                    return Err(conflict("joint binding ownership or stage conflict"));
                }
                let (kind,public_id)=if request.context.operation=="close" {("session",request.context.source_runtime_id.as_deref())}
                    else {("checkpoint",request.context.checkpoint_id.as_deref())};
                let owned:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_refs WHERE kind=?1 AND public_id=?2 AND binding_id=?3)",
                    params![kind,public_id,binding.binding_id],|r|r.get(0))?;
                if !owned {return Err(conflict("joint release does not own public reference"));}
                let count:i64=tx.query_row("SELECT count(*) FROM joint_refs WHERE binding_id=?1",[&binding.binding_id],|r|r.get(0))?;
                if count>1 {return Err(conflict("joint binding is referenced"));}
                tx.execute("UPDATE joint_bindings SET state='releasing' WHERE id=?1",[&binding.binding_id])?;
            } else {
                if !["open","load","checkpoint"].contains(&request.context.operation.as_str()) || request.reservations.is_empty() {return Err(conflict("new joint binding requires reservation"));}
                if let Some(parent)=&request.parent_binding {
                    parent.validate()?;
                    let good:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_bindings WHERE id=?1 AND owner_kind=?2 AND tenant_key=?3 AND root_id=?4 AND revision=?5 AND runtime_kind=?6 AND state='active')",
                        params![parent.binding_id,parent.owner_kind,tenant,request.root_id,parent.configuration_revision,request.context.runtime_kind],|r|r.get(0))?;
                    if !good || parent.owner_kind!=binding.owner_kind || parent.configuration_revision!=binding.configuration_revision {
                        return Err(conflict("joint parent is unavailable or foreign"));
                    }
                    let (kind,public_id)=if request.context.operation=="load" {("checkpoint",request.context.checkpoint_id.as_deref())}
                        else {("session",request.context.source_runtime_id.as_deref())};
                    let owned:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_refs WHERE kind=?1 AND public_id=?2 AND binding_id=?3)",
                        params![kind,public_id,parent.binding_id],|r|r.get(0))?;
                    if !owned {return Err(conflict("joint parent does not own requested source"));}
                }
                tx.execute("INSERT INTO joint_bindings VALUES(?1,?2,?3,?4,?5,?6,1,'prepared',?7,?8)",
                    params![binding.binding_id,binding.owner_kind,binding.configuration_revision,request.context.runtime_kind,tenant,request.root_id,id,request.parent_binding.as_ref().map(|v|&v.binding_id)])?;
            }
            if request.context.operation=="open" {
                tx.execute("INSERT INTO joint_roots VALUES(?1,?2,?3,?4,?5)",params![request.root_id,binding.owner_kind,binding.configuration_revision,tenant,binding.binding_id])?;
            } else {
                let good:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_roots WHERE id=?1 AND owner_kind=?2 AND revision=?3 AND tenant_key=?4)",
                    params![request.root_id,binding.owner_kind,binding.configuration_revision,tenant],|r|r.get(0))?;
                if !good {return Err(conflict("joint Root belongs to another owner"));}
            }
            tx.execute("INSERT OR IGNORE INTO joint_fences VALUES('root',?1,?1,?2,0,NULL)",params![request.root_id,tenant])?;
            let (root_tenant,root_epoch,root_holder):(String,u64,Option<String>)=tx.query_row("SELECT tenant_key,epoch,holder FROM joint_fences WHERE kind='root' AND id=?1",
                [&request.root_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
            if root_tenant!=tenant || root_holder.is_some() {return Err(conflict("joint Root recovery is fenced"));}
            let (root_epoch,source_epoch)=if request.exclusive_root {
                let held:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_fences WHERE kind='source' AND root_id=?1 AND holder IS NOT NULL)",[&request.root_id],|r|r.get(0))?;
                if held {return Err(conflict("joint Root has unsettled sources"));}
                tx.execute("UPDATE joint_fences SET epoch=epoch+1,holder=?1 WHERE kind='root' AND id=?2",params![id,request.root_id])?;
                (root_epoch+1,None)
            } else {
                tx.execute("INSERT OR IGNORE INTO joint_fences VALUES('source',?1,?2,?3,0,NULL)",params![request.source_id,request.root_id,tenant])?;
                let (root,holder):(String,Option<String>)=tx.query_row("SELECT root_id,holder FROM joint_fences WHERE kind='source' AND id=?1",[&request.source_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
                if root!=request.root_id || holder.is_some() {return Err(conflict("joint source is fenced"));}
                tx.execute("UPDATE joint_fences SET epoch=epoch+1,holder=?1 WHERE kind='source' AND id=?2",params![id,request.source_id])?;
                let epoch:u64=tx.query_row("SELECT epoch FROM joint_fences WHERE kind='source' AND id=?1",[&request.source_id],|r|r.get(0))?;
                (root_epoch,Some(epoch))
            };
            let admission=JointAdmission{intent_id:id.clone(),binding:binding.clone(),generation:1,root_epoch,source_epoch};
            tx.execute("INSERT INTO joint_operations VALUES(?1,?2,?3,?4,?5,'prepared',NULL)",params![id,encoded,encode(&admission)?,binding.binding_id,tenant])?;
            for reservation in &request.reservations {
                let mut used:u64=tx.query_row("SELECT coalesce(sum(r.units),0) FROM joint_reservations r JOIN joint_operations o ON o.id=r.intent_id WHERE o.tenant_key=?1 AND r.kind=?2 AND r.state!='released'",
                    params![tenant,reservation.kind],|r|r.get(0))?;
                if reservation.kind=="session" {
                    let legacy:u64=tx.query_row("SELECT count(*) FROM sessions WHERE tenant_id IS ?1 AND status IN ('opening','idle','running','paused')",
                        [request.context.tenant_id.as_deref()],|r|r.get(0))?;
                    used+=legacy;
                }
                let mut limit=reservation.limit;
                if reservation.kind=="session" && request.context.actor_role==crate::Role::Tenant {
                    if let Some(quota)=request.context.quota.max_sessions {limit=limit.min(quota);}
                }
                if used+u64::from(reservation.units)>u64::from(limit) {
                    return Err(SessionDomainError::QuotaExceeded{scope:format!("joint_{}",reservation.kind),limit}.into());
                }
                tx.execute("INSERT INTO joint_reservations VALUES(?1,?2,?3,'reserved')",params![id,reservation.kind,reservation.units])?;
            }
            if let Some(parent)=&request.parent_binding {
                tx.execute("INSERT INTO joint_refs VALUES('binding',?1,?2)",params![binding.binding_id,parent.binding_id])?;
            }
            for native in &request.native {
                tx.execute("INSERT INTO joint_native_steps VALUES(?1,?2,?3,?4,?5,NULL)",params![id,native.step,native.operation_id,native.digest,encode(native)?])?;
            }
            Ok(admission)
        }).await
    }

    /// Claims the sole initial send. A crash after this commit requires an exact UUID query.
    pub async fn claim_joint_native_send(
        &self,
        admission: JointAdmission,
        step: String,
    ) -> std::result::Result<Option<JointNativeIntent>, SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (request,stage,_)=operation(tx,&admission)?;
            check_fences(tx,&request,&admission)?;
            if !["prepared","accepted","unknown"].contains(&stage.as_str()) {return Err(conflict("joint operation no longer permits sending"));}
            let (intent,outcome):(String,Option<String>)=tx.query_row("SELECT intent_json,outcome_json FROM joint_native_steps WHERE intent_id=?1 AND step=?2",
                params![admission.intent_id,step],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if outcome.is_some() {return Ok(None);}
            let unknown=JointNativeOutcome {state:NativeOperationState::Unknown,resources:vec![],payload:serde_json::json!({"delivery":"sent_unknown"})};
            let changed=tx.execute("UPDATE joint_native_steps SET outcome_json=?1 WHERE intent_id=?2 AND step=?3 AND outcome_json IS NULL",
                params![encode(&unknown)?,admission.intent_id,step])?;
            if changed!=1 {return Err(conflict("joint send claim changed"));}
            tx.execute("UPDATE joint_operations SET stage='unknown' WHERE id=?1",[&admission.intent_id])?;
            Ok(Some(decode(&intent)?))
        }).await
    }

    pub async fn append_joint_native_step(
        &self,
        admission: JointAdmission,
        native: JointNativeIntent,
    ) -> std::result::Result<(), SessionDomainError> {
        validate_native(&native).map_err(|e| SessionDomainError::InvalidRequest {
            message: e.to_string(),
        })?;
        self.joint_transaction(move |tx| {
            let (request, stage, _) = operation(tx, &admission)?;
            check_fences(tx, &request, &admission)?;
            if !["prepared", "accepted", "unknown", "releasing"].contains(&stage.as_str()) {
                return Err(conflict("joint operation no longer admits native steps"));
            }
            if let Some(old) = tx
                .query_row(
                    "SELECT intent_json FROM joint_native_steps WHERE intent_id=?1 AND step=?2",
                    params![admission.intent_id, native.step],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
            {
                if old != encode(&native)? {
                    return Err(conflict("joint native identity conflict"));
                }
                return Ok(());
            }
            tx.execute(
                "INSERT INTO joint_native_steps VALUES(?1,?2,?3,?4,?5,NULL)",
                params![
                    admission.intent_id,
                    native.step,
                    native.operation_id,
                    native.digest,
                    encode(&native)?
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn record_joint_native_result(
        &self,
        admission: JointAdmission,
        step: String,
        operation_id: String,
        digest: String,
        outcome: JointNativeOutcome,
    ) -> std::result::Result<(), SessionDomainError> {
        if !no_credentials(&outcome.payload) {
            return Err(SessionDomainError::InvalidRequest {
                message: "credential-bearing native result".into(),
            });
        }
        self.joint_transaction(move |tx| {
            let (request,stage,_)=operation(tx,&admission)?;check_fences(tx,&request,&admission)?;
            if !["prepared","accepted","unknown","releasing"].contains(&stage.as_str()) {return Err(conflict("joint result belongs to a settled operation"));}
            let (native,old):(String,Option<String>)=tx.query_row("SELECT intent_json,outcome_json FROM joint_native_steps WHERE intent_id=?1 AND step=?2",
                params![admission.intent_id,step],|r|Ok((r.get(0)?,r.get(1)?)))?;
            let native:JointNativeIntent=decode(&native)?;
            if native.operation_id!=operation_id || native.digest!=digest {return Err(conflict("joint native result identity mismatch"));}
            if let Some(old)=old {
                if old==encode(&outcome)? {return Ok(());}
                let old:JointNativeOutcome=decode(&old)?;
                if matches!(old.state,NativeOperationState::Completed|NativeOperationState::Failed|NativeOperationState::NotSent) {return Err(conflict("joint terminal result changed"));}
            }
            if outcome.state==NativeOperationState::Completed && native.effect==NativeEffect::Create && outcome.resources.is_empty() {
                return Err(conflict("completed create must identify owned objects"));
            }
            for resource in &outcome.resources {
                if !nonempty(&resource.id)||!nonempty(&resource.kind) {return Err(conflict("invalid native object identity"));}
                if native.effect==NativeEffect::Create {
                    if outcome.state==NativeOperationState::NotSent {return Err(conflict("not-sent operation cannot own objects"));}
                    if resource.kind=="root" && request.context.operation!="open" {return Err(conflict("shared Root requires parent reference"));}
                    tx.execute("INSERT INTO joint_objects VALUES(?1,?2,?3,?4,?5,0) ON CONFLICT(binding_id,generation,kind,native_id) DO UPDATE SET absent=0",
                        params![admission.binding.binding_id,admission.generation,request.root_id,resource.kind,resource.id])?;
                }
                if outcome.state==NativeOperationState::Completed && native.effect==NativeEffect::ObserveAbsence {
                    let changed=tx.execute("UPDATE joint_objects SET absent=1 WHERE binding_id=?1 AND generation=?2 AND kind=?3 AND native_id=?4",
                        params![admission.binding.binding_id,admission.generation,resource.kind,resource.id])?;
                    if changed!=1 {return Err(conflict("absence does not belong to the joint binding generation"));}
                }
            }
            tx.execute("UPDATE joint_native_steps SET outcome_json=?1 WHERE intent_id=?2 AND step=?3",params![encode(&outcome)?,admission.intent_id,step])?;
            let unsettled:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_native_steps WHERE intent_id=?1 AND (outcome_json IS NULL OR json_extract(outcome_json,'$.state') IN ('unknown','needs_recovery','failed')))",
                [&admission.intent_id],|r|r.get(0))?;
            let next=if unsettled {"unknown"}else{"accepted"};
            tx.execute("UPDATE joint_operations SET stage=?1 WHERE id=?2",params![next,admission.intent_id])?;Ok(())
        }).await
    }

    pub async fn publish_joint_session(
        &self,
        admission: JointAdmission,
        record: SessionRecord,
    ) -> std::result::Result<SessionRecord, SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (request, stage, old) = operation(tx, &admission)?;
            let publication = JointPublication::Session {
                record: record.clone(),
            };
            if stage == "published" {
                if old.as_deref() != Some(&encode(&publication)?) {
                    return Err(conflict("joint publication replay changed"));
                }
                return Ok(record);
            }
            check_fences(tx, &request, &admission)?;
            completed_native(tx, &admission.intent_id)?;
            if !["open", "load"].contains(&request.context.operation.as_str()) {
                return Err(conflict("operation cannot publish session"));
            }
            if request.context.operation=="open" {
                let roots:i64=tx.query_row("SELECT count(*) FROM joint_objects WHERE binding_id=?1 AND generation=?2 AND kind='root' AND absent=0",
                    params![admission.binding.binding_id,admission.generation],|r|r.get(0))?;
                if roots!=1 {return Err(conflict("new session must identify its owned Root"));}
            }
            crate::joint_lifecycle::validate_committed_session(
                &record,
                &request.context,
                &admission.binding.owner_kind,
            )?;
            if record.execution_backing.joint() != Some(&admission.binding)
                || record.status != SessionStatus::Idle
            {
                return Err(conflict(
                    "joint session publication binding or status mismatch",
                ));
            }
            tx.execute(
                "INSERT INTO joint_sessions VALUES(?1,?2,?3,'idle',?4,?5)",
                params![
                    record.runtime_id,
                    admission.binding.binding_id,
                    record.tenant_id,
                    record.updated_at_ms,
                    encode(&record)?
                ],
            )?;
            tx.execute(
                "INSERT INTO joint_refs VALUES('session',?1,?2)",
                params![record.runtime_id, admission.binding.binding_id],
            )?;
            publish(tx, &admission, &publication)?;
            Ok(record)
        })
        .await
    }

    pub async fn publish_joint_checkpoint(
        &self,
        admission: JointAdmission,
        record: JointCheckpointRecord,
    ) -> std::result::Result<JointCheckpointRecord, SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (request, stage, old) = operation(tx, &admission)?;
            let publication = JointPublication::Checkpoint {
                record: record.clone(),
            };
            if stage == "published" {
                if old.as_deref() != Some(&encode(&publication)?) {
                    return Err(conflict("joint checkpoint replay changed"));
                }
                return Ok(record);
            }
            check_fences(tx, &request, &admission)?;
            completed_native(tx, &admission.intent_id)?;
            let c = &request.context;
            if c.operation != "checkpoint" {
                return Err(conflict("operation cannot publish checkpoint"));
            }
            if record.binding != admission.binding
                || record.tenant_id != c.tenant_id
                || record.owner_ref != c.owner_ref
                || record.runtime_kind != c.runtime_kind
                || Some(&record.checkpoint_id) != c.checkpoint_id.as_ref()
                || Some(&record.source_runtime_id) != c.source_runtime_id.as_ref()
            {
                return Err(conflict("joint checkpoint publication identity mismatch"));
            }
            tx.execute(
                "INSERT INTO joint_checkpoints VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    record.checkpoint_id,
                    record.binding.binding_id,
                    record.tenant_id,
                    record.source_runtime_id,
                    record.created_by,
                    record.created_at_ms,
                    encode(&record)?
                ],
            )?;
            tx.execute(
                "INSERT INTO joint_refs VALUES('checkpoint',?1,?2)",
                params![record.checkpoint_id, record.binding.binding_id],
            )?;
            publish(tx, &admission, &publication)?;
            Ok(record)
        })
        .await
    }

    /// The adapter must have validated public SDK absence observations before recording them.
    pub async fn finish_joint_release(
        &self,
        admission: JointAdmission,
        reference_kind: String,
        public_id: String,
        updated_at_ms: u64,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (request,stage,_)=operation(tx,&admission)?;
            let expected=match (reference_kind.as_str(),request.context.operation.as_str()) {
                ("session","close") => request.context.source_runtime_id.as_deref(),
                ("checkpoint","delete_checkpoint") => request.context.checkpoint_id.as_deref(),
                _ => return Err(conflict("joint release operation mismatch")),
            };
            if expected!=Some(public_id.as_str()) {return Err(conflict("joint release public identity mismatch"));}
            if stage=="released" {return Ok(());}
            check_fences(tx,&request,&admission)?;settled_native(tx,&admission.intent_id)?;
            let (state,created):(String,String)=tx.query_row("SELECT state,creation_intent FROM joint_bindings WHERE id=?1",[&admission.binding.binding_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
            if state!="releasing" {return Err(conflict("joint binding is not releasing"));}
            let alive:u64=tx.query_row("SELECT count(*) FROM joint_objects WHERE binding_id=?1 AND generation=?2 AND absent=0",params![admission.binding.binding_id,admission.generation],|r|r.get(0))?;
            let objects:u64=tx.query_row("SELECT count(*) FROM joint_objects WHERE binding_id=?1 AND generation=?2",params![admission.binding.binding_id,admission.generation],|r|r.get(0))?;
            if alive!=0||objects==0 {return Err(conflict("joint object absence is not confirmed"));}
            let other:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_refs WHERE binding_id=?1 AND NOT(kind=?2 AND public_id=?3))",params![admission.binding.binding_id,reference_kind,public_id],|r|r.get(0))?;
            if other {return Err(conflict("joint binding acquired another reference"));}
            let changed=tx.execute("DELETE FROM joint_refs WHERE kind=?1 AND public_id=?2 AND binding_id=?3",params![reference_kind,public_id,admission.binding.binding_id])?;
            if changed!=1 {return Err(conflict("joint reference is not owned"));}
            if reference_kind=="session" {
                let body:String=tx.query_row("SELECT body FROM joint_sessions WHERE runtime_id=?1",[&public_id],|r|r.get(0))?;
                let mut record:SessionRecord=decode(&body)?;record.status=SessionStatus::Closed;record.updated_at_ms=updated_at_ms;record.lease=None;
                tx.execute("UPDATE joint_sessions SET status='closed',updated_at_ms=?1,body=?2 WHERE runtime_id=?3",params![updated_at_ms,encode(&record)?,public_id])?;
            } else {tx.execute("DELETE FROM joint_checkpoints WHERE checkpoint_id=?1",[public_id])?;}
            tx.execute("DELETE FROM joint_refs WHERE kind='binding' AND public_id=?1",[&admission.binding.binding_id])?;
            tx.execute("UPDATE joint_bindings SET state='removed' WHERE id=?1",[&admission.binding.binding_id])?;
            tx.execute("UPDATE joint_reservations SET state='released' WHERE intent_id IN (?1,?2)",params![created,admission.intent_id])?;
            tx.execute("UPDATE joint_operations SET stage='released' WHERE id IN (?1,?2)",params![created,admission.intent_id])?;
            release_fences(tx,&admission.intent_id)?;Ok(())
        }).await
    }

    /// Completes compensation for an operation that never published a public reference.
    pub async fn finish_joint_compensation(
        &self,
        admission: JointAdmission,
    ) -> std::result::Result<(), SessionDomainError> {
        self.joint_transaction(move |tx| {
            let (request,stage,_)=operation(tx,&admission)?;
            if stage=="compensated" {return Ok(());}
            check_fences(tx,&request,&admission)?;
            let valid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_bindings WHERE id=?1 AND state='prepared' AND creation_intent=?2)",
                params![admission.binding.binding_id,admission.intent_id],|r|r.get(0))?;
            if !valid {return Err(conflict("published binding cannot use creation compensation"));}
            let unsettled:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_native_steps WHERE intent_id=?1 AND (outcome_json IS NULL OR json_extract(outcome_json,'$.state') NOT IN ('completed','failed','not_sent')))",
                [&admission.intent_id],|r|r.get(0))?;
            let refs:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_refs WHERE binding_id=?1)",[&admission.binding.binding_id],|r|r.get(0))?;
            let alive:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM joint_objects WHERE binding_id=?1 AND generation=?2 AND absent=0)",
                params![admission.binding.binding_id,admission.generation],|r|r.get(0))?;
            if unsettled || refs || alive {return Err(conflict("joint compensation is not confirmed"));}
            tx.execute("DELETE FROM joint_refs WHERE kind='binding' AND public_id=?1",[&admission.binding.binding_id])?;
            tx.execute("UPDATE joint_bindings SET state='removed' WHERE id=?1",[&admission.binding.binding_id])?;
            tx.execute("UPDATE joint_reservations SET state='released' WHERE intent_id=?1",[&admission.intent_id])?;
            tx.execute("UPDATE joint_operations SET stage='compensated' WHERE id=?1",[&admission.intent_id])?;
            tx.execute("DELETE FROM joint_public_claims WHERE intent_id=?1",[&admission.intent_id])?;
            release_fences(tx,&admission.intent_id)?;Ok(())
        }).await
    }

    pub async fn read_joint_checkpoint(
        &self,
        id: &str,
    ) -> std::result::Result<Option<JointCheckpointRecord>, SessionDomainError> {
        let id = id.to_owned();
        self.joint_transaction(move |tx| {
            let body: Option<String> = tx
                .query_row(
                    "SELECT body FROM joint_checkpoints WHERE checkpoint_id=?1",
                    [id],
                    |r| r.get(0),
                )
                .optional()?;
            body.as_deref().map(decode).transpose()
        })
        .await
    }

    pub async fn pending_joint_operations(
        &self,
    ) -> std::result::Result<Vec<JointPendingOperation>, SessionDomainError> {
        self.joint_transaction(|tx| {
            let rows=tx.prepare("SELECT id,request_json,admission_json,stage FROM joint_operations WHERE stage NOT IN ('published','released','compensated') ORDER BY id")?
                .query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter().map(|(id,request,admission,stage)| {
                let native=tx.prepare("SELECT intent_json,outcome_json FROM joint_native_steps WHERE intent_id=?1 ORDER BY step")?
                    .query_map([id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?
                    .into_iter().map(|(intent,outcome)|Ok((decode(&intent)?,outcome.as_deref().map(decode).transpose()?))).collect::<Result<Vec<_>>>()?;
                Ok(JointPendingOperation{request:decode(&request)?,admission:decode(&admission)?,stage,native})
            }).collect()
        }).await
    }
}
fn publish(
    tx: &Transaction<'_>,
    admission: &JointAdmission,
    publication: &JointPublication,
) -> Result<()> {
    let changed=tx.execute("UPDATE joint_operations SET stage='published',publication_json=?1 WHERE id=?2 AND stage IN ('prepared','accepted')",params![encode(publication)?,admission.intent_id])?;
    if changed != 1 {
        return Err(conflict("joint publication stage changed"));
    }
    let changed=tx.execute("UPDATE joint_bindings SET state='active' WHERE id=?1 AND generation=?2 AND state='prepared'",params![admission.binding.binding_id,admission.generation])?;
    if changed != 1 {
        return Err(conflict("joint binding publication stage changed"));
    }
    let changed = tx.execute(
        "UPDATE joint_reservations SET state='committed' WHERE intent_id=?1 AND state='reserved'",
        [&admission.intent_id],
    )?;
    if changed == 0 {
        return Err(conflict("joint publication has no reservation"));
    }
    release_fences(tx, &admission.intent_id)?;
    Ok(())
}

#[cfg(test)]
#[path = "joint_store_tests.rs"]
mod tests;
